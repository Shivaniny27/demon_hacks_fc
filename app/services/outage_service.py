"""
outage_service.py — CTA Service Outages & Line Disruptions
===========================================================
Tracks real-time CTA train service outages, suspensions, and delays
for all lines serving the Chicago Loop. Classifies severity, identifies
Loop-specific station impacts, and advises the routing engine.

CTA Customer Alerts API:
  https://www.transitchicago.com/api/1.0/alerts.aspx?outputType=JSON&activeonly=true

Does NOT modify any existing service file.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import aiohttp

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

CTA_ALERTS_URL = (
    "https://www.transitchicago.com/api/1.0/alerts.aspx"
    "?outputType=JSON&activeonly=true"
)
OUTAGE_POLL_INTERVAL_S    = 90     # poll every 90 seconds
REDIS_OUTAGE_SNAPSHOT_TTL = 180    # 3 min snapshot cache
REDIS_OUTAGE_HISTORY_KEY  = "loopnav:outages:history"
REDIS_OUTAGE_HISTORY_TTL  = 86_400 # 24 hours
MAX_HISTORY_RECORDS       = 100

# CTA line color codes → display names
CTA_LINES: Dict[str, Dict[str, str]] = {
    "Red":    {"name": "Red Line",    "color": "#C60C30"},
    "Blue":   {"name": "Blue Line",   "color": "#00A1DE"},
    "Brown":  {"name": "Brown Line",  "color": "#62361B"},
    "Green":  {"name": "Green Line",  "color": "#009B3A"},
    "Orange": {"name": "Orange Line", "color": "#F9461C"},
    "Pink":   {"name": "Pink Line",   "color": "#E27EA6"},
    "Purple": {"name": "Purple Line", "color": "#522398"},
    "Yellow": {"name": "Yellow Line", "color": "#F9E300"},
    "P":      {"name": "Purple Line", "color": "#522398"},
    "G":      {"name": "Green Line",  "color": "#009B3A"},
    "Org":    {"name": "Orange Line", "color": "#F9461C"},
    "Pnk":    {"name": "Pink Line",   "color": "#E27EA6"},
    "Brn":    {"name": "Brown Line",  "color": "#62361B"},
}

# Lines that serve the Chicago Loop (L elevated structure + underground)
LOOP_LINES: Set[str] = {"Red", "Blue", "Brown", "Green", "Orange", "Pink", "Purple"}

# Loop station names for impact detection
LOOP_STATION_NAMES: Set[str] = {
    "Clark/Lake", "State/Lake", "Randolph/Wabash", "Washington/Wabash",
    "Madison/Wabash", "Adams/Wabash", "Harold Washington Library",
    "LaSalle/Van Buren", "Quincy/Wells", "Washington/Wells",
    "Washington/Dearborn", "Monroe/Dearborn", "Jackson/Dearborn",
    "Monroe/State", "Jackson/State", "Lake/State",
}

# CTA Impact text → our severity categories
IMPACT_SEVERITY_MAP: Dict[str, str] = {
    "service suspension":        "critical",
    "major delay":               "critical",
    "significant delay":         "critical",
    "no service":                "critical",
    "service disruption":        "high",
    "delay":                     "high",
    "reduced service":           "high",
    "reroute":                   "high",
    "service change":            "medium",
    "planned work":              "medium",
    "construction":              "medium",
    "advisory":                  "low",
    "planned":                   "low",
    "bus substitute":            "medium",
    "elevator outage":           "skip",   # handled by elevator_service
    "escalator outage":          "skip",
}


# ── Enums ─────────────────────────────────────────────────────────────────────

class OutageSeverity(str, Enum):
    CRITICAL = "critical"   # service suspension / no service
    HIGH     = "high"       # major delay / significant disruption
    MEDIUM   = "medium"     # planned work / reduced service
    LOW      = "low"        # advisory / informational
    INFO     = "info"       # general informational


class OutageCategory(str, Enum):
    SUSPENSION  = "suspension"
    DELAY       = "delay"
    REROUTE     = "reroute"
    PLANNED     = "planned_work"
    WEATHER     = "weather"
    SIGNAL      = "signal"
    TRACK       = "track"
    POWER       = "power"
    SHUTTLE     = "shuttle_bus"
    ADVISORY    = "advisory"
    OTHER       = "other"


class LineHealth(str, Enum):
    NORMAL    = "normal"
    MINOR     = "minor_delays"
    MODERATE  = "moderate_delays"
    MAJOR     = "major_disruption"
    SUSPENDED = "suspended"


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class ServiceOutage:
    alert_id:          str
    headline:          str
    short_description: str
    full_description:  str
    severity:          OutageSeverity
    category:          OutageCategory
    affected_lines:    List[str]
    affected_stations: List[str]         # Loop stations specifically impacted
    loop_impacted:     bool              # True if any Loop line/station affected
    start_time:        datetime
    end_time:          Optional[datetime]
    is_tbd:            bool
    is_major:          bool              # CTA marks some as "major"
    impact_text:       str               # CTA Impact field
    alert_url:         str
    routing_impact:    Dict[str, Any]    # structured impact for routing engine
    source:            str = "cta_api"


@dataclass
class LineStatus:
    line:         str
    display_name: str
    color:        str
    health:       LineHealth
    active_outages: List[ServiceOutage]
    severity_score: float               # 0.0 (normal) – 1.0 (suspended)
    loop_affected:  bool
    summary:        str


@dataclass
class OutageSnapshot:
    fetched_at:         datetime
    active_outages:     List[ServiceOutage]
    line_statuses:      Dict[str, LineStatus]
    loop_outages:       List[ServiceOutage]
    total_outages:      int
    critical_count:     int
    high_count:         int
    loop_impact_score:  float
    affected_lines:     Set[str]
    api_ok:             bool
    consecutive_errors: int


@dataclass
class RoutingOutageImpact:
    """Simplified structure for the routing engine."""
    suspend_lines:     List[str]         # completely suspended lines
    delay_lines:       List[str]         # delayed but running
    avoid_stations:    List[str]         # Loop stations to avoid
    alternate_lines:   Dict[str, List[str]]  # line → suggested alternates
    severity:          str
    message:           str


# ── CTA Outage API Client ─────────────────────────────────────────────────────

class CTAOutageApiClient:
    """
    Async CTA Customer Alerts API client.
    Fetches all active alerts, excluding elevator/escalator alerts
    (those are handled by elevator_service.py).
    """

    MAX_RETRIES  = 3
    TIMEOUT_S    = 12.0
    BACKOFF_BASE = 1.5

    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self._session      = session
        self._session_owned = session is None
        self._errors       = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(ssl=False)
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.TIMEOUT_S),
                connector=connector,
            )
        return self._session

    async def close(self):
        if self._session_owned and self._session and not self._session.closed:
            await self._session.close()

    async def fetch_service_alerts(self) -> List[Dict[str, Any]]:
        """Fetch all active service alerts (non-elevator)."""
        for attempt in range(self.MAX_RETRIES):
            try:
                session = await self._get_session()
                async with session.get(CTA_ALERTS_URL) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                    self._errors = 0
                    raw = self._extract_alerts(data)
                    return [a for a in raw if not self._is_accessibility_alert(a)]
            except Exception as exc:
                self._errors += 1
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.BACKOFF_BASE ** attempt)
                else:
                    logger.error("CTA outage API failed after %d attempts: %s", self.MAX_RETRIES, exc)
                    raise
        return []

    @staticmethod
    def _extract_alerts(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            alerts = data["CTAAlerts"]["Alert"]
            if isinstance(alerts, dict):
                return [alerts]
            return alerts or []
        except (KeyError, TypeError):
            return []

    @staticmethod
    def _is_accessibility_alert(alert: Dict[str, Any]) -> bool:
        impact   = (alert.get("Impact") or "").lower()
        headline = (alert.get("Headline") or "").lower()
        return "elevator" in impact or "escalator" in impact or "elevator" in headline

    @property
    def consecutive_errors(self) -> int:
        return self._errors


# ── Outage Parser ─────────────────────────────────────────────────────────────

class OutageParser:
    """Converts raw CTA alert dicts into typed ServiceOutage objects."""

    CTA_DT_FORMATS = [
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
    ]

    @staticmethod
    def _s(val: Any) -> str:
        """Coerce a CTA field to str — handles XML→JSON dicts like {"#text": "..."}."""
        if val is None:
            return ""
        if isinstance(val, dict):
            return str(val.get("#text", "") or "")
        return str(val)

    def parse(self, raw: Dict[str, Any]) -> Optional[ServiceOutage]:
        try:
            alert_id      = str(raw.get("AlertId", ""))
            headline      = self._s(raw.get("Headline", ""))
            short_desc    = self._s(raw.get("ShortDescription", ""))
            full_desc     = self._s(raw.get("FullDescription", ""))
            impact_text   = self._s(raw.get("Impact")).strip()
            alert_url     = self._s(raw.get("AlertURL", ""))
            is_major      = str(raw.get("MajorAlert", "0")) == "1"
            is_tbd        = str(raw.get("TBD", "0")) == "1"

            severity      = self._classify_severity(impact_text, headline, is_major)
            if severity == "skip":
                return None

            category      = self._classify_category(impact_text, headline, full_desc)
            affected_lines   = self._extract_lines(raw)
            affected_stations = self._extract_loop_stations(headline + " " + short_desc + " " + full_desc)
            loop_impacted    = bool(
                affected_stations
                or any(line in LOOP_LINES for line in affected_lines)
            )

            routing_impact = self._build_routing_impact(severity, affected_lines, affected_stations)

            return ServiceOutage(
                alert_id=alert_id,
                headline=headline,
                short_description=short_desc,
                full_description=full_desc,
                severity=OutageSeverity(severity) if severity in OutageSeverity._value2member_map_ else OutageSeverity.INFO,
                category=category,
                affected_lines=affected_lines,
                affected_stations=affected_stations,
                loop_impacted=loop_impacted,
                start_time=self._parse_dt(raw.get("EventStart")) or datetime.now(timezone.utc),
                end_time=self._parse_dt(raw.get("EventEnd")),
                is_tbd=is_tbd,
                is_major=is_major,
                impact_text=impact_text,
                alert_url=alert_url,
                routing_impact=routing_impact,
            )
        except Exception as exc:
            logger.debug("OutageParser: failed to parse alert: %s", exc)
            return None

    @staticmethod
    def _classify_severity(impact: str, headline: str, is_major: bool) -> str:
        combined = (impact + " " + headline).lower()
        if is_major:
            return "critical"
        for keyword, sev in IMPACT_SEVERITY_MAP.items():
            if keyword in combined:
                return sev
        return "low"

    @staticmethod
    def _classify_category(impact: str, headline: str, desc: str) -> OutageCategory:
        combined = (impact + " " + headline + " " + desc).lower()
        if "suspend" in combined or "no service" in combined:
            return OutageCategory.SUSPENSION
        if "delay" in combined:
            return OutageCategory.DELAY
        if "reroute" in combined or "re-route" in combined:
            return OutageCategory.REROUTE
        if "planned" in combined or "construction" in combined:
            return OutageCategory.PLANNED
        if "weather" in combined or "wind" in combined or "snow" in combined:
            return OutageCategory.WEATHER
        if "signal" in combined:
            return OutageCategory.SIGNAL
        if "track" in combined:
            return OutageCategory.TRACK
        if "power" in combined or "third rail" in combined:
            return OutageCategory.POWER
        if "bus" in combined or "shuttle" in combined:
            return OutageCategory.SHUTTLE
        return OutageCategory.OTHER

    @staticmethod
    def _extract_lines(raw: Dict[str, Any]) -> List[str]:
        lines: List[str] = []
        try:
            services = raw["ImpactedService"]["Service"]
            if isinstance(services, dict):
                services = [services]
            for svc in services:
                stype = svc.get("ServiceType", "")
                if stype == "T":  # Train
                    name = svc.get("ServiceName", "")
                    if name and name not in lines:
                        # Normalize common aliases
                        if name in CTA_LINES:
                            lines.append(name)
        except (KeyError, TypeError):
            pass
        return lines

    @staticmethod
    def _extract_loop_stations(text: str) -> List[str]:
        """Extract Loop station names mentioned in alert text."""
        found: List[str] = []
        text_lower = text.lower()
        for station in LOOP_STATION_NAMES:
            if station.lower() in text_lower:
                found.append(station)
        return found

    @staticmethod
    def _build_routing_impact(
        severity: str,
        lines: List[str],
        stations: List[str],
    ) -> Dict[str, Any]:
        return {
            "severity":         severity,
            "suspend_lines":    lines if severity == "critical" else [],
            "delay_lines":      lines if severity in ("high", "medium") else [],
            "avoid_stations":   stations,
            "use_pedway":       severity in ("critical", "high"),
            "walk_preferred":   severity == "critical",
        }

    def _parse_dt(self, dt_str: Optional[str]) -> Optional[datetime]:
        if not dt_str:
            return None
        for fmt in self.CTA_DT_FORMATS:
            try:
                dt = datetime.strptime(dt_str.strip(), fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None


# ── Line Status Tracker ────────────────────────────────────────────────────────

class LineStatusTracker:
    """
    Maintains current health status for all CTA lines.
    Aggregates outages per line and computes a severity score.
    """

    def compute_statuses(self, outages: List[ServiceOutage]) -> Dict[str, LineStatus]:
        # Group outages by line
        by_line: Dict[str, List[ServiceOutage]] = defaultdict(list)
        for o in outages:
            for line in o.affected_lines:
                if line in CTA_LINES:
                    by_line[line].append(o)

        statuses: Dict[str, LineStatus] = {}
        for line_id, line_meta in CTA_LINES.items():
            if line_id in ("P", "G", "Org", "Pnk", "Brn"):
                continue  # Skip aliases, use canonical names only
            line_outages  = by_line.get(line_id, [])
            health, score = self._compute_health(line_outages)
            loop_affected = any(o.loop_impacted for o in line_outages)
            summary       = self._build_summary(line_id, health, line_outages)

            statuses[line_id] = LineStatus(
                line=line_id,
                display_name=line_meta["name"],
                color=line_meta["color"],
                health=health,
                active_outages=line_outages,
                severity_score=round(score, 3),
                loop_affected=loop_affected,
                summary=summary,
            )

        return statuses

    @staticmethod
    def _compute_health(outages: List[ServiceOutage]) -> Tuple[LineHealth, float]:
        if not outages:
            return LineHealth.NORMAL, 0.0
        sev_rank = {
            OutageSeverity.INFO: 0, OutageSeverity.LOW: 1, OutageSeverity.MEDIUM: 2,
            OutageSeverity.HIGH: 3, OutageSeverity.CRITICAL: 4,
        }
        max_sev = max((o.severity for o in outages), key=lambda s: sev_rank.get(s, 0))
        if max_sev == OutageSeverity.CRITICAL:
            return LineHealth.SUSPENDED, 1.0
        elif max_sev == OutageSeverity.HIGH:
            return LineHealth.MAJOR, 0.75
        elif max_sev == OutageSeverity.MEDIUM:
            return LineHealth.MODERATE, 0.45
        elif max_sev == OutageSeverity.LOW:
            return LineHealth.MINOR, 0.20
        return LineHealth.NORMAL, 0.0

    @staticmethod
    def _build_summary(line: str, health: LineHealth, outages: List[ServiceOutage]) -> str:
        if health == LineHealth.NORMAL:
            return f"{CTA_LINES.get(line, {}).get('name', line)}: Normal service."
        headlines = "; ".join(o.headline for o in outages[:2])
        return f"{CTA_LINES.get(line, {}).get('name', line)}: {headlines}"


# ── Loop Impact Analyzer ──────────────────────────────────────────────────────

class LoopImpactAnalyzer:
    """
    Determines how current outages affect the Chicago Loop specifically.
    Produces routing advice: use Pedway, avoid stations, switch lines.
    """

    LINE_ALTERNATIVES: Dict[str, List[str]] = {
        "Red":    ["Blue", "Brown", "Green"],
        "Blue":   ["Red", "Pink"],
        "Brown":  ["Orange", "Purple", "Pink"],
        "Green":  ["Orange", "Pink"],
        "Orange": ["Brown", "Pink", "Green"],
        "Pink":   ["Brown", "Orange", "Green"],
        "Purple": ["Brown"],
    }

    def analyze(self, outages: List[ServiceOutage]) -> RoutingOutageImpact:
        loop_outages = [o for o in outages if o.loop_impacted]
        if not loop_outages:
            return RoutingOutageImpact(
                suspend_lines=[],
                delay_lines=[],
                avoid_stations=[],
                alternate_lines={},
                severity="none",
                message="All CTA lines serving the Loop are operating normally.",
            )

        suspend_lines: List[str] = []
        delay_lines:   List[str] = []
        avoid_stations: Set[str] = set()

        for o in loop_outages:
            if o.severity == OutageSeverity.CRITICAL:
                suspend_lines.extend(o.affected_lines)
            elif o.severity in (OutageSeverity.HIGH, OutageSeverity.MEDIUM):
                delay_lines.extend(o.affected_lines)
            avoid_stations.update(o.affected_stations)

        suspend_lines = list(set(suspend_lines))
        delay_lines   = list(set(delay_lines) - set(suspend_lines))

        alternate_lines = {
            line: self.LINE_ALTERNATIVES.get(line, [])
            for line in suspend_lines
        }

        max_sev = "none"
        if suspend_lines:
            max_sev = "critical"
        elif delay_lines:
            max_sev = "high"
        elif loop_outages:
            max_sev = "medium"

        message = self._build_message(suspend_lines, delay_lines, avoid_stations)

        return RoutingOutageImpact(
            suspend_lines=suspend_lines,
            delay_lines=delay_lines,
            avoid_stations=list(avoid_stations),
            alternate_lines=alternate_lines,
            severity=max_sev,
            message=message,
        )

    @staticmethod
    def _build_message(
        suspend: List[str],
        delay: List[str],
        avoid: Set[str],
    ) -> str:
        parts = []
        if suspend:
            parts.append(f"Service suspended: {', '.join(suspend)}")
        if delay:
            parts.append(f"Delays reported: {', '.join(delay)}")
        if avoid:
            parts.append(f"Avoid stations: {', '.join(list(avoid)[:3])}")
        if not parts:
            return "Minor disruptions in the Loop area."
        return ". ".join(parts) + "."


# ── Outage Registry ───────────────────────────────────────────────────────────

class OutageRegistry:
    """
    In-memory store of current active outages with Redis persistence.
    Tracks which outages are new/resolved each poll cycle.
    """

    REDIS_KEY_SNAPSHOT = "loopnav:outages:snapshot"
    REDIS_KEY_HISTORY  = "loopnav:outages:history"

    def __init__(self, redis=None):
        self._redis   = redis
        self._outages: Dict[str, ServiceOutage] = {}
        self._lock    = asyncio.Lock()

    async def update(
        self, outages: List[ServiceOutage]
    ) -> Tuple[List[str], List[str]]:
        """Update store. Returns (added_ids, resolved_ids)."""
        async with self._lock:
            new_ids     = {o.alert_id for o in outages}
            current_ids = set(self._outages.keys())

            added_ids    = list(new_ids - current_ids)
            resolved_ids = list(current_ids - new_ids)

            self._outages = {o.alert_id: o for o in outages}

            await self._persist_snapshot(outages)
            for aid in added_ids:
                o = self._outages.get(aid)
                if o:
                    await self._append_history(o)

            return added_ids, resolved_ids

    async def _persist_snapshot(self, outages: List[ServiceOutage]):
        if not self._redis:
            return
        try:
            payload = json.dumps([
                {
                    "alert_id":       o.alert_id,
                    "headline":       o.headline,
                    "severity":       o.severity.value,
                    "category":       o.category.value,
                    "affected_lines": o.affected_lines,
                    "loop_impacted":  o.loop_impacted,
                    "start_time":     o.start_time.isoformat(),
                    "routing_impact": o.routing_impact,
                }
                for o in outages
            ])
            await self._redis.setex(self.REDIS_KEY_SNAPSHOT, REDIS_OUTAGE_SNAPSHOT_TTL, payload)
        except Exception as exc:
            logger.debug("OutageRegistry: Redis persist failed: %s", exc)

    async def _append_history(self, outage: ServiceOutage):
        if not self._redis:
            return
        try:
            entry = json.dumps({
                "alert_id":   outage.alert_id,
                "headline":   outage.headline,
                "severity":   outage.severity.value,
                "lines":      outage.affected_lines,
                "loop":       outage.loop_impacted,
                "detected_at": datetime.now(timezone.utc).isoformat(),
            })
            await self._redis.lpush(self.REDIS_KEY_HISTORY, entry)
            await self._redis.ltrim(self.REDIS_KEY_HISTORY, 0, MAX_HISTORY_RECORDS - 1)
            await self._redis.expire(self.REDIS_KEY_HISTORY, REDIS_OUTAGE_HISTORY_TTL)
        except Exception as exc:
            logger.debug("OutageRegistry: history append failed: %s", exc)

    async def get_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        if not self._redis:
            return []
        try:
            raw = await self._redis.lrange(self.REDIS_KEY_HISTORY, 0, limit - 1)
            return [json.loads(r) for r in raw]
        except Exception:
            return []

    def get_all(self) -> List[ServiceOutage]:
        return list(self._outages.values())

    def get_loop_outages(self) -> List[ServiceOutage]:
        return [o for o in self._outages.values() if o.loop_impacted]

    def get_by_line(self, line: str) -> List[ServiceOutage]:
        return [o for o in self._outages.values() if line in o.affected_lines]

    def get_critical(self) -> List[ServiceOutage]:
        return [o for o in self._outages.values() if o.severity == OutageSeverity.CRITICAL]

    def total(self) -> int:
        return len(self._outages)


# ── Main OutageService ────────────────────────────────────────────────────────

class OutageService:
    """
    Production CTA service outage tracker.

    Lifecycle:
      initialize() → starts background polling
      get_snapshot() → current OutageSnapshot
      get_line_status(line) → LineStatus for a specific line
      get_routing_impact() → RoutingOutageImpact for nav engine
      get_loop_outages() → only Loop-affecting outages
    """

    def __init__(self, redis=None):
        self._redis    = redis
        self._api      = CTAOutageApiClient()
        self._parser   = OutageParser()
        self._registry = OutageRegistry(redis)
        self._tracker  = LineStatusTracker()
        self._analyzer = LoopImpactAnalyzer()

        self._snapshot:    Optional[OutageSnapshot] = None
        self._subscribers: List[Callable] = []
        self._poll_task:   Optional[asyncio.Task] = None
        self._running:     bool = False
        self._poll_errors: int = 0

    async def initialize(self):
        if self._running:
            return
        self._running = True
        logger.info("OutageService: starting poll loop (interval=%ds)", OUTAGE_POLL_INTERVAL_S)
        self._poll_task = asyncio.create_task(self._polling_loop(), name="outage_poller")

    async def shutdown(self):
        self._running = False
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        await self._api.close()

    async def refresh(self) -> OutageSnapshot:
        try:
            raw_alerts = await self._api.fetch_service_alerts()
            outages    = [self._parser.parse(a) for a in raw_alerts]
            outages    = [o for o in outages if o is not None]

            added, resolved = await self._registry.update(outages)
            self._poll_errors = 0

            if added or resolved:
                await self._notify_subscribers("outage_change", {
                    "added": added, "resolved": resolved
                })

            snapshot = self._build_snapshot(outages, api_ok=True)
            self._snapshot = snapshot
            logger.info(
                "OutageService: %d active outages (%d loop), +%d/-%d this cycle",
                snapshot.total_outages,
                len(snapshot.loop_outages),
                len(added),
                len(resolved),
            )
            return snapshot

        except Exception as exc:
            self._poll_errors += 1
            logger.error("OutageService poll failed (streak=%d): %s", self._poll_errors, exc)
            current = self._registry.get_all()
            snapshot = self._build_snapshot(current, api_ok=False)
            self._snapshot = snapshot
            return snapshot

    async def get_snapshot(self) -> OutageSnapshot:
        if self._snapshot is None:
            return await self.refresh()
        age = (datetime.now(timezone.utc) - self._snapshot.fetched_at).total_seconds()
        if age > OUTAGE_POLL_INTERVAL_S * 2:
            return await self.refresh()
        return self._snapshot

    async def get_line_status(self, line: str) -> Optional[LineStatus]:
        snapshot = await self.get_snapshot()
        return snapshot.line_statuses.get(line)

    async def get_all_line_statuses(self) -> Dict[str, LineStatus]:
        snapshot = await self.get_snapshot()
        return snapshot.line_statuses

    async def get_loop_outages(self) -> List[ServiceOutage]:
        snapshot = await self.get_snapshot()
        return snapshot.loop_outages

    async def get_routing_impact(self) -> RoutingOutageImpact:
        snapshot = await self.get_snapshot()
        return self._analyzer.analyze(snapshot.active_outages)

    async def get_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        return await self._registry.get_history(limit)

    async def health_check(self) -> Dict[str, Any]:
        snapshot = await self.get_snapshot()
        return {
            "status":            "ok" if snapshot.api_ok else "degraded",
            "api_ok":            snapshot.api_ok,
            "total_outages":     snapshot.total_outages,
            "loop_outages":      len(snapshot.loop_outages),
            "critical_outages":  snapshot.critical_count,
            "loop_impact_score": snapshot.loop_impact_score,
            "last_fetched":      snapshot.fetched_at.isoformat(),
        }

    def subscribe(self, cb: Callable):
        if cb not in self._subscribers:
            self._subscribers.append(cb)

    def unsubscribe(self, cb: Callable):
        self._subscribers = [s for s in self._subscribers if s is not cb]

    async def _notify_subscribers(self, event_type: str, data: Dict[str, Any]):
        for cb in list(self._subscribers):
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(event_type, data)
                else:
                    cb(event_type, data)
            except Exception as exc:
                logger.debug("OutageService subscriber error: %s", exc)

    async def _polling_loop(self):
        await self.refresh()
        while self._running:
            try:
                await asyncio.sleep(OUTAGE_POLL_INTERVAL_S)
                if self._running:
                    await self.refresh()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("OutageService polling loop error: %s", exc)
                await asyncio.sleep(15)

    def _build_snapshot(self, outages: List[ServiceOutage], api_ok: bool) -> OutageSnapshot:
        line_statuses = self._tracker.compute_statuses(outages)
        loop_outages  = [o for o in outages if o.loop_impacted]
        affected_lines: Set[str] = {line for o in outages for line in o.affected_lines}

        critical = sum(1 for o in outages if o.severity == OutageSeverity.CRITICAL)
        high     = sum(1 for o in outages if o.severity == OutageSeverity.HIGH)
        loop_lines_impacted = len({
            line for o in loop_outages for line in o.affected_lines
            if line in LOOP_LINES
        })
        loop_impact = round(loop_lines_impacted / max(len(LOOP_LINES), 1), 3)

        return OutageSnapshot(
            fetched_at=datetime.now(timezone.utc),
            active_outages=outages,
            line_statuses=line_statuses,
            loop_outages=loop_outages,
            total_outages=len(outages),
            critical_count=critical,
            high_count=high,
            loop_impact_score=loop_impact,
            affected_lines=affected_lines,
            api_ok=api_ok,
            consecutive_errors=self._poll_errors,
        )


# ── Module-level Singleton ────────────────────────────────────────────────────

_outage_service_instance: Optional[OutageService] = None


def get_outage_service(redis=None) -> OutageService:
    global _outage_service_instance
    if _outage_service_instance is None:
        _outage_service_instance = OutageService(redis=redis)
    return _outage_service_instance
