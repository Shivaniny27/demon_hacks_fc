"""
elevator_service.py — Real-Time CTA Elevator Reliability for ADA Routing
=========================================================================
Priority 1: Pulls live elevator outage alerts from the CTA Customer Alerts
API, tracks per-unit outage history in Redis, calculates reliability scores,
advises the routing engine on ADA-safe alternatives, and streams changes via
SSE/WebSocket.

CTA Customer Alerts API:
  https://www.transitchicago.com/api/1.0/alerts.aspx?outputType=JSON&activeonly=true

Does NOT modify any existing service file.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
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
    "http://lapi.transitchicago.com/api/1.0/alerts.aspx"
    "?outputType=JSON&activeonly=true"
)
CTA_POLL_INTERVAL_S = 120          # poll every 2 minutes
REDIS_OUTAGE_HISTORY_TTL = 86_400 * 7   # 7 days of history
REDIS_SNAPSHOT_TTL = 300           # 5 min snapshot cache
RELIABILITY_POOR_THRESHOLD = 0.50  # avoid elevators below this score
RELIABILITY_HISTORY_WINDOW_HOURS = 168  # 7 days

# Chicago Loop geographic bounds
LOOP_BOUNDS = {
    "lat_min": 41.872,
    "lat_max": 41.892,
    "lon_min": -87.640,
    "lon_max": -87.623,
}

# ── Loop L Stations ───────────────────────────────────────────────────────────
# Complete database of Chicago Loop CTA stations with elevator metadata.
# map_id = CTA Train Tracker mapid; lines = lines serving station.
# elevators: list of internal elevator IDs (match graph edge attrs).
# pedway_connected: True if Pedway tunnel connects to this station.
# lat/lon: station centroid for distance calculations.

LOOP_STATIONS: Dict[str, Dict[str, Any]] = {
    "clark_lake": {
        "map_id": "40380",
        "name": "Clark/Lake",
        "lines": ["Blue", "Brown", "Green", "Orange", "Pink", "Purple"],
        "lat": 41.8856,
        "lon": -87.6319,
        "elevators": ["CL-E1", "CL-E2", "CL-E3"],
        "pedway_connected": True,
        "pedway_entrance": "Lower Level, east side",
        "accessible_entrances": ["Washington St entrance (elevator to mezzanine)"],
        "platform_level": "elevated",
        "ada_compliant": True,
    },
    "state_lake": {
        "map_id": "40260",
        "name": "State/Lake",
        "lines": ["Brown", "Green", "Orange", "Pink", "Purple"],
        "lat": 41.8858,
        "lon": -87.6278,
        "elevators": ["SL-E1", "SL-E2"],
        "pedway_connected": False,
        "pedway_entrance": None,
        "accessible_entrances": ["State St entrance (elevator to platform)"],
        "platform_level": "elevated",
        "ada_compliant": True,
    },
    "randolph_wabash": {
        "map_id": "40200",
        "name": "Randolph/Wabash",
        "lines": ["Brown", "Green", "Orange", "Pink", "Purple"],
        "lat": 41.8847,
        "lon": -87.6263,
        "elevators": ["RW-E1"],
        "pedway_connected": True,
        "pedway_entrance": "Mezzanine level, Randolph St",
        "accessible_entrances": ["Randolph St elevator"],
        "platform_level": "elevated",
        "ada_compliant": True,
    },
    "washington_wabash": {
        "map_id": "41700",
        "name": "Washington/Wabash",
        "lines": ["Brown", "Green", "Orange", "Pink", "Purple"],
        "lat": 41.8831,
        "lon": -87.6259,
        "elevators": ["WW-E1", "WW-E2"],
        "pedway_connected": True,
        "pedway_entrance": "Underground concourse, north side",
        "accessible_entrances": ["Washington St elevator", "Wabash Ave elevator"],
        "platform_level": "elevated",
        "ada_compliant": True,
    },
    "madison_wabash": {
        "map_id": "40710",
        "name": "Madison/Wabash",
        "lines": ["Brown", "Green", "Orange", "Pink", "Purple"],
        "lat": 41.8820,
        "lon": -87.6261,
        "elevators": ["MW-E1"],
        "pedway_connected": False,
        "accessible_entrances": ["Madison St elevator"],
        "platform_level": "elevated",
        "ada_compliant": True,
    },
    "adams_wabash": {
        "map_id": "40680",
        "name": "Adams/Wabash",
        "lines": ["Brown", "Green", "Orange", "Pink", "Purple"],
        "lat": 41.8793,
        "lon": -87.6260,
        "elevators": ["AW-E1", "AW-E2"],
        "pedway_connected": True,
        "pedway_entrance": "Below platform, Adams St underpass",
        "accessible_entrances": ["Adams St elevator", "Wabash Ave south elevator"],
        "platform_level": "elevated",
        "ada_compliant": True,
    },
    "harold_washington_library": {
        "map_id": "41340",
        "name": "Harold Washington Library-State/Van Buren",
        "lines": ["Brown", "Orange", "Pink", "Purple"],
        "lat": 41.8762,
        "lon": -87.6280,
        "elevators": ["HWL-E1", "HWL-E2"],
        "pedway_connected": True,
        "pedway_entrance": "Library tunnel, State St side",
        "accessible_entrances": ["State St elevator", "Van Buren elevator"],
        "platform_level": "elevated",
        "ada_compliant": True,
    },
    "lasalle_van_buren": {
        "map_id": "40160",
        "name": "LaSalle/Van Buren",
        "lines": ["Brown", "Orange", "Pink", "Purple"],
        "lat": 41.8766,
        "lon": -87.6318,
        "elevators": ["LVB-E1"],
        "pedway_connected": False,
        "accessible_entrances": ["LaSalle St elevator"],
        "platform_level": "elevated",
        "ada_compliant": True,
    },
    "quincy_wells": {
        "map_id": "40040",
        "name": "Quincy/Wells",
        "lines": ["Brown", "Orange", "Pink", "Purple"],
        "lat": 41.8784,
        "lon": -87.6379,
        "elevators": ["QW-E1"],
        "pedway_connected": False,
        "accessible_entrances": ["Quincy St elevator"],
        "platform_level": "elevated",
        "ada_compliant": True,
    },
    "washington_wells": {
        "map_id": "40730",
        "name": "Washington/Wells",
        "lines": ["Brown", "Orange", "Pink", "Purple"],
        "lat": 41.8827,
        "lon": -87.6337,
        "elevators": ["WWL-E1", "WWL-E2"],
        "pedway_connected": True,
        "pedway_entrance": "Pedway entrance at Wells St underpass",
        "accessible_entrances": ["Washington St elevator", "Wells St elevator"],
        "platform_level": "elevated",
        "ada_compliant": True,
    },
    "washington_dearborn": {
        "map_id": "40370",
        "name": "Washington/Dearborn (Blue)",
        "lines": ["Blue"],
        "lat": 41.8836,
        "lon": -87.6295,
        "elevators": ["WD-E1", "WD-E2"],
        "pedway_connected": True,
        "pedway_entrance": "Concourse level, connects to City Hall Pedway",
        "accessible_entrances": ["Dearborn St elevator", "Washington St elevator"],
        "platform_level": "underground",
        "ada_compliant": True,
    },
    "monroe_dearborn": {
        "map_id": "40790",
        "name": "Monroe/Dearborn (Blue)",
        "lines": ["Blue"],
        "lat": 41.8807,
        "lon": -87.6297,
        "elevators": ["MD-E1"],
        "pedway_connected": True,
        "pedway_entrance": "Monroe St Pedway connection",
        "accessible_entrances": ["Dearborn St elevator"],
        "platform_level": "underground",
        "ada_compliant": True,
    },
    "jackson_dearborn": {
        "map_id": "40920",
        "name": "Jackson/Dearborn (Blue)",
        "lines": ["Blue"],
        "lat": 41.8781,
        "lon": -87.6297,
        "elevators": ["JD-E1", "JD-E2"],
        "pedway_connected": True,
        "pedway_entrance": "Jackson Blvd underground corridor",
        "accessible_entrances": ["Dearborn St elevator", "Jackson Blvd elevator"],
        "platform_level": "underground",
        "ada_compliant": True,
    },
    "monroe_state": {
        "map_id": "41660",
        "name": "Monroe/State (Red)",
        "lines": ["Red"],
        "lat": 41.8806,
        "lon": -87.6279,
        "elevators": ["MS-E1"],
        "pedway_connected": False,
        "accessible_entrances": ["State St elevator"],
        "platform_level": "underground",
        "ada_compliant": True,
    },
    "jackson_state": {
        "map_id": "40070",
        "name": "Jackson/State (Red)",
        "lines": ["Red"],
        "lat": 41.8780,
        "lon": -87.6279,
        "elevators": ["JS-E1", "JS-E2"],
        "pedway_connected": False,
        "accessible_entrances": ["State St north elevator", "State St south elevator"],
        "platform_level": "underground",
        "ada_compliant": True,
    },
    "lake_state": {
        "map_id": "41660",
        "name": "Lake/State (Red)",
        "lines": ["Red"],
        "lat": 41.8847,
        "lon": -87.6280,
        "elevators": ["LS-E1"],
        "pedway_connected": False,
        "accessible_entrances": ["State St elevator"],
        "platform_level": "underground",
        "ada_compliant": True,
    },
}

# All elevator IDs across all stations (for quick lookup)
ALL_ELEVATOR_IDS: Set[str] = {
    eid
    for station in LOOP_STATIONS.values()
    for eid in station["elevators"]
}

# Reverse map: elevator_id → station_key
ELEVATOR_TO_STATION: Dict[str, str] = {
    eid: sk
    for sk, sdata in LOOP_STATIONS.items()
    for eid in sdata["elevators"]
}

# CTA station name fragments → station_key (for fuzzy matching alert headlines)
STATION_NAME_PATTERNS: Dict[str, str] = {
    "clark": "clark_lake",
    "clark/lake": "clark_lake",
    "state/lake": "state_lake",
    "randolph": "randolph_wabash",
    "washington/wabash": "washington_wabash",
    "madison/wabash": "madison_wabash",
    "adams/wabash": "adams_wabash",
    "harold washington": "harold_washington_library",
    "van buren": "harold_washington_library",
    "lasalle/van buren": "lasalle_van_buren",
    "quincy": "quincy_wells",
    "washington/wells": "washington_wells",
    "washington/dearborn": "washington_dearborn",
    "monroe/dearborn": "monroe_dearborn",
    "jackson/dearborn": "jackson_dearborn",
    "monroe/state": "monroe_state",
    "jackson/state": "jackson_state",
    "jackson": "jackson_dearborn",  # Blue line default if ambiguous
    "lake/state": "lake_state",
}

# ── Enums ─────────────────────────────────────────────────────────────────────

class ElevatorSeverity(str, Enum):
    OUTAGE    = "elevator_outage"       # elevator completely down
    ESCALATOR = "escalator_outage"      # escalator down (elevator still up)
    PARTIAL   = "partial_access"        # some entrances accessible
    UNKNOWN   = "unknown"


class ReliabilityTier(str, Enum):
    EXCELLENT = "excellent"    # ≥ 0.95
    GOOD      = "good"         # 0.85 – 0.95
    MODERATE  = "moderate"     # 0.70 – 0.85
    DEGRADED  = "degraded"     # 0.50 – 0.70
    POOR      = "poor"         # < 0.50  → routing avoids


class ImpactLevel(str, Enum):
    CRITICAL = "critical"   # no ADA access to station
    HIGH     = "high"       # primary elevator down, secondary available
    MEDIUM   = "medium"     # escalator only
    LOW      = "low"        # minor impact


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class ElevatorOutage:
    alert_id:          str
    station_key:       str                    # internal station key
    station_name:      str
    station_map_id:    str                    # CTA map ID
    elevator_ids:      List[str]              # internal elevator IDs affected
    severity:          ElevatorSeverity
    impact_level:      ImpactLevel
    headline:          str
    short_description: str
    full_description:  str
    affected_lines:    List[str]
    start_time:        datetime
    end_time:          Optional[datetime]
    is_tbd:            bool                   # True if end time unknown
    ada_alternatives:  List[str]              # human-readable alternatives
    source:            str = "cta_api"


@dataclass
class ElevatorReliabilityScore:
    elevator_id:     str
    station_key:     str
    station_name:    str
    score:           float                    # 0.0 – 1.0
    tier:            ReliabilityTier
    outages_7d:      int                      # outages in last 7 days
    outages_24h:     int                      # outages in last 24 hours
    is_currently_down: bool
    last_outage_at:  Optional[datetime]
    chronic:         bool                     # 3+ outages in 7 days
    routing_avoid:   bool                     # True if score < POOR threshold


@dataclass
class StationAccessibilityReport:
    station_key:         str
    station_name:        str
    station_map_id:      str
    lines:               List[str]
    lat:                 float
    lon:                 float
    is_fully_accessible: bool
    has_active_outage:   bool
    active_outages:      List[ElevatorOutage]
    elevator_scores:     List[ElevatorReliabilityScore]
    worst_score:         float
    pedway_connected:    bool
    pedway_entrance:     Optional[str]
    accessible_entrances: List[str]
    ada_alternatives:    List[str]            # nearby accessible station names
    platform_level:      str
    access_rating:       str                  # "full" | "limited" | "none"
    notes:               List[str]


@dataclass
class ElevatorSnapshot:
    fetched_at:          datetime
    active_outages:      List[ElevatorOutage]
    outaged_station_keys: Set[str]
    outaged_elevator_ids: Set[str]
    affected_lines:      Set[str]
    total_outages:       int
    critical_count:      int
    high_count:          int
    medium_count:        int
    loop_impact_score:   float               # 0-1, how much Loop ADA is affected
    api_ok:              bool
    poll_errors_consecutive: int


@dataclass
class OutagePattern:
    elevator_id:       str
    is_chronic:        bool
    outages_7d:        int
    peak_hour:         Optional[int]         # hour of day most outages start
    peak_day:          Optional[str]         # day of week most outages
    avg_duration_min:  Optional[float]       # average outage duration
    predictive_risk:   Dict[str, float]      # hour_str → risk 0-1


# ── CTA Elevator API Client ───────────────────────────────────────────────────

class CTAElevatorApiClient:
    """
    Thin async client for the CTA Customer Alerts API.
    Fetches all active alerts and filters for elevator/escalator outages.
    Implements 3-attempt exponential backoff with jitter.
    """

    MAX_RETRIES   = 3
    TIMEOUT_S     = 10.0
    BACKOFF_BASE  = 1.5

    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self._session_owned = session is None
        self._session       = session
        self._consecutive_errors = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.TIMEOUT_S)
            )
        return self._session

    async def close(self):
        if self._session_owned and self._session and not self._session.closed:
            await self._session.close()

    async def fetch_accessibility_alerts(self) -> List[Dict[str, Any]]:
        """
        Fetch all active CTA alerts and return only those related to
        elevator or escalator accessibility.
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                session = await self._get_session()
                async with session.get(CTA_ALERTS_URL) as resp:
                    if resp.status != 200:
                        raise aiohttp.ClientResponseError(
                            resp.request_info, resp.history, status=resp.status
                        )
                    data = await resp.json(content_type=None)
                    self._consecutive_errors = 0
                    raw_alerts = self._extract_alert_list(data)
                    return [a for a in raw_alerts if self._is_accessibility_alert(a)]

            except Exception as exc:
                self._consecutive_errors += 1
                if attempt < self.MAX_RETRIES - 1:
                    wait = self.BACKOFF_BASE ** attempt + (time.time() % 0.5)
                    logger.warning(
                        "CTA alerts fetch attempt %d failed: %s — retrying in %.1fs",
                        attempt + 1, exc, wait,
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(
                        "CTA alerts API unavailable after %d attempts: %s",
                        self.MAX_RETRIES, exc,
                    )
                    raise

        return []

    @staticmethod
    def _extract_alert_list(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Navigate the CTA JSON envelope to the Alert list."""
        try:
            alerts = data["CTAAlerts"]["Alert"]
            if isinstance(alerts, dict):
                return [alerts]
            return alerts or []
        except (KeyError, TypeError):
            return []

    @staticmethod
    def _is_accessibility_alert(alert: Dict[str, Any]) -> bool:
        """Return True if alert is elevator or escalator related."""
        impact = (alert.get("Impact") or "").lower()
        headline = (alert.get("Headline") or "").lower()
        return (
            "elevator" in impact
            or "escalator" in impact
            or "elevator" in headline
            or "escalator" in headline
            or "accessibility" in impact
        )

    @property
    def consecutive_errors(self) -> int:
        return self._consecutive_errors


# ── Outage Parser ─────────────────────────────────────────────────────────────

class CTAOutageParser:
    """
    Parses raw CTA alert dicts into typed ElevatorOutage objects.
    Uses regex-based station extraction and fuzzy name matching.
    """

    # Patterns to extract station names from CTA headlines
    STATION_PATTERNS = [
        re.compile(r"(?:at|@|near|for)\s+([A-Za-z/& ]+?)(?:\s*[-–—]|\s*elevator|\s*station|$)", re.I),
        re.compile(r"^([A-Za-z/& ]+?)\s+(?:elevator|escalator|station)", re.I),
    ]

    CTA_DT_FORMATS = [
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
    ]

    def parse(self, raw: Dict[str, Any]) -> Optional[ElevatorOutage]:
        """Parse a single raw CTA alert dict. Returns None if parsing fails."""
        try:
            alert_id   = str(raw.get("AlertId", ""))
            headline   = raw.get("Headline", "") or ""
            short_desc = raw.get("ShortDescription", "") or ""
            full_desc  = raw.get("FullDescription", "") or ""
            impact     = (raw.get("Impact") or "").lower()

            station_key, station_name = self._identify_station(headline + " " + short_desc)
            if not station_key:
                station_key  = "unknown"
                station_name = self._extract_raw_station_name(headline)

            station_data  = LOOP_STATIONS.get(station_key, {})
            station_map_id = station_data.get("map_id", "")
            elevator_ids   = station_data.get("elevators", [])
            affected_lines = self._extract_lines(raw)

            severity     = self._classify_severity(impact, headline)
            impact_level = self._classify_impact(severity, elevator_ids, station_data)
            start_time   = self._parse_dt(raw.get("EventStart"))
            end_time     = self._parse_dt(raw.get("EventEnd"))
            is_tbd       = str(raw.get("TBD", "0")) == "1"

            ada_alternatives = self._build_ada_alternatives(station_key)

            return ElevatorOutage(
                alert_id=alert_id,
                station_key=station_key,
                station_name=station_name or station_data.get("name", "Unknown"),
                station_map_id=station_map_id,
                elevator_ids=elevator_ids,
                severity=severity,
                impact_level=impact_level,
                headline=headline,
                short_description=short_desc,
                full_description=full_desc,
                affected_lines=affected_lines,
                start_time=start_time or datetime.now(timezone.utc),
                end_time=end_time,
                is_tbd=is_tbd,
                ada_alternatives=ada_alternatives,
            )
        except Exception as exc:
            logger.debug("Failed to parse elevator alert: %s", exc)
            return None

    def _identify_station(self, text: str) -> Tuple[str, str]:
        """Return (station_key, station_name) by matching text against known names."""
        text_lower = text.lower()
        # Direct substring match (longest match wins)
        best_key = ""
        best_pattern = ""
        for pattern, key in STATION_NAME_PATTERNS.items():
            if pattern in text_lower and len(pattern) > len(best_pattern):
                best_key = key
                best_pattern = pattern

        if best_key:
            return best_key, LOOP_STATIONS[best_key]["name"]

        # Regex extraction fallback
        for rx in self.STATION_PATTERNS:
            m = rx.search(text)
            if m:
                extracted = m.group(1).strip().lower()
                for pattern, key in STATION_NAME_PATTERNS.items():
                    if pattern in extracted or extracted in pattern:
                        return key, LOOP_STATIONS[key]["name"]

        return "", ""

    @staticmethod
    def _extract_raw_station_name(headline: str) -> str:
        """Best-effort station name extraction from headline."""
        # Remove common prefixes/suffixes
        cleaned = re.sub(r"(?i)elevator|escalator|outage|alert|accessibility|at |for ", "", headline)
        return cleaned.strip()[:60]

    @staticmethod
    def _extract_lines(raw: Dict[str, Any]) -> List[str]:
        lines: List[str] = []
        try:
            services = raw["ImpactedService"]["Service"]
            if isinstance(services, dict):
                services = [services]
            for svc in services:
                name = svc.get("ServiceName", "")
                if name and name not in lines:
                    lines.append(name)
        except (KeyError, TypeError):
            pass
        return lines

    @staticmethod
    def _classify_severity(impact: str, headline: str) -> ElevatorSeverity:
        hl = headline.lower()
        if "escalator" in impact or "escalator" in hl:
            return ElevatorSeverity.ESCALATOR
        if "elevator" in impact or "elevator" in hl:
            return ElevatorSeverity.OUTAGE
        if "partial" in impact:
            return ElevatorSeverity.PARTIAL
        return ElevatorSeverity.UNKNOWN

    @staticmethod
    def _classify_impact(
        severity: ElevatorSeverity,
        elevator_ids: List[str],
        station_data: Dict[str, Any],
    ) -> ImpactLevel:
        if severity == ElevatorSeverity.ESCALATOR:
            return ImpactLevel.MEDIUM
        if not elevator_ids or len(elevator_ids) == 0:
            return ImpactLevel.HIGH
        if len(elevator_ids) == 1:
            return ImpactLevel.CRITICAL   # only one elevator, now down
        if len(elevator_ids) == 2:
            return ImpactLevel.HIGH       # primary down, secondary available
        return ImpactLevel.MEDIUM

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

    @staticmethod
    def _build_ada_alternatives(station_key: str) -> List[str]:
        """Return nearest accessible stations as text alternatives."""
        station_data = LOOP_STATIONS.get(station_key)
        if not station_data:
            return []
        station_lat = station_data["lat"]
        station_lon = station_data["lon"]

        alternatives: List[Tuple[float, str]] = []
        for key, sdata in LOOP_STATIONS.items():
            if key == station_key:
                continue
            if not sdata.get("ada_compliant"):
                continue
            dist = _haversine(station_lat, station_lon, sdata["lat"], sdata["lon"])
            alternatives.append((dist, sdata["name"]))

        alternatives.sort(key=lambda x: x[0])
        return [name for _, name in alternatives[:3]]


# ── Elevator Status Registry ──────────────────────────────────────────────────

class ElevatorStatusRegistry:
    """
    Maintains current active elevator outages in memory and Redis.
    Provides fast O(1) lookups by elevator_id and station_key.
    """

    REDIS_KEY_SNAPSHOT    = "loopnav:elevator:snapshot"
    REDIS_KEY_HISTORY_PFX = "loopnav:elevator:history:"

    def __init__(self, redis=None):
        self._redis = redis
        self._outages: Dict[str, ElevatorOutage] = {}   # alert_id → outage
        self._lock = asyncio.Lock()

    async def update(self, outages: List[ElevatorOutage]) -> Tuple[List[str], List[str]]:
        """
        Update registry with fresh outage list.
        Returns (added_ids, resolved_ids) for change detection.
        """
        async with self._lock:
            new_ids     = {o.alert_id for o in outages}
            current_ids = set(self._outages.keys())

            added_ids   = list(new_ids - current_ids)
            resolved_ids = list(current_ids - new_ids)

            self._outages = {o.alert_id: o for o in outages}

            # Persist to Redis
            await self._persist_to_redis(outages)

            # Record history for new outages
            for alert_id in added_ids:
                outage = self._outages.get(alert_id)
                if outage:
                    await self._record_outage_history(outage)

            return added_ids, resolved_ids

    async def _persist_to_redis(self, outages: List[ElevatorOutage]):
        if not self._redis:
            return
        try:
            import json
            payload = json.dumps([
                {
                    "alert_id":       o.alert_id,
                    "station_key":    o.station_key,
                    "station_name":   o.station_name,
                    "elevator_ids":   o.elevator_ids,
                    "severity":       o.severity.value,
                    "impact_level":   o.impact_level.value,
                    "headline":       o.headline,
                    "affected_lines": o.affected_lines,
                    "start_time":     o.start_time.isoformat(),
                    "end_time":       o.end_time.isoformat() if o.end_time else None,
                }
                for o in outages
            ])
            await self._redis.setex(self.REDIS_KEY_SNAPSHOT, REDIS_SNAPSHOT_TTL, payload)
        except Exception as exc:
            logger.debug("Failed to persist elevator snapshot to Redis: %s", exc)

    async def _record_outage_history(self, outage: ElevatorOutage):
        """Record a new outage in per-elevator Redis sorted sets for history."""
        if not self._redis:
            return
        ts = outage.start_time.timestamp()
        try:
            for eid in outage.elevator_ids:
                key = f"{self.REDIS_KEY_HISTORY_PFX}{eid}"
                await self._redis.zadd(key, {outage.alert_id: ts})
                await self._redis.expire(key, REDIS_OUTAGE_HISTORY_TTL)
        except Exception as exc:
            logger.debug("Failed to record elevator outage history: %s", exc)

    def get_active_outages(self) -> List[ElevatorOutage]:
        return list(self._outages.values())

    def get_station_outages(self, station_key: str) -> List[ElevatorOutage]:
        return [o for o in self._outages.values() if o.station_key == station_key]

    def get_outaged_elevator_ids(self) -> Set[str]:
        ids: Set[str] = set()
        for o in self._outages.values():
            ids.update(o.elevator_ids)
        return ids

    def get_outaged_station_keys(self) -> Set[str]:
        return {o.station_key for o in self._outages.values()}

    def is_station_impacted(self, station_key: str) -> bool:
        return any(o.station_key == station_key for o in self._outages.values())

    def is_elevator_down(self, elevator_id: str) -> bool:
        down = self.get_outaged_elevator_ids()
        return elevator_id in down

    def total_active(self) -> int:
        return len(self._outages)


# ── Elevator Reliability Engine ───────────────────────────────────────────────

class ElevatorReliabilityEngine:
    """
    Calculates reliability scores for each elevator based on outage history
    stored in Redis sorted sets.

    Score formula:
      outages_7d = 0  → 0.95  (excellent)
      outages_7d = 1  → 0.80  (good)
      outages_7d = 2  → 0.65  (moderate)
      outages_7d = 3  → 0.50  (degraded)
      outages_7d ≥ 4  → 0.35  (poor — routing avoids)

    Additionally applies a -0.10 penalty for any outage in last 24h.
    Floor: 0.10. Ceiling: 0.95.
    """

    SCORE_TABLE = {0: 0.95, 1: 0.80, 2: 0.65, 3: 0.50}
    SCORE_FLOOR = 0.10
    RECENT_24H_PENALTY = 0.10
    REDIS_KEY_PFX = "loopnav:elevator:history:"

    def __init__(self, redis=None, registry: Optional[ElevatorStatusRegistry] = None):
        self._redis    = redis
        self._registry = registry

    async def calculate_score(self, elevator_id: str) -> ElevatorReliabilityScore:
        outages_7d   = await self._count_outages(elevator_id, hours=168)
        outages_24h  = await self._count_outages(elevator_id, hours=24)
        is_currently_down = (
            self._registry.is_elevator_down(elevator_id) if self._registry else False
        )
        last_outage_at = await self._last_outage_time(elevator_id)
        station_key    = ELEVATOR_TO_STATION.get(elevator_id, "unknown")
        station_name   = LOOP_STATIONS.get(station_key, {}).get("name", "Unknown")

        raw_score = self.SCORE_TABLE.get(outages_7d, max(0.35, 0.50 - (outages_7d - 3) * 0.05))
        if outages_24h > 0:
            raw_score -= self.RECENT_24H_PENALTY
        if is_currently_down:
            raw_score -= 0.15

        score = max(self.SCORE_FLOOR, min(0.95, raw_score))
        tier  = self._score_to_tier(score)

        return ElevatorReliabilityScore(
            elevator_id=elevator_id,
            station_key=station_key,
            station_name=station_name,
            score=round(score, 3),
            tier=tier,
            outages_7d=outages_7d,
            outages_24h=outages_24h,
            is_currently_down=is_currently_down,
            last_outage_at=last_outage_at,
            chronic=outages_7d >= 3,
            routing_avoid=score < RELIABILITY_POOR_THRESHOLD,
        )

    async def get_all_scores(self) -> Dict[str, ElevatorReliabilityScore]:
        """Return reliability scores for all known elevators."""
        scores: Dict[str, ElevatorReliabilityScore] = {}
        for eid in ALL_ELEVATOR_IDS:
            try:
                scores[eid] = await self.calculate_score(eid)
            except Exception as exc:
                logger.debug("Score calculation failed for %s: %s", eid, exc)
        return scores

    async def get_station_score(self, station_key: str) -> float:
        """Worst elevator score for a station (determines ADA routing decision)."""
        station_data = LOOP_STATIONS.get(station_key, {})
        eids = station_data.get("elevators", [])
        if not eids:
            return 0.0
        scores = [await self.calculate_score(eid) for eid in eids]
        return min(s.score for s in scores)

    async def should_avoid(self, elevator_id: str) -> bool:
        score_obj = await self.calculate_score(elevator_id)
        return score_obj.routing_avoid

    async def get_chronic_elevators(self) -> List[str]:
        """Return elevator IDs with 3+ outages in 7 days."""
        chronic = []
        for eid in ALL_ELEVATOR_IDS:
            count = await self._count_outages(eid, hours=168)
            if count >= 3:
                chronic.append(eid)
        return chronic

    async def _count_outages(self, elevator_id: str, hours: int) -> int:
        if not self._redis:
            return 0
        try:
            since = time.time() - hours * 3600
            key = f"{self.REDIS_KEY_PFX}{elevator_id}"
            count = await self._redis.zcount(key, since, "+inf")
            return int(count)
        except Exception:
            return 0

    async def _last_outage_time(self, elevator_id: str) -> Optional[datetime]:
        if not self._redis:
            return None
        try:
            key = f"{self.REDIS_KEY_PFX}{elevator_id}"
            results = await self._redis.zrevrangebyscore(key, "+inf", "-inf", start=0, num=1, withscores=True)
            if results:
                _, ts = results[0]
                return datetime.fromtimestamp(float(ts), tz=timezone.utc)
        except Exception:
            pass
        return None

    @staticmethod
    def _score_to_tier(score: float) -> ReliabilityTier:
        if score >= 0.95:
            return ReliabilityTier.EXCELLENT
        elif score >= 0.85:
            return ReliabilityTier.GOOD
        elif score >= 0.70:
            return ReliabilityTier.MODERATE
        elif score >= 0.50:
            return ReliabilityTier.DEGRADED
        else:
            return ReliabilityTier.POOR


# ── Outage Pattern Analyzer ───────────────────────────────────────────────────

class OutagePatternAnalyzer:
    """
    Detects temporal patterns in elevator outage history.
    Helps predict future outage risk by hour-of-day and day-of-week.
    """

    REDIS_KEY_PFX = "loopnav:elevator:history:"

    def __init__(self, redis=None):
        self._redis = redis

    async def analyze(self, elevator_id: str) -> OutagePattern:
        """Full pattern analysis for a single elevator."""
        outages_7d    = await self._get_outage_timestamps(elevator_id, hours=168)
        peak_hour     = self._find_peak_hour(outages_7d)
        peak_day      = self._find_peak_day(outages_7d)
        predictive    = self._build_predictive_risk(outages_7d)

        return OutagePattern(
            elevator_id=elevator_id,
            is_chronic=len(outages_7d) >= 3,
            outages_7d=len(outages_7d),
            peak_hour=peak_hour,
            peak_day=peak_day,
            avg_duration_min=None,   # CTA doesn't give end time reliably
            predictive_risk=predictive,
        )

    async def get_predictive_risk(self, elevator_id: str, dt: datetime) -> float:
        """0.0–1.0 risk score for an elevator at a specific datetime."""
        pattern = await self.analyze(elevator_id)
        if not pattern.is_chronic:
            return 0.1
        hour_key = str(dt.hour)
        return pattern.predictive_risk.get(hour_key, 0.2)

    async def _get_outage_timestamps(self, elevator_id: str, hours: int) -> List[float]:
        if not self._redis:
            return []
        try:
            since = time.time() - hours * 3600
            key   = f"{self.REDIS_KEY_PFX}{elevator_id}"
            results = await self._redis.zrangebyscore(key, since, "+inf", withscores=True)
            return [float(ts) for _, ts in results]
        except Exception:
            return []

    @staticmethod
    def _find_peak_hour(timestamps: List[float]) -> Optional[int]:
        if not timestamps:
            return None
        hour_counts: Dict[int, int] = defaultdict(int)
        for ts in timestamps:
            h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
            hour_counts[h] += 1
        return max(hour_counts, key=hour_counts.__getitem__)

    @staticmethod
    def _find_peak_day(timestamps: List[float]) -> Optional[str]:
        if not timestamps:
            return None
        DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        day_counts: Dict[int, int] = defaultdict(int)
        for ts in timestamps:
            d = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
            day_counts[d] += 1
        peak_day_idx = max(day_counts, key=day_counts.__getitem__)
        return DAYS[peak_day_idx]

    @staticmethod
    def _build_predictive_risk(timestamps: List[float]) -> Dict[str, float]:
        """Build hourly risk map from historical outage timestamps."""
        if not timestamps:
            return {}
        hour_counts: Dict[int, int] = defaultdict(int)
        for ts in timestamps:
            h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
            hour_counts[h] += 1
        total = sum(hour_counts.values()) or 1
        return {
            str(h): round(min(0.95, count / total + 0.1), 3)
            for h, count in hour_counts.items()
        }


# ── ADA Routing Advisor ───────────────────────────────────────────────────────

class ADARoutingAdvisor:
    """
    Provides ADA-specific routing advice:
    - Which stations are fully accessible right now
    - Per-station accessibility reports
    - Graph edge modifications for routing engine
    - Route-level ADA warnings
    """

    def __init__(
        self,
        registry: ElevatorStatusRegistry,
        reliability_engine: ElevatorReliabilityEngine,
    ):
        self._registry    = registry
        self._reliability = reliability_engine

    async def get_station_report(self, station_key: str) -> StationAccessibilityReport:
        station_data  = LOOP_STATIONS.get(station_key)
        if not station_data:
            raise ValueError(f"Unknown station key: {station_key}")

        active_outages   = self._registry.get_station_outages(station_key)
        elevator_ids     = station_data.get("elevators", [])
        elevator_scores  = [await self._reliability.calculate_score(eid) for eid in elevator_ids]
        worst_score      = min((s.score for s in elevator_scores), default=1.0)

        has_active_outage   = len(active_outages) > 0
        is_fully_accessible = (not has_active_outage) and (worst_score >= RELIABILITY_POOR_THRESHOLD)
        access_rating       = self._compute_access_rating(has_active_outage, worst_score, elevator_scores)
        ada_alternatives    = self._get_nearest_accessible(station_key)
        notes               = self._build_notes(active_outages, elevator_scores)

        return StationAccessibilityReport(
            station_key=station_key,
            station_name=station_data["name"],
            station_map_id=station_data["map_id"],
            lines=station_data["lines"],
            lat=station_data["lat"],
            lon=station_data["lon"],
            is_fully_accessible=is_fully_accessible,
            has_active_outage=has_active_outage,
            active_outages=active_outages,
            elevator_scores=elevator_scores,
            worst_score=round(worst_score, 3),
            pedway_connected=station_data.get("pedway_connected", False),
            pedway_entrance=station_data.get("pedway_entrance"),
            accessible_entrances=station_data.get("accessible_entrances", []),
            ada_alternatives=ada_alternatives,
            platform_level=station_data.get("platform_level", "unknown"),
            access_rating=access_rating,
            notes=notes,
        )

    async def get_all_accessible_stations(self) -> List[str]:
        """Return station keys that are currently fully accessible."""
        accessible = []
        for key in LOOP_STATIONS:
            report = await self.get_station_report(key)
            if report.is_fully_accessible:
                accessible.append(key)
        return accessible

    async def get_graph_edge_mods(self) -> Dict[str, Any]:
        """
        Returns dict of elevator_id → {disable: bool, multiplier: float}
        for the routing engine to apply to graph edges.
        """
        mods: Dict[str, Any] = {}
        for eid in ALL_ELEVATOR_IDS:
            is_down    = self._registry.is_elevator_down(eid)
            score_obj  = await self._reliability.calculate_score(eid)
            if is_down or score_obj.routing_avoid:
                mods[eid] = {"disable": True, "multiplier": 999.0}
            elif score_obj.score < 0.70:
                mods[eid] = {"disable": False, "multiplier": 1.5}
            else:
                mods[eid] = {"disable": False, "multiplier": 1.0}
        return mods

    async def build_route_warnings(self, station_keys: List[str]) -> List[Dict[str, Any]]:
        """Generate ADA warnings for a list of stations on a planned route."""
        warnings: List[Dict[str, Any]] = []
        for key in station_keys:
            if key not in LOOP_STATIONS:
                continue
            report = await self.get_station_report(key)
            if report.has_active_outage:
                for outage in report.active_outages:
                    warnings.append({
                        "type":         "elevator_outage",
                        "station":      report.station_name,
                        "station_key":  key,
                        "impact":       outage.impact_level.value,
                        "headline":     outage.headline,
                        "alternatives": outage.ada_alternatives,
                        "severity":     outage.severity.value,
                    })
            elif report.worst_score < RELIABILITY_POOR_THRESHOLD:
                warnings.append({
                    "type":        "low_reliability",
                    "station":     report.station_name,
                    "station_key": key,
                    "score":       report.worst_score,
                    "tier":        report.access_rating,
                    "message":     f"Elevator reliability at {report.station_name} is low ({report.worst_score:.0%}). Consider an alternative.",
                })
        return warnings

    @staticmethod
    def _compute_access_rating(
        has_outage: bool,
        worst_score: float,
        scores: List[ElevatorReliabilityScore],
    ) -> str:
        if has_outage and all(s.is_currently_down for s in scores):
            return "none"
        if has_outage or worst_score < RELIABILITY_POOR_THRESHOLD:
            return "limited"
        return "full"

    @staticmethod
    def _get_nearest_accessible(station_key: str) -> List[str]:
        station_data = LOOP_STATIONS.get(station_key)
        if not station_data:
            return []
        alts: List[Tuple[float, str]] = []
        for key, sdata in LOOP_STATIONS.items():
            if key == station_key:
                continue
            dist = _haversine(station_data["lat"], station_data["lon"], sdata["lat"], sdata["lon"])
            alts.append((dist, sdata["name"]))
        alts.sort()
        return [name for _, name in alts[:3]]

    @staticmethod
    def _build_notes(
        outages: List[ElevatorOutage],
        scores: List[ElevatorReliabilityScore],
    ) -> List[str]:
        notes: List[str] = []
        for o in outages:
            end = o.end_time.strftime("%H:%M") if o.end_time else "TBD"
            notes.append(f"Active outage: {o.headline} — expected resolution: {end}")
        for s in scores:
            if s.chronic:
                notes.append(
                    f"Elevator {s.elevator_id} is chronically unreliable "
                    f"({s.outages_7d} outages in 7 days)."
                )
        return notes


# ── Main ElevatorService ──────────────────────────────────────────────────────

class ElevatorService:
    """
    Production-grade CTA elevator reliability service.

    Lifecycle:
      1. initialize() — start background polling loop
      2. get_snapshot() — current full state
      3. get_station_report(key) — per-station ADA report
      4. get_reliability_report() — all elevator scores
      5. get_graph_edge_mods() — for routing engine integration
      6. shutdown() — clean up

    Background polling hits the CTA API every CTA_POLL_INTERVAL_S seconds
    and broadcasts change events to registered subscribers.
    """

    def __init__(self, redis=None):
        self._redis    = redis
        self._api      = CTAElevatorApiClient()
        self._parser   = CTAOutageParser()
        self._registry = ElevatorStatusRegistry(redis)
        self._reliability = ElevatorReliabilityEngine(redis, self._registry)
        self._patterns = OutagePatternAnalyzer(redis)
        self._advisor  = ADARoutingAdvisor(self._registry, self._reliability)

        self._snapshot:     Optional[ElevatorSnapshot] = None
        self._subscribers:  List[Callable] = []
        self._poll_task:    Optional[asyncio.Task] = None
        self._running:      bool = False
        self._poll_errors:  int = 0

    async def initialize(self):
        """Start the background polling loop."""
        if self._running:
            return
        self._running = True
        logger.info("ElevatorService: starting background polling (interval=%ds)", CTA_POLL_INTERVAL_S)
        self._poll_task = asyncio.create_task(self._polling_loop(), name="elevator_poller")

    async def shutdown(self):
        """Stop background polling and clean up."""
        self._running = False
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        await self._api.close()
        logger.info("ElevatorService: shut down.")

    async def refresh(self) -> ElevatorSnapshot:
        """Single poll cycle — fetch, parse, update, snapshot."""
        try:
            raw_alerts = await self._api.fetch_accessibility_alerts()
            outages    = [self._parser.parse(a) for a in raw_alerts]
            outages    = [o for o in outages if o is not None]

            added, resolved = await self._registry.update(outages)
            self._poll_errors = 0

            if added or resolved:
                await self._notify_subscribers("outage_change", {
                    "added":    added,
                    "resolved": resolved,
                })

            snapshot = await self._build_snapshot(api_ok=True)
            self._snapshot = snapshot
            logger.info(
                "ElevatorService: %d active outage(s) — %d added, %d resolved this cycle",
                snapshot.total_outages, len(added), len(resolved),
            )
            return snapshot

        except Exception as exc:
            self._poll_errors += 1
            logger.error("ElevatorService poll failed (consecutive=%d): %s", self._poll_errors, exc)
            snapshot = await self._build_snapshot(api_ok=False)
            self._snapshot = snapshot
            return snapshot

    async def get_snapshot(self) -> ElevatorSnapshot:
        """Return current snapshot, refreshing if stale."""
        if self._snapshot is None:
            return await self.refresh()
        age = (datetime.now(timezone.utc) - self._snapshot.fetched_at).total_seconds()
        if age > CTA_POLL_INTERVAL_S * 2:
            return await self.refresh()
        return self._snapshot

    async def get_station_report(self, station_key: str) -> StationAccessibilityReport:
        return await self._advisor.get_station_report(station_key)

    async def get_all_station_reports(self) -> Dict[str, StationAccessibilityReport]:
        return {key: await self._advisor.get_station_report(key) for key in LOOP_STATIONS}

    async def get_reliability_report(self) -> Dict[str, ElevatorReliabilityScore]:
        return await self._reliability.get_all_scores()

    async def get_graph_edge_mods(self) -> Dict[str, Any]:
        return await self._advisor.get_graph_edge_mods()

    async def get_outaged_elevator_ids(self) -> Set[str]:
        snapshot = await self.get_snapshot()
        return snapshot.outaged_elevator_ids

    async def get_route_warnings(self, station_keys: List[str]) -> List[Dict[str, Any]]:
        return await self._advisor.build_route_warnings(station_keys)

    async def get_chronic_elevators(self) -> List[str]:
        return await self._reliability.get_chronic_elevators()

    async def get_pattern_analysis(self, elevator_id: str) -> OutagePattern:
        return await self._patterns.analyze(elevator_id)

    async def get_accessible_stations(self) -> List[str]:
        return await self._advisor.get_all_accessible_stations()

    async def health_check(self) -> Dict[str, Any]:
        snapshot = await self.get_snapshot()
        return {
            "status":              "ok" if snapshot.api_ok else "degraded",
            "api_ok":              snapshot.api_ok,
            "active_outages":      snapshot.total_outages,
            "critical_outages":    snapshot.critical_count,
            "loop_impact_score":   snapshot.loop_impact_score,
            "poll_errors_streak":  self._poll_errors,
            "last_fetched":        snapshot.fetched_at.isoformat(),
            "subscriber_count":    len(self._subscribers),
        }

    def subscribe(self, callback: Callable) -> None:
        """Register a callback for real-time outage change events."""
        if callback not in self._subscribers:
            self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable) -> None:
        self._subscribers = [s for s in self._subscribers if s is not callback]

    async def _notify_subscribers(self, event_type: str, data: Dict[str, Any]):
        for cb in list(self._subscribers):
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(event_type, data)
                else:
                    cb(event_type, data)
            except Exception as exc:
                logger.debug("Subscriber callback error: %s", exc)

    async def _polling_loop(self):
        """Continuous background polling task."""
        # Initial fetch immediately
        await self.refresh()
        while self._running:
            try:
                await asyncio.sleep(CTA_POLL_INTERVAL_S)
                if self._running:
                    await self.refresh()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("ElevatorService polling loop error: %s", exc)
                await asyncio.sleep(10)

    async def _build_snapshot(self, api_ok: bool) -> ElevatorSnapshot:
        outages          = self._registry.get_active_outages()
        outaged_stations = self._registry.get_outaged_station_keys()
        outaged_elevs    = self._registry.get_outaged_elevator_ids()
        affected_lines: Set[str] = set()
        critical = high = medium = 0
        for o in outages:
            affected_lines.update(o.affected_lines)
            if o.impact_level == ImpactLevel.CRITICAL:
                critical += 1
            elif o.impact_level == ImpactLevel.HIGH:
                high += 1
            else:
                medium += 1

        total_stations    = len(LOOP_STATIONS)
        affected_stations = len(outaged_stations)
        loop_impact       = round(affected_stations / max(total_stations, 1), 3)

        return ElevatorSnapshot(
            fetched_at=datetime.now(timezone.utc),
            active_outages=outages,
            outaged_station_keys=outaged_stations,
            outaged_elevator_ids=outaged_elevs,
            affected_lines=affected_lines,
            total_outages=len(outages),
            critical_count=critical,
            high_count=high,
            medium_count=medium,
            loop_impact_score=loop_impact,
            api_ok=api_ok,
            poll_errors_consecutive=self._poll_errors,
        )


# ── Module-level Singleton ────────────────────────────────────────────────────

_elevator_service_instance: Optional[ElevatorService] = None


def get_elevator_service(redis=None) -> ElevatorService:
    """
    Return the module-level singleton ElevatorService.
    First call creates and returns it (without starting polling).
    Call .initialize() to start background polling.
    """
    global _elevator_service_instance
    if _elevator_service_instance is None:
        _elevator_service_instance = ElevatorService(redis=redis)
    return _elevator_service_instance


# ── Utility ───────────────────────────────────────────────────────────────────

def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return distance in meters between two lat/lon points."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a    = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def get_station_by_map_id(map_id: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    """Return (station_key, station_data) for a CTA map_id."""
    for key, data in LOOP_STATIONS.items():
        if data["map_id"] == map_id:
            return key, data
    return None


def get_nearest_station(lat: float, lon: float) -> Tuple[str, Dict[str, Any], float]:
    """Return (station_key, station_data, distance_m) for nearest Loop station."""
    best_key  = ""
    best_data = {}
    best_dist = float("inf")
    for key, data in LOOP_STATIONS.items():
        d = _haversine(lat, lon, data["lat"], data["lon"])
        if d < best_dist:
            best_dist = d
            best_key  = key
            best_data = data
    return best_key, best_data, best_dist
