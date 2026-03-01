"""
LoopNav — Production CTA Elevator & Alert Service.

Integrates with the Chicago Transit Authority Customer Alerts API to provide:
  - Real-time L train service disruptions for Loop stations
  - Elevator and escalator outage tracking with per-unit reliability scoring
  - Pedway closure tracking (mocked — no public API exists)
  - Graph edge modification: marks elevator edges as "unavailable" when outaged
  - Historical outage log for reliability scoring (session-scoped in Redis)

Architecture
────────────
  CTAService          — main singleton class, attached to app.state.cta_svc
  CTAApiClient        — HTTP client with retry + structured error handling
  ElevatorReliability — per-elevator historical reliability tracker
  AlertCache          — Redis-backed cache (1-min TTL for alerts)
  PedwayClosureStore  — mocked pedway closure registry
  MockAlertManager    — demo alert injection for hackathon presentations

CTA API reference
────────────────
  https://www.transitchicago.com/developers/alerts/
  Base URL: http://lapi.transitchicago.com/api/1.0/alerts.aspx
  Params: outputType=JSON, routeid=Brn,G,Org,P,Pink,Red,Blue,...

Elevator ID format (internal)
─────────────────────────────
  "elev_washington_wabash_N"   — northbound at Washington/Wabash
  "elev_quincy_wells_S"        — southbound at Quincy/Wells
  Used as edge attribute "elevator_id" in the NetworkX graph.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx

from app.config import settings

log = logging.getLogger("loopnav.cta")

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

CTA_ALERTS_BASE_URL = "http://lapi.transitchicago.com/api/1.0/alerts.aspx"

# Loop L routes — all lines that serve the inner Loop rectangle
LOOP_ROUTE_IDS = ["Brn", "G", "Org", "P", "Pink", "Red", "Blue"]

# CTA station IDs for Loop stations (used for station-specific alerts)
LOOP_STATION_IDS: Dict[str, str] = {
    "Washington/Wabash":          "41700",
    "Adams/Wabash":               "40680",
    "Harold Washington Library":  "40850",
    "Clark/Lake":                 "41660",
    "Washington/Wells":           "41410",
    "Quincy/Wells":               "40040",
    "LaSalle/Van Buren":          "40160",
    "LaSalle (Blue)":             "41340",
    "Lake (Red/Green)":           "41660",
    "Monroe (Red)":               "41090",
    "Jackson (Red)":              "40560",
    "Millennium Station (Metra)": "METRA_MIL",
}

# Elevator IDs matched to graph edge "elevator_id" attributes
ELEVATOR_IDS: Dict[str, str] = {
    "elev_washington_wabash_N": "Washington/Wabash — Northbound platform",
    "elev_washington_wabash_S": "Washington/Wabash — Southbound platform",
    "elev_quincy_wells_N":      "Quincy/Wells — Northbound",
    "elev_quincy_wells_S":      "Quincy/Wells — Southbound",
    "elev_clark_lake_N":        "Clark/Lake — Northbound",
    "elev_clark_lake_S":        "Clark/Lake — Southbound",
    "elev_washington_wells_N":  "Washington/Wells — Northbound",
    "elev_washington_wells_S":  "Washington/Wells — Southbound",
    "elev_lasalle_vanburen_N":  "LaSalle/Van Buren — Northbound",
    "elev_millennium_main":     "Millennium Station — Main concourse",
    "elev_chase_tower_B1":      "Chase Tower — Basement 1 to concourse",
    "elev_chase_tower_B2":      "Chase Tower — Basement 2 to Pedway",
    "elev_block37_N":           "Block 37 — North tunnel entrance",
    "elev_block37_S":           "Block 37 — South tunnel entrance",
    "elev_daley_center_W":      "Daley Center — West underground entrance",
    "elev_union_station_main":  "Union Station — Main concourse",
}

# Cache TTL (seconds)
ALERTS_CACHE_TTL   = 60     # 1 minute — alerts can change quickly
PEDWAY_CACHE_TTL   = 300    # 5 minutes — pedway closures change less often
HISTORY_TTL        = 3600   # 1 hour — reliability history window

# HTTP settings
HTTP_TIMEOUT_S = 8.0
RETRY_DELAYS   = [0.5, 1.0, 2.0]

# Reliability thresholds
RELIABILITY_UNKNOWN    = 0.9    # default when no history available
RELIABILITY_POOR       = 0.5    # below this → routing avoids elevator
RELIABILITY_DEGRADED   = 0.7    # warn user

# Pedway closure hours (these are real operating hours)
PEDWAY_OPEN_HOUR  = 6
PEDWAY_CLOSE_HOUR = 22


# ─────────────────────────────────────────────────────────────────────────────
# Enums and data classes
# ─────────────────────────────────────────────────────────────────────────────

class AlertSeverity(str, Enum):
    MINOR    = "Minor"
    MODERATE = "Moderate"
    MAJOR    = "Major"
    CRITICAL = "Critical"


class AlertCategory(str, Enum):
    ELEVATOR   = "elevator"
    ESCALATOR  = "escalator"
    TRAIN      = "train"
    PEDWAY     = "pedway"
    SIGNAL     = "signal"
    WEATHER    = "weather"
    OTHER      = "other"


@dataclass
class CTAAlert:
    """Parsed CTA service alert."""
    alert_id:           str
    headline:           str
    short_description:  str
    full_description:   str
    impact:             str
    severity:           AlertSeverity
    severity_score:     int             # 1–5 for sorting
    category:           AlertCategory
    is_elevator_outage: bool
    is_escalator_outage: bool
    affected_routes:    List[str]
    affected_stops:     List[str]
    elevator_ids:       List[str]       # matched internal elevator IDs
    start_time:         Optional[str]
    end_time:           Optional[str]
    source:             str             # "cta_api" / "mock"
    fetched_at:         str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "alert_id":           self.alert_id,
            "headline":           self.headline,
            "short_description":  self.short_description,
            "full_description":   self.full_description,
            "impact":             self.impact,
            "severity":           self.severity.value,
            "severity_score":     self.severity_score,
            "category":           self.category.value,
            "is_elevator_outage": self.is_elevator_outage,
            "is_escalator_outage": self.is_escalator_outage,
            "affected_routes":    self.affected_routes,
            "affected_stops":     self.affected_stops,
            "elevator_ids":       self.elevator_ids,
            "start_time":         self.start_time,
            "end_time":           self.end_time,
            "source":             self.source,
            "fetched_at":         self.fetched_at,
        }


@dataclass
class PedwaySegment:
    """A pedway segment with closure status."""
    segment_id:   str
    name:         str
    description:  str
    from_building: str
    to_building:  str
    is_closed:    bool
    closure_reason: Optional[str]
    reopen_time:  Optional[str]         # ISO timestamp or None
    alternative:  Optional[str]         # suggested alternative route
    lat:          float
    lon:          float

    def as_dict(self) -> Dict[str, Any]:
        return {
            "segment_id":     self.segment_id,
            "name":           self.name,
            "description":    self.description,
            "from_building":  self.from_building,
            "to_building":    self.to_building,
            "is_closed":      self.is_closed,
            "closure_reason": self.closure_reason,
            "reopen_time":    self.reopen_time,
            "alternative":    self.alternative,
            "lat":            self.lat,
            "lon":            self.lon,
        }


@dataclass
class ElevatorStatus:
    """Live status of a single elevator unit."""
    elevator_id:   str
    description:   str
    is_operational: bool
    reliability:   float    # 0.0–1.0
    outage_count:  int       # in the last hour
    last_outage:   Optional[str]
    alert_id:      Optional[str]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "elevator_id":   self.elevator_id,
            "description":   self.description,
            "is_operational": self.is_operational,
            "reliability":   self.reliability,
            "outage_count":  self.outage_count,
            "last_outage":   self.last_outage,
            "alert_id":      self.alert_id,
        }


@dataclass
class AlertsSnapshot:
    """Complete alerts snapshot returned to API layer."""
    alerts:             List[CTAAlert]
    elevator_outages:   List[CTAAlert]
    pedway_closures:    List[PedwaySegment]
    elevator_statuses:  List[ElevatorStatus]
    total_alerts:       int
    total_elevators_down: int
    total_pedway_closed:  int
    any_critical:       bool
    fetched_at:         str
    source:             str


# ─────────────────────────────────────────────────────────────────────────────
# CTA API HTTP client
# ─────────────────────────────────────────────────────────────────────────────

class CTAApiClient:
    """
    Async HTTP client for the CTA Customer Alerts API.
    Implements retry with exponential back-off. Falls back to mock on all errors.
    """

    def __init__(self, api_key: str = ""):
        self._key     = api_key
        self._timeout = HTTP_TIMEOUT_S

    async def fetch_alerts(self, route_ids: List[str]) -> Optional[List[Dict]]:
        params: Dict[str, Any] = {
            "outputType": "JSON",
            "routeid":    ",".join(route_ids),
            "activeonly": "true",
        }
        if self._key:
            params["key"] = self._key

        for attempt, delay in enumerate(RETRY_DELAYS + [0], start=1):
            try:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    resp = await client.get(CTA_ALERTS_BASE_URL, params=params)
                    resp.raise_for_status()
                    data = resp.json()
                    alerts_raw = (
                        data.get("CTAAlerts", {}).get("Alert", [])
                    )
                    if isinstance(alerts_raw, dict):
                        alerts_raw = [alerts_raw]
                    return alerts_raw
            except httpx.TimeoutException:
                log.warning("CTA API timeout attempt %d/%d", attempt, len(RETRY_DELAYS) + 1)
            except httpx.HTTPStatusError as exc:
                log.warning("CTA API HTTP %d on attempt %d", exc.response.status_code, attempt)
                if exc.response.status_code in (401, 403):
                    log.error("CTA API key rejected")
                    return None
            except Exception as exc:
                log.warning("CTA API error attempt %d: %s", attempt, exc)

            if delay > 0:
                await asyncio.sleep(delay)

        return None


# ─────────────────────────────────────────────────────────────────────────────
# Alert parser
# ─────────────────────────────────────────────────────────────────────────────

def _classify_alert(raw: Dict) -> AlertCategory:
    text = " ".join([
        raw.get("Headline", ""),
        raw.get("ShortDescription", ""),
        raw.get("FullDescription", ""),
    ]).lower()

    if any(k in text for k in ("elevator", "lift")):
        return AlertCategory.ELEVATOR
    if "escalator" in text:
        return AlertCategory.ESCALATOR
    if any(k in text for k in ("pedway", "tunnel", "underground")):
        return AlertCategory.PEDWAY
    if any(k in text for k in ("signal", "track", "switch")):
        return AlertCategory.SIGNAL
    if any(k in text for k in ("weather", "wind", "snow", "ice")):
        return AlertCategory.WEATHER
    if any(k in text for k in ("train", "service", "delay", "route")):
        return AlertCategory.TRAIN
    return AlertCategory.OTHER


def _match_elevator_ids(text: str) -> List[str]:
    """Match free-text alert description to known internal elevator IDs."""
    text_lower = text.lower()
    matched = []
    lookup = {
        "washington/wabash": ["elev_washington_wabash_N", "elev_washington_wabash_S"],
        "quincy":            ["elev_quincy_wells_N", "elev_quincy_wells_S"],
        "quincy/wells":      ["elev_quincy_wells_N", "elev_quincy_wells_S"],
        "clark/lake":        ["elev_clark_lake_N", "elev_clark_lake_S"],
        "washington/wells":  ["elev_washington_wells_N", "elev_washington_wells_S"],
        "lasalle":           ["elev_lasalle_vanburen_N"],
        "millennium":        ["elev_millennium_main"],
        "chase":             ["elev_chase_tower_B1", "elev_chase_tower_B2"],
        "block 37":          ["elev_block37_N", "elev_block37_S"],
        "daley":             ["elev_daley_center_W"],
        "union station":     ["elev_union_station_main"],
    }
    for keyword, ids in lookup.items():
        if keyword in text_lower:
            matched.extend(ids)
    return list(set(matched))


def _parse_raw_alert(raw: Dict, source: str = "cta_api") -> CTAAlert:
    headline   = raw.get("Headline", "Unknown alert")
    short_desc = raw.get("ShortDescription", "")
    full_desc  = raw.get("FullDescription", "")
    impact     = raw.get("Impact", "")
    severity   = raw.get("Severity", "Minor")
    service_id = raw.get("ServiceId", str(uuid.uuid4())[:8])

    category   = _classify_alert(raw)
    is_elev    = category == AlertCategory.ELEVATOR
    is_escal   = category == AlertCategory.ESCALATOR

    severity_map = {"Major": 4, "Moderate": 3, "Minor": 2}
    score        = severity_map.get(severity, 2)
    if is_elev or is_escal:
        score = max(score, 4)
    if "critical" in (headline + short_desc).lower():
        score = 5

    sev_enum = AlertSeverity.MAJOR if score >= 4 else (
        AlertSeverity.MODERATE if score == 3 else AlertSeverity.MINOR
    )

    text_combined = f"{headline} {short_desc} {full_desc}"
    elevator_ids  = _match_elevator_ids(text_combined) if (is_elev or is_escal) else []

    # Parse routes and stops
    routes = []
    for rk in ("Route", "Routes", "route"):
        rv = raw.get(rk)
        if rv:
            routes = rv if isinstance(rv, list) else [rv]
            break

    stops = []
    for sk in ("Stop", "Stops", "stop"):
        sv = raw.get(sk)
        if sv:
            stops = sv if isinstance(sv, list) else [sv]
            break

    return CTAAlert(
        alert_id           = str(service_id),
        headline           = headline,
        short_description  = short_desc,
        full_description   = full_desc,
        impact             = impact,
        severity           = sev_enum,
        severity_score     = score,
        category           = category,
        is_elevator_outage = is_elev,
        is_escalator_outage = is_escal,
        affected_routes    = routes,
        affected_stops     = stops,
        elevator_ids       = elevator_ids,
        start_time         = raw.get("EventStart"),
        end_time           = raw.get("EventEnd"),
        source             = source,
        fetched_at         = datetime.now(timezone.utc).isoformat(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Elevator reliability tracker
# ─────────────────────────────────────────────────────────────────────────────

class ElevatorReliabilityTracker:
    """
    Tracks per-elevator outage history within the session.
    Uses Redis sorted set (score = timestamp) for persistence.
    Falls back to in-memory deque if Redis unavailable.
    """

    def __init__(self, redis=None):
        self._redis    = redis
        self._memory:  Dict[str, List[float]] = {}     # elevator_id → list of outage timestamps
        self._outaged: Set[str]               = set()  # currently outaged elevator IDs

    async def record_outage(self, elevator_id: str) -> None:
        """Record an outage event for an elevator."""
        now = time.time()
        if self._redis:
            try:
                key = f"nav:cta:elev_history:{elevator_id}"
                await self._redis.zadd(key, now, str(now))
                await self._redis.zremrangebyscore(key, 0, now - HISTORY_TTL)
                await self._redis.expire(key, HISTORY_TTL + 60)
            except Exception:
                pass
        else:
            if elevator_id not in self._memory:
                self._memory[elevator_id] = []
            cutoff = now - HISTORY_TTL
            self._memory[elevator_id] = [t for t in self._memory[elevator_id] if t > cutoff]
            self._memory[elevator_id].append(now)

        self._outaged.add(elevator_id)

    def clear_outage(self, elevator_id: str) -> None:
        """Mark elevator as restored."""
        self._outaged.discard(elevator_id)

    def is_currently_outaged(self, elevator_id: str) -> bool:
        return elevator_id in self._outaged

    async def get_outage_count(self, elevator_id: str) -> int:
        """Count outages in the last hour."""
        now = time.time()
        if self._redis:
            try:
                key = f"nav:cta:elev_history:{elevator_id}"
                return await self._redis.zcount(key, now - HISTORY_TTL, now)
            except Exception:
                pass
        history = self._memory.get(elevator_id, [])
        cutoff  = now - HISTORY_TTL
        return sum(1 for t in history if t > cutoff)

    async def get_reliability(self, elevator_id: str) -> float:
        """
        Reliability = 1 - (outage_rate).
        Models based on outage count in last hour:
          0 outages  → 0.95 (never perfect due to known CTA reliability issues)
          1 outage   → 0.80
          2 outages  → 0.65
          3+ outages → 0.40
        """
        if self.is_currently_outaged(elevator_id):
            return 0.0
        count = await self.get_outage_count(elevator_id)
        if count == 0:
            return 0.95
        elif count == 1:
            return 0.80
        elif count == 2:
            return 0.65
        else:
            return max(0.40, 0.65 - (count - 2) * 0.10)

    async def get_all_statuses(self) -> List[ElevatorStatus]:
        """Return ElevatorStatus for all known elevators."""
        statuses = []
        for elev_id, desc in ELEVATOR_IDS.items():
            reliability   = await self.get_reliability(elev_id)
            outage_count  = await self.get_outage_count(elev_id)
            is_operational = not self.is_currently_outaged(elev_id)
            statuses.append(ElevatorStatus(
                elevator_id    = elev_id,
                description    = desc,
                is_operational = is_operational,
                reliability    = round(reliability, 3),
                outage_count   = outage_count,
                last_outage    = None,
                alert_id       = None,
            ))
        statuses.sort(key=lambda s: s.reliability)
        return statuses


# ─────────────────────────────────────────────────────────────────────────────
# Pedway closure store
# ─────────────────────────────────────────────────────────────────────────────

# Known pedway segments
_PEDWAY_SEGMENTS: List[PedwaySegment] = [
    PedwaySegment(
        segment_id="PW001", name="Washington–Block 37 Tunnel",
        description="Underground connection from Washington/State CTA to Block 37 mall",
        from_building="Washington/State CTA Station",
        to_building="Block 37",
        is_closed=False, closure_reason=None, reopen_time=None,
        alternative="Use State Street surface route",
        lat=41.8834, lon=-87.6279,
    ),
    PedwaySegment(
        segment_id="PW002", name="City Hall – Thompson Center Connector",
        description="Classic Pedway segment heated to 70°F year-round",
        from_building="Chicago City Hall",
        to_building="James R Thompson Center",
        is_closed=False, closure_reason=None, reopen_time=None,
        alternative="Use Washington St surface",
        lat=41.8843, lon=-87.6316,
    ),
    PedwaySegment(
        segment_id="PW003", name="Chase Tower Pedway",
        description="One of the warmest underground stretches — Chase to Dearborn",
        from_building="Chase Tower",
        to_building="Dearborn Street",
        is_closed=False, closure_reason=None, reopen_time=None,
        alternative="Use Monroe St surface",
        lat=41.8821, lon=-87.6313,
    ),
    PedwaySegment(
        segment_id="PW004", name="Union Station Underground",
        description="Deep connection from Union Station to Canal Street",
        from_building="Union Station",
        to_building="Canal Street",
        is_closed=False, closure_reason=None, reopen_time=None,
        alternative="Use Adams St surface",
        lat=41.8805, lon=-87.6354,
    ),
    PedwaySegment(
        segment_id="PW005", name="Millennium Park Underground Garage",
        description="Connects Millennium Park parking to Michigan Ave businesses",
        from_building="Millennium Park Garage",
        to_building="Michigan Avenue",
        is_closed=False, closure_reason=None, reopen_time=None,
        alternative="Use Randolph St surface",
        lat=41.8858, lon=-87.6231,
    ),
    PedwaySegment(
        segment_id="PW006", name="Daley Plaza Concourse",
        description="Under the Daley Center plaza connecting to City Hall",
        from_building="Daley Center",
        to_building="City Hall",
        is_closed=False, closure_reason=None, reopen_time=None,
        alternative="Use Washington St",
        lat=41.8839, lon=-87.6295,
    ),
]


class PedwayClosureStore:
    """
    Registry of pedway segment closures.
    In production this would sync with Chicago Data Portal / building management APIs.
    For the hackathon: supports manual closure injection via the mock endpoint.
    Also enforces operating hours (6 AM – 10 PM) automatically.
    """

    def __init__(self):
        self._segments: Dict[str, PedwaySegment] = {s.segment_id: s for s in _PEDWAY_SEGMENTS}
        self._manual_closures: Dict[str, Dict] = {}   # segment_id → closure metadata

    def get_all(self) -> List[PedwaySegment]:
        now   = datetime.now(timezone.utc)
        hour  = now.hour
        segs  = []
        for seg in self._segments.values():
            s = PedwaySegment(**seg.__dict__)
            # Enforce hours
            if not (PEDWAY_OPEN_HOUR <= hour < PEDWAY_CLOSE_HOUR):
                s.is_closed      = True
                s.closure_reason = f"Pedway closed outside operating hours ({PEDWAY_OPEN_HOUR}:00–{PEDWAY_CLOSE_HOUR}:00)"
                s.reopen_time    = None
            # Manual override
            if seg.segment_id in self._manual_closures:
                mc = self._manual_closures[seg.segment_id]
                s.is_closed      = mc.get("is_closed", True)
                s.closure_reason = mc.get("reason", "Manual closure")
                s.reopen_time    = mc.get("reopen_time")
            segs.append(s)
        return segs

    def get_closed(self) -> List[PedwaySegment]:
        return [s for s in self.get_all() if s.is_closed]

    def get_open(self) -> List[PedwaySegment]:
        return [s for s in self.get_all() if not s.is_closed]

    def close_segment(self, segment_id: str, reason: str, reopen_time: Optional[str] = None) -> bool:
        if segment_id not in self._segments:
            return False
        self._manual_closures[segment_id] = {
            "is_closed": True, "reason": reason, "reopen_time": reopen_time,
        }
        log.info("Pedway segment %s manually closed: %s", segment_id, reason)
        return True

    def restore_segment(self, segment_id: str) -> bool:
        if segment_id in self._manual_closures:
            del self._manual_closures[segment_id]
            log.info("Pedway segment %s restored", segment_id)
            return True
        return False

    def is_closed(self, segment_id: str) -> bool:
        segs = {s.segment_id: s for s in self.get_all()}
        seg  = segs.get(segment_id)
        return seg.is_closed if seg else False


# ─────────────────────────────────────────────────────────────────────────────
# Mock alert manager (for hackathon demo injection)
# ─────────────────────────────────────────────────────────────────────────────

class MockAlertManager:
    """
    Manages injected mock alerts for the hackathon demo.
    Supports adding / removing mock alerts at runtime via the API.
    """

    def __init__(self):
        self._mocks: Dict[str, CTAAlert] = {}
        self._preloaded = self._build_preloaded()

    def _build_preloaded(self) -> Dict[str, CTAAlert]:
        """Standard hackathon demo alerts."""
        return {
            "MOCK-ELEV-001": CTAAlert(
                alert_id="MOCK-ELEV-001",
                headline="Elevator Out: Washington/Wabash — Northbound Platform",
                short_description="The northbound elevator at Washington/Wabash (41700) is out of service for emergency repairs.",
                full_description="Customers requiring elevator access should use Adams/Wabash or Clark/Lake stations. Estimated repair: 4 hours.",
                impact="ADA customers must use adjacent stations with operational elevators.",
                severity=AlertSeverity.MAJOR,
                severity_score=5,
                category=AlertCategory.ELEVATOR,
                is_elevator_outage=True,
                is_escalator_outage=False,
                affected_routes=["Brn", "G", "Org", "P", "Pink"],
                affected_stops=["Washington/Wabash"],
                elevator_ids=["elev_washington_wabash_N"],
                start_time=datetime.now(timezone.utc).isoformat(),
                end_time=None,
                source="mock",
                fetched_at=datetime.now(timezone.utc).isoformat(),
            ),
            "MOCK-ESCAL-001": CTAAlert(
                alert_id="MOCK-ESCAL-001",
                headline="Escalator Maintenance: Quincy/Wells",
                short_description="Southbound escalator at Quincy/Wells undergoing routine maintenance. Stairs available.",
                full_description="Escalator #2 at Quincy/Wells station is under routine maintenance. Expected completion by 6PM.",
                impact="Customers may use adjacent stairs or elevator.",
                severity=AlertSeverity.MODERATE,
                severity_score=3,
                category=AlertCategory.ESCALATOR,
                is_elevator_outage=False,
                is_escalator_outage=True,
                affected_routes=["Brn", "P", "Pink"],
                affected_stops=["Quincy/Wells"],
                elevator_ids=["elev_quincy_wells_S"],
                start_time=datetime.now(timezone.utc).isoformat(),
                end_time=None,
                source="mock",
                fetched_at=datetime.now(timezone.utc).isoformat(),
            ),
            "MOCK-TRAIN-001": CTAAlert(
                alert_id="MOCK-TRAIN-001",
                headline="Brown/Purple Line: 10-15 min Delays Near Merchandise Mart",
                short_description="Signal malfunction causing Brown and Purple line delays.",
                full_description="Brown and Purple Line trains are operating 10-15 minutes behind schedule due to a signal problem near Merchandise Mart. Shuttle buses are not available.",
                impact="Significant delays on all Brown and Purple Line trains.",
                severity=AlertSeverity.MAJOR,
                severity_score=4,
                category=AlertCategory.SIGNAL,
                is_elevator_outage=False,
                is_escalator_outage=False,
                affected_routes=["Brn", "P"],
                affected_stops=[],
                elevator_ids=[],
                start_time=datetime.now(timezone.utc).isoformat(),
                end_time=None,
                source="mock",
                fetched_at=datetime.now(timezone.utc).isoformat(),
            ),
        }

    def use_preloaded(self) -> None:
        """Load all preloaded demo alerts into active mocks."""
        self._mocks.update(self._preloaded)
        log.info("Loaded %d preloaded mock alerts", len(self._preloaded))

    def add(self, alert: CTAAlert) -> None:
        self._mocks[alert.alert_id] = alert

    def remove(self, alert_id: str) -> bool:
        if alert_id in self._mocks:
            del self._mocks[alert_id]
            return True
        return False

    def clear(self) -> None:
        self._mocks.clear()

    def get_all(self) -> List[CTAAlert]:
        return list(self._mocks.values())


# ─────────────────────────────────────────────────────────────────────────────
# Graph edge modifier
# ─────────────────────────────────────────────────────────────────────────────

def apply_outages_to_graph(G, outaged_elevator_ids: Set[str]) -> int:
    """
    Mark graph edges with outaged elevator_ids as unavailable.
    Returns count of edges modified.

    The routing engine checks edge["outage"] == True and inflates cost
    by a large factor (or returns inf for accessible mode).
    """
    modified = 0
    try:
        import networkx as nx
        for u, v, k, data in G.edges(keys=True, data=True):
            eid = data.get("elevator_id")
            if eid and eid in outaged_elevator_ids:
                G[u][v][k]["outage"] = True
                G[u][v][k]["outage_cost_multiplier"] = 999.0
                modified += 1
            elif "outage" in data:
                del G[u][v][k]["outage"]
    except Exception as exc:
        log.warning("Failed to apply outages to graph: %s", exc)
    return modified


def get_route_outage_warnings(
    segments: List[Dict],
    outaged_ids: Set[str],
) -> List[str]:
    """
    Given a list of route segments and outaged elevator IDs,
    return human-readable warnings for any affected segments.
    """
    warnings = []
    for seg in segments:
        eid = seg.get("elevator_id")
        if eid and eid in outaged_ids:
            desc = ELEVATOR_IDS.get(eid, eid)
            warnings.append(f"⚠️  Elevator unavailable: {desc}")
    return warnings


# ─────────────────────────────────────────────────────────────────────────────
# Main CTAService
# ─────────────────────────────────────────────────────────────────────────────

class CTAService:
    """
    Production CTA alerts service — attach to app.state.cta_svc.

    Usage:
        snapshot = await app.state.cta_svc.get_snapshot()
        outaged  = await app.state.cta_svc.get_outaged_elevator_ids()
    """

    def __init__(self, redis=None):
        self._client      = CTAApiClient(settings.CTA_API_KEY)
        self._reliability = ElevatorReliabilityTracker(redis=redis)
        self._pedway      = PedwayClosureStore()
        self._mock_mgr    = MockAlertManager()
        self._redis       = redis
        self._stats       = {
            "api_calls":    0,
            "cache_hits":   0,
            "mock_used":    0,
            "errors":       0,
            "alerts_seen":  0,
        }
        self._use_mock    = not bool(settings.CTA_API_KEY)
        if self._use_mock:
            self._mock_mgr.use_preloaded()
            log.info("CTA service started in mock mode (no API key)")

    @property
    def pedway_store(self) -> PedwayClosureStore:
        return self._pedway

    @property
    def mock_manager(self) -> MockAlertManager:
        return self._mock_mgr

    @property
    def reliability_tracker(self) -> ElevatorReliabilityTracker:
        return self._reliability

    # ── Primary API ──────────────────────────────────────────────────────────

    async def get_snapshot(self) -> AlertsSnapshot:
        """Full snapshot: all alerts + elevator statuses + pedway closures."""
        alerts = await self._get_alerts()

        # Update reliability tracker
        outaged_ids: Set[str] = set()
        for alert in alerts:
            if alert.is_elevator_outage or alert.is_escalator_outage:
                for eid in alert.elevator_ids:
                    await self._reliability.record_outage(eid)
                    outaged_ids.add(eid)

        elevator_statuses = await self._reliability.get_all_statuses()
        pedway_closures   = self._pedway.get_all()
        elevator_outages  = [a for a in alerts if a.is_elevator_outage or a.is_escalator_outage]
        pedway_closed_cnt = sum(1 for s in pedway_closures if s.is_closed)
        any_critical      = any(a.severity_score >= 5 for a in alerts)

        return AlertsSnapshot(
            alerts             = alerts,
            elevator_outages   = elevator_outages,
            pedway_closures    = pedway_closures,
            elevator_statuses  = elevator_statuses,
            total_alerts       = len(alerts),
            total_elevators_down = len(elevator_outages),
            total_pedway_closed  = pedway_closed_cnt,
            any_critical       = any_critical,
            fetched_at         = datetime.now(timezone.utc).isoformat(),
            source             = "mock" if self._use_mock else "cta_api",
        )

    async def get_outaged_elevator_ids(self) -> Set[str]:
        """Return set of currently outaged elevator IDs (for graph modification)."""
        alerts = await self._get_alerts()
        ids: Set[str] = set()
        for alert in alerts:
            if alert.is_elevator_outage or alert.is_escalator_outage:
                ids.update(alert.elevator_ids)
        return ids

    async def get_elevator_reliability(self, elevator_id: str) -> float:
        """Return reliability score for a specific elevator (0.0–1.0)."""
        return await self._reliability.get_reliability(elevator_id)

    async def check_route_warnings(self, segments: List[Dict]) -> List[str]:
        """Check if a route's segments have elevator outage warnings."""
        outaged = await self.get_outaged_elevator_ids()
        return get_route_outage_warnings(segments, outaged)

    async def apply_to_graph(self, G) -> int:
        """Apply current outages to graph edge weights. Returns modified edge count."""
        outaged = await self.get_outaged_elevator_ids()
        return apply_outages_to_graph(G, outaged)

    # ── Internal fetch with cache ────────────────────────────────────────────

    async def _get_alerts(self) -> List[CTAAlert]:
        """Fetch alerts from cache → API → mock (priority order)."""

        # Redis cache
        if self._redis:
            try:
                cached = await self._redis.get("nav:cta:alerts")
                if cached:
                    self._stats["cache_hits"] += 1
                    # cached is a list of dicts
                    if isinstance(cached, list):
                        # Reconstruct CTAAlert objects
                        return [
                            CTAAlert(**{k: v for k, v in a.items()
                                       if k in CTAAlert.__dataclass_fields__})
                            for a in cached
                        ]
            except Exception as exc:
                log.debug("CTA Redis cache error: %s", exc)

        # Mock mode or no API key
        if self._use_mock:
            return self._mock_alerts()

        # Real API
        self._stats["api_calls"] += 1
        raw_alerts = await self._client.fetch_alerts(LOOP_ROUTE_IDS)
        if raw_alerts is None:
            self._stats["errors"] += 1
            log.warning("CTA API failed — using mock alerts")
            return self._mock_alerts()

        # Parse
        alerts = []
        for raw in raw_alerts:
            try:
                alerts.append(_parse_raw_alert(raw))
            except Exception as exc:
                log.debug("Failed to parse alert: %s", exc)

        # Add any manually injected mocks
        alerts.extend(self._mock_mgr.get_all())

        # Sort: elevator/escalator outages first, then severity
        alerts.sort(key=lambda a: (-int(a.is_elevator_outage or a.is_escalator_outage), -a.severity_score))

        self._stats["alerts_seen"] += len(alerts)

        # Cache
        if self._redis:
            try:
                await self._redis.set(
                    "nav:cta:alerts",
                    [a.as_dict() for a in alerts],
                    ttl=ALERTS_CACHE_TTL,
                )
            except Exception:
                pass

        return alerts

    def _mock_alerts(self) -> List[CTAAlert]:
        """Return all active mock alerts."""
        self._stats["mock_used"] += 1
        mocks = self._mock_mgr.get_all()
        if not mocks:
            self._mock_mgr.use_preloaded()
            mocks = self._mock_mgr.get_all()
        return mocks

    # ── Stats / health ───────────────────────────────────────────────────────

    def stats_snapshot(self) -> Dict[str, Any]:
        return dict(self._stats)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "api_key_set":    bool(settings.CTA_API_KEY),
            "use_mock":       self._use_mock,
            "mock_alerts":    len(self._mock_mgr.get_all()),
            "pedway_segments": len(self._pedway.get_all()),
            "pedway_closed":   len(self._pedway.get_closed()),
            "elevator_count":  len(ELEVATOR_IDS),
            "stats":          self.stats_snapshot(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton + backward-compatible helpers
# ─────────────────────────────────────────────────────────────────────────────

_svc: Optional[CTAService] = None


def get_service(redis=None) -> CTAService:
    global _svc
    if _svc is None:
        _svc = CTAService(redis=redis)
    return _svc


# Backward-compatible helpers used by existing LoopSense code

async def fetch_loop_alerts() -> List[Dict]:
    """Legacy helper — returns list of flat dicts."""
    svc     = get_service()
    alerts  = await svc._get_alerts()
    return [a.as_dict() for a in alerts]


async def fetch_elevator_outages() -> List[Dict]:
    """Legacy helper — elevator outages only."""
    svc    = get_service()
    alerts = await svc._get_alerts()
    return [a.as_dict() for a in alerts if a.is_elevator_outage or a.is_escalator_outage]
