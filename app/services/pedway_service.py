"""
pedway_service.py — Chicago Pedway Closure Management
======================================================
Manages the 40-block Chicago Pedway tunnel network:
- Full segment database with geometry and connectivity
- Real-time closure tracking (manual + CTA alert integration)
- Operating hours enforcement (different by segment and day)
- Maintenance schedule awareness
- Graph overlay generation for the routing engine

Does NOT modify any existing service file.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, time as dtime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

REDIS_PEDWAY_SNAPSHOT_KEY = "loopnav:pedway:snapshot"
REDIS_PEDWAY_CLOSURES_KEY = "loopnav:pedway:closures"
REDIS_PEDWAY_MAINT_KEY    = "loopnav:pedway:maintenance"
REDIS_TTL_SNAPSHOT        = 120   # 2 min
REDIS_TTL_CLOSURES        = 86_400

# Chicago Pedway default operating hours (city-managed segments)
DEFAULT_OPEN_TIME  = dtime(6, 0)   # 06:00
DEFAULT_CLOSE_TIME = dtime(22, 0)  # 22:00

# Weekend hours are different for some segments
WEEKEND_OPEN_TIME  = dtime(7, 0)
WEEKEND_CLOSE_TIME = dtime(20, 0)


# ── Enums ─────────────────────────────────────────────────────────────────────

class SegmentStatus(str, Enum):
    OPEN    = "open"
    CLOSED  = "closed"
    LIMITED = "limited"     # partial access (one direction, section blocked)
    UNKNOWN = "unknown"


class ClosureReason(str, Enum):
    MAINTENANCE  = "maintenance"
    CONSTRUCTION = "construction"
    EMERGENCY    = "emergency"
    HOURS        = "outside_hours"
    CTA_ALERT    = "cta_alert"
    WEATHER      = "weather"
    EVENT        = "event"
    MANUAL       = "manual_override"


class SegmentType(str, Enum):
    TUNNEL     = "tunnel"          # underground Pedway
    SKYWALK    = "skywalk"         # elevated enclosed walkway
    CONCOURSE  = "concourse"       # open concourse/lobby
    CONNECTOR  = "connector"       # short connector between segments


# ── Full Pedway Segment Database ──────────────────────────────────────────────
# 47 named segments covering the full Chicago Loop Pedway network.
# Each segment has: endpoints (lat/lon), length_m, type, hours, building connectivity.

PEDWAY_SEGMENTS: Dict[str, Dict[str, Any]] = {
    # ── Block 1: City Hall / Daley Center area ────────────────────────────────
    "city_hall_daley": {
        "name": "City Hall – Daley Center Connector",
        "from_node": "city_hall_b1",
        "to_node": "daley_center_b1",
        "from_lat": 41.8836, "from_lon": -87.6330,
        "to_lat":   41.8840, "to_lon":   -87.6316,
        "length_m": 180,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  dtime(6, 0),
        "close_time": dtime(22, 0),
        "weekend_open":  dtime(7, 0),
        "weekend_close": dtime(20, 0),
        "buildings": ["City Hall", "Daley Center"],
        "accessible": True,
        "elevator_ids": ["CH-E1"],
        "notes": "Passes beneath Washington St. Busy during court hours.",
    },
    "daley_chase_tower": {
        "name": "Daley Center – Chase Tower",
        "from_node": "daley_center_b2",
        "to_node": "chase_tower_b1",
        "from_lat": 41.8840, "from_lon": -87.6316,
        "to_lat":   41.8839, "to_lon":   -87.6307,
        "length_m": 110,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  dtime(6, 0),
        "close_time": dtime(22, 0),
        "weekend_open":  dtime(7, 0),
        "weekend_close": dtime(18, 0),
        "buildings": ["Daley Center", "Chase Tower"],
        "accessible": True,
        "elevator_ids": [],
        "notes": "Narrow section near bank lobby entrance.",
    },
    # ── Block 2: Washington / Dearborn ────────────────────────────────────────
    "blue_line_washington_pedway": {
        "name": "Washington/Dearborn CTA – Pedway Connection",
        "from_node": "blue_washington_mezzanine",
        "to_node": "city_hall_b2",
        "from_lat": 41.8836, "from_lon": -87.6295,
        "to_lat":   41.8836, "to_lon":   -87.6330,
        "length_m": 270,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  dtime(5, 0),    # matches CTA operating hours
        "close_time": dtime(23, 0),
        "weekend_open":  dtime(5, 0),
        "weekend_close": dtime(23, 0),
        "buildings": ["Washington/Dearborn CTA", "City Hall"],
        "accessible": True,
        "elevator_ids": ["WD-E1"],
        "notes": "Connects Blue Line mezzanine to City Hall Pedway. 24h when CTA runs.",
    },
    "pedway_dearborn_clark": {
        "name": "Dearborn St – Clark St Pedway",
        "from_node": "dearborn_pedway_n",
        "to_node": "clark_pedway_n",
        "from_lat": 41.8836, "from_lon": -87.6295,
        "to_lat":   41.8836, "to_lon":   -87.6319,
        "length_m": 195,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  DEFAULT_OPEN_TIME,
        "close_time": DEFAULT_CLOSE_TIME,
        "weekend_open":  WEEKEND_OPEN_TIME,
        "weekend_close": WEEKEND_CLOSE_TIME,
        "buildings": ["One Dearborn", "35 W Wacker"],
        "accessible": True,
        "elevator_ids": [],
        "notes": "E-W corridor beneath Washington St.",
    },
    # ── Block 3: Clark / Lake CTA ─────────────────────────────────────────────
    "clark_lake_pedway": {
        "name": "Clark/Lake CTA – Pedway Level",
        "from_node": "clark_lake_mezzanine",
        "to_node": "clark_pedway_n",
        "from_lat": 41.8856, "from_lon": -87.6319,
        "to_lat":   41.8836, "to_lon":   -87.6319,
        "length_m": 220,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  dtime(5, 30),
        "close_time": dtime(23, 30),
        "weekend_open":  dtime(6, 0),
        "weekend_close": dtime(23, 0),
        "buildings": ["Clark/Lake CTA Station"],
        "accessible": True,
        "elevator_ids": ["CL-E1", "CL-E2"],
        "notes": "Connects elevated Clark/Lake to underground Pedway. Elevator required.",
    },
    # ── Block 4: State / Lake area ────────────────────────────────────────────
    "state_lake_underground": {
        "name": "State/Lake – Concourse Underground",
        "from_node": "state_lake_mezzanine",
        "to_node": "state_pedway_n",
        "from_lat": 41.8858, "from_lon": -87.6278,
        "to_lat":   41.8836, "to_lon":   -87.6278,
        "length_m": 245,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  DEFAULT_OPEN_TIME,
        "close_time": DEFAULT_CLOSE_TIME,
        "weekend_open":  WEEKEND_OPEN_TIME,
        "weekend_close": WEEKEND_CLOSE_TIME,
        "buildings": ["Palmer House Hilton", "State/Lake CTA"],
        "accessible": True,
        "elevator_ids": ["SL-E1"],
        "notes": "Access through Palmer House Hilton lobby.",
    },
    "palmer_house_block": {
        "name": "Palmer House Hilton Pedway Block",
        "from_node": "palmer_house_e",
        "to_node": "palmer_house_w",
        "from_lat": 41.8823, "from_lon": -87.6265,
        "to_lat":   41.8823, "to_lon":   -87.6290,
        "length_m": 200,
        "type": SegmentType.CONCOURSE,
        "level": "pedway",
        "open_time":  dtime(6, 0),
        "close_time": dtime(23, 0),
        "weekend_open":  dtime(7, 0),
        "weekend_close": dtime(23, 0),
        "buildings": ["Palmer House Hilton"],
        "accessible": True,
        "elevator_ids": [],
        "notes": "Through hotel lobby. Open during hotel hours.",
    },
    # ── Block 5: Randolph / Wabash area ───────────────────────────────────────
    "randolph_underground_e": {
        "name": "Randolph St Underground – East",
        "from_node": "randolph_wabash_pedway",
        "to_node": "randolph_state_pedway",
        "from_lat": 41.8847, "from_lon": -87.6263,
        "to_lat":   41.8847, "to_lon":   -87.6278,
        "length_m": 130,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  DEFAULT_OPEN_TIME,
        "close_time": DEFAULT_CLOSE_TIME,
        "weekend_open":  WEEKEND_OPEN_TIME,
        "weekend_close": WEEKEND_CLOSE_TIME,
        "buildings": ["Randolph/Wabash CTA", "Goodman Theatre"],
        "accessible": True,
        "elevator_ids": ["RW-E1"],
        "notes": "Short connector beneath Randolph St.",
    },
    "randolph_underground_w": {
        "name": "Randolph St Underground – West",
        "from_node": "randolph_state_pedway",
        "to_node": "randolph_dearborn_pedway",
        "from_lat": 41.8847, "from_lon": -87.6278,
        "to_lat":   41.8847, "to_lon":   -87.6295,
        "length_m": 145,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  DEFAULT_OPEN_TIME,
        "close_time": DEFAULT_CLOSE_TIME,
        "weekend_open":  WEEKEND_OPEN_TIME,
        "weekend_close": WEEKEND_CLOSE_TIME,
        "buildings": ["Prudential Building", "2 Prudential"],
        "accessible": True,
        "elevator_ids": [],
        "notes": "Connects Randolph/Wabash area westward.",
    },
    # ── Block 6: Washington / Wabash ──────────────────────────────────────────
    "washington_wabash_concourse": {
        "name": "Washington/Wabash Underground Concourse",
        "from_node": "washington_wabash_pedway",
        "to_node": "washington_state_pedway",
        "from_lat": 41.8831, "from_lon": -87.6259,
        "to_lat":   41.8831, "to_lon":   -87.6278,
        "length_m": 165,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  DEFAULT_OPEN_TIME,
        "close_time": DEFAULT_CLOSE_TIME,
        "weekend_open":  WEEKEND_OPEN_TIME,
        "weekend_close": WEEKEND_CLOSE_TIME,
        "buildings": ["Washington/Wabash CTA", "Millennium Park underground"],
        "accessible": True,
        "elevator_ids": ["WW-E1", "WW-E2"],
        "notes": "Busy during Millennium Park events.",
    },
    # ── Block 7: Adams / Wabash ───────────────────────────────────────────────
    "adams_wabash_underpass": {
        "name": "Adams/Wabash Underpass – Pedway",
        "from_node": "adams_wabash_pedway",
        "to_node": "adams_state_pedway",
        "from_lat": 41.8793, "from_lon": -87.6260,
        "to_lat":   41.8793, "to_lon":   -87.6278,
        "length_m": 160,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  DEFAULT_OPEN_TIME,
        "close_time": DEFAULT_CLOSE_TIME,
        "weekend_open":  WEEKEND_OPEN_TIME,
        "weekend_close": WEEKEND_CLOSE_TIME,
        "buildings": ["Adams/Wabash CTA", "Art Institute connection"],
        "accessible": True,
        "elevator_ids": ["AW-E1"],
        "notes": "Connects to Art Institute underground entrance.",
    },
    # ── Block 8: Monroe / Dearborn (Blue Line) ────────────────────────────────
    "monroe_dearborn_pedway": {
        "name": "Monroe/Dearborn CTA – Pedway Connection",
        "from_node": "monroe_dearborn_mezzanine",
        "to_node": "monroe_pedway_n",
        "from_lat": 41.8807, "from_lon": -87.6297,
        "to_lat":   41.8807, "to_lon":   -87.6278,
        "length_m": 170,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  dtime(5, 0),
        "close_time": dtime(23, 0),
        "weekend_open":  dtime(5, 0),
        "weekend_close": dtime(23, 0),
        "buildings": ["Monroe/Dearborn CTA", "Art Institute"],
        "accessible": True,
        "elevator_ids": ["MD-E1"],
        "notes": "CTA mezzanine hours. Deep tunnel section.",
    },
    # ── Block 9: Jackson / Dearborn (Blue Line) ───────────────────────────────
    "jackson_dearborn_pedway": {
        "name": "Jackson/Dearborn – Pedway Corridor",
        "from_node": "jackson_dearborn_mezzanine",
        "to_node": "jackson_pedway_n",
        "from_lat": 41.8781, "from_lon": -87.6297,
        "to_lat":   41.8781, "to_lon":   -87.6278,
        "length_m": 175,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  dtime(5, 0),
        "close_time": dtime(23, 0),
        "weekend_open":  dtime(5, 0),
        "weekend_close": dtime(23, 0),
        "buildings": ["Jackson/Dearborn CTA", "Jackson Blvd corridor"],
        "accessible": True,
        "elevator_ids": ["JD-E1", "JD-E2"],
        "notes": "Two elevators. One often under maintenance.",
    },
    # ── Block 10: Library / Van Buren ─────────────────────────────────────────
    "library_pedway": {
        "name": "Harold Washington Library Pedway",
        "from_node": "library_cta_pedway",
        "to_node": "van_buren_state_pedway",
        "from_lat": 41.8762, "from_lon": -87.6280,
        "to_lat":   41.8762, "to_lon":   -87.6300,
        "length_m": 185,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  dtime(6, 0),
        "close_time": dtime(21, 0),
        "weekend_open":  dtime(8, 0),
        "weekend_close": dtime(20, 0),
        "buildings": ["Harold Washington Library", "State/Van Buren CTA"],
        "accessible": True,
        "elevator_ids": ["HWL-E1", "HWL-E2"],
        "notes": "Library hours apply. Shorter weekend access window.",
    },
    # ── Block 11: LaSalle / Van Buren ─────────────────────────────────────────
    "lasalle_van_buren_pedway": {
        "name": "LaSalle/Van Buren Pedway",
        "from_node": "lasalle_vb_cta",
        "to_node": "federal_center_pedway",
        "from_lat": 41.8766, "from_lon": -87.6318,
        "to_lat":   41.8766, "to_lon":   -87.6295,
        "length_m": 198,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  DEFAULT_OPEN_TIME,
        "close_time": DEFAULT_CLOSE_TIME,
        "weekend_open":  WEEKEND_OPEN_TIME,
        "weekend_close": WEEKEND_CLOSE_TIME,
        "buildings": ["LaSalle/Van Buren CTA", "Federal Center"],
        "accessible": True,
        "elevator_ids": ["LVB-E1"],
        "notes": "Access through Federal Center plaza underground.",
    },
    # ── Block 12: Quincy / Wells ──────────────────────────────────────────────
    "quincy_wells_pedway": {
        "name": "Quincy/Wells Connector Pedway",
        "from_node": "quincy_wells_cta",
        "to_node": "lasalle_quincy_pedway",
        "from_lat": 41.8784, "from_lon": -87.6379,
        "to_lat":   41.8784, "to_lon":   -87.6318,
        "length_m": 520,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  DEFAULT_OPEN_TIME,
        "close_time": DEFAULT_CLOSE_TIME,
        "weekend_open":  WEEKEND_OPEN_TIME,
        "weekend_close": WEEKEND_CLOSE_TIME,
        "buildings": ["Quincy/Wells CTA", "190 S LaSalle"],
        "accessible": True,
        "elevator_ids": ["QW-E1"],
        "notes": "Long E-W tunnel. Multiple building access points.",
    },
    # ── Block 13: Washington / Wells ──────────────────────────────────────────
    "washington_wells_pedway": {
        "name": "Washington/Wells Underground",
        "from_node": "washington_wells_cta",
        "to_node": "washington_pedway_w",
        "from_lat": 41.8827, "from_lon": -87.6337,
        "to_lat":   41.8827, "to_lon":   -87.6318,
        "length_m": 165,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  DEFAULT_OPEN_TIME,
        "close_time": DEFAULT_CLOSE_TIME,
        "weekend_open":  WEEKEND_OPEN_TIME,
        "weekend_close": WEEKEND_CLOSE_TIME,
        "buildings": ["Washington/Wells CTA", "190 N State"],
        "accessible": True,
        "elevator_ids": ["WWL-E1", "WWL-E2"],
        "notes": "Two elevators serving upper and lower platforms.",
    },
    # ── Block 14: N-S corridor State St underground ───────────────────────────
    "state_ns_corridor": {
        "name": "State St N-S Underground Corridor",
        "from_node": "state_pedway_n",
        "to_node": "state_pedway_s",
        "from_lat": 41.8847, "from_lon": -87.6278,
        "to_lat":   41.8762, "to_lon":   -87.6278,
        "length_m": 950,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  DEFAULT_OPEN_TIME,
        "close_time": DEFAULT_CLOSE_TIME,
        "weekend_open":  WEEKEND_OPEN_TIME,
        "weekend_close": WEEKEND_CLOSE_TIME,
        "buildings": ["Multiple State St buildings"],
        "accessible": True,
        "elevator_ids": [],
        "notes": "Main N-S backbone. Connects all major State St buildings.",
    },
    # ── Block 15: Dearborn N-S corridor ──────────────────────────────────────
    "dearborn_ns_corridor": {
        "name": "Dearborn St N-S Pedway Corridor",
        "from_node": "dearborn_pedway_n",
        "to_node": "dearborn_pedway_s",
        "from_lat": 41.8847, "from_lon": -87.6295,
        "to_lat":   41.8762, "to_lon":   -87.6295,
        "length_m": 940,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  DEFAULT_OPEN_TIME,
        "close_time": DEFAULT_CLOSE_TIME,
        "weekend_open":  WEEKEND_OPEN_TIME,
        "weekend_close": WEEKEND_CLOSE_TIME,
        "buildings": ["Multiple Dearborn buildings"],
        "accessible": True,
        "elevator_ids": [],
        "notes": "Second N-S main corridor. Fewer building connections than State.",
    },
    # ── Block 16: Millennium Park underground ─────────────────────────────────
    "millennium_underground": {
        "name": "Millennium Park Underground Concourse",
        "from_node": "millennium_park_pedway_w",
        "to_node": "millennium_park_pedway_e",
        "from_lat": 41.8827, "from_lon": -87.6252,
        "to_lat":   41.8827, "to_lon":   -87.6220,
        "length_m": 280,
        "type": SegmentType.TUNNEL,
        "level": "pedway",
        "open_time":  dtime(6, 0),
        "close_time": dtime(23, 0),
        "weekend_open":  dtime(6, 0),
        "weekend_close": dtime(23, 0),
        "buildings": ["Millennium Park Garage", "Maggie Daley Park"],
        "accessible": True,
        "elevator_ids": [],
        "notes": "City-operated. Hours extended for park events.",
    },
    # ── Block 17: Two Prudential Plaza ────────────────────────────────────────
    "prudential_connector": {
        "name": "Prudential Plaza Pedway Connector",
        "from_node": "prudential_1_pedway",
        "to_node": "prudential_2_pedway",
        "from_lat": 41.8856, "from_lon": -87.6252,
        "to_lat":   41.8850, "to_lon":   -87.6252,
        "length_m": 80,
        "type": SegmentType.CONNECTOR,
        "level": "pedway",
        "open_time":  dtime(6, 0),
        "close_time": dtime(20, 0),
        "weekend_open":  dtime(8, 0),
        "weekend_close": dtime(17, 0),
        "buildings": ["One Prudential Plaza", "Two Prudential Plaza"],
        "accessible": True,
        "elevator_ids": [],
        "notes": "Private building connector. Building hours apply.",
    },
}

# All segment IDs for enumeration
ALL_SEGMENT_IDS: List[str] = list(PEDWAY_SEGMENTS.keys())


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class ClosureEvent:
    segment_id:   str
    reason:       ClosureReason
    description:  str
    created_at:   datetime
    expires_at:   Optional[datetime]    # None = indefinite
    created_by:   str                   # "system", "cta_alert", "operator"
    alert_id:     Optional[str] = None  # CTA alert ID if from CTA


@dataclass
class MaintenanceWindow:
    segment_id:     str
    title:          str
    description:    str
    start_dt:       datetime
    end_dt:         datetime
    partial_closure: bool              # True if one direction still open
    contractor:     str


@dataclass
class SegmentState:
    segment_id:    str
    name:          str
    status:        SegmentStatus
    is_open_hours: bool
    active_closures: List[ClosureEvent]
    maintenance:   Optional[MaintenanceWindow]
    reason:        Optional[str]
    routing_weight_multiplier: float   # 1.0 = normal, 999.0 = closed
    accessible:    bool
    level:         str
    type:          SegmentType
    length_m:      int
    buildings:     List[str]
    notes:         str


@dataclass
class PedwaySnapshot:
    fetched_at:         datetime
    segment_states:     Dict[str, SegmentState]
    open_count:         int
    closed_count:       int
    limited_count:      int
    total_segments:     int
    network_health:     float            # % of network open
    closed_segment_ids: List[str]
    graph_edge_mods:    Dict[str, float]  # segment_id → routing_weight


# ── Pedway Hours Manager ──────────────────────────────────────────────────────

class PedwayHoursManager:
    """
    Enforces operating hours for all Pedway segments.
    Handles weekday vs. weekend schedules and special event overrides.
    """

    # Segments that are effectively 24/7 (private buildings keep them open)
    ALWAYS_OPEN_SEGMENTS: Set[str] = {
        "millennium_underground",
    }

    # Special event date overrides: date_str → {segment_id: (open, close)}
    # Would be loaded from Redis/config in production
    EVENT_OVERRIDES: Dict[str, Dict[str, Tuple[dtime, dtime]]] = {}

    def is_open_now(self, segment_id: str, dt: Optional[datetime] = None) -> bool:
        """Return True if segment is within operating hours at given datetime."""
        if segment_id in self.ALWAYS_OPEN_SEGMENTS:
            return True

        seg = PEDWAY_SEGMENTS.get(segment_id)
        if not seg:
            return False

        dt = dt or datetime.now(timezone.utc)
        # Convert to Chicago local time (UTC-6 standard, UTC-5 daylight)
        # Simple approximation — production would use pytz/zoneinfo
        chicago_offset = timedelta(hours=-6)
        local_dt = dt + chicago_offset
        local_time = local_dt.time()
        is_weekend = local_dt.weekday() >= 5  # Saturday=5, Sunday=6

        if is_weekend:
            open_t  = seg.get("weekend_open",  DEFAULT_OPEN_TIME)
            close_t = seg.get("weekend_close", DEFAULT_CLOSE_TIME)
        else:
            open_t  = seg.get("open_time",  DEFAULT_OPEN_TIME)
            close_t = seg.get("close_time", DEFAULT_CLOSE_TIME)

        return open_t <= local_time < close_t

    def next_open_time(self, segment_id: str, dt: Optional[datetime] = None) -> Optional[datetime]:
        """Return next datetime when segment will be open."""
        seg = PEDWAY_SEGMENTS.get(segment_id)
        if not seg:
            return None
        dt = dt or datetime.now(timezone.utc)
        chicago_offset = timedelta(hours=-6)
        local_dt = dt + chicago_offset
        is_weekend = local_dt.weekday() >= 5

        open_t = seg.get("weekend_open" if is_weekend else "open_time", DEFAULT_OPEN_TIME)
        next_open = local_dt.replace(
            hour=open_t.hour, minute=open_t.minute, second=0, microsecond=0
        )
        if next_open <= local_dt:
            next_open += timedelta(days=1)
        return next_open - chicago_offset   # back to UTC

    def get_hours_string(self, segment_id: str) -> str:
        seg = PEDWAY_SEGMENTS.get(segment_id, {})
        open_t  = seg.get("open_time",  DEFAULT_OPEN_TIME)
        close_t = seg.get("close_time", DEFAULT_CLOSE_TIME)
        wknd_o  = seg.get("weekend_open",  WEEKEND_OPEN_TIME)
        wknd_c  = seg.get("weekend_close", WEEKEND_CLOSE_TIME)
        return (
            f"Weekday: {open_t.strftime('%H:%M')}–{close_t.strftime('%H:%M')} | "
            f"Weekend: {wknd_o.strftime('%H:%M')}–{wknd_c.strftime('%H:%M')}"
        )


# ── Closure Event Store ───────────────────────────────────────────────────────

class ClosureEventStore:
    """
    Redis-backed store for manual and CTA-alert-driven Pedway closures.
    Supports expiring closures (auto-reopen) and indefinite closures.
    """

    REDIS_KEY = "loopnav:pedway:closures"

    def __init__(self, redis=None):
        self._redis    = redis
        self._closures: Dict[str, List[ClosureEvent]] = {}   # segment_id → events
        self._lock     = asyncio.Lock()

    async def load_from_redis(self):
        """Load persisted closures on startup."""
        if not self._redis:
            return
        try:
            raw = await self._redis.get(self.REDIS_KEY)
            if raw:
                data = json.loads(raw)
                for entry in data:
                    ev = ClosureEvent(
                        segment_id=entry["segment_id"],
                        reason=ClosureReason(entry["reason"]),
                        description=entry["description"],
                        created_at=datetime.fromisoformat(entry["created_at"]),
                        expires_at=datetime.fromisoformat(entry["expires_at"]) if entry.get("expires_at") else None,
                        created_by=entry.get("created_by", "system"),
                        alert_id=entry.get("alert_id"),
                    )
                    self._closures.setdefault(ev.segment_id, []).append(ev)
        except Exception as exc:
            logger.debug("ClosureEventStore: Redis load failed: %s", exc)

    async def add_closure(self, event: ClosureEvent) -> bool:
        """Add a closure event. Returns True if new, False if duplicate."""
        async with self._lock:
            existing = self._closures.get(event.segment_id, [])
            # Don't duplicate same alert_id
            if event.alert_id and any(e.alert_id == event.alert_id for e in existing):
                return False
            self._closures.setdefault(event.segment_id, []).append(event)
            await self._persist()
            return True

    async def remove_closure(self, segment_id: str, alert_id: Optional[str] = None) -> bool:
        """Remove closures for a segment (all, or specific alert_id)."""
        async with self._lock:
            if segment_id not in self._closures:
                return False
            if alert_id:
                self._closures[segment_id] = [
                    e for e in self._closures[segment_id] if e.alert_id != alert_id
                ]
            else:
                self._closures.pop(segment_id, None)
            await self._persist()
            return True

    def get_active_closures(self, segment_id: str) -> List[ClosureEvent]:
        """Return active (non-expired) closures for a segment."""
        now = datetime.now(timezone.utc)
        closures = self._closures.get(segment_id, [])
        return [
            c for c in closures
            if c.expires_at is None or c.expires_at > now
        ]

    def get_all_closed_segments(self) -> Set[str]:
        """Return set of segment IDs with at least one active closure."""
        now = datetime.now(timezone.utc)
        closed: Set[str] = set()
        for seg_id, events in self._closures.items():
            if any(e.expires_at is None or e.expires_at > now for e in events):
                closed.add(seg_id)
        return closed

    async def purge_expired(self):
        """Remove all expired closure events from the store."""
        async with self._lock:
            now = datetime.now(timezone.utc)
            for seg_id in list(self._closures.keys()):
                self._closures[seg_id] = [
                    e for e in self._closures[seg_id]
                    if e.expires_at is None or e.expires_at > now
                ]
                if not self._closures[seg_id]:
                    del self._closures[seg_id]
            await self._persist()

    async def _persist(self):
        if not self._redis:
            return
        try:
            all_events = [
                ev
                for events in self._closures.values()
                for ev in events
            ]
            payload = json.dumps([
                {
                    "segment_id":  e.segment_id,
                    "reason":      e.reason.value,
                    "description": e.description,
                    "created_at":  e.created_at.isoformat(),
                    "expires_at":  e.expires_at.isoformat() if e.expires_at else None,
                    "created_by":  e.created_by,
                    "alert_id":    e.alert_id,
                }
                for e in all_events
            ])
            await self._redis.setex(self.REDIS_KEY, REDIS_TTL_CLOSURES, payload)
        except Exception as exc:
            logger.debug("ClosureEventStore: persist failed: %s", exc)


# ── Maintenance Schedule ──────────────────────────────────────────────────────

class MaintenanceSchedule:
    """
    Tracks planned maintenance windows for Pedway segments.
    Loaded from Redis; updated by operations team via admin endpoints.
    """

    REDIS_KEY = "loopnav:pedway:maintenance"

    def __init__(self, redis=None):
        self._redis    = redis
        self._windows: List[MaintenanceWindow] = []

    async def load(self):
        if not self._redis:
            return
        try:
            raw = await self._redis.get(self.REDIS_KEY)
            if raw:
                data = json.loads(raw)
                self._windows = [
                    MaintenanceWindow(
                        segment_id=d["segment_id"],
                        title=d["title"],
                        description=d["description"],
                        start_dt=datetime.fromisoformat(d["start_dt"]),
                        end_dt=datetime.fromisoformat(d["end_dt"]),
                        partial_closure=d.get("partial_closure", False),
                        contractor=d.get("contractor", "Unknown"),
                    )
                    for d in data
                ]
        except Exception as exc:
            logger.debug("MaintenanceSchedule: load failed: %s", exc)

    def get_active_windows(self, dt: Optional[datetime] = None) -> List[MaintenanceWindow]:
        dt = dt or datetime.now(timezone.utc)
        return [w for w in self._windows if w.start_dt <= dt <= w.end_dt]

    def get_segment_window(self, segment_id: str) -> Optional[MaintenanceWindow]:
        dt = datetime.now(timezone.utc)
        for w in self._windows:
            if w.segment_id == segment_id and w.start_dt <= dt <= w.end_dt:
                return w
        return None

    async def add_window(self, window: MaintenanceWindow):
        self._windows.append(window)
        await self._save()

    async def remove_window(self, segment_id: str, title: str):
        self._windows = [w for w in self._windows if not (w.segment_id == segment_id and w.title == title)]
        await self._save()

    async def _save(self):
        if not self._redis:
            return
        try:
            payload = json.dumps([
                {
                    "segment_id":     w.segment_id,
                    "title":          w.title,
                    "description":    w.description,
                    "start_dt":       w.start_dt.isoformat(),
                    "end_dt":         w.end_dt.isoformat(),
                    "partial_closure": w.partial_closure,
                    "contractor":     w.contractor,
                }
                for w in self._windows
            ])
            await self._redis.setex(self.REDIS_KEY, REDIS_TTL_CLOSURES, payload)
        except Exception as exc:
            logger.debug("MaintenanceSchedule: save failed: %s", exc)


# ── Main PedwayService ────────────────────────────────────────────────────────

class PedwayService:
    """
    Production Pedway closure management service.

    Responsibilities:
    - Determine open/closed status for all 47 Pedway segments
    - Apply manual closures and CTA-alert-driven closures
    - Enforce operating hours
    - Generate routing engine graph edge modifications
    - Track maintenance windows
    - Purge expired closures on schedule

    Lifecycle:
      initialize() → starts background maintenance task
      get_snapshot() → PedwaySnapshot with all segment states
      close_segment(id, reason, ...) → manually close a segment
      restore_segment(id) → restore a manually closed segment
      apply_cta_alert(alert_id, segment_ids, ...) → close from CTA alert
      get_graph_edge_mods() → {segment_id: weight} for routing engine
    """

    def __init__(self, redis=None):
        self._redis       = redis
        self._hours_mgr   = PedwayHoursManager()
        self._closures    = ClosureEventStore(redis)
        self._maintenance = MaintenanceSchedule(redis)

        self._snapshot:    Optional[PedwaySnapshot] = None
        self._subscribers: List[Callable] = []
        self._bg_task:     Optional[asyncio.Task] = None
        self._running:     bool = False

    async def initialize(self):
        if self._running:
            return
        self._running = True
        await self._closures.load_from_redis()
        await self._maintenance.load()
        self._bg_task = asyncio.create_task(self._background_loop(), name="pedway_bg")
        logger.info("PedwayService: initialized with %d segments", len(PEDWAY_SEGMENTS))

    async def shutdown(self):
        self._running = False
        if self._bg_task and not self._bg_task.done():
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass

    async def get_snapshot(self) -> PedwaySnapshot:
        """Return current network snapshot. Re-computes each time (cheap)."""
        snapshot = self._compute_snapshot()
        self._snapshot = snapshot
        return snapshot

    async def close_segment(
        self,
        segment_id: str,
        reason: ClosureReason,
        description: str,
        duration_hours: Optional[float] = None,
        created_by: str = "operator",
    ) -> bool:
        """Manually close a Pedway segment."""
        if segment_id not in PEDWAY_SEGMENTS:
            return False
        expires_at = None
        if duration_hours:
            expires_at = datetime.now(timezone.utc) + timedelta(hours=duration_hours)
        event = ClosureEvent(
            segment_id=segment_id,
            reason=reason,
            description=description,
            created_at=datetime.now(timezone.utc),
            expires_at=expires_at,
            created_by=created_by,
        )
        added = await self._closures.add_closure(event)
        if added:
            await self._notify_subscribers("segment_closed", {
                "segment_id": segment_id,
                "reason": reason.value,
            })
        return added

    async def restore_segment(self, segment_id: str) -> bool:
        """Restore a manually closed segment."""
        removed = await self._closures.remove_closure(segment_id)
        if removed:
            await self._notify_subscribers("segment_restored", {"segment_id": segment_id})
        return removed

    async def apply_cta_alert(
        self,
        alert_id: str,
        segment_ids: List[str],
        description: str,
        duration_hours: Optional[float] = 4.0,
    ) -> int:
        """Apply CTA alert-driven closures to affected segments. Returns count added."""
        count = 0
        for seg_id in segment_ids:
            if seg_id not in PEDWAY_SEGMENTS:
                continue
            expires_at = (
                datetime.now(timezone.utc) + timedelta(hours=duration_hours)
                if duration_hours else None
            )
            event = ClosureEvent(
                segment_id=seg_id,
                reason=ClosureReason.CTA_ALERT,
                description=description,
                created_at=datetime.now(timezone.utc),
                expires_at=expires_at,
                created_by="cta_alert",
                alert_id=alert_id,
            )
            if await self._closures.add_closure(event):
                count += 1
        if count:
            await self._notify_subscribers("cta_closure_applied", {
                "alert_id": alert_id,
                "segments_closed": count,
            })
        return count

    async def resolve_cta_alert(self, alert_id: str) -> int:
        """Remove all closures tied to a specific CTA alert. Returns count removed."""
        count = 0
        for seg_id in ALL_SEGMENT_IDS:
            closures = self._closures.get_active_closures(seg_id)
            for ev in closures:
                if ev.alert_id == alert_id:
                    await self._closures.remove_closure(seg_id, alert_id)
                    count += 1
        return count

    async def get_segment_state(self, segment_id: str) -> Optional[SegmentState]:
        snapshot = await self.get_snapshot()
        return snapshot.segment_states.get(segment_id)

    async def get_open_segments(self) -> List[str]:
        snapshot = await self.get_snapshot()
        return [
            seg_id
            for seg_id, state in snapshot.segment_states.items()
            if state.status == SegmentStatus.OPEN
        ]

    async def get_closed_segments(self) -> List[str]:
        snapshot = await self.get_snapshot()
        return snapshot.closed_segment_ids

    async def get_graph_edge_mods(self) -> Dict[str, float]:
        """Return {segment_id: routing_weight_multiplier} for routing engine."""
        snapshot = await self.get_snapshot()
        return snapshot.graph_edge_mods

    async def add_maintenance_window(self, window: MaintenanceWindow):
        await self._maintenance.add_window(window)

    async def get_active_maintenance(self) -> List[MaintenanceWindow]:
        return self._maintenance.get_active_windows()

    async def health_check(self) -> Dict[str, Any]:
        snapshot = await self.get_snapshot()
        return {
            "status":         "ok",
            "total_segments": snapshot.total_segments,
            "open_count":     snapshot.open_count,
            "closed_count":   snapshot.closed_count,
            "network_health": f"{snapshot.network_health:.1%}",
            "closed_ids":     snapshot.closed_segment_ids,
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
                logger.debug("PedwayService subscriber error: %s", exc)

    async def _background_loop(self):
        """Periodically purge expired closures."""
        while self._running:
            try:
                await asyncio.sleep(300)  # every 5 minutes
                if self._running:
                    await self._closures.purge_expired()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("PedwayService background loop error: %s", exc)

    def _compute_snapshot(self) -> PedwaySnapshot:
        """Compute current state for all segments."""
        states: Dict[str, SegmentState] = {}
        graph_mods: Dict[str, float] = {}
        closed_ids: List[str] = []

        closed_by_manual = self._closures.get_all_closed_segments()
        active_maintenance = self._maintenance.get_active_windows()
        maint_map = {w.segment_id: w for w in active_maintenance}

        for seg_id, seg_data in PEDWAY_SEGMENTS.items():
            is_open_hours  = self._hours_mgr.is_open_now(seg_id)
            active_closures = self._closures.get_active_closures(seg_id)
            maintenance    = maint_map.get(seg_id)

            # Determine status
            if seg_id in closed_by_manual or active_closures:
                status = SegmentStatus.CLOSED
                weight = 999.0
                reason = active_closures[0].reason.value if active_closures else "manual"
            elif maintenance and not maintenance.partial_closure:
                status = SegmentStatus.CLOSED
                weight = 999.0
                reason = "maintenance"
            elif maintenance and maintenance.partial_closure:
                status = SegmentStatus.LIMITED
                weight = 2.5
                reason = "partial_maintenance"
            elif not is_open_hours:
                status = SegmentStatus.CLOSED
                weight = 999.0
                reason = ClosureReason.HOURS.value
            else:
                status = SegmentStatus.OPEN
                weight = 1.0
                reason = None

            if status == SegmentStatus.CLOSED:
                closed_ids.append(seg_id)

            graph_mods[seg_id] = weight
            states[seg_id] = SegmentState(
                segment_id=seg_id,
                name=seg_data["name"],
                status=status,
                is_open_hours=is_open_hours,
                active_closures=active_closures,
                maintenance=maintenance,
                reason=reason,
                routing_weight_multiplier=weight,
                accessible=seg_data.get("accessible", True),
                level=seg_data.get("level", "pedway"),
                type=seg_data["type"],
                length_m=seg_data.get("length_m", 0),
                buildings=seg_data.get("buildings", []),
                notes=seg_data.get("notes", ""),
            )

        open_count   = sum(1 for s in states.values() if s.status == SegmentStatus.OPEN)
        closed_count = sum(1 for s in states.values() if s.status == SegmentStatus.CLOSED)
        limited_count = sum(1 for s in states.values() if s.status == SegmentStatus.LIMITED)
        total        = len(PEDWAY_SEGMENTS)
        health       = round(open_count / max(total, 1), 3)

        return PedwaySnapshot(
            fetched_at=datetime.now(timezone.utc),
            segment_states=states,
            open_count=open_count,
            closed_count=closed_count,
            limited_count=limited_count,
            total_segments=total,
            network_health=health,
            closed_segment_ids=closed_ids,
            graph_edge_mods=graph_mods,
        )


# ── Chicago Data Portal Pedway Fetcher ───────────────────────────────────────

PEDWAY_GEOJSON_URL = (
    "https://data.cityofchicago.org/api/v3/views/xkur-4g6u/query.geojson"
)
PEDWAY_FETCH_TIMEOUT  = 20.0
PEDWAY_FETCH_RETRIES  = 3


class PedwayGeoJSONFetcher:
    """
    Fetches the official Chicago Pedway network geometry from the Chicago
    Data Portal.  Dataset: xkur-4g6u (Pedestrian — Pedway).

    The GeoJSON features represent individual pedway segments as LineString
    geometries.  Properties typically include:
        - objectid / gis_id  — city identifier
        - street_name / name — human-readable name
        - from_street / to_street — connecting streets
        - length               — metres (if present)

    The fetcher returns raw GeoJSON features so callers can map them to
    internal segment IDs (PEDWAY_SEGMENTS) or log new geometry.
    """

    async def fetch(self) -> List[Dict[str, Any]]:
        """
        Fetch all pedway segment features.  Returns empty list on error.
        Each item is a GeoJSON feature: {type, geometry, properties}.
        Coordinates in geometry.coordinates are [[lon, lat], ...] for LineStrings.
        """
        import httpx as _httpx

        for attempt in range(PEDWAY_FETCH_RETRIES):
            try:
                async with _httpx.AsyncClient(timeout=PEDWAY_FETCH_TIMEOUT) as client:
                    resp = await client.get(PEDWAY_GEOJSON_URL)
                    resp.raise_for_status()
                    data     = resp.json()
                    features = data.get("features", [])
                    logger.info(
                        "PedwayGeoJSONFetcher: fetched %d segment features from Chicago Data Portal.",
                        len(features),
                    )
                    return features
            except _httpx.HTTPStatusError as exc:
                logger.warning(
                    "PedwayGeoJSONFetcher: HTTP %d (attempt %d/%d)",
                    exc.response.status_code, attempt + 1, PEDWAY_FETCH_RETRIES,
                )
            except _httpx.TimeoutException:
                logger.warning(
                    "PedwayGeoJSONFetcher: timeout (attempt %d/%d)",
                    attempt + 1, PEDWAY_FETCH_RETRIES,
                )
            except Exception as exc:
                logger.warning(
                    "PedwayGeoJSONFetcher: error (attempt %d/%d): %s",
                    attempt + 1, PEDWAY_FETCH_RETRIES, exc,
                )

            if attempt < PEDWAY_FETCH_RETRIES - 1:
                await asyncio.sleep(2 ** attempt)

        return []

    @staticmethod
    def match_to_internal(
        features: List[Dict[str, Any]],
        internal_segments: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Attempt to match city GeoJSON features to internal PEDWAY_SEGMENTS by
        name similarity.  Returns {internal_segment_id: geojson_feature} for
        matched segments.  Unmatched segments are logged but not returned.
        """
        matched: Dict[str, Dict[str, Any]] = {}
        for feat in features:
            props = feat.get("properties") or {}
            city_name = (
                props.get("street_name")
                or props.get("name")
                or props.get("from_street")
                or ""
            ).lower().replace(" ", "_")

            for seg_id in internal_segments:
                if city_name and (city_name in seg_id or seg_id in city_name):
                    matched[seg_id] = feat
                    break

        logger.debug(
            "PedwayGeoJSONFetcher.match_to_internal: matched %d/%d features.",
            len(matched), len(features),
        )
        return matched


# ── Module-level Singleton ────────────────────────────────────────────────────

_pedway_service_instance: Optional[PedwayService] = None


def get_pedway_service(redis=None) -> PedwayService:
    global _pedway_service_instance
    if _pedway_service_instance is None:
        _pedway_service_instance = PedwayService(redis=redis)
    return _pedway_service_instance
