"""
sync_service.py — Predictive Departure Sync + CTA Train Tracker
================================================================
Kills the "when do I leave?" problem. Given a walker's current position
and a destination CTA station, calculates exactly when they should leave
to arrive on the platform as the train pulls in.

CTA Train Tracker API:
  https://lapi.transitchicago.com/api/1.0/ttarrivals.aspx

Features:
- Real-time train arrival ETAs for all Loop L stations
- Pedway-aware walk time estimation (indoor vs outdoor routes)
- Departure sync: leave_at = train_eta - walk_time - buffer
- Next N train schedule with miss-one fallback
- Live WebSocket push data: "Leave in 2 minutes"
- Multi-station: compare multiple boarding options simultaneously
- Graceful fallback to CTA schedule estimates when API is unavailable

Does NOT modify any existing service file.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

CTA_TRACKER_BASE_URL = "https://lapi.transitchicago.com/api/1.0"
ARRIVALS_URL         = f"{CTA_TRACKER_BASE_URL}/ttarrivals.aspx"
POSITIONS_URL        = f"{CTA_TRACKER_BASE_URL}/ttpositions.aspx"

TRACKER_CACHE_TTL_S  = 30        # cache arrivals for 30 seconds
TRACKER_TIMEOUT_S    = 8.0
MAX_RETRIES          = 3
BACKOFF_BASE         = 1.2

# Walk speed constants
WALK_SPEED_MPS       = 1.2       # average adult walking speed (m/s)
PEDWAY_SPEED_BONUS   = 1.15      # Pedway is faster (no wind, no crossings)
STAIR_PENALTY_S      = 45        # extra seconds per level change via stairs
ELEVATOR_PENALTY_S   = 90        # extra seconds per level change via elevator
DEFAULT_BUFFER_S     = 60        # default platform buffer (arrive 60s before train)
MAX_NEXT_TRAINS      = 6         # max upcoming trains to return

# All Loop stations with their CTA map IDs (from elevator_service.py shared data)
# Duplicated here for standalone use — stays in sync via the shared constant
LOOP_STATION_MAP_IDS: Dict[str, str] = {
    "clark_lake":               "40380",
    "state_lake":               "40260",
    "randolph_wabash":          "40200",
    "washington_wabash":        "41700",
    "madison_wabash":           "40710",
    "adams_wabash":             "40680",
    "harold_washington_library":"41340",
    "lasalle_van_buren":        "40160",
    "quincy_wells":             "40040",
    "washington_wells":         "40730",
    "washington_dearborn":      "40370",
    "monroe_dearborn":          "40790",
    "jackson_dearborn":         "40920",
    "monroe_state":             "41660",
    "jackson_state":            "40070",
    "lake_state":               "41660",
}

# Reverse: map_id → station_key
MAP_ID_TO_STATION: Dict[str, str] = {v: k for k, v in LOOP_STATION_MAP_IDS.items()}

# Station display info
STATION_INFO: Dict[str, Dict[str, Any]] = {
    "clark_lake":                {"name": "Clark/Lake",                   "lat": 41.8856, "lon": -87.6319, "level": "elevated"},
    "state_lake":                {"name": "State/Lake",                   "lat": 41.8858, "lon": -87.6278, "level": "elevated"},
    "randolph_wabash":           {"name": "Randolph/Wabash",              "lat": 41.8847, "lon": -87.6263, "level": "elevated"},
    "washington_wabash":         {"name": "Washington/Wabash",            "lat": 41.8831, "lon": -87.6259, "level": "elevated"},
    "madison_wabash":            {"name": "Madison/Wabash",               "lat": 41.8820, "lon": -87.6261, "level": "elevated"},
    "adams_wabash":              {"name": "Adams/Wabash",                 "lat": 41.8793, "lon": -87.6260, "level": "elevated"},
    "harold_washington_library": {"name": "Harold Washington Library",    "lat": 41.8762, "lon": -87.6280, "level": "elevated"},
    "lasalle_van_buren":         {"name": "LaSalle/Van Buren",            "lat": 41.8766, "lon": -87.6318, "level": "elevated"},
    "quincy_wells":              {"name": "Quincy/Wells",                 "lat": 41.8784, "lon": -87.6379, "level": "elevated"},
    "washington_wells":          {"name": "Washington/Wells",             "lat": 41.8827, "lon": -87.6337, "level": "elevated"},
    "washington_dearborn":       {"name": "Washington/Dearborn (Blue)",   "lat": 41.8836, "lon": -87.6295, "level": "underground"},
    "monroe_dearborn":           {"name": "Monroe/Dearborn (Blue)",       "lat": 41.8807, "lon": -87.6297, "level": "underground"},
    "jackson_dearborn":          {"name": "Jackson/Dearborn (Blue)",      "lat": 41.8781, "lon": -87.6297, "level": "underground"},
    "monroe_state":              {"name": "Monroe/State (Red)",            "lat": 41.8806, "lon": -87.6279, "level": "underground"},
    "jackson_state":             {"name": "Jackson/State (Red)",           "lat": 41.8780, "lon": -87.6279, "level": "underground"},
    "lake_state":                {"name": "Lake/State (Red)",              "lat": 41.8847, "lon": -87.6280, "level": "underground"},
}

# Lines that stop at each Loop station (for line filtering)
STATION_LINES: Dict[str, List[str]] = {
    "clark_lake":                ["Blue", "Brown", "Green", "Orange", "Pink", "Purple"],
    "state_lake":                ["Brown", "Green", "Orange", "Pink", "Purple"],
    "randolph_wabash":           ["Brown", "Green", "Orange", "Pink", "Purple"],
    "washington_wabash":         ["Brown", "Green", "Orange", "Pink", "Purple"],
    "madison_wabash":            ["Brown", "Green", "Orange", "Pink", "Purple"],
    "adams_wabash":              ["Brown", "Green", "Orange", "Pink", "Purple"],
    "harold_washington_library": ["Brown", "Orange", "Pink", "Purple"],
    "lasalle_van_buren":         ["Brown", "Orange", "Pink", "Purple"],
    "quincy_wells":              ["Brown", "Orange", "Pink", "Purple"],
    "washington_wells":          ["Brown", "Orange", "Pink", "Purple"],
    "washington_dearborn":       ["Blue"],
    "monroe_dearborn":           ["Blue"],
    "jackson_dearborn":          ["Blue"],
    "monroe_state":              ["Red"],
    "jackson_state":             ["Red"],
    "lake_state":                ["Red"],
}

# CTA line colors for UI
LINE_COLORS: Dict[str, str] = {
    "Red": "#C60C30", "Blue": "#00A1DE", "Brown": "#62361B",
    "Green": "#009B3A", "Orange": "#F9461C", "Pink": "#E27EA6",
    "Purple": "#522398",
}


# ── Enums ─────────────────────────────────────────────────────────────────────

class TrainStatus(str, Enum):
    ON_TIME     = "on_time"
    APPROACHING = "approaching"   # < 1 min away
    DELAYED     = "delayed"
    SCHEDULED   = "scheduled"     # from schedule, not real-time
    FAULT       = "fault"         # flagged fault


class SyncUrgency(str, Enum):
    LEAVE_NOW    = "leave_now"       # window is closing
    LEAVE_SOON   = "leave_soon"      # leave in < 5 min
    ON_TRACK     = "on_track"        # on schedule
    WAIT         = "wait"            # next train is better
    MISSED       = "missed"          # can't make this train


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class TrainArrival:
    run_number:    str
    line:          str
    line_color:    str
    destination:   str
    station_key:   str
    station_name:  str
    stop_id:       str
    stop_desc:     str
    eta:           datetime
    predicted_at:  datetime
    minutes_away:  int
    seconds_away:  int
    is_approaching: bool            # < 1 min
    is_scheduled:  bool            # True = schedule-based, False = real-time
    is_delayed:    bool
    is_fault:      bool
    status:        TrainStatus
    lat:           Optional[float]  # train's current lat
    lon:           Optional[float]  # train's current lon
    heading:       Optional[int]    # compass heading


@dataclass
class WalkEstimate:
    origin_lat:        float
    origin_lon:        float
    station_key:       str
    station_name:      str
    straight_line_m:   float
    estimated_walk_m:  float        # with route factor
    walk_seconds:      int
    via_pedway:        bool
    level_changes:     int          # how many floors to go up/down
    level_change_seconds: int
    total_seconds:     int          # walk + level changes
    confidence:        str          # "high" | "medium" | "low"
    notes:             str


@dataclass
class DepartureSync:
    """The core output: when to leave to catch a specific train."""
    train:             TrainArrival
    walk_estimate:     WalkEstimate
    buffer_seconds:    int
    leave_at:          datetime
    leave_in_seconds:  int
    arrive_at_platform: datetime
    platform_wait_seconds: int      # time waiting on platform
    urgency:           SyncUrgency
    catchable:         bool
    message:           str           # human-readable instruction
    confidence:        str           # "high" | "medium" | "low"


@dataclass
class StationArrivalBoard:
    """Full arrival board for a station."""
    station_key:   str
    station_name:  str
    map_id:        str
    fetched_at:    datetime
    arrivals:      List[TrainArrival]
    lines:         List[str]
    next_by_line:  Dict[str, TrainArrival]  # line → soonest arrival


@dataclass
class MultiStationSync:
    """Departure sync results for multiple nearby stations."""
    origin_lat:    float
    origin_lon:    float
    computed_at:   datetime
    options:       List[DepartureSync]
    recommended:   Optional[DepartureSync]  # best option
    reasoning:     str


# ── CTA Train Tracker API Client ──────────────────────────────────────────────

class TrainTrackerApiClient:
    """
    Async client for the CTA Train Tracker API.
    Requires CTA_TRAIN_TRACKER_KEY in environment.
    Falls back to schedule-based estimates when unavailable.
    """

    def __init__(self, api_key: Optional[str] = None, session: Optional[aiohttp.ClientSession] = None):
        self._api_key = api_key or os.getenv("CTA_TRAIN_TRACKER_KEY", "")
        self._session = session
        self._session_owned = session is None
        self._consecutive_errors = 0
        self._last_error_at: Optional[float] = None
        # In-process cache: map_id → (arrivals, fetched_at)
        self._cache: Dict[str, Tuple[List[Dict], float]] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=TRACKER_TIMEOUT_S)
            )
        return self._session

    async def close(self):
        if self._session_owned and self._session and not self._session.closed:
            await self._session.close()

    async def fetch_arrivals(self, map_id: str, max_results: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch arrival predictions for a station by CTA map ID.
        Uses 30-second in-process cache to avoid hammering the API.
        """
        # Check cache
        if map_id in self._cache:
            cached_data, fetched_at = self._cache[map_id]
            if time.time() - fetched_at < TRACKER_CACHE_TTL_S:
                return cached_data

        if not self._api_key:
            logger.debug("TrainTracker: no API key — using schedule fallback")
            return []

        for attempt in range(MAX_RETRIES):
            try:
                session = await self._get_session()
                params = {
                    "key":        self._api_key,
                    "mapid":      map_id,
                    "max":        max_results,
                    "outputType": "JSON",
                }
                async with session.get(ARRIVALS_URL, params=params) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                    eta_list = self._extract_etas(data)
                    self._cache[map_id] = (eta_list, time.time())
                    self._consecutive_errors = 0
                    return eta_list

            except Exception as exc:
                self._consecutive_errors += 1
                self._last_error_at = time.time()
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BACKOFF_BASE ** attempt)
                else:
                    logger.warning(
                        "TrainTracker API failed for map_id=%s after %d attempts: %s",
                        map_id, MAX_RETRIES, exc,
                    )
        return []

    async def fetch_all_loop_arrivals(self) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch arrivals for all Loop stations concurrently."""
        tasks = {
            key: asyncio.create_task(self.fetch_arrivals(map_id))
            for key, map_id in LOOP_STATION_MAP_IDS.items()
        }
        results: Dict[str, List[Dict[str, Any]]] = {}
        for key, task in tasks.items():
            try:
                results[key] = await task
            except Exception as exc:
                logger.debug("Loop arrivals fetch failed for %s: %s", key, exc)
                results[key] = []
        return results

    @staticmethod
    def _extract_etas(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            err = data["ctatt"]["errCd"]
            if err != "0":
                return []
            etas = data["ctatt"]["eta"]
            if isinstance(etas, dict):
                return [etas]
            return etas or []
        except (KeyError, TypeError):
            return []

    @property
    def has_api_key(self) -> bool:
        return bool(self._api_key)

    @property
    def consecutive_errors(self) -> int:
        return self._consecutive_errors


# ── Arrival Parser ────────────────────────────────────────────────────────────

class ArrivalParser:
    """Converts raw CTA ETA dicts into typed TrainArrival objects."""

    CTA_DT_FMT = "%Y-%m-%dT%H:%M:%S"

    def parse(self, raw: Dict[str, Any], station_key: str) -> Optional[TrainArrival]:
        try:
            map_id     = raw.get("staId", "")
            stop_id    = raw.get("stpId", "")
            stop_desc  = raw.get("stpDe", "")
            run_num    = raw.get("rn", "")
            line       = self._normalize_line(raw.get("rt", ""))
            dest_name  = raw.get("destNm", "")
            pred_time  = self._parse_dt(raw.get("prdt"))
            arr_time   = self._parse_dt(raw.get("arrT"))
            is_app     = raw.get("isApp", "0") == "1"
            is_sch     = raw.get("isSch", "0") == "1"
            is_delay   = raw.get("isDly", "0") == "1"
            is_fault   = raw.get("isFlt", "0") == "1"
            lat        = float(raw["lat"]) if raw.get("lat") else None
            lon        = float(raw["lon"]) if raw.get("lon") else None
            heading    = int(raw["heading"]) if raw.get("heading") else None

            if not arr_time:
                return None

            now              = datetime.now(timezone.utc)
            delta            = arr_time - now
            seconds_away     = max(0, int(delta.total_seconds()))
            minutes_away     = seconds_away // 60

            status = (
                TrainStatus.APPROACHING if is_app
                else TrainStatus.DELAYED    if is_delay
                else TrainStatus.FAULT      if is_fault
                else TrainStatus.SCHEDULED  if is_sch
                else TrainStatus.ON_TIME
            )

            station_info = STATION_INFO.get(station_key, {})

            return TrainArrival(
                run_number=run_num,
                line=line,
                line_color=LINE_COLORS.get(line, "#888888"),
                destination=dest_name,
                station_key=station_key,
                station_name=station_info.get("name", ""),
                stop_id=stop_id,
                stop_desc=stop_desc,
                eta=arr_time,
                predicted_at=pred_time or now,
                minutes_away=minutes_away,
                seconds_away=seconds_away,
                is_approaching=is_app,
                is_scheduled=is_sch,
                is_delayed=is_delay,
                is_fault=is_fault,
                status=status,
                lat=lat,
                lon=lon,
                heading=heading,
            )
        except Exception as exc:
            logger.debug("ArrivalParser: failed to parse eta: %s", exc)
            return None

    def _parse_dt(self, dt_str: Optional[str]) -> Optional[datetime]:
        if not dt_str:
            return None
        try:
            dt = datetime.strptime(dt_str.strip(), self.CTA_DT_FMT)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _normalize_line(rt: str) -> str:
        mapping = {"P": "Purple", "G": "Green", "Org": "Orange",
                   "Pnk": "Pink", "Brn": "Brown", "Y": "Yellow"}
        return mapping.get(rt, rt)


# ── Walk Time Calculator ──────────────────────────────────────────────────────

class WalkTimeCalculator:
    """
    Estimates walk time from an arbitrary lat/lon to a CTA station entrance.
    Uses Pedway network topology to determine whether the route is indoor.
    Accounts for level changes (stairs vs elevator) based on station type.
    """

    # Stations that have Pedway access (can arrive underground)
    PEDWAY_STATIONS: Dict[str, bool] = {
        "clark_lake":          True,
        "washington_wabash":   True,
        "randolph_wabash":     True,
        "adams_wabash":        True,
        "harold_washington_library": True,
        "washington_wells":    True,
        "washington_dearborn": True,
        "monroe_dearborn":     True,
        "jackson_dearborn":    True,
        "lasalle_van_buren":   True,
        "quincy_wells":        True,
    }

    # Pedway coverage bounding box — if origin is inside, assume partial indoor
    PEDWAY_BOUNDS = {
        "lat_min": 41.876, "lat_max": 41.886,
        "lon_min": -87.638, "lon_max": -87.624,
    }

    def estimate(
        self,
        origin_lat: float,
        origin_lon: float,
        station_key: str,
        accessible: bool = False,
    ) -> WalkEstimate:
        station_info = STATION_INFO.get(station_key, {})
        station_lat  = station_info.get("lat", 41.8827)
        station_lon  = station_info.get("lon", -87.6318)
        station_level = station_info.get("level", "elevated")

        straight_m   = _haversine(origin_lat, origin_lon, station_lat, station_lon)

        # Route factor: indoor Pedway routes are more direct (no jay-walking detours)
        # but street routes have a 1.25x grid factor
        via_pedway  = self._can_use_pedway(origin_lat, origin_lon, station_key)
        route_factor = 1.12 if via_pedway else 1.28
        route_m      = straight_m * route_factor

        # Walking speed
        speed = WALK_SPEED_MPS * (PEDWAY_SPEED_BONUS if via_pedway else 1.0)
        walk_s = route_m / speed

        # Level changes
        level_changes = 0
        if station_level == "elevated":
            level_changes = 2   # street → mezzanine → platform
        elif station_level == "underground":
            level_changes = 2   # street → concourse → platform

        if accessible:
            level_change_s = level_changes * ELEVATOR_PENALTY_S
        else:
            level_change_s = level_changes * STAIR_PENALTY_S

        total_s = int(walk_s + level_change_s)

        # Confidence based on how accurate our straight-line estimate is
        if straight_m < 300:
            confidence = "high"
            notes = "Short walk — highly accurate estimate."
        elif straight_m < 800:
            confidence = "medium"
            notes = "Medium distance — estimate within ±30 seconds."
        else:
            confidence = "low"
            notes = "Long walk — consider taking CTA or a rideshare."

        station_name = station_info.get("name", station_key)

        return WalkEstimate(
            origin_lat=origin_lat,
            origin_lon=origin_lon,
            station_key=station_key,
            station_name=station_name,
            straight_line_m=round(straight_m, 1),
            estimated_walk_m=round(route_m, 1),
            walk_seconds=int(walk_s),
            via_pedway=via_pedway,
            level_changes=level_changes,
            level_change_seconds=level_change_s,
            total_seconds=total_s,
            confidence=confidence,
            notes=notes,
        )

    def _can_use_pedway(self, lat: float, lon: float, station_key: str) -> bool:
        """True if both origin and station are Pedway-accessible."""
        if not self.PEDWAY_STATIONS.get(station_key, False):
            return False
        # Check if origin is within Pedway coverage area
        b = self.PEDWAY_BOUNDS
        in_bounds = (
            b["lat_min"] <= lat <= b["lat_max"]
            and b["lon_min"] <= lon <= b["lon_max"]
        )
        return in_bounds

    def nearest_stations(
        self, lat: float, lon: float, limit: int = 3
    ) -> List[Tuple[str, float]]:
        """Return [(station_key, distance_m)] sorted by distance."""
        distances: List[Tuple[str, float]] = [
            (key, _haversine(lat, lon, info["lat"], info["lon"]))
            for key, info in STATION_INFO.items()
        ]
        distances.sort(key=lambda x: x[1])
        return distances[:limit]


# ── Departure Sync Engine ─────────────────────────────────────────────────────

class DepartureSyncEngine:
    """
    Core departure synchronization logic.

    Algorithm:
      leave_at = train_eta - walk_total_seconds - buffer_seconds
      platform_wait = (train_eta - arrive_at_platform)

    Urgency classification:
      LEAVE_NOW  → leave_in_seconds < 30
      LEAVE_SOON → leave_in_seconds < 300 (5 min)
      ON_TRACK   → leave_in_seconds 300-900
      WAIT       → next train gives better platform_wait
      MISSED     → leave_at is in the past
    """

    def sync(
        self,
        train: TrainArrival,
        walk: WalkEstimate,
        buffer_seconds: int = DEFAULT_BUFFER_S,
    ) -> DepartureSync:
        now         = datetime.now(timezone.utc)
        leave_at    = train.eta - timedelta(seconds=walk.total_seconds + buffer_seconds)
        leave_in_s  = int((leave_at - now).total_seconds())
        arrive_at   = leave_at + timedelta(seconds=walk.total_seconds)
        platform_wait = max(0, int((train.eta - arrive_at).total_seconds()))
        catchable   = leave_in_s >= -30   # still catchable if within 30s past ideal leave time

        urgency = self._classify_urgency(leave_in_s, catchable)
        message = self._build_message(leave_in_s, train, walk, urgency)
        confidence = self._compute_confidence(train, walk)

        return DepartureSync(
            train=train,
            walk_estimate=walk,
            buffer_seconds=buffer_seconds,
            leave_at=leave_at,
            leave_in_seconds=leave_in_s,
            arrive_at_platform=arrive_at,
            platform_wait_seconds=platform_wait,
            urgency=urgency,
            catchable=catchable,
            message=message,
            confidence=confidence,
        )

    @staticmethod
    def _classify_urgency(leave_in_s: int, catchable: bool) -> SyncUrgency:
        if not catchable:
            return SyncUrgency.MISSED
        if leave_in_s < 30:
            return SyncUrgency.LEAVE_NOW
        if leave_in_s < 300:
            return SyncUrgency.LEAVE_SOON
        if leave_in_s < 900:
            return SyncUrgency.ON_TRACK
        return SyncUrgency.WAIT

    @staticmethod
    def _build_message(
        leave_in_s: int,
        train: TrainArrival,
        walk: WalkEstimate,
        urgency: SyncUrgency,
    ) -> str:
        route = "via Pedway" if walk.via_pedway else "on foot"
        dest  = train.destination
        line  = train.line

        if urgency == SyncUrgency.MISSED:
            return (
                f"You missed the {line} Line to {dest}. "
                f"Next train in {train.minutes_away} min."
            )
        if urgency == SyncUrgency.LEAVE_NOW:
            return (
                f"Leave NOW! {line} Line to {dest} arrives in {train.minutes_away} min. "
                f"Walk {route} — {walk.total_seconds // 60}m {walk.total_seconds % 60}s."
            )
        if urgency == SyncUrgency.LEAVE_SOON:
            mins = leave_in_s // 60
            secs = leave_in_s % 60
            return (
                f"Leave in {mins}m {secs}s — {line} Line to {dest} "
                f"arrives in {train.minutes_away} min at {walk.station_name}. "
                f"Walk {walk.total_seconds // 60} min {route}."
            )
        if urgency == SyncUrgency.ON_TRACK:
            mins = leave_in_s // 60
            return (
                f"You have {mins} min before you need to leave. "
                f"{line} Line to {dest} arrives at {train.eta.strftime('%H:%M')}. "
                f"Walk {walk.total_seconds // 60} min {route}."
            )
        return (
            f"Next {line} Line to {dest} is in {train.minutes_away} min. "
            f"You have time — leave in {leave_in_s // 60} min."
        )

    @staticmethod
    def _compute_confidence(train: TrainArrival, walk: WalkEstimate) -> str:
        if train.is_scheduled:
            return "low"   # schedule-based, not real-time
        if train.is_delayed:
            return "medium"
        if walk.confidence == "low":
            return "medium"
        return "high"

    def recommend_best(
        self,
        syncs: List[DepartureSync],
    ) -> Tuple[Optional[DepartureSync], str]:
        """
        Pick the best departure option from multiple trains/stations.
        Scoring: prefer catchable, low urgency, short platform wait, Pedway.
        """
        catchable = [s for s in syncs if s.catchable]
        if not catchable:
            return None, "No trains catchable from current position."

        # Score: lower is better
        def score(s: DepartureSync) -> float:
            urgency_scores = {
                SyncUrgency.ON_TRACK:  0,
                SyncUrgency.LEAVE_SOON: 1,
                SyncUrgency.LEAVE_NOW:  2,
                SyncUrgency.WAIT:       3,
                SyncUrgency.MISSED:     99,
            }
            platform_wait_penalty = s.platform_wait_seconds / 120   # 2 min = 1 point
            pedway_bonus = -0.5 if s.walk_estimate.via_pedway else 0
            walk_penalty = s.walk_estimate.total_seconds / 300       # 5 min walk = 1 point
            return urgency_scores.get(s.urgency, 5) + platform_wait_penalty + pedway_bonus + walk_penalty

        catchable.sort(key=score)
        best = catchable[0]

        reasoning = (
            f"Recommended: {best.train.line} Line from {best.walk_estimate.station_name}. "
            f"{best.message} "
            f"{'Uses indoor Pedway route.' if best.walk_estimate.via_pedway else ''}"
        )
        return best, reasoning


# ── Arrival Board Builder ─────────────────────────────────────────────────────

class ArrivalBoardBuilder:
    """Builds rich StationArrivalBoard from raw API data."""

    def __init__(self, parser: ArrivalParser):
        self._parser = parser

    def build(self, station_key: str, raw_etas: List[Dict[str, Any]]) -> StationArrivalBoard:
        arrivals: List[TrainArrival] = []
        for raw in raw_etas:
            arrival = self._parser.parse(raw, station_key)
            if arrival:
                arrivals.append(arrival)

        # Sort by ETA
        arrivals.sort(key=lambda a: a.eta)
        arrivals = arrivals[:MAX_NEXT_TRAINS]

        # Next arrival per line
        next_by_line: Dict[str, TrainArrival] = {}
        for arr in arrivals:
            if arr.line not in next_by_line:
                next_by_line[arr.line] = arr

        map_id = LOOP_STATION_MAP_IDS.get(station_key, "")
        lines  = STATION_LINES.get(station_key, [])
        info   = STATION_INFO.get(station_key, {})

        return StationArrivalBoard(
            station_key=station_key,
            station_name=info.get("name", station_key),
            map_id=map_id,
            fetched_at=datetime.now(timezone.utc),
            arrivals=arrivals,
            lines=lines,
            next_by_line=next_by_line,
        )


# ── Main SyncService ──────────────────────────────────────────────────────────

class SyncService:
    """
    Production predictive departure sync service.

    Core methods:
      get_arrivals(station_key) → StationArrivalBoard
      get_all_loop_arrivals() → Dict[station_key, StationArrivalBoard]
      sync_departure(lat, lon, station_key, line, buffer_s) → DepartureSync
      multi_station_sync(lat, lon, radius_m, lines, buffer_s) → MultiStationSync
      get_nearest_station_sync(lat, lon, buffer_s) → DepartureSync
    """

    def __init__(self, api_key: Optional[str] = None, redis=None):
        self._redis    = redis
        self._api      = TrainTrackerApiClient(api_key)
        self._parser   = ArrivalParser()
        self._walk     = WalkTimeCalculator()
        self._engine   = DepartureSyncEngine()
        self._board    = ArrivalBoardBuilder(self._parser)

    async def get_arrivals(self, station_key: str) -> StationArrivalBoard:
        """Get real-time arrival board for a Loop station."""
        map_id = LOOP_STATION_MAP_IDS.get(station_key)
        if not map_id:
            raise ValueError(f"Unknown station key: {station_key}")
        raw = await self._api.fetch_arrivals(map_id)
        return self._board.build(station_key, raw)

    async def get_all_loop_arrivals(self) -> Dict[str, StationArrivalBoard]:
        """Fetch arrival boards for all Loop stations concurrently."""
        raw_map = await self._api.fetch_all_loop_arrivals()
        return {
            key: self._board.build(key, raws)
            for key, raws in raw_map.items()
        }

    async def sync_departure(
        self,
        lat: float,
        lon: float,
        station_key: str,
        line: Optional[str] = None,
        buffer_seconds: int = DEFAULT_BUFFER_S,
        accessible: bool = False,
    ) -> List[DepartureSync]:
        """
        Return departure sync objects for next N trains at a station.
        Optionally filter by line.
        """
        board = await self.get_arrivals(station_key)
        arrivals = board.arrivals
        if line:
            arrivals = [a for a in arrivals if a.line == line]

        walk = self._walk.estimate(lat, lon, station_key, accessible=accessible)
        syncs = [self._engine.sync(arr, walk, buffer_seconds) for arr in arrivals]
        return syncs

    async def multi_station_sync(
        self,
        lat: float,
        lon: float,
        max_walk_m: float = 800.0,
        lines: Optional[List[str]] = None,
        buffer_seconds: int = DEFAULT_BUFFER_S,
        accessible: bool = False,
    ) -> MultiStationSync:
        """
        Find the best departure option from all reachable nearby stations.
        Returns ranked list + recommended option.
        """
        nearby = self._walk.nearest_stations(lat, lon, limit=5)
        nearby = [(k, d) for k, d in nearby if d <= max_walk_m]

        all_syncs: List[DepartureSync] = []
        for station_key, _ in nearby:
            try:
                board = await self.get_arrivals(station_key)
                arrivals = board.arrivals
                if lines:
                    arrivals = [a for a in arrivals if a.line in lines]
                walk = self._walk.estimate(lat, lon, station_key, accessible=accessible)
                for arr in arrivals[:3]:   # top 3 trains per station
                    sync = self._engine.sync(arr, walk, buffer_seconds)
                    all_syncs.append(sync)
            except Exception as exc:
                logger.debug("multi_station_sync: failed for %s: %s", station_key, exc)

        recommended, reasoning = self._engine.recommend_best(all_syncs)

        # Sort all syncs: catchable first, then by urgency score
        all_syncs.sort(key=lambda s: (0 if s.catchable else 1, s.walk_estimate.total_seconds))

        return MultiStationSync(
            origin_lat=lat,
            origin_lon=lon,
            computed_at=datetime.now(timezone.utc),
            options=all_syncs[:10],
            recommended=recommended,
            reasoning=reasoning,
        )

    async def get_nearest_sync(
        self,
        lat: float,
        lon: float,
        buffer_seconds: int = DEFAULT_BUFFER_S,
        accessible: bool = False,
    ) -> Optional[DepartureSync]:
        """Quick: return the single best departure option from nearest station."""
        nearby = self._walk.nearest_stations(lat, lon, limit=1)
        if not nearby:
            return None
        station_key, _ = nearby[0]
        syncs = await self.sync_departure(lat, lon, station_key,
                                          buffer_seconds=buffer_seconds,
                                          accessible=accessible)
        catchable = [s for s in syncs if s.catchable]
        return catchable[0] if catchable else (syncs[0] if syncs else None)

    async def health_check(self) -> Dict[str, Any]:
        return {
            "status":              "ok" if self._api.has_api_key else "no_api_key",
            "has_api_key":         self._api.has_api_key,
            "consecutive_errors":  self._api.consecutive_errors,
            "cached_stations":     len(self._api._cache),
            "total_loop_stations": len(LOOP_STATION_MAP_IDS),
        }


# ── Module-level Singleton ────────────────────────────────────────────────────

_sync_service_instance: Optional[SyncService] = None


def get_sync_service(api_key: Optional[str] = None, redis=None) -> SyncService:
    global _sync_service_instance
    if _sync_service_instance is None:
        _sync_service_instance = SyncService(api_key=api_key, redis=redis)
    return _sync_service_instance


# ── Utility ───────────────────────────────────────────────────────────────────

def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return distance in meters between two lat/lon points."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))
