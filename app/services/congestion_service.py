"""
congestion_service.py — Loop Station Congestion & Rush Hour Model
=================================================================
Estimates real-time and predicted crowding levels for Chicago Loop
CTA stations using a combination of:

1. Real CTA ridership data (Chicago Data Portal — CTA 'L' Daily Entries)
   Fetched async, refreshed every 6 hours, Redis-cached. Scale-factors
   from real daily averages calibrate the historical base scores.

2. Historical ridership patterns (by station, hour, day-of-week)
   Used as fallback when live data is unavailable or CHICAGO_APP_TOKEN
   is not configured.

3. Rush hour classification (AM/PM peaks, shoulder, off-peak)

4. Special event detection (Millennium Park, conventions, sports)

5. Weather multipliers (bad weather → more CTA riders)

6. Traffic crash hotspot data (Chicago Data Portal — Traffic Crashes)
   Crash counts and injuries within 300 m of each station produce a
   pedestrian-congestion penalty (up to ×1.20).

7. Weekday vs. weekend patterns

Output: crowding scores, alternative station suggestions, and routing
hints ("avoid Washington/Wabash at 5:30 PM, use Adams instead").

Does NOT modify any existing service file.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)


# ── Enums ─────────────────────────────────────────────────────────────────────

class CongestionLevel(str, Enum):
    LIGHT    = "light"      # < 30% capacity
    MODERATE = "moderate"   # 30–60%
    HEAVY    = "heavy"      # 60–85%
    SEVERE   = "severe"     # > 85%


class PeriodType(str, Enum):
    AM_PEAK     = "am_peak"       # 7:00–9:00
    AM_SHOULDER = "am_shoulder"   # 6:00–7:00 | 9:00–10:00
    MIDDAY      = "midday"        # 10:00–15:30
    PM_SHOULDER = "pm_shoulder"   # 15:30–16:00
    PM_PEAK     = "pm_peak"       # 16:00–19:00
    EVENING     = "evening"       # 19:00–22:00
    LATE_NIGHT  = "late_night"    # 22:00–5:00
    EARLY       = "early"         # 5:00–6:00


class EventType(str, Enum):
    CONCERT      = "concert"
    SPORTS       = "sports"
    CONVENTION   = "convention"
    FESTIVAL     = "festival"
    HOLIDAY      = "holiday"
    WEATHER_PEAK = "weather_peak"  # extreme weather → more riders


# ── Historical Ridership Model ────────────────────────────────────────────────
# Base ridership scores per station per hour (0–100 scale).
# Calibrated from CTA annual ridership reports for Loop stations.
# Scale: 0 = empty, 100 = maximum observed crowding.
#
# Key: station_key
# Value: Dict[weekday|weekend, Dict[hour_int, int_score]]

HISTORICAL_RIDERSHIP: Dict[str, Dict[str, Dict[int, int]]] = {
    "clark_lake": {
        "weekday": {
            5: 5, 6: 15, 7: 55, 8: 85, 9: 60, 10: 35, 11: 30,
            12: 40, 13: 35, 14: 30, 15: 40, 16: 65, 17: 90, 18: 80,
            19: 55, 20: 40, 21: 30, 22: 20, 23: 10,
        },
        "weekend": {
            6: 5, 7: 8, 8: 12, 9: 18, 10: 25, 11: 35, 12: 45, 13: 50,
            14: 55, 15: 55, 16: 50, 17: 55, 18: 55, 19: 50, 20: 40,
            21: 35, 22: 25, 23: 15,
        },
    },
    "state_lake": {
        "weekday": {
            5: 4, 6: 12, 7: 45, 8: 75, 9: 50, 10: 30, 11: 25,
            12: 35, 13: 30, 14: 25, 15: 35, 16: 55, 17: 80, 18: 70,
            19: 45, 20: 30, 21: 20, 22: 12, 23: 8,
        },
        "weekend": {
            6: 4, 7: 7, 8: 10, 9: 20, 10: 30, 11: 40, 12: 55, 13: 60,
            14: 65, 15: 70, 16: 65, 17: 60, 18: 55, 19: 45, 20: 35,
            21: 25, 22: 18, 23: 12,
        },
    },
    "washington_wabash": {
        "weekday": {
            5: 3, 6: 10, 7: 40, 8: 70, 9: 50, 10: 30, 11: 25,
            12: 35, 13: 30, 14: 25, 15: 35, 16: 60, 17: 85, 18: 75,
            19: 50, 20: 35, 21: 25, 22: 15, 23: 8,
        },
        "weekend": {
            6: 3, 7: 6, 8: 10, 9: 18, 10: 28, 11: 45, 12: 60, 13: 65,
            14: 70, 15: 75, 16: 70, 17: 65, 18: 55, 19: 45, 20: 35,
            21: 25, 22: 15, 23: 10,
        },
    },
    "randolph_wabash": {
        "weekday": {
            5: 3, 6: 8, 7: 35, 8: 60, 9: 40, 10: 25, 11: 20,
            12: 30, 13: 25, 14: 20, 15: 30, 16: 50, 17: 70, 18: 65,
            19: 40, 20: 30, 21: 20, 22: 12, 23: 6,
        },
        "weekend": {
            6: 3, 7: 5, 8: 8, 9: 15, 10: 22, 11: 35, 12: 45, 13: 50,
            14: 55, 15: 55, 16: 50, 17: 48, 18: 45, 19: 38, 20: 28,
            21: 20, 22: 12, 23: 8,
        },
    },
    "adams_wabash": {
        "weekday": {
            5: 2, 6: 7, 7: 30, 8: 55, 9: 40, 10: 25, 11: 22,
            12: 30, 13: 28, 14: 25, 15: 32, 16: 52, 17: 72, 18: 62,
            19: 42, 20: 30, 21: 20, 22: 12, 23: 5,
        },
        "weekend": {
            6: 2, 7: 4, 8: 8, 9: 18, 10: 30, 11: 45, 12: 55, 13: 60,
            14: 65, 15: 65, 16: 58, 17: 55, 18: 50, 19: 42, 20: 32,
            21: 22, 22: 14, 23: 8,
        },
    },
    "harold_washington_library": {
        "weekday": {
            5: 2, 6: 8, 7: 25, 8: 45, 9: 35, 10: 22, 11: 20,
            12: 28, 13: 26, 14: 22, 15: 28, 16: 45, 17: 60, 18: 52,
            19: 36, 20: 26, 21: 18, 22: 10, 23: 5,
        },
        "weekend": {
            6: 2, 7: 3, 8: 6, 9: 14, 10: 22, 11: 35, 12: 48, 13: 55,
            14: 58, 15: 55, 16: 48, 17: 45, 18: 38, 19: 30, 20: 22,
            21: 15, 22: 10, 23: 6,
        },
    },
    "washington_dearborn": {
        "weekday": {
            5: 4, 6: 12, 7: 45, 8: 75, 9: 55, 10: 32, 11: 28,
            12: 38, 13: 35, 14: 30, 15: 38, 16: 60, 17: 82, 18: 72,
            19: 48, 20: 32, 21: 22, 22: 14, 23: 8,
        },
        "weekend": {
            6: 4, 7: 7, 8: 12, 9: 20, 10: 28, 11: 38, 12: 48, 13: 52,
            14: 55, 15: 52, 16: 48, 17: 50, 18: 48, 19: 40, 20: 32,
            21: 24, 22: 16, 23: 10,
        },
    },
    "monroe_dearborn": {
        "weekday": {
            5: 3, 6: 10, 7: 38, 8: 65, 9: 48, 10: 28, 11: 24,
            12: 32, 13: 30, 14: 26, 15: 32, 16: 52, 17: 72, 18: 62,
            19: 42, 20: 30, 21: 20, 22: 12, 23: 6,
        },
        "weekend": {
            6: 3, 7: 6, 8: 10, 9: 18, 10: 28, 11: 42, 12: 55, 13: 60,
            14: 65, 15: 65, 16: 58, 17: 52, 18: 45, 19: 38, 20: 28,
            21: 20, 22: 12, 23: 8,
        },
    },
    "jackson_dearborn": {
        "weekday": {
            5: 3, 6: 9, 7: 32, 8: 55, 9: 40, 10: 24, 11: 20,
            12: 28, 13: 26, 14: 22, 15: 28, 16: 48, 17: 65, 18: 56,
            19: 38, 20: 26, 21: 18, 22: 10, 23: 5,
        },
        "weekend": {
            6: 2, 7: 4, 8: 8, 9: 15, 10: 24, 11: 35, 12: 45, 13: 50,
            14: 52, 15: 50, 16: 45, 17: 42, 18: 36, 19: 28, 20: 20,
            21: 14, 22: 9, 23: 5,
        },
    },
    "quincy_wells": {
        "weekday": {
            5: 2, 6: 6, 7: 20, 8: 38, 9: 28, 10: 18, 11: 15,
            12: 22, 13: 20, 14: 16, 15: 22, 16: 38, 17: 52, 18: 44,
            19: 30, 20: 20, 21: 14, 22: 8, 23: 4,
        },
        "weekend": {
            6: 2, 7: 3, 8: 5, 9: 10, 10: 16, 11: 24, 12: 32, 13: 36,
            14: 38, 15: 36, 16: 32, 17: 30, 18: 26, 19: 22, 20: 16,
            21: 11, 22: 7, 23: 4,
        },
    },
    "washington_wells": {
        "weekday": {
            5: 3, 6: 9, 7: 32, 8: 55, 9: 40, 10: 24, 11: 20,
            12: 28, 13: 26, 14: 22, 15: 30, 16: 50, 17: 68, 18: 58,
            19: 40, 20: 28, 21: 19, 22: 11, 23: 6,
        },
        "weekend": {
            6: 2, 7: 4, 8: 8, 9: 14, 10: 22, 11: 30, 12: 38, 13: 42,
            14: 44, 15: 42, 16: 38, 17: 36, 18: 30, 19: 24, 20: 18,
            21: 12, 22: 8, 23: 5,
        },
    },
    "madison_wabash": {
        "weekday": {
            5: 2, 6: 7, 7: 25, 8: 45, 9: 35, 10: 22, 11: 20,
            12: 28, 13: 26, 14: 22, 15: 30, 16: 48, 17: 65, 18: 56,
            19: 38, 20: 26, 21: 18, 22: 10, 23: 5,
        },
        "weekend": {
            6: 2, 7: 3, 8: 6, 9: 12, 10: 20, 11: 30, 12: 42, 13: 48,
            14: 52, 15: 50, 16: 44, 17: 42, 18: 36, 19: 28, 20: 20,
            21: 14, 22: 9, 23: 5,
        },
    },
    "lasalle_van_buren": {
        "weekday": {
            5: 2, 6: 7, 7: 24, 8: 42, 9: 32, 10: 20, 11: 17,
            12: 24, 13: 22, 14: 18, 15: 24, 16: 40, 17: 55, 18: 47,
            19: 32, 20: 22, 21: 15, 22: 8, 23: 4,
        },
        "weekend": {
            6: 2, 7: 3, 8: 5, 9: 10, 10: 16, 11: 24, 12: 32, 13: 36,
            14: 38, 15: 36, 16: 32, 17: 30, 18: 26, 19: 20, 20: 14,
            21: 10, 22: 6, 23: 3,
        },
    },
}

# Default for stations not in historical data
DEFAULT_RIDERSHIP: Dict[str, Dict[int, int]] = {
    "weekday": {h: max(5, 30 - abs(h - 8) * 3) for h in range(24)},
    "weekend": {h: max(3, 20 - abs(h - 13) * 2) for h in range(24)},
}


# ── Real-Data Integration Constants ──────────────────────────────────────────
# Chicago Data Portal (Socrata) dataset IDs

# CTA - Ridership - 'L' Station Entries - Daily Totals
CTA_RIDERSHIP_DATASET_ID = "5neh-572f"

# Traffic Crashes - Crashes (Chicago citywide)
TRAFFIC_CRASHES_DATASET_ID = "85ca-t3if"

# Chicago Pedway network geometry
PEDWAY_DATASET_ID = "xkur-4g6u"

# CTA Fare Media Sales Outlets (Ventra reload locations)
FARE_OUTLETS_DATASET_ID = "ag7u-gr9m"

# Loop bounding box for crash spatial queries
LOOP_BOUNDS: Dict[str, float] = {
    "lat_min": 41.873,
    "lat_max": 41.892,
    "lon_min": -87.641,
    "lon_max": -87.621,
}

# CTA dataset stationname → our station key
# Source: CTA open data station names (verbatim from dataset)
CTA_STATION_NAME_MAP: Dict[str, str] = {
    "Clark/Lake":                                    "clark_lake",
    "State/Lake":                                    "state_lake",
    "Washington/Wabash":                             "washington_wabash",
    "Randolph/Wabash":                               "randolph_wabash",
    "Adams/Wabash":                                  "adams_wabash",
    "Harold Washington Library-State/Van Buren":     "harold_washington_library",
    "Washington/Dearborn":                           "washington_dearborn",
    "Monroe/Dearborn":                               "monroe_dearborn",
    "Jackson/Dearborn":                              "jackson_dearborn",
    "Quincy/Wells":                                  "quincy_wells",
    "Washington/Wells":                              "washington_wells",
    "Madison/Wabash":                                "madison_wabash",
    "LaSalle/Van Buren":                             "lasalle_van_buren",
}

# Calibration baselines: expected avg daily ridership (rides/day) that
# corresponds to the historical model's "peak 100 score" at each station.
# Used to compute: scale_factor = real_avg_rides / baseline
RIDERSHIP_CALIBRATION_BASELINE: Dict[str, Dict[str, float]] = {
    "clark_lake":                  {"W": 10_000, "A": 6_500, "U": 5_000},
    "state_lake":                  {"W":  8_500, "A": 5_500, "U": 4_200},
    "washington_wabash":           {"W":  8_000, "A": 5_200, "U": 4_000},
    "randolph_wabash":             {"W":  7_000, "A": 4_500, "U": 3_500},
    "adams_wabash":                {"W":  7_500, "A": 4_800, "U": 3_700},
    "harold_washington_library":   {"W":  5_500, "A": 3_800, "U": 3_000},
    "washington_dearborn":         {"W":  8_000, "A": 5_000, "U": 3_800},
    "monroe_dearborn":             {"W":  7_000, "A": 4_500, "U": 3_500},
    "jackson_dearborn":            {"W":  6_500, "A": 4_200, "U": 3_200},
    "quincy_wells":                {"W":  5_000, "A": 3_200, "U": 2_500},
    "washington_wells":            {"W":  6_000, "A": 3_800, "U": 2_900},
    "madison_wabash":              {"W":  5_500, "A": 3_500, "U": 2_700},
    "lasalle_van_buren":           {"W":  5_000, "A": 3_200, "U": 2_400},
}

# Station lat/lon for crash proximity calculations (WGS84)
STATION_COORDS: Dict[str, Tuple[float, float]] = {
    "clark_lake":                (41.8858, -87.6317),
    "state_lake":                (41.8858, -87.6280),
    "washington_wabash":         (41.8829, -87.6262),
    "randolph_wabash":           (41.8845, -87.6262),
    "adams_wabash":              (41.8793, -87.6261),
    "harold_washington_library": (41.8766, -87.6280),
    "washington_dearborn":       (41.8833, -87.6295),
    "monroe_dearborn":           (41.8810, -87.6295),
    "jackson_dearborn":          (41.8784, -87.6295),
    "quincy_wells":              (41.8787, -87.6383),
    "washington_wells":          (41.8833, -87.6383),
    "madison_wabash":            (41.8815, -87.6262),
    "lasalle_van_buren":         (41.8759, -87.6317),
}

# Radius (metres) for crash proximity scan
CRASH_PROXIMITY_M = 300


# ── Special Event Multipliers ─────────────────────────────────────────────────
# Events affect specific stations at specific times
# Format: {month_day: (event_name, affected_stations, hours, multiplier)}

KNOWN_ANNUAL_EVENTS: List[Dict[str, Any]] = [
    {
        "name": "Chicago Jazz Festival (Labor Day Weekend)",
        "months": [9],
        "days_of_week": [5, 6],  # Sat, Sun
        "hours": list(range(11, 22)),
        "affected_stations": ["state_lake", "washington_wabash", "randolph_wabash"],
        "multiplier": 1.45,
        "type": EventType.FESTIVAL,
    },
    {
        "name": "Lollapalooza (Grant Park - early August)",
        "months": [8],
        "days_of_week": [3, 4, 5, 6],  # Thu–Sun
        "hours": list(range(11, 23)),
        "affected_stations": ["adams_wabash", "washington_wabash", "state_lake"],
        "multiplier": 1.65,
        "type": EventType.FESTIVAL,
    },
    {
        "name": "Chicago Marathon (Columbus Day - second Sunday October)",
        "months": [10],
        "days_of_week": [6],  # Sunday
        "hours": list(range(6, 16)),
        "affected_stations": ["clark_lake", "state_lake", "washington_dearborn"],
        "multiplier": 1.50,
        "type": EventType.SPORTS,
    },
    {
        "name": "Taste of Chicago (4th of July week)",
        "months": [7],
        "days_of_week": [3, 4, 5, 6, 0],
        "hours": list(range(10, 22)),
        "affected_stations": ["adams_wabash", "state_lake", "washington_wabash"],
        "multiplier": 1.40,
        "type": EventType.FESTIVAL,
    },
    {
        "name": "Chicago Auto Show (February)",
        "months": [2],
        "days_of_week": list(range(7)),
        "hours": list(range(9, 21)),
        "affected_stations": ["washington_wabash", "state_lake"],
        "multiplier": 1.25,
        "type": EventType.CONVENTION,
    },
    {
        "name": "St. Patrick's Day (March 17)",
        "months": [3],
        "days": [17],
        "hours": list(range(10, 22)),
        "affected_stations": ["clark_lake", "state_lake", "washington_dearborn"],
        "multiplier": 1.55,
        "type": EventType.HOLIDAY,
    },
]


# ── Weather Multipliers ───────────────────────────────────────────────────────
# More CTA riders when weather is bad (they avoid walking)
WEATHER_RIDERSHIP_MULTIPLIERS: Dict[str, float] = {
    "blizzard":      1.35,
    "heavy_rain":    1.25,
    "freezing_rain": 1.30,
    "extreme_cold":  1.20,   # < 0°F
    "heavy_snow":    1.28,
    "normal":        1.00,
    "nice":          0.90,   # great weather → more walkers
}


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class RidershipRecord:
    """Real ridership data record from Chicago Data Portal."""
    station_key:     str
    daytype:         str    # "W" (weekday) / "A" (Saturday) / "U" (Sunday/holiday)
    avg_daily_rides: float  # average rides per day over lookback window
    scale_factor:    float  # avg_daily_rides / calibration_baseline (clamped 0.5–2.0)
    data_date_range: str    # e.g., "Last 90 days"


@dataclass
class CrashHotspot:
    """Traffic crash density near a Loop station (last 30 days)."""
    station_key:        str
    crash_count_30d:    int
    injury_count_30d:   int
    congestion_penalty: float  # 1.00–1.20 multiplier applied to adjusted_score


@dataclass
class CongestionPrediction:
    station_key:      str
    station_name:     str
    timestamp:        datetime
    period_type:      PeriodType
    base_score:       int                 # 0–100 raw historical
    adjusted_score:   float               # with all multipliers
    congestion_level: CongestionLevel
    percent_capacity: float               # 0.0–1.0
    is_peak:          bool
    weather_factor:   float
    event_factor:     float
    day_type:         str                 # "weekday" | "weekend"
    recommendation:   str
    alternative_stations: List[str]
    next_lighter_period: Optional[str]    # e.g. "In 20 minutes (after 6PM rush)"


@dataclass
class NetworkCongestionSnapshot:
    timestamp:         datetime
    station_scores:    Dict[str, CongestionPrediction]
    most_congested:    List[str]          # top 3 station keys
    least_congested:   List[str]          # bottom 3 station keys
    network_avg_score: float
    period_type:       PeriodType
    active_events:     List[str]
    routing_advice:    str


@dataclass
class CongestionTrend:
    station_key:    str
    current_score:  float
    in_30min_score: float
    in_60min_score: float
    trend:          str              # "increasing" | "decreasing" | "stable"
    peak_eta:       Optional[str]   # "In 25 min" if peak approaching


@dataclass
class AlternativeStation:
    station_key:   str
    station_name:  str
    congestion:    CongestionLevel
    score:         float
    extra_walk_m:  float
    time_saved_s:  int              # time saved vs. crowded station (boarding faster)
    reason:        str


# ── Rush Hour Classifier ──────────────────────────────────────────────────────

class RushHourClassifier:
    """Classifies a datetime into a period type."""

    PERIOD_DEFINITIONS: List[Tuple[int, int, PeriodType]] = [
        (0,  5,  PeriodType.LATE_NIGHT),
        (5,  6,  PeriodType.EARLY),
        (6,  7,  PeriodType.AM_SHOULDER),
        (7,  9,  PeriodType.AM_PEAK),
        (9,  10, PeriodType.AM_SHOULDER),
        (10, 16, PeriodType.MIDDAY),
        (16, 19, PeriodType.PM_PEAK),
        (19, 22, PeriodType.EVENING),
        (22, 24, PeriodType.LATE_NIGHT),
    ]

    def classify(self, dt: datetime) -> PeriodType:
        # Convert to Chicago time (approximate)
        chicago_dt = dt + timedelta(hours=-6)
        hour = chicago_dt.hour
        for start, end, period in self.PERIOD_DEFINITIONS:
            if start <= hour < end:
                return period
        return PeriodType.LATE_NIGHT

    def is_peak(self, period: PeriodType) -> bool:
        return period in (PeriodType.AM_PEAK, PeriodType.PM_PEAK)

    def minutes_to_next_period_change(self, dt: datetime) -> int:
        """Return minutes until the next period boundary."""
        chicago_dt = dt + timedelta(hours=-6)
        hour = chicago_dt.hour
        minute = chicago_dt.minute
        for start, end, _ in self.PERIOD_DEFINITIONS:
            if start <= hour < end:
                remaining_mins = (end - hour) * 60 - minute
                return max(1, remaining_mins)
        return 60


# ── Special Event Detector ────────────────────────────────────────────────────

class SpecialEventDetector:
    """
    Detects active special events and returns their ridership multipliers.
    Uses KNOWN_ANNUAL_EVENTS list; extensible via Redis-stored custom events.
    """

    def get_active_events(
        self,
        dt: datetime,
        station_key: str,
    ) -> List[Tuple[str, float]]:
        """Return [(event_name, multiplier)] for active events affecting station."""
        chicago_dt = dt + timedelta(hours=-6)
        month  = chicago_dt.month
        day    = chicago_dt.day
        hour   = chicago_dt.hour
        dow    = chicago_dt.weekday()  # 0=Mon, 6=Sun

        active = []
        for event in KNOWN_ANNUAL_EVENTS:
            if month not in event.get("months", []):
                continue
            if "days" in event and day not in event["days"]:
                continue
            if "days_of_week" in event and dow not in event["days_of_week"]:
                continue
            if hour not in event.get("hours", list(range(24))):
                continue
            if station_key in event.get("affected_stations", []):
                active.append((event["name"], event["multiplier"]))

        return active


# ── Congestion Model ──────────────────────────────────────────────────────────

class CongestionModel:
    """
    Blends historical base scores with event multipliers, weather multipliers,
    and day-type adjustments to produce final congestion predictions.
    """

    def __init__(self):
        self._classifier = RushHourClassifier()
        self._events     = SpecialEventDetector()

    def predict(
        self,
        station_key: str,
        station_name: str,
        dt: Optional[datetime] = None,
        weather_condition: str = "normal",
    ) -> CongestionPrediction:
        dt = dt or datetime.now(timezone.utc)
        chicago_dt = dt + timedelta(hours=-6)
        hour       = chicago_dt.hour
        is_weekend = chicago_dt.weekday() >= 5
        day_type   = "weekend" if is_weekend else "weekday"
        period     = self._classifier.classify(dt)
        is_peak    = self._classifier.is_peak(period)

        # Base score from historical data
        ridership_data = HISTORICAL_RIDERSHIP.get(station_key, DEFAULT_RIDERSHIP)
        day_data       = ridership_data.get(day_type, ridership_data.get("weekday", {}))
        base_score     = day_data.get(hour, 10)

        # Event multiplier (take highest if multiple events)
        events         = self._events.get_active_events(dt, station_key)
        event_factor   = max((m for _, m in events), default=1.0)
        event_names    = [name for name, _ in events]

        # Weather multiplier
        weather_factor = WEATHER_RIDERSHIP_MULTIPLIERS.get(weather_condition, 1.0)

        # Day-type fine-tune
        day_factor = 0.75 if is_weekend else 1.0

        adjusted = base_score * event_factor * weather_factor * day_factor
        adjusted = min(100.0, max(0.0, adjusted))

        level  = self._score_to_level(adjusted)
        pct    = round(adjusted / 100, 3)
        rec, alts, next_lighter = self._build_recommendation(
            station_key, adjusted, level, period, day_type, chicago_dt
        )

        return CongestionPrediction(
            station_key=station_key,
            station_name=station_name,
            timestamp=dt,
            period_type=period,
            base_score=base_score,
            adjusted_score=round(adjusted, 1),
            congestion_level=level,
            percent_capacity=pct,
            is_peak=is_peak,
            weather_factor=weather_factor,
            event_factor=event_factor,
            day_type=day_type,
            recommendation=rec,
            alternative_stations=alts,
            next_lighter_period=next_lighter,
        )

    def predict_trend(
        self,
        station_key: str,
        station_name: str,
        weather_condition: str = "normal",
    ) -> CongestionTrend:
        """Predict congestion now, +30min, +60min."""
        now   = datetime.now(timezone.utc)
        p0    = self.predict(station_key, station_name, now, weather_condition)
        p30   = self.predict(station_key, station_name, now + timedelta(minutes=30), weather_condition)
        p60   = self.predict(station_key, station_name, now + timedelta(minutes=60), weather_condition)

        delta = p30.adjusted_score - p0.adjusted_score
        if delta > 8:
            trend = "increasing"
        elif delta < -8:
            trend = "decreasing"
        else:
            trend = "stable"

        # Estimate when peak hits (if increasing)
        peak_eta = None
        if trend == "increasing" and p30.adjusted_score > 70:
            peak_eta = "Peak in ~30 minutes"
        elif trend == "decreasing" and p0.adjusted_score > 70:
            mins = self._classifier.minutes_to_next_period_change(now)
            peak_eta = f"Easing in ~{mins} minutes"

        return CongestionTrend(
            station_key=station_key,
            current_score=p0.adjusted_score,
            in_30min_score=p30.adjusted_score,
            in_60min_score=p60.adjusted_score,
            trend=trend,
            peak_eta=peak_eta,
        )

    @staticmethod
    def _score_to_level(score: float) -> CongestionLevel:
        if score < 30:
            return CongestionLevel.LIGHT
        elif score < 60:
            return CongestionLevel.MODERATE
        elif score < 85:
            return CongestionLevel.HEAVY
        else:
            return CongestionLevel.SEVERE

    def _build_recommendation(
        self,
        station_key: str,
        score: float,
        level: CongestionLevel,
        period: PeriodType,
        day_type: str,
        local_dt: datetime,
    ) -> Tuple[str, List[str], Optional[str]]:
        # Alternatives: find less-crowded nearby stations
        alts = self._find_alternatives(station_key, local_dt, day_type)
        mins_to_change = self._classifier.minutes_to_next_period_change(
            local_dt.replace(tzinfo=timezone.utc)
        )

        if level == CongestionLevel.LIGHT:
            rec = "Light traffic — good time to travel."
            next_lighter = None
        elif level == CongestionLevel.MODERATE:
            rec = "Moderate crowding. Platform wait is typical."
            next_lighter = None
        elif level == CongestionLevel.HEAVY:
            alt_names = ", ".join(alts[:2]) if alts else "nearby stations"
            rec = (
                f"Heavy crowding. Consider {alt_names} for a less crowded platform. "
                f"Easing in approximately {mins_to_change} minutes."
            )
            next_lighter = f"In ~{mins_to_change} minutes"
        else:  # SEVERE
            alt_names = ", ".join(alts[:2]) if alts else "nearby stations"
            rec = (
                f"SEVERE crowding — multiple trains may pass without boarding. "
                f"Strongly recommend {alt_names} or wait {mins_to_change} min."
            )
            next_lighter = f"In ~{mins_to_change} minutes"

        return rec, alts, next_lighter

    @staticmethod
    def _find_alternatives(
        station_key: str, local_dt: datetime, day_type: str
    ) -> List[str]:
        """Return station keys with lower predicted congestion nearby."""
        hour = local_dt.hour
        alternatives: List[Tuple[float, str]] = []
        for key, data in HISTORICAL_RIDERSHIP.items():
            if key == station_key:
                continue
            day_data = data.get(day_type, data.get("weekday", {}))
            score = day_data.get(hour, 10)
            alternatives.append((score, key))
        alternatives.sort(key=lambda x: x[0])
        return [key for _, key in alternatives[:3]]


# ── Chicago Data Portal Client ────────────────────────────────────────────────

class ChicagoDataPortalClient:
    """
    Async HTTP client for the Chicago Data Portal (Socrata) API.
    Supports optional X-App-Token header for higher rate limits.

    v1 (SoQL): https://data.cityofchicago.org/resource/{dataset_id}.json
    v3 GeoJSON: https://data.cityofchicago.org/api/v3/views/{dataset_id}/query.geojson
    v3 JSON:    https://data.cityofchicago.org/api/v3/views/{dataset_id}/query.json
    """

    BASE_URL        = "https://data.cityofchicago.org/resource"
    V3_BASE_URL     = "https://data.cityofchicago.org/api/v3/views"
    TIMEOUT         = 25.0
    MAX_RETRY       = 3

    def __init__(self, app_token: str = ""):
        self._token = app_token

    async def get(
        self,
        dataset_id: str,
        params: Dict[str, str],
        limit: int = 50_000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch rows from a Socrata dataset.
        Returns a list of row dicts on success, empty list on error.
        """
        url = f"{self.BASE_URL}/{dataset_id}.json"
        headers: Dict[str, str] = {"Accept": "application/json"}
        if self._token:
            headers["X-App-Token"] = self._token

        query_params = {**params, "$limit": str(limit)}

        for attempt in range(self.MAX_RETRY):
            try:
                async with httpx.AsyncClient(timeout=self.TIMEOUT) as client:
                    resp = await client.get(url, params=query_params, headers=headers)
                    resp.raise_for_status()
                    data = resp.json()
                    if isinstance(data, list):
                        return data
                    logger.warning(
                        "ChicagoDataPortalClient: unexpected response type for %s", dataset_id
                    )
                    return []
            except httpx.HTTPStatusError as exc:
                logger.warning(
                    "ChicagoDataPortalClient: HTTP %d for dataset %s (attempt %d/%d)",
                    exc.response.status_code, dataset_id, attempt + 1, self.MAX_RETRY,
                )
            except httpx.TimeoutException:
                logger.warning(
                    "ChicagoDataPortalClient: timeout for dataset %s (attempt %d/%d)",
                    dataset_id, attempt + 1, self.MAX_RETRY,
                )
            except Exception as exc:
                logger.warning(
                    "ChicagoDataPortalClient: error for dataset %s (attempt %d/%d): %s",
                    dataset_id, attempt + 1, self.MAX_RETRY, exc,
                )

            if attempt < self.MAX_RETRY - 1:
                await asyncio.sleep(2 ** attempt)

        return []

    async def get_geojson(
        self,
        dataset_id: str,
        params: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch GeoJSON features from Chicago Data Portal v3 API.
        URL: /api/v3/views/{dataset_id}/query.geojson
        Returns a list of GeoJSON feature dicts: {type, geometry, properties}.
        Geometry coordinates are [lon, lat] (GeoJSON standard).
        """
        url     = f"{self.V3_BASE_URL}/{dataset_id}/query.geojson"
        headers: Dict[str, str] = {"Accept": "application/geo+json"}
        if self._token:
            headers["X-App-Token"] = self._token

        query_params: Dict[str, str] = dict(params or {})

        for attempt in range(self.MAX_RETRY):
            try:
                async with httpx.AsyncClient(timeout=self.TIMEOUT) as client:
                    resp = await client.get(url, params=query_params, headers=headers)
                    resp.raise_for_status()
                    data = resp.json()
                    features = data.get("features", [])
                    logger.debug(
                        "ChicagoDataPortalClient.get_geojson: %d features from %s",
                        len(features), dataset_id,
                    )
                    return features
            except httpx.HTTPStatusError as exc:
                logger.warning(
                    "ChicagoDataPortalClient.get_geojson: HTTP %d for %s (attempt %d/%d)",
                    exc.response.status_code, dataset_id, attempt + 1, self.MAX_RETRY,
                )
            except httpx.TimeoutException:
                logger.warning(
                    "ChicagoDataPortalClient.get_geojson: timeout for %s (attempt %d/%d)",
                    dataset_id, attempt + 1, self.MAX_RETRY,
                )
            except Exception as exc:
                logger.warning(
                    "ChicagoDataPortalClient.get_geojson: error for %s (attempt %d/%d): %s",
                    dataset_id, attempt + 1, self.MAX_RETRY, exc,
                )

            if attempt < self.MAX_RETRY - 1:
                await asyncio.sleep(2 ** attempt)

        return []


# ── CTA Ridership Fetcher ─────────────────────────────────────────────────────

class CTARidershipFetcher:
    """
    Fetches CTA 'L' Station Daily Ridership data from Chicago Data Portal.
    Dataset: CTA - Ridership - 'L' Station Entries - Daily Totals (5neh-572f)

    Computes a scale_factor per station per day-type (W/A/U) over a 90-day
    rolling window. Scale factor = real avg daily rides / calibration baseline.

    The scale factor is later blended into CongestionModel predictions to
    calibrate historical base scores against current actual ridership volumes.
    """

    LOOKBACK_DAYS = 180  # dataset lags ~3 months; use 6-month window to ensure coverage

    def __init__(self, client: ChicagoDataPortalClient):
        self._client = client

    async def fetch(self) -> Dict[str, Dict[str, RidershipRecord]]:
        """
        Fetch and return: {station_key: {"W": RidershipRecord, "A": ..., "U": ...}}
        Returns empty dict on failure.
        """
        cutoff     = datetime.now(timezone.utc) - timedelta(days=self.LOOKBACK_DAYS)
        cutoff_str = cutoff.strftime("%Y-%m-%dT00:00:00.000")

        station_names  = list(CTA_STATION_NAME_MAP.keys())
        names_in_clause = ", ".join(f"'{n}'" for n in station_names)

        params = {
            "$select": "stationname, daytype, avg(rides) as avg_rides",
            "$where":  (
                f"date > '{cutoff_str}'"
                f" AND stationname IN ({names_in_clause})"
            ),
            "$group": "stationname, daytype",
        }

        rows = await self._client.get(CTA_RIDERSHIP_DATASET_ID, params, limit=500)
        if not rows:
            logger.warning("CTARidershipFetcher: no data returned from dataset.")
            return {}

        result: Dict[str, Dict[str, RidershipRecord]] = {}
        date_range_label = f"Last {self.LOOKBACK_DAYS} days"

        for row in rows:
            station_name = row.get("stationname", "")
            station_key  = CTA_STATION_NAME_MAP.get(station_name)
            if not station_key:
                continue

            daytype = row.get("daytype", "W")  # W / A / U
            try:
                avg_rides = float(row.get("avg_rides", 0) or 0)
            except (ValueError, TypeError):
                continue

            # Compute scale factor vs. calibration baseline
            baseline_val = (
                RIDERSHIP_CALIBRATION_BASELINE
                .get(station_key, {})
                .get(daytype, 5_000.0)
            )
            scale_factor = avg_rides / baseline_val if baseline_val > 0 else 1.0
            # Clamp to a sane range — extreme outliers get floored/capped
            scale_factor = max(0.5, min(2.0, round(scale_factor, 4)))

            rec = RidershipRecord(
                station_key=station_key,
                daytype=daytype,
                avg_daily_rides=round(avg_rides, 1),
                scale_factor=scale_factor,
                data_date_range=date_range_label,
            )
            if station_key not in result:
                result[station_key] = {}
            result[station_key][daytype] = rec

        logger.info(
            "CTARidershipFetcher: loaded ridership for %d stations (%d rows).",
            len(result), len(rows),
        )
        return result


# ── Traffic Crash Fetcher ─────────────────────────────────────────────────────

class TrafficCrashFetcher:
    """
    Fetches recent traffic crash data for the Loop from Chicago Data Portal.
    Dataset: Traffic Crashes - Crashes (85ca-t3if)

    Counts crashes and injuries within CRASH_PROXIMITY_M metres of each Loop
    station over the last 30 days. Outputs a congestion_penalty (1.00–1.20)
    that represents increased street-level pedestrian friction around the
    station entrance.

    Penalty formula:
        base 1.0 + (crash_count × 0.02) + (injury_count × 0.01), capped 1.20
    """

    LOOKBACK_DAYS = 30

    def __init__(self, client: ChicagoDataPortalClient):
        self._client = client

    @staticmethod
    def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Return distance in metres between two WGS84 points."""
        R = 6_371_000.0
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi    = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = (
            math.sin(dphi / 2) ** 2
            + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
        )
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    async def fetch(self) -> Dict[str, CrashHotspot]:
        """
        Returns: {station_key: CrashHotspot}
        Fetches from Chicago Data Portal v3 GeoJSON API.
        Coordinates come from feature.geometry (GeoJSON [lon, lat]),
        properties carry crash_date and injuries_total.
        """
        cutoff     = datetime.now(timezone.utc) - timedelta(days=self.LOOKBACK_DAYS)
        cutoff_str = cutoff.strftime("%Y-%m-%dT00:00:00.000")

        params = {
            "$where": (
                f"crash_date > '{cutoff_str}'"
                f" AND latitude  BETWEEN {LOOP_BOUNDS['lat_min']} AND {LOOP_BOUNDS['lat_max']}"
                f" AND longitude BETWEEN {LOOP_BOUNDS['lon_min']} AND {LOOP_BOUNDS['lon_max']}"
                " AND latitude IS NOT NULL AND longitude IS NOT NULL"
            ),
            "$select": "crash_date, latitude, longitude, injuries_total",
            "$limit":  "5000",
        }

        features = await self._client.get_geojson(TRAFFIC_CRASHES_DATASET_ID, params)
        if not features:
            logger.info("TrafficCrashFetcher: no crash data returned (token may not be set).")
            return {}

        # Accumulate per station
        crash_counts:  Dict[str, int] = {k: 0 for k in STATION_COORDS}
        injury_counts: Dict[str, int] = {k: 0 for k in STATION_COORDS}

        for feat in features:
            try:
                geom   = feat.get("geometry") or {}
                coords = geom.get("coordinates") or []
                props  = feat.get("properties") or {}

                # GeoJSON coordinates are [longitude, latitude]
                if coords and len(coords) >= 2 and coords[0] is not None:
                    c_lon, c_lat = float(coords[0]), float(coords[1])
                else:
                    # Fallback: properties may still carry lat/lon
                    c_lat = float(props.get("latitude",  0) or 0)
                    c_lon = float(props.get("longitude", 0) or 0)

                injuries = int(float(props.get("injuries_total", 0) or 0))
            except (ValueError, TypeError):
                continue

            if c_lat == 0.0 and c_lon == 0.0:
                continue

            for station_key, (s_lat, s_lon) in STATION_COORDS.items():
                dist = self._haversine_m(c_lat, c_lon, s_lat, s_lon)
                if dist <= CRASH_PROXIMITY_M:
                    crash_counts[station_key]  += 1
                    injury_counts[station_key] += injuries

        result: Dict[str, CrashHotspot] = {}
        for station_key in STATION_COORDS:
            crashes  = crash_counts[station_key]
            injuries = injury_counts[station_key]
            penalty  = 1.0 + (crashes * 0.02) + (injuries * 0.01)
            penalty  = min(1.20, round(penalty, 3))
            result[station_key] = CrashHotspot(
                station_key=station_key,
                crash_count_30d=crashes,
                injury_count_30d=injuries,
                congestion_penalty=penalty,
            )

        logger.info(
            "TrafficCrashFetcher: processed %d crashes in Loop bounding box.", len(features)
        )
        return result


# ── Fare Outlet Fetcher ───────────────────────────────────────────────────────

@dataclass
class FareOutlet:
    """A single CTA Ventra fare media sales outlet."""
    name:        str
    address:     str
    lat:         float
    lon:         float
    outlet_type: str        # e.g. "Currency Exchange", "Pharmacy", "CTA Vending"
    zip_code:    str = ""


class FareOutletFetcher:
    """
    Fetches CTA fare media sales outlet locations from Chicago Data Portal.
    Dataset: CTA Fare Media Sales Outlets (ag7u-gr9m)
    URL: https://data.cityofchicago.org/api/v3/views/ag7u-gr9m/query.json

    Returns nearby Ventra card reload locations — useful for route planning
    when a commuter needs to add funds before boarding.
    """

    DATASET_ID = FARE_OUTLETS_DATASET_ID

    def __init__(self, client: "ChicagoDataPortalClient"):
        self._client = client

    async def fetch(self) -> List[FareOutlet]:
        """
        Fetch all fare outlet locations.  Returns an empty list on error.
        The v3 query.geojson endpoint provides geometry + properties per feature.
        """
        features = await self._client.get_geojson(self.DATASET_ID)
        outlets: List[FareOutlet] = []

        for feat in features:
            try:
                props  = feat.get("properties") or {}
                geom   = feat.get("geometry") or {}
                coords = geom.get("coordinates") or []

                if not coords or len(coords) < 2:
                    continue

                c_lon, c_lat = float(coords[0]), float(coords[1])
                outlets.append(FareOutlet(
                    name        = str(props.get("business_name") or props.get("name") or "Unknown"),
                    address     = str(props.get("address") or ""),
                    lat         = c_lat,
                    lon         = c_lon,
                    outlet_type = str(props.get("outlet_type") or props.get("type") or "Retail"),
                    zip_code    = str(props.get("zip_code") or props.get("zip") or ""),
                ))
            except (ValueError, TypeError, KeyError):
                continue

        logger.info("FareOutletFetcher: loaded %d outlets.", len(outlets))
        return outlets

    @staticmethod
    def nearest(
        outlets: List[FareOutlet],
        lat: float,
        lon: float,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """Return up to `limit` outlets nearest to (lat, lon), with distance_m."""
        def haversine(o: FareOutlet) -> float:
            R = 6_371_000.0
            phi1, phi2 = math.radians(lat), math.radians(o.lat)
            dphi    = math.radians(o.lat - lat)
            dlambda = math.radians(o.lon - lon)
            a = (
                math.sin(dphi / 2) ** 2
                + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
            )
            return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        ranked = sorted(outlets, key=haversine)[:limit]
        return [
            {
                "name":        o.name,
                "address":     o.address,
                "outlet_type": o.outlet_type,
                "lat":         o.lat,
                "lon":         o.lon,
                "zip_code":    o.zip_code,
                "distance_m":  round(haversine(o), 1),
            }
            for o in ranked
        ]


# ── Real-Time Data Cache ──────────────────────────────────────────────────────

class RealTimeDataCache:
    """
    Redis-backed cache for real-time ridership scale factors and crash hotspot
    data. Falls back silently when Redis is unavailable.
    """

    RIDERSHIP_KEY = "congestion:ridership_data"
    CRASH_KEY     = "congestion:crash_data"
    RIDERSHIP_TTL = 6 * 3600   # 6 hours — refreshed by background task
    CRASH_TTL     = 1 * 3600   # 1 hour  — refreshed by background task

    def __init__(self, redis: Optional[Any]):
        self._redis = redis

    async def store_ridership(self, data: Dict) -> None:
        if not self._redis:
            return
        try:
            await self._redis.set(
                self.RIDERSHIP_KEY, json.dumps(data), ex=self.RIDERSHIP_TTL
            )
        except Exception as exc:
            logger.debug("RealTimeDataCache.store_ridership failed: %s", exc)

    async def get_ridership(self) -> Optional[Dict]:
        if not self._redis:
            return None
        try:
            raw = await self._redis.get(self.RIDERSHIP_KEY)
            return json.loads(raw) if raw else None
        except Exception:
            return None

    async def store_crashes(self, data: Dict) -> None:
        if not self._redis:
            return
        try:
            await self._redis.set(
                self.CRASH_KEY, json.dumps(data), ex=self.CRASH_TTL
            )
        except Exception as exc:
            logger.debug("RealTimeDataCache.store_crashes failed: %s", exc)

    async def get_crashes(self) -> Optional[Dict]:
        if not self._redis:
            return None
        try:
            raw = await self._redis.get(self.CRASH_KEY)
            return json.loads(raw) if raw else None
        except Exception:
            return None


# ── Main CongestionService ────────────────────────────────────────────────────

class CongestionService:
    """
    Production Loop station congestion and rush hour model.

    Key methods:
      predict(station_key, dt, weather) → CongestionPrediction
      predict_all(dt, weather) → NetworkCongestionSnapshot
      predict_trend(station_key, weather) → CongestionTrend
      get_routing_advice(station_key, dt) → routing hints
      get_best_time_to_travel(station_key) → {hour: score} for today
      initialize(redis)   → fetch real data, start background refresh
      shutdown()          → cancel background refresh task
      health_check()      → service status
    """

    # Background refresh intervals
    _RIDERSHIP_REFRESH_S = 6 * 3600   # 6 hours
    _CRASH_REFRESH_S     = 1 * 3600   # 1 hour

    def __init__(self, redis: Optional[Any] = None):
        self._model      = CongestionModel()
        self._classifier = RushHourClassifier()

        # ── Real-data layer ───────────────────────────────────────────────────
        self._redis = redis

        # Load CHICAGO_APP_TOKEN from settings (graceful if settings not ready)
        self._token: str = ""
        try:
            from app.config import settings  # noqa: PLC0415
            self._token = settings.CHICAGO_APP_TOKEN or ""
        except Exception:
            pass

        _portal_client = ChicagoDataPortalClient(app_token=self._token)
        self._data_cache         = RealTimeDataCache(redis) if redis else None
        self._ridership_fetcher  = CTARidershipFetcher(_portal_client)
        self._crash_fetcher      = TrafficCrashFetcher(_portal_client)

        # In-memory real-data stores
        self._ridership_data: Dict[str, Dict[str, RidershipRecord]] = {}
        self._crash_data:     Dict[str, CrashHotspot]               = {}

        # Background task handle
        self._bg_task:    Optional[asyncio.Task] = None
        self._initialized: bool                  = False

        # ── Station names (avoid circular import from station_service) ────────
        self._station_names: Dict[str, str] = {
            "clark_lake": "Clark/Lake",
            "state_lake": "State/Lake",
            "randolph_wabash": "Randolph/Wabash",
            "washington_wabash": "Washington/Wabash",
            "madison_wabash": "Madison/Wabash",
            "adams_wabash": "Adams/Wabash",
            "harold_washington_library": "Harold Washington Library",
            "lasalle_van_buren": "LaSalle/Van Buren",
            "quincy_wells": "Quincy/Wells",
            "washington_wells": "Washington/Wells",
            "washington_dearborn": "Washington/Dearborn (Blue)",
            "monroe_dearborn": "Monroe/Dearborn (Blue)",
            "jackson_dearborn": "Jackson/Dearborn (Blue)",
            "monroe_state": "Monroe/State (Red)",
            "jackson_state": "Jackson/State (Red)",
        }

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def initialize(self, redis: Optional[Any] = None) -> None:
        """
        Fetch real ridership + crash data from Chicago Data Portal.
        Try Redis cache first; fall back to live API; fall back to historical model.
        Starts a background task to refresh data on schedule.
        """
        if redis:
            self._redis = redis
            self._data_cache = RealTimeDataCache(redis)

        if not self._token:
            logger.info(
                "CongestionService: CHICAGO_APP_TOKEN not configured — "
                "running on historical model only."
            )
            return

        # ── Load ridership ────────────────────────────────────────────────────
        cached_ridership = (
            await self._data_cache.get_ridership() if self._data_cache else None
        )
        if cached_ridership:
            self._ridership_data = self._deserialize_ridership(cached_ridership)
            logger.info(
                "CongestionService: ridership loaded from cache (%d stations).",
                len(self._ridership_data),
            )
        else:
            await self._refresh_ridership()

        # ── Load crash hotspots ───────────────────────────────────────────────
        cached_crashes = (
            await self._data_cache.get_crashes() if self._data_cache else None
        )
        if cached_crashes:
            self._crash_data = self._deserialize_crashes(cached_crashes)
            logger.info(
                "CongestionService: crash data loaded from cache (%d stations).",
                len(self._crash_data),
            )
        else:
            await self._refresh_crashes()

        # ── Start background refresh ──────────────────────────────────────────
        self._bg_task    = asyncio.create_task(self._background_refresh())
        self._initialized = True
        logger.info(
            "CongestionService: real-data integration active "
            "(ridership=%d, crashes=%d stations).",
            len(self._ridership_data),
            len(self._crash_data),
        )

    async def shutdown(self) -> None:
        if self._bg_task and not self._bg_task.done():
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass
        logger.info("CongestionService: shut down.")

    async def _background_refresh(self) -> None:
        """
        Long-running background coroutine.
        Refreshes ridership every 6 h and crash data every 1 h.
        """
        last_ridership = datetime.now(timezone.utc)
        last_crashes   = datetime.now(timezone.utc)

        while True:
            try:
                await asyncio.sleep(60)  # wake up every minute to check intervals
                now = datetime.now(timezone.utc)

                if (now - last_ridership).total_seconds() >= self._RIDERSHIP_REFRESH_S:
                    await self._refresh_ridership()
                    last_ridership = now

                if (now - last_crashes).total_seconds() >= self._CRASH_REFRESH_S:
                    await self._refresh_crashes()
                    last_crashes = now

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("CongestionService background refresh error: %s", exc)
                await asyncio.sleep(120)  # back off on unexpected errors

    async def _refresh_ridership(self) -> None:
        try:
            data = await self._ridership_fetcher.fetch()
            if data:
                self._ridership_data = data
                if self._data_cache:
                    await self._data_cache.store_ridership(
                        self._serialize_ridership(data)
                    )
                logger.info(
                    "CongestionService: ridership refreshed (%d stations).", len(data)
                )
        except Exception as exc:
            logger.error("CongestionService: ridership refresh failed: %s", exc)

    async def _refresh_crashes(self) -> None:
        try:
            data = await self._crash_fetcher.fetch()
            if data:
                self._crash_data = data
                if self._data_cache:
                    await self._data_cache.store_crashes(
                        self._serialize_crashes(data)
                    )
                logger.info(
                    "CongestionService: crash data refreshed (%d stations).", len(data)
                )
        except Exception as exc:
            logger.error("CongestionService: crash data refresh failed: %s", exc)

    # ── Serialization helpers (for Redis JSON storage) ────────────────────────

    @staticmethod
    def _serialize_ridership(data: Dict[str, Dict[str, RidershipRecord]]) -> Dict:
        return {
            sk: {
                dt: {
                    "station_key":     rec.station_key,
                    "daytype":         rec.daytype,
                    "avg_daily_rides": rec.avg_daily_rides,
                    "scale_factor":    rec.scale_factor,
                    "data_date_range": rec.data_date_range,
                }
                for dt, rec in by_day.items()
            }
            for sk, by_day in data.items()
        }

    @staticmethod
    def _deserialize_ridership(raw: Dict) -> Dict[str, Dict[str, RidershipRecord]]:
        result: Dict[str, Dict[str, RidershipRecord]] = {}
        for sk, by_day in raw.items():
            result[sk] = {}
            for dt, rec_dict in by_day.items():
                try:
                    result[sk][dt] = RidershipRecord(**rec_dict)
                except Exception:
                    pass
        return result

    @staticmethod
    def _serialize_crashes(data: Dict[str, CrashHotspot]) -> Dict:
        return {
            sk: {
                "station_key":        h.station_key,
                "crash_count_30d":    h.crash_count_30d,
                "injury_count_30d":   h.injury_count_30d,
                "congestion_penalty": h.congestion_penalty,
            }
            for sk, h in data.items()
        }

    @staticmethod
    def _deserialize_crashes(raw: Dict) -> Dict[str, CrashHotspot]:
        result: Dict[str, CrashHotspot] = {}
        for sk, h_dict in raw.items():
            try:
                result[sk] = CrashHotspot(**h_dict)
            except Exception:
                pass
        return result

    # ── Real-data adjustment ──────────────────────────────────────────────────

    def _apply_real_data_adjustments(
        self, pred: CongestionPrediction
    ) -> CongestionPrediction:
        """
        Blend real CTA ridership scale factors and crash hotspot penalties into
        the historical model prediction.

        Ridership blend:
          Only 40% of the scale-factor deviation is applied to avoid
          over-correcting the historical shape curves.
          adjusted *= 1.0 + (scale_factor - 1.0) * 0.40

        Crash penalty:
          Full congestion_penalty multiplier applied on top.
        """
        score = pred.adjusted_score

        # ── Ridership scale factor ─────────────────────────────────────────
        station_ridership = self._ridership_data.get(pred.station_key)
        if station_ridership:
            # Map day_type + weekday to CTA daytype code
            chicago_dt = pred.timestamp + timedelta(hours=-6)
            dow = chicago_dt.weekday()  # 0=Mon … 6=Sun
            if pred.day_type == "weekday":
                daytype = "W"
            elif dow == 6:
                daytype = "U"   # Sunday / holiday
            else:
                daytype = "A"   # Saturday

            rec = station_ridership.get(daytype) or station_ridership.get("W")
            if rec and rec.scale_factor != 1.0:
                # 40% blend of scale factor deviation
                blend = 1.0 + (rec.scale_factor - 1.0) * 0.40
                score = min(100.0, max(0.0, score * blend))

        # ── Crash hotspot penalty ──────────────────────────────────────────
        hotspot = self._crash_data.get(pred.station_key)
        if hotspot and hotspot.congestion_penalty > 1.0:
            score = min(100.0, score * hotspot.congestion_penalty)

        if abs(score - pred.adjusted_score) < 0.05:
            return pred  # no meaningful change

        # Rebuild prediction fields that depend on adjusted_score
        pred.adjusted_score   = round(score, 1)
        pred.congestion_level = CongestionModel._score_to_level(score)
        pred.percent_capacity = round(score / 100, 3)
        return pred

    def get_real_data_summary(self, station_key: str) -> Dict[str, Any]:
        """Return real-data inputs used for a station's prediction."""
        ridership = self._ridership_data.get(station_key, {})
        hotspot   = self._crash_data.get(station_key)
        return {
            "ridership_scale_factors": {
                dt: {"avg_daily_rides": rec.avg_daily_rides, "scale_factor": rec.scale_factor}
                for dt, rec in ridership.items()
            },
            "crash_hotspot": (
                {
                    "crash_count_30d":    hotspot.crash_count_30d,
                    "injury_count_30d":   hotspot.injury_count_30d,
                    "congestion_penalty": hotspot.congestion_penalty,
                }
                if hotspot else None
            ),
            "real_data_active": self._initialized,
        }

    # ── Core prediction methods ───────────────────────────────────────────────

    def predict(
        self,
        station_key: str,
        dt: Optional[datetime] = None,
        weather_condition: str = "normal",
    ) -> Optional[CongestionPrediction]:
        name = self._station_names.get(station_key, station_key)
        try:
            pred = self._model.predict(station_key, name, dt, weather_condition)
        except Exception as exc:
            logger.error("CongestionService.predict failed for %s: %s", station_key, exc)
            return None

        # Blend in real ridership + crash data if available
        if self._initialized and (self._ridership_data or self._crash_data):
            pred = self._apply_real_data_adjustments(pred)

        return pred

    def predict_all(
        self,
        dt: Optional[datetime] = None,
        weather_condition: str = "normal",
    ) -> NetworkCongestionSnapshot:
        dt = dt or datetime.now(timezone.utc)
        predictions: Dict[str, CongestionPrediction] = {}

        for key in HISTORICAL_RIDERSHIP:
            pred = self.predict(key, dt, weather_condition)
            if pred:
                predictions[key] = pred

        sorted_by_score = sorted(predictions.items(), key=lambda x: x[1].adjusted_score, reverse=True)
        most_congested  = [k for k, _ in sorted_by_score[:3]]
        least_congested = [k for k, _ in sorted_by_score[-3:]][::-1]

        avg_score = (
            sum(p.adjusted_score for p in predictions.values()) / len(predictions)
            if predictions else 0
        )

        period = self._classifier.classify(dt)
        events = SpecialEventDetector()
        active_events: List[str] = []
        for key in predictions:
            for ev_name, _ in events.get_active_events(dt, key):
                if ev_name not in active_events:
                    active_events.append(ev_name)

        routing_advice = self._build_network_advice(most_congested, period, avg_score)

        return NetworkCongestionSnapshot(
            timestamp=dt,
            station_scores=predictions,
            most_congested=most_congested,
            least_congested=least_congested,
            network_avg_score=round(avg_score, 1),
            period_type=period,
            active_events=active_events,
            routing_advice=routing_advice,
        )

    def predict_trend(
        self,
        station_key: str,
        weather_condition: str = "normal",
    ) -> Optional[CongestionTrend]:
        name = self._station_names.get(station_key, station_key)
        try:
            return self._model.predict_trend(station_key, name, weather_condition)
        except Exception as exc:
            logger.error("CongestionService.predict_trend failed for %s: %s", station_key, exc)
            return None

    def get_best_time_to_travel(
        self,
        station_key: str,
        date_type: str = "weekday",
    ) -> List[Dict[str, Any]]:
        """
        Return hourly congestion scores for a full day at a station.
        Useful for planning: "What hour is quietest for Clark/Lake?"
        """
        ridership = HISTORICAL_RIDERSHIP.get(station_key, DEFAULT_RIDERSHIP)
        day_data  = ridership.get(date_type, ridership.get("weekday", {}))
        result    = []
        for hour in range(5, 24):
            score = day_data.get(hour, 5)
            level = CongestionModel._score_to_level(score)
            result.append({
                "hour":             hour,
                "hour_label":       f"{hour:02d}:00",
                "score":            score,
                "congestion_level": level.value,
                "is_recommended":   level in (CongestionLevel.LIGHT, CongestionLevel.MODERATE),
            })
        return result

    def get_routing_advice(
        self,
        station_key: str,
        dt: Optional[datetime] = None,
        weather: str = "normal",
    ) -> Dict[str, Any]:
        """Compact routing advice for embedding in route responses."""
        pred = self.predict(station_key, dt, weather)
        if not pred:
            return {"congestion": "unknown", "advice": "No data available."}
        return {
            "station_key":    station_key,
            "congestion":     pred.congestion_level.value,
            "score":          pred.adjusted_score,
            "is_peak":        pred.is_peak,
            "recommendation": pred.recommendation,
            "alternatives":   pred.alternative_stations[:2],
        }

    def compare_stations(
        self,
        station_keys: List[str],
        dt: Optional[datetime] = None,
        weather: str = "normal",
    ) -> List[Dict[str, Any]]:
        """Compare congestion across multiple stations at the same time."""
        results = []
        for key in station_keys:
            pred = self.predict(key, dt, weather)
            if pred:
                results.append({
                    "station_key":   key,
                    "station_name":  self._station_names.get(key, key),
                    "score":         pred.adjusted_score,
                    "level":         pred.congestion_level.value,
                    "is_peak":       pred.is_peak,
                })
        results.sort(key=lambda x: x["score"])
        return results

    @staticmethod
    def _build_network_advice(
        most_congested: List[str],
        period: PeriodType,
        avg_score: float,
    ) -> str:
        if period == PeriodType.AM_PEAK:
            return (
                "AM rush hour in progress. "
                f"Most congested: {', '.join(most_congested[:2])}. "
                "Consider Pedway routes to less crowded stations."
            )
        if period == PeriodType.PM_PEAK:
            return (
                "PM rush hour in progress. "
                f"Most congested: {', '.join(most_congested[:2])}. "
                "Expect multiple trains to pass before boarding."
            )
        if avg_score < 30:
            return "Loop network is light. Good time to travel."
        if avg_score < 60:
            return "Moderate traffic across Loop stations. Normal wait times."
        return f"Above-average crowding. Most congested: {', '.join(most_congested[:2])}."

    def health_check(self) -> Dict[str, Any]:
        return {
            "status":                  "ok",
            "stations_modeled":        len(HISTORICAL_RIDERSHIP),
            "annual_events":           len(KNOWN_ANNUAL_EVENTS),
            "weather_conditions":      list(WEATHER_RIDERSHIP_MULTIPLIERS.keys()),
            "real_data_active":        self._initialized,
            "chicago_token_set":       bool(self._token),
            "ridership_stations":      len(self._ridership_data),
            "crash_hotspot_stations":  len(self._crash_data),
            "ridership_refresh_h":     self._RIDERSHIP_REFRESH_S // 3600,
            "crash_refresh_h":         self._CRASH_REFRESH_S // 3600,
        }


# ── Module-level Singleton ────────────────────────────────────────────────────

_congestion_service_instance: Optional[CongestionService] = None


def get_congestion_service(redis: Optional[Any] = None) -> CongestionService:
    """
    Return the module-level CongestionService singleton.
    Pass redis on first call (or when transitioning from no-redis to redis)
    to enable real-data integration and cache-backed refresh.
    """
    global _congestion_service_instance
    if _congestion_service_instance is None:
        _congestion_service_instance = CongestionService(redis=redis)
    elif redis is not None and _congestion_service_instance._redis is None:
        # Late-bind redis when lifespan provides it after initial import
        _congestion_service_instance._redis = redis
        _congestion_service_instance._data_cache = RealTimeDataCache(redis)
    return _congestion_service_instance
