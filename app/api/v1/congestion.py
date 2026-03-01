"""
congestion.py — Loop Station Congestion & Rush Hour API
=======================================================
Exposes real-time and predicted crowding levels for Chicago Loop
CTA stations. Helps users avoid packed platforms and find the
quietest boarding option.

All data flows from congestion_service.py.

Endpoints:
  GET  /nav/congestion/{station_key}           — current crowding prediction
  GET  /nav/congestion/{station_key}/trend     — 60-min crowding trend
  GET  /nav/congestion/{station_key}/best-time — best times to travel today
  GET  /nav/congestion/all                     — all Loop stations congestion snapshot
  GET  /nav/congestion/worst                   — most congested stations right now
  GET  /nav/congestion/best                    — least congested stations right now
  POST /nav/congestion/compare                 — compare multiple stations
  GET  /nav/congestion/routing-advice          — compact advice for nav engine
  GET  /nav/congestion/events                  — active special events
  GET  /nav/congestion/schedule                — congestion forecast for next 12h
  GET  /nav/congestion/health                  — service health
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.congestion_service import (
    CongestionService,
    CongestionLevel,
    CongestionPrediction,
    NetworkCongestionSnapshot,
    CongestionTrend,
    PeriodType,
    HISTORICAL_RIDERSHIP,
    KNOWN_ANNUAL_EVENTS,
    WEATHER_RIDERSHIP_MULTIPLIERS,
    get_congestion_service,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Congestion / Rush Hour"])

_svc = get_congestion_service()

CONGESTION_COLOR_MAP = {
    CongestionLevel.LIGHT:    "#27AE60",   # green
    CongestionLevel.MODERATE: "#F39C12",   # amber
    CongestionLevel.HEAVY:    "#E67E22",   # orange
    CongestionLevel.SEVERE:   "#C0392B",   # red
}

PERIOD_LABELS = {
    PeriodType.AM_PEAK:     "AM Rush (7–9am)",
    PeriodType.AM_SHOULDER: "AM Shoulder",
    PeriodType.MIDDAY:      "Midday",
    PeriodType.PM_SHOULDER: "PM Shoulder",
    PeriodType.PM_PEAK:     "PM Rush (4–7pm)",
    PeriodType.EVENING:     "Evening",
    PeriodType.LATE_NIGHT:  "Late Night",
    PeriodType.EARLY:       "Early Morning",
}

VALID_STATION_KEYS = set(HISTORICAL_RIDERSHIP.keys())


# ── Pydantic Schemas ──────────────────────────────────────────────────────────

class PredictionOut(BaseModel):
    station_key:       str
    station_name:      str
    period_type:       str
    period_label:      str
    congestion_level:  str
    congestion_color:  str
    percent_capacity:  float
    score:             float
    is_peak:           bool
    weather_factor:    float
    event_factor:      float
    day_type:          str
    recommendation:    str
    alternative_stations: List[str]
    next_lighter_period: Optional[str]
    timestamp:         str

    @classmethod
    def from_pred(cls, p: CongestionPrediction) -> "PredictionOut":
        return cls(
            station_key=p.station_key,
            station_name=p.station_name,
            period_type=p.period_type.value,
            period_label=PERIOD_LABELS.get(p.period_type, p.period_type.value),
            congestion_level=p.congestion_level.value,
            congestion_color=CONGESTION_COLOR_MAP.get(p.congestion_level, "#888888"),
            percent_capacity=p.percent_capacity,
            score=p.adjusted_score,
            is_peak=p.is_peak,
            weather_factor=p.weather_factor,
            event_factor=p.event_factor,
            day_type=p.day_type,
            recommendation=p.recommendation,
            alternative_stations=p.alternative_stations,
            next_lighter_period=p.next_lighter_period,
            timestamp=p.timestamp.isoformat(),
        )


class TrendOut(BaseModel):
    station_key:    str
    current_score:  float
    in_30min_score: float
    in_60min_score: float
    trend:          str
    trend_icon:     str
    peak_eta:       Optional[str]

    @classmethod
    def from_trend(cls, t: CongestionTrend) -> "TrendOut":
        icons = {"increasing": "↑", "decreasing": "↓", "stable": "→"}
        return cls(
            station_key=t.station_key,
            current_score=t.current_score,
            in_30min_score=t.in_30min_score,
            in_60min_score=t.in_60min_score,
            trend=t.trend,
            trend_icon=icons.get(t.trend, "→"),
            peak_eta=t.peak_eta,
        )


class CompareRequest(BaseModel):
    station_keys:      List[str] = Field(..., min_length=2, max_length=8,
                                         description="Station keys to compare")
    weather_condition: str       = Field(default="normal",
                                         description="Weather condition: normal|heavy_rain|blizzard|etc.")


# ── Endpoints ──────────────────────────────────────────────────────────────────
# NOTE: Static routes must be registered BEFORE the dynamic /{station_key} route
# so that Starlette matches them first (first-match-wins routing).

@router.get("/nav/congestion/all", summary="Network-wide congestion snapshot")
async def get_network_congestion(
    weather_condition: str = Query("normal"),
) -> Dict[str, Any]:
    """
    Returns a network-wide congestion snapshot for all modeled Loop stations.
    Includes most/least congested stations and routing advice.
    """
    snapshot = _svc.predict_all(weather_condition=weather_condition)

    stations_out = [
        {
            **PredictionOut.from_pred(pred).model_dump(),
        }
        for pred in sorted(
            snapshot.station_scores.values(),
            key=lambda p: p.adjusted_score,
            reverse=True,
        )
    ]

    return {
        "timestamp":         snapshot.timestamp.isoformat(),
        "period_type":       snapshot.period_type.value,
        "period_label":      PERIOD_LABELS.get(snapshot.period_type, ""),
        "network_avg_score": snapshot.network_avg_score,
        "most_congested":    snapshot.most_congested,
        "least_congested":   snapshot.least_congested,
        "active_events":     snapshot.active_events,
        "routing_advice":    snapshot.routing_advice,
        "stations":          stations_out,
    }


@router.get("/nav/congestion/worst", summary="Most congested Loop stations right now")
async def get_most_congested(
    limit:             int = Query(5, ge=1, le=10),
    weather_condition: str = Query("normal"),
) -> Dict[str, Any]:
    """
    Returns the N most congested Loop stations at the current time.
    Useful for route planning: "avoid these stations right now."
    """
    snapshot   = _svc.predict_all(weather_condition=weather_condition)
    all_preds  = list(snapshot.station_scores.values())
    all_preds.sort(key=lambda p: p.adjusted_score, reverse=True)
    top        = all_preds[:limit]

    return {
        "timestamp":     snapshot.timestamp.isoformat(),
        "period":        PERIOD_LABELS.get(snapshot.period_type, ""),
        "most_congested": [
            {
                "rank":            i + 1,
                **PredictionOut.from_pred(p).model_dump(),
            }
            for i, p in enumerate(top)
        ],
        "avoid_advice": (
            f"Avoid {', '.join(snapshot.most_congested[:2])} during this period. "
            f"Use Pedway to reach less-crowded alternatives."
        ),
    }


@router.get("/nav/congestion/best", summary="Least congested Loop stations right now")
async def get_least_congested(
    limit:             int = Query(5, ge=1, le=10),
    weather_condition: str = Query("normal"),
) -> Dict[str, Any]:
    """
    Returns the N least congested Loop stations at the current time.
    The ideal boarding options when you have flexibility on which station to use.
    """
    snapshot  = _svc.predict_all(weather_condition=weather_condition)
    all_preds = list(snapshot.station_scores.values())
    all_preds.sort(key=lambda p: p.adjusted_score)
    best      = all_preds[:limit]

    return {
        "timestamp": snapshot.timestamp.isoformat(),
        "period":    PERIOD_LABELS.get(snapshot.period_type, ""),
        "least_congested": [
            {
                "rank":  i + 1,
                **PredictionOut.from_pred(p).model_dump(),
            }
            for i, p in enumerate(best)
        ],
    }


@router.post("/nav/congestion/compare", summary="Compare crowding across multiple stations")
async def compare_stations(req: CompareRequest) -> Dict[str, Any]:
    """
    Compares crowding levels across 2–8 Loop stations simultaneously.
    Returns ranked list from least to most congested.

    Example: "I can board at Clark/Lake, State/Lake, or Randolph/Wabash — which is least crowded?"
    """
    invalid = [k for k in req.station_keys if k not in HISTORICAL_RIDERSHIP]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown station keys: {invalid}. Valid: {sorted(VALID_STATION_KEYS)}",
        )

    results = _svc.compare_stations(req.station_keys, weather=req.weather_condition)

    return {
        "compared_at":       datetime.now(timezone.utc).isoformat(),
        "weather_condition": req.weather_condition,
        "station_count":     len(results),
        "ranked_least_to_most_congested": results,
        "recommendation": (
            f"Board at {results[0]['station_name']} — least crowded option right now."
            if results else "No data available."
        ),
    }


@router.get("/nav/congestion/routing-advice", summary="Compact congestion advice for nav engine")
async def get_routing_advice(
    station_key:       str   = Query(..., description="Station to check"),
    weather_condition: str   = Query("normal"),
) -> Dict[str, Any]:
    """
    Compact routing advice for embedding in nav route responses.
    Returns: congestion level, score, is_peak, recommendations, alternatives.
    """
    if station_key not in HISTORICAL_RIDERSHIP:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")

    return _svc.get_routing_advice(station_key, weather=weather_condition)


@router.get("/nav/congestion/events", summary="Active special events affecting Loop congestion")
async def get_active_events(
    station_key: Optional[str] = Query(None, description="Filter to a specific station"),
) -> Dict[str, Any]:
    """
    Returns special events currently affecting congestion at Loop stations.
    Events include: festivals, concerts, conventions, sports games, holidays.
    """
    from app.services.congestion_service import SpecialEventDetector
    now      = datetime.now(timezone.utc)
    detector = SpecialEventDetector()

    if station_key:
        if station_key not in VALID_STATION_KEYS:
            raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")
        events = detector.get_active_events(now, station_key)
        return {
            "station_key": station_key,
            "active_events": [{"name": n, "multiplier": m} for n, m in events],
        }

    # All stations
    all_events: Dict[str, List[Dict]] = {}
    for key in VALID_STATION_KEYS:
        events = detector.get_active_events(now, key)
        if events:
            all_events[key] = [{"name": n, "multiplier": m} for n, m in events]

    unique_event_names = list({e["name"] for evs in all_events.values() for e in evs})

    return {
        "checked_at":     now.isoformat(),
        "active_event_count": len(unique_event_names),
        "active_events":      unique_event_names,
        "affected_stations":  all_events,
        "total_known_events": len(KNOWN_ANNUAL_EVENTS),
    }


@router.get("/nav/congestion/schedule", summary="Hourly congestion forecast for next 12 hours")
async def get_congestion_schedule(
    station_key:       str = Query(..., description="Loop station key"),
    weather_condition: str = Query("normal"),
) -> Dict[str, Any]:
    """
    Returns congestion predictions for the next 12 hours at a station.
    Use this to plan the day: "My 5:30 PM commute from Clark/Lake will be severe — leave by 5:10 PM."
    """
    if station_key not in HISTORICAL_RIDERSHIP:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")

    now    = datetime.now(timezone.utc)
    hours  = []

    for h in range(13):   # now + 0..12 hours
        future_dt = now + timedelta(hours=h)
        pred      = _svc.predict(station_key, dt=future_dt, weather_condition=weather_condition)
        if pred:
            # Chicago local display time
            local_dt = future_dt + timedelta(hours=-6)
            hours.append({
                "offset_hours":    h,
                "datetime_utc":    future_dt.isoformat(),
                "time_local":      local_dt.strftime("%H:%M"),
                "congestion_level": pred.congestion_level.value,
                "congestion_color": CONGESTION_COLOR_MAP.get(pred.congestion_level, "#888"),
                "score":           pred.adjusted_score,
                "is_peak":         pred.is_peak,
                "period":          PERIOD_LABELS.get(pred.period_type, ""),
            })

    # Find best window (lowest score in next 12h)
    best_hour    = min(hours, key=lambda x: x["score"]) if hours else None
    worst_hour   = max(hours, key=lambda x: x["score"]) if hours else None

    return {
        "station_key":       station_key,
        "weather_condition": weather_condition,
        "generated_at":      now.isoformat(),
        "forecast_hours":    hours,
        "best_window":       best_hour,
        "worst_window":      worst_hour,
        "summary": (
            f"Best time: {best_hour['time_local']} ({best_hour['congestion_level']}). "
            f"Worst: {worst_hour['time_local']} ({worst_hour['congestion_level']})."
            if best_hour and worst_hour else "No forecast data."
        ),
    }


@router.get("/nav/congestion/health", summary="Congestion service health")
async def congestion_health() -> Dict[str, Any]:
    return _svc.health_check()


@router.get("/nav/congestion/{station_key}", summary="Current crowding prediction for a station")
async def get_station_congestion(
    station_key:       str,
    weather_condition: str = Query("normal"),
) -> Dict[str, Any]:
    """
    Returns current crowding prediction for a single Loop CTA station.
    Includes congestion level, score, peak status, and routing recommendations.
    """
    if station_key not in HISTORICAL_RIDERSHIP:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")

    pred = _svc.predict(station_key, weather_condition=weather_condition)
    if not pred:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")

    return {
        "station_key":  station_key,
        "prediction":   PredictionOut.from_pred(pred).model_dump(),
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get(
    "/nav/congestion/{station_key}/real-data",
    summary="Real-data inputs (ridership + crash) used for a station's prediction",
)
async def get_real_data_summary(station_key: str) -> Dict[str, Any]:
    """
    Inspect which real-data sources are influencing a station's congestion score.

    Returns:
      - ridership_scale_factors: per day-type (W/A/U) actual avg rides vs baseline
      - crash_hotspot: 30-day crash/injury count + congestion penalty near entrance
      - real_data_active: whether live Chicago Data Portal data is loaded
    """
    if station_key not in VALID_STATION_KEYS:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")
    return _svc.get_real_data_summary(station_key)


@router.get("/nav/congestion/{station_key}/trend", summary="60-minute crowding trend for a station")
async def get_station_trend(
    station_key:       str,
    weather_condition: str = Query("normal"),
) -> Dict[str, Any]:
    """
    Returns crowding trend: current, +30 min, +60 min.
    Use this to decide: "Should I leave now or wait 20 minutes for the rush to ease?"
    """
    if station_key not in HISTORICAL_RIDERSHIP:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")

    trend = _svc.predict_trend(station_key, weather_condition=weather_condition)
    if not trend:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")

    current_pred = _svc.predict(station_key, weather_condition=weather_condition)

    return {
        "station_key":  station_key,
        "trend":        TrendOut.from_trend(trend).model_dump(),
        "current":      PredictionOut.from_pred(current_pred).model_dump() if current_pred else None,
        "interpretation": _build_trend_interpretation(trend),
    }


@router.get("/nav/congestion/{station_key}/best-time", summary="Best times to travel today")
async def get_best_travel_times(
    station_key: str,
    day_type:    str = Query("weekday", description="weekday or weekend"),
) -> Dict[str, Any]:
    """
    Returns hourly congestion scores for a full day at a station.
    Helps commuters find the quietest window for their journey.

    Example: "Quincy/Wells is quietest between 10am–3pm and after 7pm."
    """
    if station_key not in HISTORICAL_RIDERSHIP:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")

    if day_type not in ("weekday", "weekend"):
        raise HTTPException(status_code=400, detail="day_type must be 'weekday' or 'weekend'")

    hourly = _svc.get_best_time_to_travel(station_key, day_type)

    recommended_hours = [h for h in hourly if h["is_recommended"]]
    avoid_hours       = [h for h in hourly if not h["is_recommended"]]

    return {
        "station_key":      station_key,
        "day_type":         day_type,
        "hourly_congestion": hourly,
        "recommended_windows": [h["hour_label"] for h in recommended_hours],
        "avoid_windows":       [h["hour_label"] for h in avoid_hours],
        "quietest_hour":       min(hourly, key=lambda x: x["score"])["hour_label"],
        "busiest_hour":        max(hourly, key=lambda x: x["score"])["hour_label"],
    }


# ── Helper ─────────────────────────────────────────────────────────────────────

def _build_trend_interpretation(trend: "CongestionTrend") -> str:
    delta = trend.in_30min_score - trend.current_score
    if trend.trend == "increasing" and delta > 15:
        return f"Crowding is increasing sharply. Board now if possible. {trend.peak_eta or ''}"
    if trend.trend == "increasing":
        return f"Slight increase expected in 30 minutes. {trend.peak_eta or ''}"
    if trend.trend == "decreasing" and abs(delta) > 15:
        return "Rush hour is ending. Wait 15–20 minutes for significantly lighter crowds."
    if trend.trend == "decreasing":
        return "Crowding is gradually easing over the next hour."
    return "Crowding levels are stable for the next hour."
