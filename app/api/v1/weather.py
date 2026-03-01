"""
LoopNav — Production Weather API.

Exposes Chicago Loop weather data, pedestrian impact scoring, death corner
risk analysis, and an SSE live-update stream to the frontend.

Endpoints
─────────
GET  /nav/weather                  — full weather + routing context
GET  /nav/weather/impact           — quick impact level (none/low/medium/high)
GET  /nav/weather/forecast         — 3-hour / 12-hour forecast slots
GET  /nav/weather/death-corners    — per-intersection Venturi risk scores
GET  /nav/weather/recommendation   — routing recommendation string only
GET  /nav/weather/wind             — wind data (backward compat)
GET  /nav/weather/wind-alert       — simple boolean danger flag (backward compat)
GET  /nav/weather/stream           — SSE live update stream (every 60s)
GET  /nav/weather/sunny-sides      — south-facing stretches warmer in winter
GET  /nav/weather/bridges          — bridge crossing warnings
GET  /nav/weather/history          — recent weather snapshots (Redis-backed)
GET  /nav/weather/health           — service health check
POST /nav/weather/mock             — set mock scenario (blizzard/rain/etc.)
DELETE /nav/weather/mock           — clear mock, return to real API
GET  /nav/weather/scenarios        — list available mock scenarios

Data flow
─────────
  1. Request hits endpoint
  2. WeatherService.get_full() called — checks L1 (30s in-memory) then L2 (Redis 5min)
  3. On cache miss: OpenWeatherMap O'Hare API fetch
  4. WeatherImpactScorer computes routing impact
  5. DeathCornerAnalyzer scores each intersection
  6. RecommendationBuilder generates human-readable advice
  7. Full WeatherPayload returned — cached in both layers

SSE Stream protocol
───────────────────
  Content-Type: text/event-stream
  Each event: data: {JSON payload}\n\n
  Events:
    - type: "weather_update"  — full payload (every 60s)
    - type: "alert"           — immediate push if impact changes level
    - type: "ping"            — keepalive every 30s
  Client subscribes via EventSource('/api/v1/nav/weather/stream')
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from app.services.weather_service import (
    VALID_MOCK_SCENARIOS,
    WeatherImpactLevel,
    WeatherPayload,
    CornerRisk,
    ForecastSlot,
    get_service,
    fetch_wind_data,
)

log = logging.getLogger("loopnav.weather_api")

router = APIRouter(prefix="/nav", tags=["LoopNav Weather"])

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

SSE_WEATHER_INTERVAL_S = 60    # full weather push every 60s
SSE_PING_INTERVAL_S    = 30    # keepalive ping every 30s
SSE_MAX_DURATION_S     = 3600  # disconnect after 1 hour
HISTORY_MAX_RECORDS    = 20    # keep 20 historical snapshots in Redis
HISTORY_REDIS_KEY      = "nav:weather:history"

IMPACT_COLOR = {
    WeatherImpactLevel.NONE:   "#27AE60",   # green
    WeatherImpactLevel.LOW:    "#F39C12",   # amber
    WeatherImpactLevel.MEDIUM: "#E67E22",   # orange
    WeatherImpactLevel.HIGH:   "#C0392B",   # red
}

IMPACT_EMOJI = {
    WeatherImpactLevel.NONE:   "✅",
    WeatherImpactLevel.LOW:    "🌤",
    WeatherImpactLevel.MEDIUM: "🌧",
    WeatherImpactLevel.HIGH:   "🌪",
}


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic schemas
# ─────────────────────────────────────────────────────────────────────────────

class MockScenarioRequest(BaseModel):
    scenario: str = Field(
        ...,
        description="Mock scenario name",
        examples=["blizzard", "normal_winter", "clear_summer", "rain", "heat", "fog"],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Serialization helpers
# ─────────────────────────────────────────────────────────────────────────────

def _current_to_dict(c) -> Dict[str, Any]:
    return {
        "temp_f":         c.temp_f,
        "feels_like_f":   c.feels_like_f,
        "humidity_pct":   c.humidity_pct,
        "pressure_hpa":   c.pressure_hpa,
        "visibility_m":   c.visibility_m,
        "wind_speed_mph": c.wind_speed_mph,
        "wind_gust_mph":  c.wind_gust_mph,
        "wind_deg":       c.wind_deg,
        "wind_label":     c.wind_label,
        "condition_main": c.condition_main,
        "condition_desc": c.condition_desc,
        "is_raining":     c.is_raining,
        "is_snowing":     c.is_snowing,
        "is_foggy":       c.is_foggy,
        "precip_1h_mm":   c.precip_1h_mm,
        "source":         c.source,
        "fetched_at":     c.fetched_at,
    }


def _corner_to_dict(r: CornerRisk) -> Dict[str, Any]:
    return {
        "name":                r.name,
        "lat":                 r.lat,
        "lon":                 r.lon,
        "note":                r.note,
        "venturi_score":       r.venturi_score,
        "amplified_gust_mph":  r.amplified_gust_mph,
        "danger_level":        r.danger_level,
        "canyon_alignment":    r.canyon_alignment,
        "canyon_axis_deg":     r.canyon_axis_deg,
    }


def _forecast_slot_to_dict(f: ForecastSlot) -> Dict[str, Any]:
    return {
        "dt_utc":         f.dt_utc,
        "temp_f":         f.temp_f,
        "feels_like_f":   f.feels_like_f,
        "wind_speed_mph": f.wind_speed_mph,
        "wind_gust_mph":  f.wind_gust_mph,
        "condition_main": f.condition_main,
        "condition_desc": f.condition_desc,
        "pop":            f.pop,
        "precip_3h_mm":   f.precip_3h_mm,
    }


def _payload_to_dict(payload: WeatherPayload, include_forecast: bool = False) -> Dict[str, Any]:
    d: Dict[str, Any] = {
        "current":              _current_to_dict(payload.current),
        "impact":               payload.impact.value,
        "impact_score":         payload.impact_score,
        "impact_color":         IMPACT_COLOR.get(payload.impact, "#888"),
        "impact_emoji":         IMPACT_EMOJI.get(payload.impact, "❓"),
        "loop_danger_summary":  payload.loop_danger_summary,
        "recommend_pedway":     payload.recommend_pedway,
        "recommend_covered":    payload.recommend_covered,
        "routing_multiplier":   payload.routing_multiplier,
        "recommendation":       payload.recommendation,
        "death_corner_risks":   [_corner_to_dict(r) for r in payload.death_corner_risks],
        "worst_corner":         _corner_to_dict(payload.worst_corner) if payload.worst_corner else None,
        "sunny_sides":          payload.sunny_sides,
        "bridge_warnings":      payload.bridge_warnings,
        "seasonal_note":        payload.seasonal_note,
        "routing_context":      payload.routing_context,
        "timestamp":            datetime.now(timezone.utc).isoformat(),
    }
    if include_forecast and payload.forecast:
        d["forecast"] = [_forecast_slot_to_dict(f) for f in payload.forecast]
    return d


# ─────────────────────────────────────────────────────────────────────────────
# History helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _record_history(request: Request, payload: WeatherPayload) -> None:
    """Push a compact weather snapshot to Redis history list."""
    cache = getattr(getattr(request, "app", None), "state", None)
    cache = getattr(cache, "cache", None)
    if not cache:
        return
    snapshot = {
        "ts":             datetime.now(timezone.utc).isoformat(),
        "temp_f":         payload.current.temp_f,
        "wind_speed_mph": payload.current.wind_speed_mph,
        "wind_gust_mph":  payload.current.wind_gust_mph,
        "condition":      payload.current.condition_main,
        "impact":         payload.impact.value,
        "recommend_pedway": payload.recommend_pedway,
    }
    try:
        await cache.lpush(HISTORY_REDIS_KEY, snapshot)
        await cache.ltrim(HISTORY_REDIS_KEY, 0, HISTORY_MAX_RECORDS - 1)
        await cache.expire(HISTORY_REDIS_KEY, 86400)   # 24h
    except Exception:
        pass


async def _get_history(request: Request) -> List[Dict]:
    cache = getattr(getattr(request, "app", None), "state", None)
    cache = getattr(cache, "cache", None)
    if not cache:
        return []
    try:
        return await cache.lrange(HISTORY_REDIS_KEY, 0, HISTORY_MAX_RECORDS - 1)
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Service accessor
# ─────────────────────────────────────────────────────────────────────────────

def _svc(request: Request):
    """Get WeatherService from app.state or fall back to module singleton."""
    svc = getattr(getattr(request, "app", None), "state", None)
    svc = getattr(svc, "weather_svc", None)
    if svc is None:
        svc = get_service()
    return svc


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/weather")
async def get_full_weather(
    request:         Request,
    include_forecast: bool = Query(default=False, description="Include 12-hour forecast slots"),
    include_history:  bool = Query(default=False, description="Include recent weather history"),
):
    """
    Full weather payload for the Chicago Loop.

    Returns:
    - Current conditions (temp, wind, precip, visibility)
    - Routing impact level (none/low/medium/high) with continuous score
    - Death corner Venturi risk scores for all 5 notorious intersections
    - Pedway / covered route recommendation
    - Human-readable routing suggestion
    - Routing multiplier (cost weight for uncovered edges in graph engine)
    - Optional 12-hour forecast (pass ?include_forecast=true)
    """
    t0 = time.monotonic()
    svc = _svc(request)

    try:
        payload = await svc.get_full(include_forecast=include_forecast)
    except Exception as exc:
        log.error("Weather service error: %s", exc)
        raise HTTPException(status_code=502, detail={
            "error":   "WEATHER_SERVICE_ERROR",
            "message": "Could not fetch weather data",
        })

    result = _payload_to_dict(payload, include_forecast=include_forecast)
    result["_elapsed_ms"] = round((time.monotonic() - t0) * 1000, 2)

    if include_history:
        result["history"] = await _get_history(request)

    # Record snapshot in background
    asyncio.create_task(_record_history(request, payload))

    return JSONResponse(content=result)


@router.get("/weather/impact")
async def get_weather_impact(request: Request):
    """
    Quick impact level endpoint — poll every 5 minutes from frontend.

    Returns minimal JSON:
      { level: "none"|"low"|"medium"|"high", score: 0.0-1.0, recommend_pedway: bool,
        color: "#hex", emoji: "✅", summary: "...", wind_mph: N, temp_f: N }

    Optimised for high-frequency polling (L1 cache hit rate ~98%).
    """
    svc     = _svc(request)
    payload = await svc.get_full()

    return {
        "level":           payload.impact.value,
        "score":           payload.impact_score,
        "color":           IMPACT_COLOR.get(payload.impact, "#888"),
        "emoji":           IMPACT_EMOJI.get(payload.impact, "❓"),
        "summary":         payload.loop_danger_summary,
        "recommend_pedway": payload.recommend_pedway,
        "recommend_covered": payload.recommend_covered,
        "wind_speed_mph":  payload.current.wind_speed_mph,
        "wind_gust_mph":   payload.current.wind_gust_mph,
        "wind_label":      payload.current.wind_label,
        "temp_f":          payload.current.temp_f,
        "feels_like_f":    payload.current.feels_like_f,
        "condition":       payload.current.condition_main,
        "routing_multiplier": payload.routing_multiplier,
        "fetched_at":      payload.current.fetched_at,
        "timestamp":       datetime.now(timezone.utc).isoformat(),
    }


@router.get("/weather/forecast")
async def get_weather_forecast(
    request: Request,
    hours:   int = Query(default=12, ge=3, le=12, description="Forecast horizon in hours (3 or 12)"),
):
    """
    3-hour or 12-hour forecast for the Chicago Loop area.

    Each slot covers a 3-hour window. The `pop` field (probability of precipitation)
    is used by the frontend to show rain/snow likelihood indicators.
    """
    svc     = _svc(request)
    payload = await svc.get_full(include_forecast=True)

    forecast = payload.forecast or []
    slots    = int(hours / 3)
    forecast = forecast[:slots]

    return {
        "horizon_hours": hours,
        "slot_hours":    3,
        "slots":         [_forecast_slot_to_dict(f) for f in forecast],
        "count":         len(forecast),
        "current_impact": payload.impact.value,
        "timestamp":     datetime.now(timezone.utc).isoformat(),
    }


@router.get("/weather/death-corners")
async def get_death_corners(
    request:    Request,
    min_danger: str = Query(default="low", description="Minimum danger level to include (low/moderate/high/extreme)"),
):
    """
    Per-intersection Venturi amplification risk scores.

    The Chicago Loop has 5 notorious wind tunnel intersections where the
    Venturi effect between skyscrapers can double the wind speed:
      - Wacker & Adams
      - State & Madison
      - Willis Tower Plaza
      - Michigan & Wacker
      - Dearborn & Monroe

    Each corner returns:
      - venturi_score (0–1): how dangerous the corner is
      - amplified_gust_mph: estimated peak gust at the intersection
      - danger_level: low / moderate / high / extreme
      - canyon_alignment: how aligned the current wind is with the street canyon

    Use this endpoint to inject "avoid this corner" warnings into route instructions.
    """
    svc     = _svc(request)
    payload = await svc.get_full()

    DANGER_ORDER = {"low": 0, "moderate": 1, "high": 2, "extreme": 3}
    min_level    = DANGER_ORDER.get(min_danger.lower(), 0)

    corners = [
        _corner_to_dict(r)
        for r in payload.death_corner_risks
        if DANGER_ORDER.get(r.danger_level, 0) >= min_level
    ]
    corners.sort(key=lambda c: -c["venturi_score"])

    return {
        "corners":          corners,
        "count":            len(corners),
        "wind_speed_mph":   payload.current.wind_speed_mph,
        "wind_gust_mph":    payload.current.wind_gust_mph,
        "wind_direction":   payload.current.wind_label,
        "worst_corner":     corners[0] if corners else None,
        "any_extreme":      any(c["danger_level"] == "extreme" for c in corners),
        "timestamp":        datetime.now(timezone.utc).isoformat(),
    }


@router.get("/weather/recommendation")
async def get_routing_recommendation(request: Request):
    """
    Single recommendation string for the frontend tooltip / info card.

    Returns exactly what the routing engine would show users when they
    open the app: a plain-English suggestion for which type of route to use.

    Example:
      "Heavy snow — use Pedway tunnels to avoid slippery sidewalks."
      "Wind at 34 mph — avoid open plazas like Willis Tower."
      "Clear conditions — all routes open. Enjoy the walk!"
    """
    svc     = _svc(request)
    payload = await svc.get_full()

    return {
        "recommendation":    payload.recommendation,
        "impact":            payload.impact.value,
        "recommend_pedway":  payload.recommend_pedway,
        "recommend_covered": payload.recommend_covered,
        "seasonal_note":     payload.seasonal_note,
        "bridge_warnings":   payload.bridge_warnings,
        "sunny_sides":       payload.sunny_sides if payload.impact in (WeatherImpactLevel.LOW, WeatherImpactLevel.NONE) else [],
        "timestamp":         datetime.now(timezone.utc).isoformat(),
    }


@router.get("/weather/sunny-sides")
async def get_sunny_sides(request: Request):
    """
    South-facing street stretches that are 8–12°F warmer in winter.

    These are real Chicago pedestrian tips — walking the sunny side of Wacker
    or Wells Street during winter can make a significant comfort difference.
    Returns empty list when it's summer (not relevant).
    """
    svc     = _svc(request)
    payload = await svc.get_full()

    # Only relevant when it's cold
    month    = datetime.now(timezone.utc).month
    is_cold  = month in (11, 12, 1, 2, 3) or payload.current.temp_f < 45

    return {
        "sunny_sides":    payload.sunny_sides if is_cold else [],
        "relevant":       is_cold,
        "temp_f":         payload.current.temp_f,
        "note":           "South-facing streets receive more winter sun in the Northern Hemisphere.",
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }


@router.get("/weather/bridges")
async def get_bridge_warnings(request: Request):
    """
    Chicago River bridge crossing warnings based on current wind conditions.

    River crossings are particularly exposed to wind; the gap between buildings
    creates a venturi acceleration. Returns specific bridge-by-bridge advice
    when wind exceeds 25 mph.
    """
    from app.data.chicago_constants import LOOP_BRIDGES

    svc     = _svc(request)
    payload = await svc.get_full()

    wind    = payload.current.wind_speed_mph
    gust    = payload.current.wind_gust_mph

    if wind < 15:
        severity = "none"
        msg      = "Bridges are safe for pedestrians."
    elif wind < 25:
        severity = "low"
        msg      = "Some wind on bridge crossings. Hold onto railings."
    elif wind < 35:
        severity = "medium"
        msg      = "Exposed bridge crossings are uncomfortable. Use Wacker Drive lower level."
    else:
        severity = "high"
        msg      = "Dangerous bridge conditions. Avoid all river crossings. Use underground routes."

    bridges = [
        {
            "name":     b.name,
            "lat":      b.lat,
            "lon":      b.lng,
            "note":     b.note,
            "severity": severity,
            "safe":     severity in ("none", "low"),
        }
        for b in LOOP_BRIDGES
    ]

    return {
        "severity":       severity,
        "message":        msg,
        "wind_speed_mph": wind,
        "wind_gust_mph":  gust,
        "bridge_warnings": payload.bridge_warnings,
        "bridges":        bridges,
        "recommend_underground": severity in ("medium", "high"),
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }


@router.get("/weather/history")
async def get_weather_history(
    request: Request,
    limit:   int = Query(default=10, ge=1, le=HISTORY_MAX_RECORDS),
):
    """
    Recent weather snapshots from the last 24 hours.

    Each snapshot contains: timestamp, temp_f, wind_speed_mph, condition, impact.
    Useful for the frontend analytics panel showing "how the weather changed
    during the hackathon demo."

    Note: history is session-scoped (stored in Redis, cleared on restart).
    """
    history = await _get_history(request)
    return {
        "records":   history[:limit],
        "count":     len(history[:limit]),
        "total":     len(history),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/weather/wind")
async def get_wind_backward_compat(request: Request):
    """
    Backward-compatible wind endpoint (matches original LoopSense API).
    Frontend can migrate to GET /nav/weather but this remains for compatibility.
    """
    return await fetch_wind_data()


@router.get("/weather/wind-alert")
async def get_wind_alert(request: Request):
    """
    Lightweight alert endpoint — frontend polls this every 5 minutes.
    Returns a simple danger flag and routing recommendation.
    """
    svc     = _svc(request)
    payload = await svc.get_full()
    max_score = payload.worst_corner.venturi_score if payload.worst_corner else 0.0

    return {
        "danger":           max_score > 0.5 or payload.current.wind_gust_mph >= 30,
        "level":            payload.loop_danger_summary,
        "impact":           payload.impact.value,
        "recommend_pedway": payload.recommend_pedway,
        "wind_speed_mph":   payload.current.wind_speed_mph,
        "gust_mph":         payload.current.wind_gust_mph,
        "direction":        payload.current.wind_label,
        "worst_corner":     payload.worst_corner.name if payload.worst_corner else None,
        "timestamp":        datetime.now(timezone.utc).isoformat(),
    }


@router.get("/weather/health")
async def weather_service_health(request: Request):
    """
    Weather service health check — shows API key status, cache state, mock mode.

    Use this endpoint from the demo dashboard to confirm weather data is live.
    """
    svc = _svc(request)

    # Quick ping
    payload = None
    try:
        payload = await svc.get_full()
        status  = "ok"
    except Exception as exc:
        status = f"error: {exc}"

    return {
        "status":      status,
        "service":     svc.as_dict(),
        "current": {
            "impact":    payload.impact.value if payload else "unknown",
            "temp_f":    payload.current.temp_f if payload else None,
            "wind_mph":  payload.current.wind_speed_mph if payload else None,
            "source":    payload.current.source if payload else None,
        } if payload else None,
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }


@router.get("/weather/scenarios")
async def list_mock_scenarios():
    """List available mock weather scenarios for the hackathon demo."""
    return {
        "scenarios": sorted(VALID_MOCK_SCENARIOS),
        "descriptions": {
            "blizzard":      "Heavy snow, 35 mph NW wind, feels like 2°F — extreme conditions",
            "normal_winter": "Clear but cold Chicago winter, 18 mph gust — moderate conditions",
            "clear_summer":  "Sunny summer day, 8 mph S wind, 74°F — ideal conditions",
            "rain":          "Heavy rain, 14 mph SE wind, 45°F — medium impact",
            "heat":          "Heat wave, 96°F, feels like 105°F — high impact",
            "fog":           "Dense fog, visibility 400m — medium impact",
        },
        "usage": "POST /nav/weather/mock with {\"scenario\": \"blizzard\"}",
    }


@router.post("/weather/mock")
async def set_mock_scenario(req: MockScenarioRequest, request: Request):
    """
    Override weather data with a mock scenario for hackathon demos.

    Perfect for showing judges:
      blizzard      → extreme wind/snow → full pedway routing activated
      clear_summer  → ideal conditions → all routes available
      rain          → moderate impact  → covered routes preferred

    The mock scenario persists until cleared via DELETE /nav/weather/mock.
    """
    svc = _svc(request)
    try:
        svc.mock_manager.set_scenario(req.scenario)
        svc.invalidate_cache()   # force next request to pick up new mock
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={
            "error":     "INVALID_SCENARIO",
            "message":   str(exc),
            "available": sorted(VALID_MOCK_SCENARIOS),
        })

    # Fetch immediately to return the activated payload
    payload = await svc.get_full()

    return {
        "success":         True,
        "active_scenario": req.scenario,
        "impact":          payload.impact.value,
        "recommendation":  payload.recommendation,
        "message":         f"Mock scenario '{req.scenario}' activated. Weather API now returns mock data.",
        "timestamp":       datetime.now(timezone.utc).isoformat(),
    }


@router.delete("/weather/mock")
async def clear_mock_scenario(request: Request):
    """
    Clear the mock scenario and return to real OpenWeatherMap data.

    Call this after the demo to restore live weather data.
    """
    svc = _svc(request)
    svc.mock_manager.clear_scenario()
    svc.invalidate_cache()

    return {
        "success":   True,
        "message":   "Mock scenario cleared. Weather API now uses real data.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SSE Stream
# ─────────────────────────────────────────────────────────────────────────────

async def _weather_sse_generator(request: Request) -> AsyncGenerator[str, None]:
    """
    Async generator for the SSE weather stream.

    Protocol:
      - Every 30s: ping event (keepalive)
      - Every 60s: full weather_update event
      - On impact level change: immediate alert event

    Clients should reconnect on disconnect (standard SSE behaviour).
    """
    svc          = _svc(request)
    started_at   = time.monotonic()
    last_full    = 0.0
    last_ping    = 0.0
    last_impact  = None

    while True:
        # Check client disconnect
        if await request.is_disconnected():
            log.debug("SSE client disconnected")
            break

        # Max duration guard
        if time.monotonic() - started_at > SSE_MAX_DURATION_S:
            yield "event: close\ndata: {\"reason\": \"max_duration\"}\n\n"
            break

        now = time.monotonic()

        # Fetch current weather (L1 cache hit = essentially free)
        try:
            payload = await svc.get_full()
        except Exception as exc:
            log.warning("SSE weather fetch error: %s", exc)
            await asyncio.sleep(5)
            continue

        # Detect impact level change → immediate alert
        current_impact = payload.impact.value
        if last_impact is not None and current_impact != last_impact:
            alert_data = {
                "type":         "alert",
                "previous":     last_impact,
                "current":      current_impact,
                "summary":      payload.loop_danger_summary,
                "recommend_pedway": payload.recommend_pedway,
                "timestamp":    datetime.now(timezone.utc).isoformat(),
            }
            yield f"event: alert\ndata: {json.dumps(alert_data)}\n\n"
            log.info("SSE alert: impact changed %s → %s", last_impact, current_impact)
        last_impact = current_impact

        # Full weather update every 60s
        if now - last_full >= SSE_WEATHER_INTERVAL_S:
            compact = {
                "type":            "weather_update",
                "impact":          payload.impact.value,
                "impact_score":    payload.impact_score,
                "color":           IMPACT_COLOR.get(payload.impact, "#888"),
                "temp_f":          payload.current.temp_f,
                "feels_like_f":    payload.current.feels_like_f,
                "wind_speed_mph":  payload.current.wind_speed_mph,
                "wind_gust_mph":   payload.current.wind_gust_mph,
                "wind_label":      payload.current.wind_label,
                "condition":       payload.current.condition_main,
                "recommend_pedway": payload.recommend_pedway,
                "recommendation":  payload.recommendation,
                "routing_multiplier": payload.routing_multiplier,
                "worst_corner":    payload.worst_corner.name if payload.worst_corner else None,
                "timestamp":       datetime.now(timezone.utc).isoformat(),
            }
            yield f"event: weather_update\ndata: {json.dumps(compact)}\n\n"
            last_full = now

        # Ping every 30s
        if now - last_ping >= SSE_PING_INTERVAL_S:
            ping = {
                "type":      "ping",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "uptime_s":  round(now - started_at, 0),
            }
            yield f"event: ping\ndata: {json.dumps(ping)}\n\n"
            last_ping = now

        await asyncio.sleep(5)   # check every 5s for impact changes


@router.get("/weather/stream")
async def weather_sse_stream(request: Request):
    """
    Server-Sent Events stream of live weather updates.

    Events:
      weather_update  — full weather snapshot (every 60s)
      alert           — immediate push on impact level change
      ping            — keepalive (every 30s)

    Frontend usage (JavaScript):
        const es = new EventSource('/api/v1/nav/weather/stream');
        es.addEventListener('weather_update', e => {
            const data = JSON.parse(e.data);
            updateWeatherBar(data);
        });
        es.addEventListener('alert', e => {
            const data = JSON.parse(e.data);
            showImpactAlert(data);
        });

    The stream automatically disconnects after 1 hour. Frontend EventSource
    will automatically reconnect via the standard browser SSE mechanism.
    """
    return StreamingResponse(
        _weather_sse_generator(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin": "*",
            "Connection":                  "keep-alive",
        },
    )
