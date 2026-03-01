"""
LoopNav — Production Routing API
=================================
POST /api/v1/nav/route          — Main multi-layer route (3 alternatives)
POST /api/v1/nav/route/batch    — Batch routing (up to 5 pairs)
POST /api/v1/nav/route/compare  — Street-only vs optimal side-by-side
GET  /api/v1/nav/route/share/{token}  — Decode a shared route token
GET  /api/v1/nav/demo/{scenario}      — Pre-canned demo routes
GET  /api/v1/nav/locations            — Preset origin/destination points
GET  /api/v1/nav/locations/search     — Fuzzy search preset locations
POST /api/v1/nav/route/feedback       — User submits route quality feedback
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from math import atan2, cos, radians, sin, sqrt
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

from app.models.nav_schemas import (
    Coords,
    NavRouteRequest,
    NavRouteResponse,
    RouteSegment,
    SingleRoute,
)
from app.services.cache import Cache
from app.services.cta_service import fetch_elevator_outages
from app.services.weather_service import fetch_wind_data

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/nav", tags=["LoopNav — Routing"])

# ── Constants ─────────────────────────────────────────────────────────────────

LOOP_BOUNDS = {
    "min_lat": 41.870,
    "max_lat": 41.895,
    "min_lng": -87.650,
    "max_lng": -87.615,
}

WALK_SPEED_MS    = 1.4
PEDWAY_SPEED_MS  = 1.2
STREET_BASELINE_S = 600   # benchmark for "time saved" metric

PRESET_LOCATIONS: list[dict] = [
    {"id": "ogilvie",        "name": "Ogilvie Transportation Center",  "lat": 41.8827, "lng": -87.6414, "level": "street",  "tags": ["train", "station", "metra"]},
    {"id": "union",          "name": "Union Station",                  "lat": 41.8786, "lng": -87.6399, "level": "street",  "tags": ["train", "station", "amtrak", "metra"]},
    {"id": "millennium",     "name": "Millennium Station",             "lat": 41.8843, "lng": -87.6243, "level": "pedway",  "tags": ["train", "station", "metra", "pedway"]},
    {"id": "art_institute",  "name": "Art Institute of Chicago",       "lat": 41.8796, "lng": -87.6237, "level": "street",  "tags": ["museum", "culture", "michigan"]},
    {"id": "city_hall",      "name": "Chicago City Hall",              "lat": 41.8836, "lng": -87.6323, "level": "street",  "tags": ["government", "lasalle"]},
    {"id": "block37",        "name": "Block 37",                       "lat": 41.8831, "lng": -87.6278, "level": "pedway",  "tags": ["mall", "shopping", "state", "pedway"]},
    {"id": "macys",          "name": "Macy's on State Street",         "lat": 41.8845, "lng": -87.6278, "level": "street",  "tags": ["shopping", "state", "department store"]},
    {"id": "willis",         "name": "Willis Tower",                   "lat": 41.8789, "lng": -87.6359, "level": "street",  "tags": ["skyscraper", "office", "wacker"]},
    {"id": "chase",          "name": "Chase Tower",                    "lat": 41.8818, "lng": -87.6312, "level": "pedway",  "tags": ["office", "bank", "pedway", "lasalle"]},
    {"id": "daley",          "name": "Daley Center",                   "lat": 41.8838, "lng": -87.6308, "level": "street",  "tags": ["government", "court", "plaza"]},
    {"id": "cultural",       "name": "Chicago Cultural Center",        "lat": 41.8836, "lng": -87.6249, "level": "street",  "tags": ["culture", "michigan", "museum"]},
    {"id": "lake_state",     "name": "Lake/State Red Line",            "lat": 41.8860, "lng": -87.6280, "level": "street",  "tags": ["cta", "red line", "train"]},
    {"id": "washington_blue","name": "Washington/Dearborn Blue Line",  "lat": 41.8832, "lng": -87.6296, "level": "street",  "tags": ["cta", "blue line", "train"]},
    {"id": "quincy_wells",   "name": "Quincy/Wells (Brown Line)",      "lat": 41.8786, "lng": -87.6375, "level": "street",  "tags": ["cta", "brown line", "train"]},
    {"id": "millennium_park","name": "Millennium Park",                "lat": 41.8827, "lng": -87.6233, "level": "street",  "tags": ["park", "bean", "michigan"]},
    {"id": "riverwalk",      "name": "Chicago Riverwalk",              "lat": 41.8876, "lng": -87.6290, "level": "mid",     "tags": ["riverwalk", "outdoor", "water"]},
]

DEMO_SCENARIOS: dict[str, dict] = {
    "blizzard": {
        "name":       "The Blizzard Route",
        "description": "Ogilvie Station → Art Institute — pedway saves you from the snow.",
        "origin_id":  "ogilvie",
        "dest_id":    "art_institute",
        "mode":       "pedway_preferred",
        "accessible": False,
        "story": (
            "It's a polar vortex day. -10°F, 40 mph gusts. Street route: 18 min exposed. "
            "Pedway route: 13 min fully covered. This is the route Google Maps doesn't know exists."
        ),
    },
    "accessible": {
        "name":       "The Accessible Route",
        "description": "Union Station → City Hall — elevator-only, live CTA alerts checked.",
        "origin_id":  "union",
        "dest_id":    "city_hall",
        "mode":       "optimal",
        "accessible": True,
        "story": (
            "Every connector uses verified elevators. Live CTA elevator alerts are checked "
            "before the route is returned. If an elevator is flagged as out of service, "
            "we route around it automatically."
        ),
    },
    "commuter": {
        "name":       "The Rush Hour Commuter",
        "description": "Lake/State Red Line → Willis Tower — rush hour optimised.",
        "origin_id":  "lake_state",
        "dest_id":    "willis",
        "mode":       "optimal",
        "accessible": False,
        "story": (
            "Morning rush hour. Street-level crowds near State/Madison add 3 minutes. "
            "LoopNav routes you through Lower Wacker, arriving 5 minutes faster."
        ),
    },
    "tourist": {
        "name":       "The Scenic Walk",
        "description": "Millennium Park → Willis Tower — street-level, see the city.",
        "origin_id":  "millennium_park",
        "dest_id":    "willis",
        "mode":       "street_only",
        "accessible": False,
        "story": (
            "Not every route should be underground. Sometimes you want to walk past "
            "the Bean, cross the river, and see the skyline."
        ),
    },
}

# Known Loop landmarks used for segment enrichment
_LANDMARKS = [
    {"name": "Daley Plaza / Picasso",   "lat": 41.8838, "lng": -87.6308, "radius_m": 90},
    {"name": "Cloud Gate (The Bean)",   "lat": 41.8827, "lng": -87.6233, "radius_m": 110},
    {"name": "Chicago Riverwalk",       "lat": 41.8876, "lng": -87.6290, "radius_m": 130},
    {"name": "Willis Tower Skydeck",    "lat": 41.8789, "lng": -87.6359, "radius_m": 85},
    {"name": "The Loop L Tracks",       "lat": 41.8855, "lng": -87.6278, "radius_m": 160},
    {"name": "Chicago Cultural Center", "lat": 41.8836, "lng": -87.6249, "radius_m": 75},
    {"name": "Millennium Park",         "lat": 41.8827, "lng": -87.6233, "radius_m": 200},
]


# ── Extra request / response models ───────────────────────────────────────────

class BatchRouteRequest(BaseModel):
    pairs: list[NavRouteRequest] = Field(..., min_length=1, max_length=5)


class BatchRouteResponse(BaseModel):
    results: list[dict]
    total:   int
    failed:  int


class CompareRequest(BaseModel):
    origin:         dict = Field(..., description="{'lat': float, 'lng': float}")
    destination:    dict = Field(..., description="{'lat': float, 'lng': float}")
    accessible:     bool = False
    simulated_hour: Optional[int] = Field(default=None, ge=0, le=23)


class CompareResponse(BaseModel):
    street_only:      SingleRoute
    optimal:          SingleRoute
    time_saved_s:     int
    covered_gain_pct: float
    verdict:          str


class RouteFeedback(BaseModel):
    route_token: str
    rating:      int = Field(..., ge=1, le=5)
    comment:     Optional[str] = Field(default=None, max_length=500)
    issues:      list[str] = Field(default_factory=list)

    @field_validator("issues")
    @classmethod
    def validate_issues(cls, v: list[str]) -> list[str]:
        allowed = {
            "wrong_path", "missing_connector", "closed_pedway",
            "inaccurate_time", "elevator_wrong", "other",
        }
        return [i for i in v if i in allowed]


# ── Geometry helpers ───────────────────────────────────────────────────────────

def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6_371_000
    φ1, φ2 = radians(lat1), radians(lat2)
    a = (sin((φ2 - φ1) / 2) ** 2
         + cos(φ1) * cos(φ2) * sin(radians(lng2 - lng1) / 2) ** 2)
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def _bounds_check(lat: float, lng: float) -> bool:
    return (
        LOOP_BOUNDS["min_lat"] <= lat <= LOOP_BOUNDS["max_lat"]
        and LOOP_BOUNDS["min_lng"] <= lng <= LOOP_BOUNDS["max_lng"]
    )


# ── Internal helpers ───────────────────────────────────────────────────────────

def _get_routing_fns():
    if os.getenv("USE_STUB_GRAPH", "true").lower() == "true":
        from app.routing.stub_engine import snap_to_graph, get_route_alternatives
    else:
        from app.routing.engine import snap_to_graph, get_route_alternatives  # type: ignore
    return snap_to_graph, get_route_alternatives


def _wind_to_nav_weather(wind_data: dict) -> dict:
    speed_mph        = wind_data.get("wind_speed_mph", 0.0)
    gust_mph         = wind_data.get("gust_mph", speed_mph)
    recommend_pedway = wind_data.get("recommend_pedway", False)
    temp_f           = wind_data.get("temp_f", 45.0)

    if recommend_pedway or speed_mph > 30 or (temp_f is not None and temp_f < 15):
        impact         = "high"
        recommendation = (
            f"Dangerous conditions ({speed_mph:.0f} mph wind, {temp_f:.0f}°F) — "
            f"pedway strongly recommended."
        )
    elif speed_mph > 20 or (temp_f is not None and temp_f < 32):
        impact         = "medium"
        recommendation = f"Windy or cold ({speed_mph:.0f} mph, {temp_f:.0f}°F) — pedway recommended."
    elif speed_mph > 12:
        impact         = "low"
        recommendation = f"Breezy today ({speed_mph:.0f} mph) — covered route available."
    else:
        impact         = "none"
        recommendation = None

    return {
        "condition":        "Windy" if speed_mph > 15 else "Clear",
        "wind_speed_mph":   round(speed_mph, 1),
        "gust_mph":         round(gust_mph, 1),
        "temp_f":           temp_f,
        "impact":           impact,
        "recommendation":   recommendation,
        "recommend_pedway": recommend_pedway,
    }


def _apply_elevator_alerts(G, alerts: list[dict]):
    """
    Returns a shallow-copy of G with elevator reliability degraded for
    connectors whose building appears in active CTA elevator alerts.
    Never mutates app.state.graph.
    """
    import copy
    if not alerts:
        return G

    G_live    = copy.deepcopy(G)
    alert_txt = " ".join(
        (a.get("headline", "") + " " + a.get("short_description", "")).lower()
        for a in alerts
    )

    for u, v, key, data in list(G_live.edges(data=True, keys=True)):
        if data.get("connector_type") != "elevator":
            continue
        building = (
            G_live.nodes[u].get("building", "")
            or G_live.nodes[v].get("building", "")
        ).lower()
        if building and building in alert_txt:
            G_live[u][v][key]["reliability"] = 0.05
            logger.warning("Elevator at '%s' degraded by live CTA alert.", building)

    return G_live


def _enrich_segments_with_landmarks(segments: list[RouteSegment]) -> list[RouteSegment]:
    """Adds nearby landmark names to segment instruction strings."""
    enriched = []
    for seg in segments:
        nearby: list[str] = []
        for coord in seg.geometry:
            for lm in _LANDMARKS:
                d = _haversine_m(coord[1], coord[0], lm["lat"], lm["lng"])
                if d <= lm["radius_m"] and lm["name"] not in nearby:
                    nearby.append(lm["name"])
        if nearby:
            suffix = f" (near {', '.join(nearby[:2])})"
            seg = seg.model_copy(update={"instruction": seg.instruction + suffix})
        enriched.append(seg)
    return enriched


def _compute_eta(total_time_s: int, simulated_hour: int | None) -> dict[str, str]:
    """Returns ISO-format departure and arrival timestamps."""
    now = datetime.now(timezone.utc)
    if simulated_hour is not None:
        now = now.replace(hour=simulated_hour, minute=0, second=0, microsecond=0)
    from datetime import timedelta
    arrival = now + timedelta(seconds=total_time_s)
    return {
        "departure_time": now.isoformat(),
        "arrival_time":   arrival.isoformat(),
    }


def _encode_share_token(
    origin_lat: float, origin_lng: float,
    dest_lat: float,   dest_lng: float,
    mode: str,         accessible: bool,
) -> str:
    payload = json.dumps({
        "olat": round(origin_lat, 4), "olng": round(origin_lng, 4),
        "dlat": round(dest_lat,   4), "dlng": round(dest_lng,   4),
        "mode": mode, "acc": accessible,
    }, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def _decode_share_token(token: str) -> dict:
    try:
        padded  = token + "=" * (4 - len(token) % 4)
        payload = base64.urlsafe_b64decode(padded).decode()
        return json.loads(payload)
    except Exception as exc:
        raise ValueError(f"Invalid token: {exc}") from exc


def _build_route_response(
    alternatives: dict[str, SingleRoute],
    weather:          dict,
    elevator_alerts:  list[dict],
    req:              NavRouteRequest,
    routing_ms:       float,
    cache_hit:        bool = False,
) -> dict:
    """Assembles the complete route response payload."""
    recommended = "fastest"
    if weather["impact"] in ("high", "medium"):
        recommended = "most_covered"

    fastest = alternatives["fastest"]
    fastest_elevator_ids = {s.elevator_id for s in fastest.segments if s.elevator_id}
    relevant_alerts = [
        a for a in elevator_alerts
        if any(
            eid and eid.lower() in (
                a.get("headline", "") + a.get("short_description", "")
            ).lower()
            for eid in fastest_elevator_ids
        )
    ]

    share_token = _encode_share_token(
        req.origin.lat, req.origin.lng,
        req.destination.lat, req.destination.lng,
        req.mode, req.accessible,
    )

    eta = _compute_eta(fastest.total_time_s, req.simulated_hour)

    enriched_routes = {}
    for key, route in alternatives.items():
        segs = _enrich_segments_with_landmarks(route.segments)
        enriched_routes[key] = route.model_copy(update={"segments": segs}).model_dump()

    return {
        "routes":              enriched_routes,
        "recommended":         recommended,
        "weather":             weather,
        "elevator_alerts":     relevant_alerts,
        "weather_recommendation": weather.get("recommendation"),
        "share_token":         share_token,
        "departure_time":      eta["departure_time"],
        "arrival_time":        eta["arrival_time"],
        "routing_ms":          round(routing_ms, 1),
        "cache_hit":           cache_hit,
        "graph_mode":          "stub" if os.getenv("USE_STUB_GRAPH", "true").lower() == "true" else "real",
        "generated_at":        datetime.now(timezone.utc).isoformat(),
    }


async def _fetch_live_data() -> tuple[dict, list[dict]]:
    """Concurrently fetch weather and elevator alerts. Both degrade gracefully."""
    results = await asyncio.gather(
        fetch_wind_data(),
        fetch_elevator_outages(),
        return_exceptions=True,
    )
    wind_data = results[0] if not isinstance(results[0], Exception) else {
        "wind_speed_mph": 0, "gust_mph": 0, "temp_f": 45,
        "recommend_pedway": False, "loop_danger_summary": "Weather unavailable",
    }
    elevator_alerts = results[1] if not isinstance(results[1], Exception) else []
    return wind_data, elevator_alerts


async def _core_route(
    req: NavRouteRequest,
    G,
    weather: dict,
    elevator_alerts: list[dict],
    snap_fn,
    route_fn,
) -> tuple[dict[str, SingleRoute], float]:
    """
    Core routing logic shared by /route and /route/batch.
    Returns (alternatives, routing_ms). Raises HTTPException on failure.
    """
    G_live = _apply_elevator_alerts(G, elevator_alerts)
    hour   = req.simulated_hour if req.simulated_hour is not None else datetime.now().hour

    if not _bounds_check(req.origin.lat, req.origin.lng):
        raise HTTPException(status_code=400, detail={
            "error":   "ORIGIN_OUT_OF_BOUNDS",
            "message": "Origin is outside the Chicago Loop service area.",
            "bounds":  LOOP_BOUNDS,
        })
    if not _bounds_check(req.destination.lat, req.destination.lng):
        raise HTTPException(status_code=400, detail={
            "error":   "DESTINATION_OUT_OF_BOUNDS",
            "message": "Destination is outside the Chicago Loop service area.",
            "bounds":  LOOP_BOUNDS,
        })

    try:
        origin_id = snap_fn(req.origin.lat, req.origin.lng, G_live)
    except ValueError as e:
        raise HTTPException(status_code=400, detail={
            "error": "SNAP_FAILED_ORIGIN", "message": str(e)
        })

    try:
        dest_id = snap_fn(req.destination.lat, req.destination.lng, G_live)
    except ValueError as e:
        raise HTTPException(status_code=400, detail={
            "error": "SNAP_FAILED_DESTINATION", "message": str(e)
        })

    if origin_id == dest_id:
        raise HTTPException(status_code=400, detail={
            "error":   "SAME_LOCATION",
            "message": "Origin and destination resolve to the same node.",
        })

    t0 = time.perf_counter()
    try:
        alternatives = route_fn(
            origin_id=origin_id,
            dest_id=dest_id,
            G=G_live,
            weather=weather,
            hour=hour,
            accessible=req.accessible,
        )
    except ValueError as exc:
        err = str(exc)
        if "NO_ROUTE_FOUND" in err:
            raise HTTPException(status_code=404, detail={
                "error":      "NO_ROUTE_FOUND",
                "message":    "No route connects these two points. Try a different mode.",
                "suggestion": "Switch to 'street_only' mode if the pedway is closed.",
            })
        if "NO_ACCESSIBLE_ROUTE" in err:
            raise HTTPException(status_code=404, detail={
                "error":           "NO_ACCESSIBLE_ROUTE",
                "message":         "No fully accessible route found.",
                "elevator_alerts": elevator_alerts,
                "suggestion":      "Try an adjacent origin or destination point.",
            })
        raise HTTPException(status_code=500, detail={
            "error": "ROUTING_ENGINE_ERROR", "message": err,
        })

    routing_ms = (time.perf_counter() - t0) * 1000
    logger.info(
        "Route: %s→%s | mode=%s | acc=%s | %.1fms",
        origin_id, dest_id, req.mode, req.accessible, routing_ms,
    )
    return alternatives, routing_ms


async def _update_analytics(
    cache: Cache, req: NavRouteRequest, fastest_time: int
) -> None:
    try:
        await cache.incr("nav:stats:routes_total")
        pair = (
            f"{req.origin.lat:.3f},{req.origin.lng:.3f}"
            f"→{req.destination.lat:.3f},{req.destination.lng:.3f}"
        )
        await cache.zadd("nav:stats:popular_routes", 1.0, pair)
        await cache.lpush("nav:stats:times", fastest_time)
        await cache.ltrim("nav:stats:times", 0, 199)
        await cache.incr(f"nav:stats:mode:{req.mode}")
        if req.accessible:
            await cache.incr("nav:stats:accessible_requests")
    except Exception as e:
        logger.warning("Analytics update failed (non-fatal): %s", e)


# ── Route endpoints ────────────────────────────────────────────────────────────

@router.post("/route")
async def get_nav_route(req: NavRouteRequest, request: Request):
    """
    Compute a multi-layer pedestrian route through the Chicago Loop.

    Returns three route alternatives — **fastest**, **most_covered**,
    **fewest_changes** — with per-segment level tags, coverage percentage,
    turn-by-turn instructions, and live weather/elevator context.

    Responses are Redis-cached for 3 minutes per unique
    (origin, destination, mode, accessible, hour) key.
    """
    cache = Cache(request.app.state.redis)
    hour  = req.simulated_hour if req.simulated_hour is not None else datetime.now().hour

    cache_key = (
        f"navroute:v2:{req.origin.lat:.4f},{req.origin.lng:.4f}:"
        f"{req.destination.lat:.4f},{req.destination.lng:.4f}:"
        f"{req.mode}:{req.accessible}:{hour}"
    )

    cached = await cache.get(cache_key)
    if cached:
        cached["cache_hit"] = True
        return cached

    wind_data, elevator_alerts = await _fetch_live_data()
    weather = _wind_to_nav_weather(wind_data)

    snap_fn, route_fn = _get_routing_fns()
    G = request.app.state.graph

    alternatives, routing_ms = await _core_route(
        req, G, weather, elevator_alerts, snap_fn, route_fn
    )

    response = _build_route_response(
        alternatives, weather, elevator_alerts, req, routing_ms, cache_hit=False
    )

    await cache.set(cache_key, response, ttl=180)
    await _update_analytics(cache, req, alternatives["fastest"].total_time_s)

    return response


@router.post("/route/batch", response_model=BatchRouteResponse)
async def batch_route(req: BatchRouteRequest, request: Request):
    """
    Compute up to 5 routes in a single request.

    Each pair is routed independently. Failures are returned inline
    rather than raising an exception for the whole batch.
    Useful for pre-loading all demo scenarios at once.
    """
    wind_data, elevator_alerts = await _fetch_live_data()
    weather = _wind_to_nav_weather(wind_data)

    snap_fn, route_fn = _get_routing_fns()
    G = request.app.state.graph

    results: list[dict] = []
    failed = 0

    for idx, pair_req in enumerate(req.pairs):
        try:
            alts, ms = await _core_route(
                pair_req, G, weather, elevator_alerts, snap_fn, route_fn
            )
            results.append({
                "index":   idx,
                "success": True,
                "route":   _build_route_response(
                    alts, weather, elevator_alerts, pair_req, ms
                ),
            })
        except HTTPException as exc:
            failed += 1
            results.append({"index": idx, "success": False, "error": exc.detail})

    return BatchRouteResponse(results=results, total=len(req.pairs), failed=failed)


@router.post("/route/compare", response_model=CompareResponse)
async def compare_routes(req: CompareRequest, request: Request):
    """
    Compute street-only vs optimal routes side-by-side.

    Powers the split-screen comparison view in the frontend.
    Returns both routes + a pre-formatted verdict string for display.
    """
    wind_data, elevator_alerts = await _fetch_live_data()
    weather = _wind_to_nav_weather(wind_data)

    snap_fn, route_fn = _get_routing_fns()
    G = request.app.state.graph

    origin      = Coords(lat=req.origin["lat"],      lng=req.origin["lng"])
    destination = Coords(lat=req.destination["lat"],  lng=req.destination["lng"])

    base = NavRouteRequest(
        origin=origin, destination=destination,
        accessible=req.accessible, simulated_hour=req.simulated_hour,
    )

    street_req  = base.model_copy(update={"mode": "street_only"})
    optimal_req = base.model_copy(update={"mode": "optimal"})

    try:
        street_alts, _ = await _core_route(
            street_req, G, weather, elevator_alerts, snap_fn, route_fn
        )
    except HTTPException:
        raise HTTPException(status_code=404, detail={
            "error":   "COMPARE_STREET_FAILED",
            "message": "Could not compute a street-only route between these points.",
        })

    try:
        optimal_alts, _ = await _core_route(
            optimal_req, G, weather, elevator_alerts, snap_fn, route_fn
        )
    except HTTPException:
        raise HTTPException(status_code=404, detail={
            "error":   "COMPARE_OPTIMAL_FAILED",
            "message": "Could not compute an optimal route between these points.",
        })

    street_route  = street_alts["fastest"]
    optimal_route = optimal_alts["fastest"]

    time_saved_s   = max(0, street_route.total_time_s - optimal_route.total_time_s)
    covered_gain   = round(optimal_route.covered_pct - street_route.covered_pct, 1)
    time_saved_min = round(time_saved_s / 60, 1)

    if time_saved_s > 30 and covered_gain > 5:
        verdict = (
            f"LoopNav saves {time_saved_min} min and {covered_gain}% more shelter "
            f"via the multi-layer route."
        )
    elif time_saved_s > 30:
        verdict = f"LoopNav is {time_saved_min} min faster via the multi-layer route."
    elif covered_gain > 5:
        verdict = f"LoopNav provides {covered_gain}% more shelter at similar travel time."
    else:
        verdict = "Street-level route is already optimal for this trip."

    return CompareResponse(
        street_only=street_route,
        optimal=optimal_route,
        time_saved_s=time_saved_s,
        covered_gain_pct=max(0.0, covered_gain),
        verdict=verdict,
    )


@router.get("/route/share/{token}")
async def decode_shared_route(token: str):
    """
    Decode a route share token back to routing parameters.

    Tokens are base64-encoded route params generated by /route.
    Frontend uses these for deep-link sharing: loopnav.app/?token=...
    """
    try:
        params = _decode_share_token(token)
    except ValueError as e:
        raise HTTPException(status_code=400, detail={
            "error": "INVALID_TOKEN", "message": str(e)
        })

    return {
        "token": token,
        "params": {
            "origin":      {"lat": params["olat"], "lng": params["olng"]},
            "destination": {"lat": params["dlat"], "lng": params["dlng"]},
            "mode":        params.get("mode", "optimal"),
            "accessible":  params.get("acc", False),
        },
        "deep_link": f"/?token={token}",
    }


@router.get("/demo/{scenario}")
async def get_demo_route(scenario: str, request: Request):
    """
    Return a pre-computed demo route for the given scenario.

    Scenarios: **blizzard** | **accessible** | **commuter** | **tourist**

    Each scenario ships with a story string and pre-resolved origin/destination
    names so the frontend never needs to look them up separately.
    """
    if scenario not in DEMO_SCENARIOS:
        raise HTTPException(status_code=404, detail={
            "error":     "UNKNOWN_SCENARIO",
            "message":   f"Unknown demo scenario '{scenario}'.",
            "available": list(DEMO_SCENARIOS.keys()),
        })

    sc            = DEMO_SCENARIOS[scenario]
    origin_preset = next(p for p in PRESET_LOCATIONS if p["id"] == sc["origin_id"])
    dest_preset   = next(p for p in PRESET_LOCATIONS if p["id"] == sc["dest_id"])

    req = NavRouteRequest(
        origin=Coords(lat=origin_preset["lat"], lng=origin_preset["lng"]),
        destination=Coords(lat=dest_preset["lat"], lng=dest_preset["lng"]),
        mode=sc["mode"],
        accessible=sc["accessible"],
    )

    wind_data, elevator_alerts = await _fetch_live_data()
    weather = _wind_to_nav_weather(wind_data)

    snap_fn, route_fn = _get_routing_fns()
    G = request.app.state.graph

    try:
        alternatives, routing_ms = await _core_route(
            req, G, weather, elevator_alerts, snap_fn, route_fn
        )
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail={
            **exc.detail,
            "scenario": scenario,
            "note":     "Demo route computation failed — check graph connectivity.",
        })

    route_response = _build_route_response(
        alternatives, weather, elevator_alerts, req, routing_ms
    )

    return {
        "scenario":    scenario,
        "name":        sc["name"],
        "description": sc["description"],
        "story":       sc["story"],
        "origin":      origin_preset,
        "destination": dest_preset,
        "route":       route_response,
    }


@router.get("/locations")
async def get_locations():
    """Return all preset Loop locations for origin/destination inputs."""
    return {"locations": PRESET_LOCATIONS, "total": len(PRESET_LOCATIONS)}


@router.get("/locations/search")
async def search_locations(q: str = "", limit: int = 5):
    """
    Fuzzy search preset locations by name or tag.
    Query is matched case-insensitively against name and tags fields.
    Results are scored and sorted by match quality.
    """
    if not q or len(q.strip()) < 2:
        return {"results": PRESET_LOCATIONS[:limit], "query": q, "total": len(PRESET_LOCATIONS)}

    q_lower = q.lower().strip()
    scored: list[dict] = []

    for loc in PRESET_LOCATIONS:
        score      = 0
        name_lower = loc["name"].lower()

        if name_lower.startswith(q_lower):
            score += 100
        elif q_lower in name_lower:
            score += 60
        for tag in loc.get("tags", []):
            if q_lower in tag.lower():
                score += 40
                break
        for word in name_lower.split():
            if word.startswith(q_lower):
                score += 20
                break

        if score > 0:
            scored.append({**loc, "_score": score})

    scored.sort(key=lambda x: x["_score"], reverse=True)
    results = [{k: v for k, v in loc.items() if k != "_score"} for loc in scored[:limit]]
    return {"results": results, "query": q, "total": len(results)}


@router.post("/route/feedback")
async def submit_route_feedback(feedback: RouteFeedback, request: Request):
    """
    Accept user feedback on a computed route.

    Feedback is stored in Redis for analytics. Each rating and issue type
    is tracked as a separate counter for the analytics dashboard.
    """
    cache = Cache(request.app.state.redis)

    feedback_id = str(uuid.uuid4())
    record = {
        "id":           feedback_id,
        "route_token":  feedback.route_token,
        "rating":       feedback.rating,
        "comment":      feedback.comment,
        "issues":       feedback.issues,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
    }

    await cache.set(f"nav:feedback:{feedback_id}", record, ttl=86400 * 30)
    await cache.incr("nav:stats:feedback_total")
    await cache.incr(f"nav:stats:rating:{feedback.rating}")
    for issue in feedback.issues:
        await cache.incr(f"nav:stats:issue:{issue}")

    logger.info(
        "Route feedback: id=%s rating=%d issues=%s",
        feedback_id, feedback.rating, feedback.issues,
    )

    return {"message": "Thank you for your feedback.", "feedback_id": feedback_id}
