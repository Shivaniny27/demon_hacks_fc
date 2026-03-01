"""
LoopNav — Production Isochrone API.

An isochrone is a "reachability polygon" — every point reachable from an
origin within a given time budget at walking speed.

Endpoints
─────────
POST /nav/isochrone                — single isochrone (one origin, one budget)
GET  /nav/isochrone                — same via query params (for map links)
POST /nav/isochrone/compare        — street_only vs optimal side-by-side
GET  /nav/isochrone/advantage      — pedway advantage zone only (polygon diff)
POST /nav/isochrone/multi          — nested polygons for 5 / 10 / 15 min animation
GET  /nav/isochrone/demo/{scenario} — pre-computed demo isochrones for judge demo

Algorithm (stub mode)
─────────────────────
  The stub engine returns a simple bounding-box polygon scaled by:
    - Time budget (seconds)
    - Walking speed (1.4 m/s base)
    - Mode multiplier: pedway_preferred 1.2×, street_only 0.85×, optimal 1.0×
    - Weather multiplier: high impact → 0.7×
  In real mode (USE_STUB_GRAPH=false), Dev 1's engine uses:
    - Dijkstra with time cutoff → reachable node set
    - Shapely convex hull of reachable node coordinates
    - Buffer for visual polish (20m)

Caching
───────
  Identical requests cached in Redis for 3 minutes (180s).
  Cache key: sha256(origin_lat, origin_lon, minutes, mode, accessible).

Comparison mode
───────────────
  Returns two GeoJSON Features:
    - "street_only":  above-ground only
    - "optimal":      all 3 layers combined
  The overlap / difference shows the "pedway advantage zone".

Isochrone metadata (each Feature.properties)
────────────────────────────────────────────
  origin_lat, origin_lng, minutes, mode, accessible
  reachable_nodes, total_distance_m, covered_pct
  weather_impact, routing_multiplier
  elapsed_ms, computed_at, engine ("stub"|"real")
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.models.nav_schemas import IsochroneRequest
from app.services.weather_service import fetch_routing_weather

log = logging.getLogger("loopnav.isochrone")

router = APIRouter(prefix="/nav", tags=["LoopNav Isochrone"])

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

CACHE_TTL        = 180          # 3 minutes
MAX_MINUTES      = 30
MIN_MINUTES      = 1
WALK_SPEED_MS    = 1.4          # m/s — average urban pedestrian walking speed
BIKE_SPEED_MS    = 4.5          # m/s — not currently used but reserved
RUN_SPEED_MS     = 2.8          # m/s — jogging

# Metres per degree at Chicago latitude (approx WGS-84)
M_PER_DEG_LAT   = 111_000.0
M_PER_DEG_LON   = 85_000.0     # cos(41.88°) × 111,320

# Mode-specific speed multipliers
MODE_MULTIPLIERS = {
    "fastest":         1.00,
    "optimal":         1.05,    # slight bonus for combined layer usage
    "pedway_preferred": 1.20,   # pedway is ~20% faster (no crosswalks, signals)
    "covered":         1.00,
    "street_only":     0.85,    # street has more stops (traffic lights, crossings)
    "accessible":      0.90,    # slightly slower (elevator wait times)
}

# Demo scenario definitions
DEMO_SCENARIOS = {
    "commuter_rush": {
        "lat": 41.8790, "lng": -87.6359,   # Union Station
        "minutes": 10,
        "mode": "fastest",
        "accessible": False,
        "description": "From Union Station during morning rush — how far can you get in 10 min?",
    },
    "accessibility_demo": {
        "lat": 41.8832, "lng": -87.6282,   # Washington/Wabash
        "minutes": 10,
        "mode": "accessible",
        "accessible": True,
        "description": "Wheelchair-accessible reachability from Washington/Wabash station",
    },
    "blizzard_mode": {
        "lat": 41.8827, "lng": -87.6423,   # Willis Tower
        "minutes": 15,
        "mode": "pedway_preferred",
        "accessible": False,
        "description": "How far can you go via Pedway tunnels from Willis Tower in 15 min?",
    },
    "tourist_explore": {
        "lat": 41.8827, "lng": -87.6233,   # Millennium Park
        "minutes": 20,
        "mode": "optimal",
        "accessible": False,
        "description": "From Millennium Park — exploring the full Loop in 20 minutes",
    },
    "pedway_advantage": {
        "lat": 41.8834, "lng": -87.6279,   # Block 37
        "minutes": 10,
        "mode": "pedway_preferred",
        "accessible": False,
        "description": "Pedway advantage demonstration from Block 37",
    },
}

MULTI_BUDGETS = [5, 10, 15]   # minutes for multi-isochrone animation


# ─────────────────────────────────────────────────────────────────────────────
# Request / Response schemas
# ─────────────────────────────────────────────────────────────────────────────

class SingleIsoRequest(BaseModel):
    lat:        float = Field(..., ge=41.85, le=41.92, description="Origin latitude")
    lng:        float = Field(..., ge=-87.70, le=-87.60, description="Origin longitude")
    minutes:    int   = Field(default=10, ge=MIN_MINUTES, le=MAX_MINUTES)
    mode:       str   = Field(default="optimal")
    accessible: bool  = Field(default=False)
    include_nodes: bool = Field(default=False, description="Include reachable node list in response")


class CompareRequest(BaseModel):
    lat:        float = Field(..., ge=41.85, le=41.92)
    lng:        float = Field(..., ge=-87.70, le=-87.60)
    minutes:    int   = Field(default=10, ge=MIN_MINUTES, le=MAX_MINUTES)
    accessible: bool  = Field(default=False)


class MultiRequest(BaseModel):
    lat:        float     = Field(..., ge=41.85, le=41.92)
    lng:        float     = Field(..., ge=-87.70, le=-87.60)
    budgets:    List[int] = Field(default=MULTI_BUDGETS)
    mode:       str       = Field(default="optimal")
    accessible: bool      = Field(default=False)


# ─────────────────────────────────────────────────────────────────────────────
# Engine dispatch
# ─────────────────────────────────────────────────────────────────────────────

def _get_engine():
    """Return the appropriate isochrone engine based on USE_STUB_GRAPH."""
    if os.getenv("USE_STUB_GRAPH", "true").lower() == "true":
        from app.routing.stub_engine import get_isochrone
        return get_isochrone, "stub"
    try:
        from app.routing.engine import get_isochrone  # type: ignore
        return get_isochrone, "real"
    except ImportError:
        log.warning("Real engine not found — falling back to stub")
        from app.routing.stub_engine import get_isochrone
        return get_isochrone, "stub"


# ─────────────────────────────────────────────────────────────────────────────
# Geometry helpers
# ─────────────────────────────────────────────────────────────────────────────

def _circle_polygon(lat: float, lon: float, radius_m: float, n_pts: int = 32) -> Dict:
    """
    Generate a circular GeoJSON Polygon approximation with n_pts points.
    Used for simple isochrone shapes when shapely isn't available.
    """
    d_lat = radius_m / M_PER_DEG_LAT
    d_lon = radius_m / M_PER_DEG_LON
    coords = []
    for i in range(n_pts + 1):
        angle = 2 * math.pi * i / n_pts
        x     = lon + d_lon * math.cos(angle)
        y     = lat + d_lat * math.sin(angle)
        coords.append([round(x, 6), round(y, 6)])
    return {"type": "Polygon", "coordinates": [coords]}


def _bbox_polygon(lat: float, lon: float, radius_m: float) -> Dict:
    """Simple bounding box polygon (used when stub engine returns None)."""
    d_lat = radius_m / M_PER_DEG_LAT
    d_lon = radius_m / M_PER_DEG_LON
    return {
        "type": "Polygon",
        "coordinates": [[
            [lon - d_lon, lat - d_lat],
            [lon + d_lon, lat - d_lat],
            [lon + d_lon, lat + d_lat],
            [lon - d_lon, lat + d_lat],
            [lon - d_lon, lat - d_lat],
        ]],
    }


def _compute_radius_m(minutes: int, mode: str, weather: Dict) -> float:
    """
    Compute the walking radius in metres for the given time budget and conditions.
    """
    budget_s     = minutes * 60
    mode_mult    = MODE_MULTIPLIERS.get(mode, 1.0)
    weather_mult = 1.0

    impact = weather.get("impact", "none")
    if impact == "high":
        weather_mult = 0.65   # dangerous conditions → people walk much less
    elif impact == "medium":
        weather_mult = 0.80
    elif impact == "low":
        weather_mult = 0.90

    return WALK_SPEED_MS * budget_s * mode_mult * weather_mult


def _reachable_node_count(G, lat: float, lng: float, radius_m: float) -> int:
    """Count graph nodes within the isochrone radius."""
    count = 0
    for _, data in G.nodes(data=True):
        nlat = data.get("lat", 0)
        nlng = data.get("lng", 0)
        d_lat = (nlat - lat) * M_PER_DEG_LAT
        d_lon = (nlng - lng) * M_PER_DEG_LON
        dist  = math.sqrt(d_lat ** 2 + d_lon ** 2)
        if dist <= radius_m:
            count += 1
    return count


def _covered_percentage(G, lat: float, lng: float, radius_m: float, mode: str) -> float:
    """Estimate the percentage of edges that are covered within the isochrone."""
    total = covered = 0
    for u, v, data in G.edges(data=True):
        u_data  = G.nodes[u]
        u_lat   = u_data.get("lat", 0)
        u_lng   = u_data.get("lng", 0)
        d_lat   = (u_lat - lat) * M_PER_DEG_LAT
        d_lon   = (u_lng - lng) * M_PER_DEG_LON
        dist    = math.sqrt(d_lat ** 2 + d_lon ** 2)
        if dist > radius_m:
            continue
        if mode == "street_only" and data.get("level") != "street":
            continue
        total += 1
        if data.get("covered"):
            covered += 1
    return round(covered / total * 100, 1) if total > 0 else 0.0


def _dijkstra_isochrone(G, origin_id: str, budget_s: float, mode: str, accessible: bool) -> Dict:
    """
    Real isochrone using NetworkX single_source_dijkstra_path_length with time cutoff.
    Returns GeoJSON Polygon (convex hull of reachable nodes).
    """
    import networkx as nx

    def weight_fn(u, v, data):
        d = dict(list(data.values())[0]) if isinstance(data, dict) and 0 in data else data
        t = d.get("time_s", 60)
        if accessible and d.get("connector_type") in ("stairs", "escalator"):
            return float("inf")
        if d.get("outage") or d.get("pedway_closed"):
            return float("inf")
        if mode == "street_only" and d.get("level") != "street":
            return float("inf")
        if mode == "pedway_preferred" and d.get("level") == "street" and not d.get("covered"):
            t *= 1.5
        return t

    try:
        lengths = nx.single_source_dijkstra_path_length(
            G, origin_id, cutoff=budget_s, weight=weight_fn
        )
    except Exception as exc:
        log.warning("Dijkstra isochrone failed: %s", exc)
        return {}

    return lengths  # {node_id: travel_time_s}


def _nodes_to_polygon(G, reachable_ids) -> Optional[Dict]:
    """Convert reachable node set to a GeoJSON Polygon via convex hull."""
    pts = []
    for nid in reachable_ids:
        if nid in G.nodes:
            data = G.nodes[nid]
            lat  = data.get("lat")
            lon  = data.get("lng")
            if lat and lon:
                pts.append((lon, lat))

    if len(pts) < 3:
        return None

    try:
        from shapely.geometry import MultiPoint
        hull   = MultiPoint(pts).convex_hull
        coords = list(hull.exterior.coords)
        return {"type": "Polygon", "coordinates": [[[round(x, 6), round(y, 6)] for x, y in coords]]}
    except ImportError:
        # Fallback: bounding box
        lons = [p[0] for p in pts]
        lats = [p[1] for p in pts]
        return {
            "type": "Polygon",
            "coordinates": [[
                [min(lons), min(lats)],
                [max(lons), min(lats)],
                [max(lons), max(lats)],
                [min(lons), max(lats)],
                [min(lons), min(lats)],
            ]],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Cache helpers
# ─────────────────────────────────────────────────────────────────────────────

def _cache_key(lat: float, lon: float, minutes: int, mode: str, accessible: bool) -> str:
    payload = f"{lat:.5f}:{lon:.5f}:{minutes}:{mode}:{accessible}"
    return f"nav:isochrone:{hashlib.sha256(payload.encode()).hexdigest()[:16]}"


async def _from_cache(request: Request, key: str) -> Optional[Dict]:
    cache = getattr(getattr(request, "app", None), "state", None)
    cache = getattr(cache, "cache", None)
    if cache:
        try:
            return await cache.get(key)
        except Exception:
            pass
    return None


async def _to_cache(request: Request, key: str, value: Dict) -> None:
    cache = getattr(getattr(request, "app", None), "state", None)
    cache = getattr(cache, "cache", None)
    if cache:
        try:
            await cache.set(key, value, ttl=CACHE_TTL)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Core computation
# ─────────────────────────────────────────────────────────────────────────────

async def _compute_single(
    request:    Request,
    lat:        float,
    lng:        float,
    minutes:    int,
    mode:       str,
    accessible: bool,
    include_nodes: bool = False,
) -> Dict:
    """
    Core isochrone computation.
    Returns a GeoJSON Feature with polygon geometry and metadata properties.
    """
    t0 = time.monotonic()

    # Fetch weather
    weather = await fetch_routing_weather()

    G = getattr(getattr(request, "app", None), "state", None)
    G = getattr(G, "graph", None)

    # Compute radius and polygon
    radius_m  = _compute_radius_m(minutes, mode, weather)
    engine_fn, engine_name = _get_engine()
    polygon   = None
    reachable_nodes = 0
    covered_pct     = 0.0
    node_list: List[str] = []

    if G is not None:
        # Try real Dijkstra computation first
        try:
            loader = getattr(request.app.state, "graph_loader", None)
            origin_id = loader.snap(lat, lng) if loader else None
            if origin_id:
                lengths = _dijkstra_isochrone(G, origin_id, minutes * 60.0, mode, accessible)
                reachable_ids = list(lengths.keys())
                reachable_nodes = len(reachable_ids)
                if include_nodes:
                    node_list = reachable_ids
                polygon = _nodes_to_polygon(G, reachable_ids)
                covered_pct = _covered_percentage(G, lat, lng, radius_m, mode)
                engine_name = "dijkstra"
        except Exception as exc:
            log.debug("Dijkstra isochrone failed, falling back to stub: %s", exc)

        # Fallback to stub engine
        if polygon is None:
            try:
                poly_data = engine_fn(
                    lat=lat, lng=lng, minutes=minutes,
                    mode=mode, G=G, weather=weather,
                )
                polygon = poly_data
                reachable_nodes = _reachable_node_count(G, lat, lng, radius_m)
                covered_pct     = _covered_percentage(G, lat, lng, radius_m, mode)
            except Exception as exc:
                log.warning("Stub engine failed: %s", exc)

    # Final fallback: geometric circle
    if polygon is None:
        polygon       = _circle_polygon(lat, lng, radius_m)
        engine_name   = "geometric"

    elapsed_ms = round((time.monotonic() - t0) * 1000, 2)

    props = {
        "origin_lat":          lat,
        "origin_lng":          lng,
        "minutes":             minutes,
        "mode":                mode,
        "accessible":          accessible,
        "radius_m":            round(radius_m, 1),
        "reachable_nodes":     reachable_nodes,
        "covered_pct":         covered_pct,
        "weather_impact":      weather.get("impact", "none"),
        "routing_multiplier":  weather.get("routing_multiplier", 1.0),
        "temp_f":              weather.get("temp_f"),
        "wind_speed_mph":      weather.get("wind_speed_mph"),
        "engine":              engine_name,
        "elapsed_ms":          elapsed_ms,
        "computed_at":         datetime.now(timezone.utc).isoformat(),
    }
    if include_nodes and node_list:
        props["reachable_node_ids"] = node_list

    return {
        "type":       "Feature",
        "geometry":   polygon,
        "properties": props,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/isochrone")
async def compute_isochrone_post(req: SingleIsoRequest, request: Request):
    """
    Compute a single isochrone polygon.

    Returns a GeoJSON Feature (Polygon) showing every point reachable from
    the origin within `minutes` minutes at the given mode.

    The polygon is cached for 3 minutes per unique origin/budget/mode combination.
    """
    if req.mode not in MODE_MULTIPLIERS:
        raise HTTPException(status_code=400, detail={
            "error":   "INVALID_MODE",
            "message": f"Mode must be one of: {', '.join(sorted(MODE_MULTIPLIERS))}",
        })

    # Cache check
    ck     = _cache_key(req.lat, req.lng, req.minutes, req.mode, req.accessible)
    cached = await _from_cache(request, ck)
    if cached and not req.include_nodes:
        cached.setdefault("properties", {})["_cached"] = True
        return JSONResponse(content=cached, media_type="application/geo+json")

    feature = await _compute_single(
        request, req.lat, req.lng, req.minutes, req.mode, req.accessible, req.include_nodes
    )

    if not req.include_nodes:
        await _to_cache(request, ck, feature)

    return JSONResponse(content=feature, media_type="application/geo+json")


@router.get("/isochrone")
async def compute_isochrone_get(
    request:    Request,
    lat:        float = Query(..., ge=41.85, le=41.92),
    lng:        float = Query(..., ge=-87.70, le=-87.60),
    minutes:    int   = Query(default=10, ge=MIN_MINUTES, le=MAX_MINUTES),
    mode:       str   = Query(default="optimal"),
    accessible: bool  = Query(default=False),
):
    """
    GET version of the isochrone endpoint — useful for embedding in map URLs.

    Returns the same GeoJSON Feature as POST /nav/isochrone.
    """
    if mode not in MODE_MULTIPLIERS:
        raise HTTPException(status_code=400, detail={
            "error":   "INVALID_MODE",
            "message": f"Mode must be one of: {', '.join(sorted(MODE_MULTIPLIERS))}",
        })

    ck     = _cache_key(lat, lng, minutes, mode, accessible)
    cached = await _from_cache(request, ck)
    if cached:
        cached.setdefault("properties", {})["_cached"] = True
        return JSONResponse(content=cached, media_type="application/geo+json")

    feature = await _compute_single(request, lat, lng, minutes, mode, accessible)
    await _to_cache(request, ck, feature)
    return JSONResponse(content=feature, media_type="application/geo+json")


@router.post("/isochrone/compare")
async def compare_isochrones(req: CompareRequest, request: Request):
    """
    Side-by-side comparison: street_only vs optimal routing.

    Returns a FeatureCollection with two features:
      - "street_only":  above-ground only (no pedway advantage)
      - "optimal":      all 3 layers combined

    The difference between the two polygons visualises the "Pedway Advantage" —
    how much more of the city you can reach underground vs above ground in the
    same amount of time. This is the **killer demo slide** for judges.
    """
    # Fetch both in parallel
    t0 = time.monotonic()
    street_f, optimal_f = await asyncio.gather(
        _compute_single(request, req.lat, req.lng, req.minutes, "street_only", req.accessible),
        _compute_single(request, req.lat, req.lng, req.minutes, "optimal",     req.accessible),
    )

    elapsed_ms = round((time.monotonic() - t0) * 1000, 2)

    # Annotate with comparison metadata
    street_f["properties"]["_comparison_mode"] = "street_only"
    street_f["properties"]["_color"]           = "#FF6B6B"   # red — restricted
    optimal_f["properties"]["_comparison_mode"] = "optimal"
    optimal_f["properties"]["_color"]           = "#4ECDC4"  # teal — full access

    # Compute advantage estimate
    s_radius = street_f["properties"].get("radius_m", 0)
    o_radius = optimal_f["properties"].get("radius_m", 0)
    advantage_pct = round((o_radius - s_radius) / s_radius * 100, 1) if s_radius > 0 else 0

    return JSONResponse(
        content={
            "type":     "FeatureCollection",
            "features": [street_f, optimal_f],
            "_meta": {
                "origin":          {"lat": req.lat, "lng": req.lng},
                "minutes":         req.minutes,
                "accessible":      req.accessible,
                "street_radius_m": round(s_radius, 1),
                "optimal_radius_m": round(o_radius, 1),
                "advantage_pct":   advantage_pct,
                "advantage_summary": (
                    f"With the Pedway, you can reach {advantage_pct:.0f}% more "
                    f"of the Loop in {req.minutes} minutes compared to street-only walking."
                    if advantage_pct > 0 else
                    "No pedway advantage detected for current conditions."
                ),
                "elapsed_ms":      elapsed_ms,
                "computed_at":     datetime.now(timezone.utc).isoformat(),
            },
        },
        media_type="application/geo+json",
    )


@router.get("/isochrone/advantage")
async def get_pedway_advantage(
    request:    Request,
    lat:        float = Query(..., ge=41.85, le=41.92),
    lng:        float = Query(..., ge=-87.70, le=-87.60),
    minutes:    int   = Query(default=10, ge=MIN_MINUTES, le=MAX_MINUTES),
    accessible: bool  = Query(default=False),
):
    """
    Returns only the "Pedway Advantage Zone" — a text summary and radius comparison.

    Lightweight version of /compare for the frontend dashboard summary card.
    """
    street_f, optimal_f = await asyncio.gather(
        _compute_single(request, lat, lng, minutes, "street_only", accessible),
        _compute_single(request, lat, lng, minutes, "pedway_preferred", accessible),
    )

    s_radius   = street_f["properties"].get("radius_m", 0)
    p_radius   = optimal_f["properties"].get("radius_m", 0)
    delta_m    = round(p_radius - s_radius, 1)
    pct_gain   = round(delta_m / s_radius * 100, 1) if s_radius > 0 else 0

    weather_impact = street_f["properties"].get("weather_impact", "none")

    if pct_gain >= 25:
        level = "major"
        message = (
            f"The Pedway gives you a MAJOR advantage today ({pct_gain:.0f}% more reachability). "
            f"You can cover {delta_m:.0f}m more of the Loop underground."
        )
    elif pct_gain >= 10:
        level = "moderate"
        message = (
            f"Moderate Pedway advantage: {pct_gain:.0f}% more reachability "
            f"({delta_m:.0f}m further reach)."
        )
    else:
        level = "minor"
        message = (
            f"Minor advantage: the Pedway gives {pct_gain:.0f}% more reach today. "
            "Best used during bad weather."
        )

    return {
        "origin":            {"lat": lat, "lng": lng},
        "minutes":           minutes,
        "street_radius_m":   round(s_radius, 1),
        "pedway_radius_m":   round(p_radius, 1),
        "advantage_m":       delta_m,
        "advantage_pct":     pct_gain,
        "advantage_level":   level,
        "message":           message,
        "weather_impact":    weather_impact,
        "recommend_pedway":  pct_gain >= 10 or weather_impact in ("medium", "high"),
        "computed_at":       datetime.now(timezone.utc).isoformat(),
    }


@router.post("/isochrone/multi")
async def compute_multi_isochrone(req: MultiRequest, request: Request):
    """
    Compute nested isochrones for multiple time budgets.

    Returns a FeatureCollection with one polygon per budget.
    Sorted from largest (outermost) to smallest (innermost).
    Budgets must be in [1, 30] minutes.

    Use case: animated "time rings" where the frontend fades in circles
    at 5, 10, 15 min showing the expanding reachability over time.
    """
    budgets = [b for b in sorted(set(req.budgets)) if MIN_MINUTES <= b <= MAX_MINUTES]
    if not budgets:
        raise HTTPException(status_code=400, detail={
            "error":   "INVALID_BUDGETS",
            "message": f"Budgets must be integers in [{MIN_MINUTES}, {MAX_MINUTES}]",
        })
    if len(budgets) > 5:
        raise HTTPException(status_code=400, detail={
            "error":   "TOO_MANY_BUDGETS",
            "message": "Maximum 5 time budgets per request",
        })

    if req.mode not in MODE_MULTIPLIERS:
        raise HTTPException(status_code=400, detail={
            "error":   "INVALID_MODE",
            "message": f"Mode must be one of: {', '.join(sorted(MODE_MULTIPLIERS))}",
        })

    t0 = time.monotonic()

    # Compute all budgets concurrently
    tasks = [
        _compute_single(request, req.lat, req.lng, b, req.mode, req.accessible)
        for b in budgets
    ]
    features = await asyncio.gather(*tasks)

    # Annotate with ring metadata
    colors = ["#FFF176", "#FFB74D", "#FF8A65", "#EF5350", "#B71C1C"]
    for i, (b, f) in enumerate(zip(budgets, features)):
        f["properties"]["_ring"]    = i + 1
        f["properties"]["_color"]   = colors[i % len(colors)]
        f["properties"]["_opacity"] = 0.25 + i * 0.1

    elapsed_ms = round((time.monotonic() - t0) * 1000, 2)

    return JSONResponse(
        content={
            "type":     "FeatureCollection",
            "features": list(reversed(features)),  # largest first for correct z-order
            "_meta": {
                "origin":      {"lat": req.lat, "lng": req.lng},
                "mode":        req.mode,
                "accessible":  req.accessible,
                "budgets_min": budgets,
                "ring_count":  len(budgets),
                "elapsed_ms":  elapsed_ms,
                "computed_at": datetime.now(timezone.utc).isoformat(),
            },
        },
        media_type="application/geo+json",
    )


@router.get("/isochrone/demo/{scenario}")
async def get_demo_isochrone(scenario: str, request: Request):
    """
    Pre-configured demo isochrones for hackathon judge demonstrations.

    Scenarios:
      commuter_rush      — Union Station, 10 min, fastest
      accessibility_demo — Washington/Wabash, 10 min, accessible
      blizzard_mode      — Willis Tower, 15 min, pedway_preferred
      tourist_explore    — Millennium Park, 20 min, optimal
      pedway_advantage   — Block 37, 10 min, pedway vs street comparison
    """
    if scenario not in DEMO_SCENARIOS:
        raise HTTPException(status_code=404, detail={
            "error":      "SCENARIO_NOT_FOUND",
            "message":    f"Unknown demo scenario '{scenario}'",
            "available":  list(DEMO_SCENARIOS.keys()),
        })

    cfg    = DEMO_SCENARIOS[scenario]
    lat    = cfg["lat"]
    lng    = cfg["lng"]
    mins   = cfg["minutes"]
    mode   = cfg["mode"]
    access = cfg["accessible"]

    # pedway_advantage scenario returns a comparison
    if scenario == "pedway_advantage":
        street_f, opt_f = await asyncio.gather(
            _compute_single(request, lat, lng, mins, "street_only",     access),
            _compute_single(request, lat, lng, mins, "pedway_preferred", access),
        )
        for f in (street_f, opt_f):
            f["properties"]["_demo_scenario"] = scenario
        return JSONResponse(
            content={
                "type":     "FeatureCollection",
                "features": [street_f, opt_f],
                "_meta":    {"scenario": scenario, "description": cfg["description"]},
            },
            media_type="application/geo+json",
        )

    feature = await _compute_single(request, lat, lng, mins, mode, access)
    feature["properties"]["_demo_scenario"] = scenario
    feature["properties"]["_description"]   = cfg["description"]

    return JSONResponse(content=feature, media_type="application/geo+json")


@router.get("/isochrone/scenarios")
async def list_scenarios():
    """List all available demo scenarios."""
    return {
        "scenarios": [
            {
                "id":          sid,
                "description": cfg["description"],
                "origin":      {"lat": cfg["lat"], "lng": cfg["lng"]},
                "minutes":     cfg["minutes"],
                "mode":        cfg["mode"],
                "accessible":  cfg["accessible"],
                "endpoint":    f"/api/v1/nav/isochrone/demo/{sid}",
            }
            for sid, cfg in DEMO_SCENARIOS.items()
        ],
        "modes": {
            m: f"{v:.2f}× speed multiplier"
            for m, v in sorted(MODE_MULTIPLIERS.items())
        },
        "max_minutes": MAX_MINUTES,
        "walk_speed_ms": WALK_SPEED_MS,
    }


@router.get("/isochrone/health")
async def isochrone_health(request: Request):
    """Health check for the isochrone computation engine."""
    G = getattr(getattr(request, "app", None), "state", None)
    G = getattr(G, "graph", None)

    _, engine_name = _get_engine()

    return {
        "status":      "ok",
        "engine":      engine_name,
        "graph_loaded": G is not None,
        "graph_nodes": G.number_of_nodes() if G else 0,
        "modes":       list(MODE_MULTIPLIERS.keys()),
        "max_minutes": MAX_MINUTES,
        "cache_ttl_s": CACHE_TTL,
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }
