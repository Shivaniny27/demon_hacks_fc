"""
station.py — Loop Station Intelligence API
==========================================
Deep station knowledge: platform tips, ADA entrance routing,
Pedway connections, nearby buildings, transfer advice, and
station ranking for a given origin position.

All data flows from station_service.py.

Endpoints:
  GET  /nav/station/all                         — all Loop stations summary
  GET  /nav/station/{station_key}               — full station profile
  GET  /nav/station/{station_key}/entrances     — all entrances detail
  GET  /nav/station/{station_key}/platform-tip  — boarding advice for destination
  GET  /nav/station/{station_key}/accessible-entrance — best accessible entrance
  GET  /nav/station/pedway                      — Pedway-connected stations
  GET  /nav/station/rank                        — rank stations from a position
  GET  /nav/station/nearest                     — nearest Loop stations
  GET  /nav/station/search                      — search by name
  GET  /nav/station/lines/{line}               — stations on a specific line
  GET  /nav/station/health                      — service health
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from app.services.station_service import (
    EntranceType,
    PlatformEnd,
    StationProfile,
    StationEntrance,
    PlatformTip,
    STATION_PROFILES,
    get_station_service,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Station Intelligence"])

_svc = get_station_service()


# ── Serializer helpers ────────────────────────────────────────────────────────

def _entrance_out(e: StationEntrance) -> Dict[str, Any]:
    return {
        "entrance_id":    e.entrance_id,
        "name":           e.name,
        "type":           e.type.value,
        "lat":            e.lat,
        "lon":            e.lon,
        "accessible":     e.accessible,
        "has_elevator":   e.has_elevator,
        "elevator_id":    e.elevator_id,
        "has_farecard":   e.has_farecard,
        "open_24h":       e.open_24h,
        "open_time":      e.open_time,
        "close_time":     e.close_time,
        "pedway_segment": e.pedway_segment,
        "notes":          e.notes,
    }


def _platform_tip_out(t: PlatformTip) -> Dict[str, Any]:
    return {
        "destination_station": t.destination_station,
        "destination_name":    t.destination_name,
        "line":                t.line,
        "direction":           t.direction,
        "board_end":           t.board_end.value,
        "exit_tip":            t.exit_tip,
        "time_saved_seconds":  t.time_saved_seconds,
        "exit_to_pedway":      t.exit_to_pedway,
        "exit_landmark":       t.exit_landmark,
    }


def _profile_summary(key: str, p: StationProfile) -> Dict[str, Any]:
    return {
        "station_key":       key,
        "name":              p.name,
        "short_name":        p.short_name,
        "map_id":            p.map_id,
        "lines":             p.lines,
        "lat":               p.lat,
        "lon":               p.lon,
        "platform_level":    p.platform_level,
        "pedway_connected":  p.pedway_connected,
        "pedway_entrance":   p.pedway_entrance,
        "ada_compliant":     p.ada_compliant,
        "elevator_count":    len(p.elevator_ids),
        "weather_shelter":   p.weather_shelter,
        "overall_rating":    p.overall_rating.value,
        "rating_notes":      p.rating_notes,
        "nearby_landmarks":  p.nearby_landmarks[:2],
        "transfer_stations": p.transfer_stations,
    }


def _profile_full(key: str, p: StationProfile) -> Dict[str, Any]:
    return {
        **_profile_summary(key, p),
        "entrances":         [_entrance_out(e) for e in p.entrances],
        "accessible_entrances": [_entrance_out(e) for e in p.accessible_entrances],
        "platform_layout": {
            "platform_length_cars": p.platform_layout.platform_length_cars,
            "has_center_platform":  p.platform_layout.has_center_platform,
            "has_side_platform":    p.platform_layout.has_side_platform,
            "north_end_access":     p.platform_layout.north_end_access,
            "south_end_access":     p.platform_layout.south_end_access,
            "mezzanine_level":      p.platform_layout.mezzanine_level,
            "typical_crowding":     p.platform_layout.typical_crowding,
        },
        "platform_tips":     [_platform_tip_out(t) for t in p.platform_tips],
        "pedway_segments":   p.pedway_segments,
        "nearby_buildings":  p.nearby_buildings,
        "nearby_landmarks":  p.nearby_landmarks,
        "elevator_ids":      p.elevator_ids,
        "tunnel_connection": p.tunnel_connection,
    }


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/nav/station/all", summary="All Loop stations summary")
async def get_all_stations(
    pedway_only:  bool = Query(False, description="Only return Pedway-connected stations"),
    accessible:   bool = Query(False, description="Only return ADA-compliant stations"),
    line:         Optional[str] = Query(None, description="Filter by CTA line"),
) -> Dict[str, Any]:
    """
    Returns summary data for all Chicago Loop L stations.
    """
    profiles = STATION_PROFILES.copy()

    if pedway_only:
        profiles = {k: p for k, p in profiles.items() if p.pedway_connected}
    if accessible:
        profiles = {k: p for k, p in profiles.items() if p.ada_compliant}
    if line:
        profiles = {k: p for k, p in profiles.items() if line in p.lines}

    stations_out = [_profile_summary(k, p) for k, p in profiles.items()]
    stations_out.sort(key=lambda x: x["name"])

    return {
        "total":    len(stations_out),
        "stations": stations_out,
    }


@router.get("/nav/station/pedway", summary="Pedway-connected Loop stations")
async def get_pedway_stations() -> Dict[str, Any]:
    """
    Returns all Loop L stations that have a direct Pedway tunnel connection.
    These stations allow you to arrive underground without touching the street.
    """
    pedway_keys = _svc.get_pedway_stations()
    stations    = []
    for key in pedway_keys:
        p = STATION_PROFILES.get(key)
        if not p:
            continue
        entrance = _svc.get_pedway_entrance(key)
        stations.append({
            "station_key":      key,
            "station_name":     p.name,
            "lines":            p.lines,
            "lat":              p.lat,
            "lon":              p.lon,
            "pedway_entrance":  p.pedway_entrance,
            "pedway_segments":  p.pedway_segments,
            "entrance_details": _entrance_out(entrance) if entrance else None,
            "platform_level":   p.platform_level,
            "ada_compliant":    p.ada_compliant,
        })

    return {
        "pedway_connected_count": len(stations),
        "stations": stations,
    }


@router.get("/nav/station/rank", summary="Rank Loop stations from a given position")
async def rank_stations(
    lat:        float = Query(..., description="Current latitude"),
    lon:        float = Query(..., description="Current longitude"),
    lines:      Optional[str] = Query(None, description="Comma-separated CTA lines to filter"),
    accessible: bool  = Query(False, description="Only accessible stations"),
    pedway:     bool  = Query(True,  description="Prefer Pedway-connected stations"),
    max_walk_m: float = Query(1000.0, ge=100, le=3000, description="Max walk distance in meters"),
) -> Dict[str, Any]:
    """
    Ranks all reachable Loop stations from your current location.
    Scoring considers: distance, Pedway bonus, ADA status, weather shelter, overall rating.
    """
    line_filter = [l.strip() for l in lines.split(",")] if lines else None
    ranked      = _svc.rank_stations(lat, lon, line_filter, accessible, pedway, max_walk_m)

    return {
        "origin_lat":      lat,
        "origin_lon":      lon,
        "max_walk_m":      max_walk_m,
        "reachable_count": len(ranked),
        "ranked_stations": ranked,
    }


@router.get("/nav/station/nearest", summary="Nearest Loop stations from a position")
async def get_nearest_stations(
    lat:   float = Query(...),
    lon:   float = Query(...),
    limit: int   = Query(3, ge=1, le=10),
) -> Dict[str, Any]:
    """
    Returns the N nearest Loop L stations to a given lat/lon.
    Simple distance-based (no scoring).
    """
    nearest = _svc.get_nearest(lat, lon, limit=limit)
    results = []
    for key, dist_m in nearest:
        p = STATION_PROFILES.get(key)
        if not p:
            continue
        results.append({
            "station_key":    key,
            "station_name":   p.name,
            "distance_m":     round(dist_m, 1),
            "distance_min":   round(dist_m / (1.2 * 60), 1),  # approx walk minutes
            "lines":          p.lines,
            "lat":            p.lat,
            "lon":            p.lon,
            "pedway_connected": p.pedway_connected,
            "ada_compliant":  p.ada_compliant,
        })

    return {
        "origin_lat": lat,
        "origin_lon": lon,
        "nearest":    results,
    }


@router.get("/nav/station/search", summary="Search Loop stations by name")
async def search_stations(
    q: str = Query(..., min_length=2, description="Search query"),
) -> Dict[str, Any]:
    """Search Loop stations by name fragment."""
    keys     = _svc.search_by_name(q)
    stations = [_profile_summary(k, STATION_PROFILES[k]) for k in keys if k in STATION_PROFILES]

    return {
        "query":   q,
        "count":   len(stations),
        "results": stations,
    }


@router.get("/nav/station/lines/{line}", summary="All Loop stops on a specific CTA line")
async def get_line_stations(line: str) -> Dict[str, Any]:
    """
    Returns all Chicago Loop stations served by a specific CTA line.
    """
    valid = {"Red", "Blue", "Brown", "Green", "Orange", "Pink", "Purple"}
    if line not in valid:
        raise HTTPException(status_code=400, detail=f"Invalid line. Valid: {sorted(valid)}")

    stations = [
        _profile_summary(k, p)
        for k, p in STATION_PROFILES.items()
        if line in p.lines
    ]
    stations.sort(key=lambda x: x["name"])

    return {
        "line":    line,
        "count":   len(stations),
        "stations": stations,
    }


@router.get("/nav/station/{station_key}", summary="Full station intelligence profile")
async def get_station(station_key: str) -> Dict[str, Any]:
    """
    Returns the complete intelligence profile for a Loop L station.
    Includes: entrances, platform layout, platform tips, Pedway info,
    nearby buildings, ADA details, transfer stations.
    """
    if station_key not in STATION_PROFILES:
        raise HTTPException(
            status_code=404,
            detail=f"Station '{station_key}' not found. Valid keys: {list(STATION_PROFILES.keys())}",
        )
    profile = STATION_PROFILES[station_key]
    return _profile_full(station_key, profile)


@router.get("/nav/station/{station_key}/entrances", summary="Station entrances detail")
async def get_station_entrances(station_key: str) -> Dict[str, Any]:
    """
    Returns all entrance details for a Loop station, including:
    - Pedway tunnel entrances (with segment IDs)
    - ADA elevator entrances (with elevator IDs)
    - Street-level stair entrances
    - Operating hours per entrance
    """
    if station_key not in STATION_PROFILES:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")

    p = STATION_PROFILES[station_key]
    entrances_out = [_entrance_out(e) for e in p.entrances]

    return {
        "station_key":   station_key,
        "station_name":  p.name,
        "total_entrances":     len(entrances_out),
        "accessible_count":    len(p.accessible_entrances),
        "pedway_connected":    p.pedway_connected,
        "pedway_entrance_desc": p.pedway_entrance,
        "entrances":           entrances_out,
    }


@router.get("/nav/station/{station_key}/platform-tip", summary="Boarding advice for a destination")
async def get_platform_tip(
    station_key:        str,
    destination:        str = Query(..., description="Destination station key"),
    line:               Optional[str] = Query(None, description="CTA line"),
) -> Dict[str, Any]:
    """
    Returns advice on which part of the platform to board at a Loop station
    to optimally exit at a specific destination.

    Example: "Board the rear car at Clark/Lake to exit nearest the stairs at State/Lake."
    Saves 30–60 seconds per journey — meaningful for daily commuters.
    """
    if station_key not in STATION_PROFILES:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")

    tip = _svc.get_platform_tip(station_key, destination, line)

    if tip:
        return {
            "boarding_station":    station_key,
            "destination_station": destination,
            "destination_name":    tip.destination_name,
            "tip_found":           True,
            "tip":                 _platform_tip_out(tip),
        }
    else:
        # Return all available tips for this boarding station
        all_tips = _svc.get_platform_tip.__self__._tip_engine.get_all_tips(station_key)
        return {
            "boarding_station":    station_key,
            "destination_station": destination,
            "tip_found":           False,
            "message":             f"No specific tip for {station_key} → {destination}. Showing all available tips.",
            "available_tips":      [_platform_tip_out(t) for t in all_tips],
        }


@router.get("/nav/station/{station_key}/accessible-entrance", summary="Best accessible entrance")
async def get_accessible_entrance(
    station_key:    str,
    prefer_pedway:  bool = Query(True, description="Prefer Pedway-connected entrance"),
) -> Dict[str, Any]:
    """
    Returns the recommended accessible (ADA) entrance for a station.
    If prefer_pedway is True, returns a Pedway tunnel entrance when available
    — so wheelchair users can stay underground the entire journey.
    """
    if station_key not in STATION_PROFILES:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")

    entrance = _svc.get_accessible_entrance(station_key, prefer_pedway)
    p        = STATION_PROFILES[station_key]

    if not entrance:
        return {
            "station_key":  station_key,
            "station_name": p.name,
            "found":        False,
            "message":      "No accessible entrance data available for this station.",
        }

    return {
        "station_key":    station_key,
        "station_name":   p.name,
        "found":          True,
        "preferred_entrance": _entrance_out(entrance),
        "total_accessible": len(p.accessible_entrances),
        "all_accessible": [_entrance_out(e) for e in p.accessible_entrances],
        "pedway_connected": p.pedway_connected,
        "note": (
            "This entrance connects to the Chicago Pedway — you can travel underground "
            "from your starting point without going outside."
            if entrance.type == EntranceType.PEDWAY_TUNNEL
            else "Street-level ADA elevator. Check elevator status before routing."
        ),
    }


@router.get("/nav/station/health", summary="Station service health")
async def station_health() -> Dict[str, Any]:
    return _svc.health_check()
