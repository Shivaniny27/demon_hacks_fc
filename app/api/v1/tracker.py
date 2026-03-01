"""
tracker.py — CTA Train Tracker + Predictive Departure Sync API
==============================================================
Real-time train arrivals for all Chicago Loop L stations, combined
with Pedway-aware walk-time estimation to answer the killer question:
"When do I leave my office to catch the next train?"

All data flows from sync_service.py.

Endpoints:
  GET  /nav/tracker/arrivals/{station_key}   — live arrival board for one station
  GET  /nav/tracker/arrivals                 — all Loop stations arrival boards
  GET  /nav/tracker/lines/{line}             — arrivals for all stops on a line
  POST /nav/tracker/sync                     — departure sync for one station
  POST /nav/tracker/sync/multi               — best option from all nearby stations
  GET  /nav/tracker/sync/nearest             — quick sync from nearest station
  GET  /nav/tracker/walk/{station_key}       — walk time estimate only
  GET  /nav/tracker/schedule/{station_key}   — next 6 trains (schedule only)
  GET  /nav/tracker/health                   — service health
  GET  /nav/tracker/stream/{station_key}     — SSE live arrival board
"""

import asyncio
import json
import logging
import time as _time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services.sync_service import (
    SyncService,
    DepartureSync,
    TrainArrival,
    StationArrivalBoard,
    WalkEstimate,
    MultiStationSync,
    SyncUrgency,
    TrainStatus,
    LOOP_STATION_MAP_IDS,
    STATION_INFO,
    STATION_LINES,
    LINE_COLORS,
    DEFAULT_BUFFER_S,
    get_sync_service,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["CTA Train Tracker / Departure Sync"])

SSE_MAX_DURATION_S    = 1_800   # 30 min live tracking max
ARRIVALS_REFRESH_S    = 30      # SSE refresh interval


# ── Dependency ─────────────────────────────────────────────────────────────────

def _get_svc(request: Request) -> SyncService:
    redis = getattr(request.app.state, "redis", None)
    return get_sync_service(redis=redis)


# ── Pydantic Schemas ──────────────────────────────────────────────────────────

class TrainArrivalOut(BaseModel):
    run_number:    str
    line:          str
    line_color:    str
    destination:   str
    station_name:  str
    stop_desc:     str
    eta:           str
    minutes_away:  int
    seconds_away:  int
    is_approaching: bool
    is_scheduled:  bool
    is_delayed:    bool
    status:        str

    @classmethod
    def from_arrival(cls, a: TrainArrival) -> "TrainArrivalOut":
        return cls(
            run_number=a.run_number,
            line=a.line,
            line_color=a.line_color,
            destination=a.destination,
            station_name=a.station_name,
            stop_desc=a.stop_desc,
            eta=a.eta.isoformat(),
            minutes_away=a.minutes_away,
            seconds_away=a.seconds_away,
            is_approaching=a.is_approaching,
            is_scheduled=a.is_scheduled,
            is_delayed=a.is_delayed,
            status=a.status.value,
        )


class WalkEstimateOut(BaseModel):
    station_key:      str
    station_name:     str
    straight_line_m:  float
    estimated_walk_m: float
    walk_seconds:     int
    via_pedway:       bool
    level_changes:    int
    total_seconds:    int
    confidence:       str
    notes:            str

    @classmethod
    def from_estimate(cls, w: WalkEstimate) -> "WalkEstimateOut":
        return cls(
            station_key=w.station_key,
            station_name=w.station_name,
            straight_line_m=w.straight_line_m,
            estimated_walk_m=w.estimated_walk_m,
            walk_seconds=w.walk_seconds,
            via_pedway=w.via_pedway,
            level_changes=w.level_changes,
            total_seconds=w.total_seconds,
            confidence=w.confidence,
            notes=w.notes,
        )


class DepartureSyncOut(BaseModel):
    train:               TrainArrivalOut
    walk:                WalkEstimateOut
    buffer_seconds:      int
    leave_at:            str
    leave_in_seconds:    int
    leave_in_display:    str      # "2 min 30 sec"
    arrive_at_platform:  str
    platform_wait_seconds: int
    urgency:             str
    urgency_color:       str
    catchable:           bool
    message:             str
    confidence:          str

    @classmethod
    def from_sync(cls, s: DepartureSync) -> "DepartureSyncOut":
        leave_in_s    = max(0, s.leave_in_seconds)
        mins, secs    = divmod(leave_in_s, 60)
        leave_display = (
            "NOW" if s.urgency == SyncUrgency.LEAVE_NOW
            else f"{mins}m {secs}s"
        )
        urgency_color = {
            SyncUrgency.LEAVE_NOW:  "#E74C3C",
            SyncUrgency.LEAVE_SOON: "#E67E22",
            SyncUrgency.ON_TRACK:   "#27AE60",
            SyncUrgency.WAIT:       "#3498DB",
            SyncUrgency.MISSED:     "#95A5A6",
        }.get(s.urgency, "#888888")

        return cls(
            train=TrainArrivalOut.from_arrival(s.train),
            walk=WalkEstimateOut.from_estimate(s.walk_estimate),
            buffer_seconds=s.buffer_seconds,
            leave_at=s.leave_at.isoformat(),
            leave_in_seconds=s.leave_in_seconds,
            leave_in_display=leave_display,
            arrive_at_platform=s.arrive_at_platform.isoformat(),
            platform_wait_seconds=s.platform_wait_seconds,
            urgency=s.urgency.value,
            urgency_color=urgency_color,
            catchable=s.catchable,
            message=s.message,
            confidence=s.confidence,
        )


class SyncRequest(BaseModel):
    lat:            float = Field(..., description="Current latitude")
    lon:            float = Field(..., description="Current longitude")
    station_key:    str   = Field(..., description="Target Loop station key")
    line:           Optional[str]  = Field(None, description="Filter to specific line (e.g. Blue)")
    buffer_seconds: int   = Field(default=DEFAULT_BUFFER_S, ge=0, le=300,
                                  description="Platform buffer time in seconds")
    accessible:     bool  = Field(default=False, description="Use accessible route (elevator)")


class MultiSyncRequest(BaseModel):
    lat:            float = Field(..., description="Current latitude")
    lon:            float = Field(..., description="Current longitude")
    max_walk_m:     float = Field(default=800.0, ge=100, le=2000,
                                  description="Max walk distance to station (meters)")
    lines:          Optional[List[str]] = Field(None, description="Filter to specific lines")
    buffer_seconds: int   = Field(default=DEFAULT_BUFFER_S, ge=0, le=300)
    accessible:     bool  = Field(default=False)


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/nav/tracker/arrivals/{station_key}", summary="Live arrival board for a Loop station")
async def get_station_arrivals(
    station_key: str,
    line: Optional[str] = Query(None, description="Filter arrivals by line (e.g. Blue)"),
    svc: SyncService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns real-time CTA Train Tracker arrival predictions for a Loop station.
    Requires CTA_TRAIN_TRACKER_KEY in .env. Falls back to schedule if unavailable.
    """
    if station_key not in LOOP_STATION_MAP_IDS:
        raise HTTPException(
            status_code=404,
            detail=f"Station '{station_key}' not found. Valid: {list(LOOP_STATION_MAP_IDS.keys())}",
        )

    board = await svc.get_arrivals(station_key)
    arrivals = board.arrivals
    if line:
        arrivals = [a for a in arrivals if a.line == line]

    next_by_line = {
        ln: TrainArrivalOut.from_arrival(arr).model_dump()
        for ln, arr in board.next_by_line.items()
        if not line or ln == line
    }

    return {
        "station_key":   board.station_key,
        "station_name":  board.station_name,
        "map_id":        board.map_id,
        "fetched_at":    board.fetched_at.isoformat(),
        "lines_served":  board.lines,
        "arrivals":      [TrainArrivalOut.from_arrival(a).model_dump() for a in arrivals],
        "next_by_line":  next_by_line,
        "total_shown":   len(arrivals),
    }


@router.get("/nav/tracker/arrivals", summary="All Loop stations live arrival boards")
async def get_all_arrivals(
    svc: SyncService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns live arrivals for ALL Chicago Loop L stations simultaneously.
    One API call, all stations, concurrent fetches.
    """
    all_boards = await svc.get_all_loop_arrivals()

    summary = []
    for key, board in all_boards.items():
        next_arr = board.arrivals[0] if board.arrivals else None
        summary.append({
            "station_key":   key,
            "station_name":  board.station_name,
            "next_train":    TrainArrivalOut.from_arrival(next_arr).model_dump() if next_arr else None,
            "total_arrivals": len(board.arrivals),
            "lines_active":  list(board.next_by_line.keys()),
        })

    return {
        "fetched_at":      datetime.now(timezone.utc).isoformat(),
        "total_stations":  len(summary),
        "stations":        summary,
    }


@router.get("/nav/tracker/lines/{line}", summary="Arrivals for all Loop stops on a CTA line")
async def get_line_arrivals(
    line: str,
    svc: SyncService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns next arrivals on a specific line across all its Loop stops.
    Useful for "which Blue Line stop has the soonest train?"
    """
    valid_lines = {"Red", "Blue", "Brown", "Green", "Orange", "Pink", "Purple"}
    if line not in valid_lines:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid line '{line}'. Valid lines: {sorted(valid_lines)}",
        )

    # Only fetch stations where this line stops
    matching_stations = [k for k, lines in STATION_LINES.items() if line in lines]
    all_boards        = await svc.get_all_loop_arrivals()

    results = []
    for key in matching_stations:
        board = all_boards.get(key)
        if not board:
            continue
        line_arrivals = [a for a in board.arrivals if a.line == line]
        next_arr = line_arrivals[0] if line_arrivals else None
        results.append({
            "station_key":  key,
            "station_name": board.station_name,
            "next_arrival": TrainArrivalOut.from_arrival(next_arr).model_dump() if next_arr else None,
            "arrivals":     [TrainArrivalOut.from_arrival(a).model_dump() for a in line_arrivals[:3]],
        })

    # Sort by next arrival time
    results.sort(key=lambda x: x["next_arrival"]["seconds_away"] if x["next_arrival"] else 9999)

    return {
        "line":          line,
        "line_color":    LINE_COLORS.get(line, "#888888"),
        "loop_stops":    len(results),
        "fetched_at":    datetime.now(timezone.utc).isoformat(),
        "stations":      results,
    }


@router.post("/nav/tracker/sync", summary="Departure sync: when to leave for a specific station")
async def departure_sync(
    req: SyncRequest,
    svc: SyncService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    The core LoopNav departure sync feature.

    Given your current location, computes exactly when you should leave
    to catch the next train at a specific Loop station, accounting for:
    - Pedway walk time (indoor vs outdoor)
    - Level changes (stairs / elevator)
    - Platform buffer time
    - Real-time train ETAs

    Returns next N trains with leave_at time and urgency level.
    """
    if req.station_key not in LOOP_STATION_MAP_IDS:
        raise HTTPException(status_code=404, detail=f"Station '{req.station_key}' not found.")

    syncs = await svc.sync_departure(
        lat=req.lat,
        lon=req.lon,
        station_key=req.station_key,
        line=req.line,
        buffer_seconds=req.buffer_seconds,
        accessible=req.accessible,
    )

    if not syncs:
        raise HTTPException(status_code=503, detail="No train arrivals available for this station.")

    catchable = [s for s in syncs if s.catchable]
    best      = catchable[0] if catchable else syncs[0]

    return {
        "station_key":     req.station_key,
        "station_name":    STATION_INFO.get(req.station_key, {}).get("name", req.station_key),
        "origin_lat":      req.lat,
        "origin_lon":      req.lon,
        "computed_at":     datetime.now(timezone.utc).isoformat(),
        "recommended":     DepartureSyncOut.from_sync(best).model_dump(),
        "next_trains":     [DepartureSyncOut.from_sync(s).model_dump() for s in syncs],
        "catchable_count": len(catchable),
    }


@router.post("/nav/tracker/sync/multi", summary="Best departure from all nearby stations")
async def multi_station_sync(
    req: MultiSyncRequest,
    svc: SyncService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Finds the best train departure option from all Loop stations within walking distance.

    Compares multiple stations simultaneously and recommends the optimal boarding
    station based on: walk time, train ETA, Pedway access, platform wait time.
    """
    result = await svc.multi_station_sync(
        lat=req.lat,
        lon=req.lon,
        max_walk_m=req.max_walk_m,
        lines=req.lines,
        buffer_seconds=req.buffer_seconds,
        accessible=req.accessible,
    )

    return {
        "origin_lat":    result.origin_lat,
        "origin_lon":    result.origin_lon,
        "computed_at":   result.computed_at.isoformat(),
        "options_count": len(result.options),
        "recommended":   DepartureSyncOut.from_sync(result.recommended).model_dump() if result.recommended else None,
        "reasoning":     result.reasoning,
        "all_options":   [DepartureSyncOut.from_sync(s).model_dump() for s in result.options],
    }


@router.get("/nav/tracker/sync/nearest", summary="Quick sync from nearest Loop station")
async def sync_nearest(
    lat:            float = Query(..., description="Current latitude"),
    lon:            float = Query(..., description="Current longitude"),
    buffer_seconds: int   = Query(DEFAULT_BUFFER_S, ge=0, le=300),
    accessible:     bool  = Query(False),
    svc: SyncService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Quick one-call answer to "when should I leave to catch the nearest train?"
    Returns the single best departure sync from the nearest Loop station.
    """
    sync = await svc.get_nearest_sync(lat, lon, buffer_seconds, accessible)
    if not sync:
        raise HTTPException(status_code=503, detail="No trains available nearby.")

    return {
        "origin_lat":  lat,
        "origin_lon":  lon,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "departure":   DepartureSyncOut.from_sync(sync).model_dump(),
    }


@router.get("/nav/tracker/walk/{station_key}", summary="Walk time estimate to a station")
async def walk_estimate(
    station_key: str,
    lat:         float = Query(..., description="Current latitude"),
    lon:         float = Query(..., description="Current longitude"),
    accessible:  bool  = Query(False, description="Use accessible route (elevator)"),
    svc: SyncService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns just the walk time estimate to a specific station.
    No train data — useful for planning without live API calls.
    """
    if station_key not in STATION_INFO:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")

    from app.services.sync_service import WalkTimeCalculator
    calc   = WalkTimeCalculator()
    walk   = calc.estimate(lat, lon, station_key, accessible=accessible)

    mins, secs = divmod(walk.total_seconds, 60)

    return {
        "station_key":     station_key,
        "station_name":    walk.station_name,
        "origin_lat":      lat,
        "origin_lon":      lon,
        "via_pedway":      walk.via_pedway,
        "straight_line_m": walk.straight_line_m,
        "estimated_walk_m": walk.estimated_walk_m,
        "walk_seconds":    walk.walk_seconds,
        "level_change_seconds": walk.level_change_seconds,
        "total_seconds":   walk.total_seconds,
        "display":         f"{mins}m {secs}s",
        "confidence":      walk.confidence,
        "notes":           walk.notes,
    }


@router.get("/nav/tracker/schedule/{station_key}", summary="Next 6 trains (schedule-based fallback)")
async def get_schedule(
    station_key: str,
    line: Optional[str] = Query(None),
    svc: SyncService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns the next 6 train arrivals using CTA Train Tracker.
    If the API key is not configured, returns an empty board with a notice.
    """
    if station_key not in LOOP_STATION_MAP_IDS:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")

    board = await svc.get_arrivals(station_key)
    arrivals = board.arrivals
    if line:
        arrivals = [a for a in arrivals if a.line == line]

    health = await svc.health_check()

    return {
        "station_key":  board.station_key,
        "station_name": board.station_name,
        "fetched_at":   board.fetched_at.isoformat(),
        "has_api_key":  health["has_api_key"],
        "arrivals":     [TrainArrivalOut.from_arrival(a).model_dump() for a in arrivals[:6]],
        "notice":       (
            None if health["has_api_key"]
            else "CTA_TRAIN_TRACKER_KEY not configured — add it to .env for live arrivals."
        ),
    }


@router.get("/nav/tracker/health", summary="Train Tracker service health")
async def tracker_health(
    svc: SyncService = Depends(_get_svc),
) -> Dict[str, Any]:
    return await svc.health_check()


@router.get("/nav/tracker/stream/{station_key}", summary="SSE live arrival board for a station")
async def arrival_stream(
    station_key: str,
    request: Request,
    svc: SyncService = Depends(_get_svc),
):
    """
    Server-Sent Events stream for live train arrivals at a Loop station.
    Updates every 30 seconds. Disconnects after 30 minutes.

    Events:
      arrivals_update  — full arrival board
      ping             — keepalive every 30s
    """
    if station_key not in LOOP_STATION_MAP_IDS:
        raise HTTPException(status_code=404, detail=f"Station '{station_key}' not found.")

    start = _time.time()

    async def event_generator():
        while _time.time() - start < SSE_MAX_DURATION_S:
            if await request.is_disconnected():
                break

            try:
                board = await svc.get_arrivals(station_key)
                payload = {
                    "station_key":  board.station_key,
                    "station_name": board.station_name,
                    "fetched_at":   board.fetched_at.isoformat(),
                    "arrivals":     [
                        TrainArrivalOut.from_arrival(a).model_dump()
                        for a in board.arrivals
                    ],
                }
                yield f"event: arrivals_update\ndata: {json.dumps(payload)}\n\n"
            except Exception as exc:
                logger.warning("tracker stream error: %s", exc)

            # Wait before next refresh
            for _ in range(ARRIVALS_REFRESH_S):
                if await request.is_disconnected():
                    return
                await asyncio.sleep(1)

        yield f"event: ping\ndata: {json.dumps({'ts': datetime.now(timezone.utc).isoformat()})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
