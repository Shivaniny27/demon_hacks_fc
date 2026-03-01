"""
ada.py — ADA / Real-Time Elevator Reliability API
===================================================
Exposes real-time CTA elevator status, per-station accessibility reports,
reliability scores, and ADA-aware routing integration.

All data flows from elevator_service.py (Priority 1).

Endpoints:
  GET  /nav/ada/elevators              — all active elevator outages
  GET  /nav/ada/elevators/{elev_id}    — single elevator status + reliability
  GET  /nav/ada/stations               — all Loop stations accessibility summary
  GET  /nav/ada/stations/{station_key} — full station accessibility report
  GET  /nav/ada/reliable               — fully accessible stations right now
  GET  /nav/ada/avoid                  — stations/elevators to avoid
  GET  /nav/ada/routing-params         — graph edge mods for routing engine
  GET  /nav/ada/route-warnings         — ADA warnings for a station list
  GET  /nav/ada/reliability            — all elevator reliability scores
  GET  /nav/ada/chronic                — chronically unreliable elevators
  GET  /nav/ada/patterns/{elev_id}     — outage pattern analysis
  GET  /nav/ada/health                 — service health
  POST /nav/ada/mock-outage            — inject a test outage (dev mode)
  GET  /nav/ada/stream                 — SSE stream of elevator changes
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services.elevator_service import (
    ElevatorOutage,
    ElevatorReliabilityScore,
    ElevatorService,
    ElevatorSeverity,
    ImpactLevel,
    LOOP_STATIONS,
    ALL_ELEVATOR_IDS,
    ELEVATOR_TO_STATION,
    get_elevator_service,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["ADA / Elevator Reliability"])

SSE_MAX_DURATION_S = 3_600   # 1 hour


# ── Dependency ─────────────────────────────────────────────────────────────────

def _get_svc(request: Request) -> ElevatorService:
    redis = getattr(request.app.state, "redis", None)
    svc   = get_elevator_service(redis=redis)
    return svc


# ── Pydantic Schemas ──────────────────────────────────────────────────────────

class OutageOut(BaseModel):
    alert_id:          str
    station_key:       str
    station_name:      str
    station_map_id:    str
    elevator_ids:      List[str]
    severity:          str
    impact_level:      str
    headline:          str
    short_description: str
    affected_lines:    List[str]
    start_time:        str
    end_time:          Optional[str]
    is_tbd:            bool
    ada_alternatives:  List[str]

    @classmethod
    def from_outage(cls, o: ElevatorOutage) -> "OutageOut":
        return cls(
            alert_id=o.alert_id,
            station_key=o.station_key,
            station_name=o.station_name,
            station_map_id=o.station_map_id,
            elevator_ids=o.elevator_ids,
            severity=o.severity.value,
            impact_level=o.impact_level.value,
            headline=o.headline,
            short_description=o.short_description,
            affected_lines=o.affected_lines,
            start_time=o.start_time.isoformat(),
            end_time=o.end_time.isoformat() if o.end_time else None,
            is_tbd=o.is_tbd,
            ada_alternatives=o.ada_alternatives,
        )


class ReliabilityOut(BaseModel):
    elevator_id:       str
    station_key:       str
    station_name:      str
    score:             float
    tier:              str
    outages_7d:        int
    outages_24h:       int
    is_currently_down: bool
    last_outage_at:    Optional[str]
    chronic:           bool
    routing_avoid:     bool

    @classmethod
    def from_score(cls, s: ElevatorReliabilityScore) -> "ReliabilityOut":
        return cls(
            elevator_id=s.elevator_id,
            station_key=s.station_key,
            station_name=s.station_name,
            score=s.score,
            tier=s.tier.value,
            outages_7d=s.outages_7d,
            outages_24h=s.outages_24h,
            is_currently_down=s.is_currently_down,
            last_outage_at=s.last_outage_at.isoformat() if s.last_outage_at else None,
            chronic=s.chronic,
            routing_avoid=s.routing_avoid,
        )


class MockOutageRequest(BaseModel):
    station_key:  str = Field(..., description="Internal station key (e.g. 'clark_lake')")
    headline:     str = Field(default="Test Elevator Outage")
    elevator_id:  Optional[str] = Field(default=None, description="Specific elevator ID to mark down")
    impact_level: str = Field(default="high", description="critical|high|medium|low")


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/nav/ada/elevators", summary="Active CTA elevator outages")
async def get_active_outages(
    impact: Optional[str] = Query(None, description="Filter by impact: critical|high|medium|low"),
    line:   Optional[str] = Query(None, description="Filter by CTA line (e.g. Blue)"),
    svc:    ElevatorService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns all currently active CTA elevator outages in the Chicago Loop.
    Pulled live from the CTA Customer Alerts API every 2 minutes.
    """
    snapshot = await svc.get_snapshot()
    outages  = snapshot.active_outages

    if impact:
        outages = [o for o in outages if o.impact_level.value == impact]
    if line:
        outages = [o for o in outages if line in o.affected_lines]

    return {
        "fetched_at":           snapshot.fetched_at.isoformat(),
        "api_ok":               snapshot.api_ok,
        "total_active_outages": snapshot.total_outages,
        "filtered_count":       len(outages),
        "loop_impact_score":    snapshot.loop_impact_score,
        "affected_lines":       list(snapshot.affected_lines),
        "outages": [OutageOut.from_outage(o).model_dump() for o in outages],
    }


@router.get("/nav/ada/elevators/{elevator_id}", summary="Single elevator status + reliability")
async def get_elevator_detail(
    elevator_id: str,
    svc: ElevatorService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns real-time status and reliability score for a specific elevator.
    elevator_id must be one of the known Loop elevator IDs (e.g. CL-E1, WD-E2).
    """
    if elevator_id not in ALL_ELEVATOR_IDS:
        raise HTTPException(
            status_code=404,
            detail=f"Elevator '{elevator_id}' not found. Valid IDs: {sorted(ALL_ELEVATOR_IDS)}",
        )

    snapshot    = await svc.get_snapshot()
    is_down     = elevator_id in snapshot.outaged_elevator_ids
    scores      = await svc.get_reliability_report()
    score_obj   = scores.get(elevator_id)
    pattern     = await svc.get_pattern_analysis(elevator_id)
    station_key = ELEVATOR_TO_STATION.get(elevator_id, "unknown")

    # Find active outage for this elevator
    active_outage = next(
        (o for o in snapshot.active_outages if elevator_id in o.elevator_ids),
        None,
    )

    return {
        "elevator_id":     elevator_id,
        "station_key":     station_key,
        "station_name":    LOOP_STATIONS.get(station_key, {}).get("name", "Unknown"),
        "is_currently_down": is_down,
        "reliability":     ReliabilityOut.from_score(score_obj).model_dump() if score_obj else None,
        "active_outage":   OutageOut.from_outage(active_outage).model_dump() if active_outage else None,
        "pattern": {
            "is_chronic":     pattern.is_chronic,
            "outages_7d":     pattern.outages_7d,
            "peak_hour":      pattern.peak_hour,
            "peak_day":       pattern.peak_day,
            "predictive_risk": pattern.predictive_risk,
        },
        "routing_avoid": score_obj.routing_avoid if score_obj else False,
        "fetched_at":    snapshot.fetched_at.isoformat(),
    }


@router.get("/nav/ada/stations", summary="All Loop stations accessibility summary")
async def get_all_stations_accessibility(
    accessible_only: bool = Query(False, description="Only return fully accessible stations"),
    svc: ElevatorService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns accessibility status for all Chicago Loop CTA stations.
    Includes active outages, reliability scores, Pedway connections.
    """
    reports = await svc.get_all_station_reports()
    if accessible_only:
        reports = {k: v for k, v in reports.items() if v.is_fully_accessible}

    stations_out = []
    for key, report in reports.items():
        stations_out.append({
            "station_key":         key,
            "station_name":        report.station_name,
            "lines":               report.lines,
            "lat":                 report.lat,
            "lon":                 report.lon,
            "is_fully_accessible": report.is_fully_accessible,
            "has_active_outage":   report.has_active_outage,
            "active_outage_count": len(report.active_outages),
            "worst_reliability":   report.worst_score,
            "access_rating":       report.access_rating,
            "pedway_connected":    report.pedway_connected,
            "platform_level":      report.platform_level,
            "ada_alternatives":    report.ada_alternatives[:2],
            "notes":               report.notes[:1],
        })

    fully_accessible = sum(1 for r in reports.values() if r.is_fully_accessible)
    outaged          = sum(1 for r in reports.values() if r.has_active_outage)

    return {
        "total_stations":      len(stations_out),
        "fully_accessible":    fully_accessible,
        "with_active_outage":  outaged,
        "stations":            stations_out,
    }


@router.get("/nav/ada/stations/{station_key}", summary="Full station accessibility report")
async def get_station_accessibility(
    station_key: str,
    svc: ElevatorService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns a comprehensive ADA accessibility report for a single Loop station.
    Includes active outages, per-elevator reliability, Pedway access, alternatives.
    """
    if station_key not in LOOP_STATIONS:
        raise HTTPException(
            status_code=404,
            detail=f"Station '{station_key}' not found. Valid keys: {list(LOOP_STATIONS.keys())}",
        )

    try:
        report = await svc.get_station_report(station_key)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return {
        "station_key":         report.station_key,
        "station_name":        report.station_name,
        "station_map_id":      report.station_map_id,
        "lines":               report.lines,
        "lat":                 report.lat,
        "lon":                 report.lon,
        "is_fully_accessible": report.is_fully_accessible,
        "has_active_outage":   report.has_active_outage,
        "access_rating":       report.access_rating,
        "pedway_connected":    report.pedway_connected,
        "pedway_entrance":     report.pedway_entrance,
        "platform_level":      report.platform_level,
        "accessible_entrances": report.accessible_entrances,
        "ada_alternatives":    report.ada_alternatives,
        "active_outages": [
            OutageOut.from_outage(o).model_dump()
            for o in report.active_outages
        ],
        "elevator_reliability": [
            ReliabilityOut.from_score(s).model_dump()
            for s in report.elevator_scores
        ],
        "worst_reliability_score": report.worst_score,
        "notes": report.notes,
    }


@router.get("/nav/ada/reliable", summary="Currently fully accessible Loop stations")
async def get_accessible_stations(
    svc: ElevatorService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns the list of Loop stations that are FULLY accessible right now.
    No active outages AND reliability score ≥ 50%.
    """
    accessible_keys = await svc.get_accessible_stations()
    stations_out = []
    for key in accessible_keys:
        data = LOOP_STATIONS.get(key, {})
        stations_out.append({
            "station_key": key,
            "station_name": data.get("name", key),
            "lines": data.get("lines", []),
            "lat": data.get("lat"),
            "lon": data.get("lon"),
            "pedway_connected": data.get("pedway_connected", False),
            "platform_level": data.get("platform_level", "unknown"),
        })

    return {
        "fully_accessible_count": len(accessible_keys),
        "total_loop_stations":    len(LOOP_STATIONS),
        "stations":               stations_out,
    }


@router.get("/nav/ada/avoid", summary="Stations and elevators to avoid for ADA routing")
async def get_avoid_list(
    svc: ElevatorService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns the current list of elevators and stations to avoid in ADA routing.
    Elevators with reliability < 50% are flagged even without active outages.
    """
    snapshot          = await svc.get_snapshot()
    scores            = await svc.get_reliability_report()
    avoid_elevators   = [eid for eid, s in scores.items() if s.routing_avoid]
    avoid_stations    = list(snapshot.outaged_station_keys)
    low_reliability   = [eid for eid, s in scores.items() if s.score < 0.70 and not s.routing_avoid]

    return {
        "fetched_at":            snapshot.fetched_at.isoformat(),
        "avoid_elevators":       avoid_elevators,
        "avoid_stations":        avoid_stations,
        "low_reliability_watch": low_reliability,
        "outaged_elevator_ids":  list(snapshot.outaged_elevator_ids),
        "loop_impact_score":     snapshot.loop_impact_score,
    }


@router.get("/nav/ada/routing-params", summary="Graph edge modifications for ADA routing engine")
async def get_routing_params(
    svc: ElevatorService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns edge modification parameters for the LoopNav routing engine.
    elevator_id → {disable: bool, multiplier: float}
    multiplier 999.0 = edge disabled; 1.5 = degraded but usable.
    """
    mods = await svc.get_graph_edge_mods()
    return {
        "edge_modifications": mods,
        "disabled_count": sum(1 for m in mods.values() if m["disable"]),
        "degraded_count": sum(1 for m in mods.values() if not m["disable"] and m["multiplier"] > 1.0),
        "normal_count":   sum(1 for m in mods.values() if m["multiplier"] == 1.0),
    }


@router.get("/nav/ada/route-warnings", summary="ADA warnings for a list of stations")
async def get_route_warnings(
    stations: str = Query(..., description="Comma-separated station keys"),
    svc: ElevatorService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Given a comma-separated list of station keys on a planned route,
    returns ADA warnings (outages, low reliability) for each station.
    Use this before presenting a route to a wheelchair user.
    """
    station_keys = [s.strip() for s in stations.split(",") if s.strip()]
    warnings     = await svc.get_route_warnings(station_keys)

    return {
        "station_keys_checked": station_keys,
        "warning_count":        len(warnings),
        "has_critical":         any(w.get("impact") == "critical" for w in warnings),
        "warnings":             warnings,
    }


@router.get("/nav/ada/reliability", summary="All elevator reliability scores")
async def get_reliability_report(
    tier:    Optional[str] = Query(None, description="Filter by tier: excellent|good|moderate|degraded|poor"),
    chronic: bool = Query(False, description="Only return chronically unreliable elevators"),
    svc: ElevatorService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns reliability scores for all known Loop L elevator units.
    Based on 7-day outage history from the CTA Customer Alerts API.
    """
    scores = await svc.get_reliability_report()

    if tier:
        scores = {k: v for k, v in scores.items() if v.tier.value == tier}
    if chronic:
        scores = {k: v for k, v in scores.items() if v.chronic}

    scored_list = [ReliabilityOut.from_score(s).model_dump() for s in scores.values()]
    scored_list.sort(key=lambda x: x["score"])

    return {
        "total_elevators":     len(ALL_ELEVATOR_IDS),
        "filtered_count":      len(scored_list),
        "avoid_count":         sum(1 for s in scores.values() if s.routing_avoid),
        "chronic_count":       sum(1 for s in scores.values() if s.chronic),
        "avg_score":           round(
            sum(s.score for s in scores.values()) / max(len(scores), 1), 3
        ),
        "reliability_scores":  scored_list,
    }


@router.get("/nav/ada/chronic", summary="Chronically unreliable elevators (3+ outages in 7d)")
async def get_chronic_elevators(
    svc: ElevatorService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns elevator IDs that have had 3 or more outages in the past 7 days.
    These should be avoided in ADA routing until maintenance is completed.
    """
    chronic_ids = await svc.get_chronic_elevators()
    scores      = await svc.get_reliability_report()

    chronic_detail = []
    for eid in chronic_ids:
        s = scores.get(eid)
        if s:
            chronic_detail.append({
                "elevator_id":  eid,
                "station_key":  s.station_key,
                "station_name": s.station_name,
                "score":        s.score,
                "tier":         s.tier.value,
                "outages_7d":   s.outages_7d,
            })
    chronic_detail.sort(key=lambda x: x["outages_7d"], reverse=True)

    return {
        "chronic_count":     len(chronic_ids),
        "chronic_elevators": chronic_detail,
        "recommendation":    (
            "These elevators should be excluded from ADA route planning until "
            "maintenance records show improved reliability."
        ) if chronic_ids else "No chronically unreliable elevators detected.",
    }


@router.get("/nav/ada/patterns/{elevator_id}", summary="Outage pattern analysis for an elevator")
async def get_outage_patterns(
    elevator_id: str,
    svc: ElevatorService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Returns temporal outage pattern analysis for a specific elevator.
    Identifies peak outage hours, day of week patterns, and predictive risk.
    """
    if elevator_id not in ALL_ELEVATOR_IDS:
        raise HTTPException(status_code=404, detail=f"Elevator '{elevator_id}' not found.")

    pattern     = await svc.get_pattern_analysis(elevator_id)
    station_key = ELEVATOR_TO_STATION.get(elevator_id, "unknown")

    return {
        "elevator_id":      elevator_id,
        "station_key":      station_key,
        "station_name":     LOOP_STATIONS.get(station_key, {}).get("name", "Unknown"),
        "is_chronic":       pattern.is_chronic,
        "outages_7d":       pattern.outages_7d,
        "peak_hour":        f"{pattern.peak_hour:02d}:00" if pattern.peak_hour is not None else None,
        "peak_day":         pattern.peak_day,
        "avg_duration_min": pattern.avg_duration_min,
        "predictive_risk":  pattern.predictive_risk,
        "interpretation": (
            f"This elevator is most likely to fail around "
            f"{pattern.peak_hour:02d}:00 on {pattern.peak_day}s."
            if pattern.is_chronic and pattern.peak_hour is not None
            else "Not enough outage history for pattern analysis."
        ),
    }


@router.get("/nav/ada/health", summary="ADA service health check")
async def ada_health(
    svc: ElevatorService = Depends(_get_svc),
) -> Dict[str, Any]:
    return await svc.health_check()


@router.post("/nav/ada/mock-outage", summary="Inject a test elevator outage (dev mode)")
async def mock_outage(
    req: MockOutageRequest,
    svc: ElevatorService = Depends(_get_svc),
) -> Dict[str, Any]:
    """
    Developer endpoint: inject a synthetic elevator outage for testing
    ADA routing and SSE stream behavior. Not for production use.
    """
    if req.station_key not in LOOP_STATIONS:
        raise HTTPException(status_code=404, detail=f"Station key '{req.station_key}' not found.")

    from app.services.elevator_service import (
        ElevatorOutage, ElevatorSeverity, ImpactLevel
    )
    import uuid

    station_data = LOOP_STATIONS[req.station_key]
    elevator_ids = [req.elevator_id] if req.elevator_id else station_data.get("elevators", [])[:1]

    from datetime import timezone as tz
    mock_outage_obj = ElevatorOutage(
        alert_id=f"mock-{uuid.uuid4().hex[:8]}",
        station_key=req.station_key,
        station_name=station_data["name"],
        station_map_id=station_data["map_id"],
        elevator_ids=elevator_ids,
        severity=ElevatorSeverity.OUTAGE,
        impact_level=ImpactLevel(req.impact_level) if req.impact_level in ImpactLevel._value2member_map_ else ImpactLevel.HIGH,
        headline=req.headline,
        short_description=req.headline,
        full_description="Injected by LoopNav dev endpoint for testing.",
        affected_lines=station_data.get("lines", []),
        start_time=datetime.now(tz.utc),
        end_time=None,
        is_tbd=True,
        ada_alternatives=[],
        source="mock",
    )

    current = svc._registry.get_active_outages()
    current.append(mock_outage_obj)
    await svc._registry.update(current)

    return {
        "injected": True,
        "alert_id": mock_outage_obj.alert_id,
        "station":  station_data["name"],
        "elevator_ids": elevator_ids,
        "note": "This mock outage will be cleared on the next CTA API poll (~2 min).",
    }


@router.get("/nav/ada/stream", summary="SSE stream of elevator outage changes")
async def ada_sse_stream(
    request: Request,
    svc: ElevatorService = Depends(_get_svc),
):
    """
    Server-Sent Events stream for real-time elevator outage changes.

    Events:
      elevator_update  — snapshot of current outages (every 2 min)
      outage_added     — new outage detected
      outage_resolved  — outage cleared
      ping             — keepalive every 30s

    Disconnect after 1 hour.
    """
    import time as _time
    start = _time.time()

    # Shared state for change events detected by subscriber
    change_queue: asyncio.Queue = asyncio.Queue()

    async def on_change(event_type: str, data: dict):
        await change_queue.put((event_type, data))

    svc.subscribe(on_change)

    async def event_generator():
        nonlocal start
        try:
            ping_counter = 0
            snapshot     = await svc.get_snapshot()

            # Initial state
            yield _sse_event("elevator_update", {
                "total_outages": snapshot.total_outages,
                "critical_count": snapshot.critical_count,
                "loop_impact_score": snapshot.loop_impact_score,
                "outages": [
                    {
                        "alert_id":    o.alert_id,
                        "station":     o.station_name,
                        "severity":    o.severity.value,
                        "impact":      o.impact_level.value,
                        "headline":    o.headline,
                        "lines":       o.affected_lines,
                    }
                    for o in snapshot.active_outages
                ],
                "fetched_at": snapshot.fetched_at.isoformat(),
            })

            while _time.time() - start < SSE_MAX_DURATION_S:
                if await request.is_disconnected():
                    break

                # Check for change events (non-blocking, 1s timeout)
                try:
                    event_type, data = await asyncio.wait_for(change_queue.get(), timeout=1.0)
                    yield _sse_event(event_type, data)
                except asyncio.TimeoutError:
                    pass

                ping_counter += 1
                if ping_counter >= 30:
                    ping_counter = 0
                    yield _sse_event("ping", {"ts": datetime.now(timezone.utc).isoformat()})

        finally:
            svc.unsubscribe(on_change)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


def _sse_event(event: str, data: Any) -> str:
    payload = json.dumps(data)
    return f"event: {event}\ndata: {payload}\n\n"
