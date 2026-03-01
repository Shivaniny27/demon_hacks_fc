"""
LoopNav — Production CTA Alerts & Accessibility API.

Serves real-time CTA service disruptions, elevator/escalator outages,
pedway closure status, and elevator reliability scores to the frontend
and routing engine.

Endpoints
─────────
GET  /nav/alerts                      — all active alerts (CTA + pedway combined)
GET  /nav/alerts/elevators            — elevator & escalator outages only
GET  /nav/alerts/pedway               — pedway segment closures
GET  /nav/alerts/pedway/{segment_id}  — single pedway segment status
GET  /nav/alerts/stations             — Loop station reference + live elevator status
GET  /nav/alerts/reliability          — per-elevator reliability scores
GET  /nav/alerts/affected-routes      — which routes are currently impacted
GET  /nav/alerts/{alert_id}           — single alert detail
GET  /nav/alerts/stream               — SSE real-time alert stream
GET  /nav/alerts/health               — service health check
GET  /nav/alerts/history              — recent alert history (Redis)
POST /nav/alerts/mock                 — inject a mock alert (demo)
DELETE /nav/alerts/mock/{alert_id}    — remove a specific mock alert
DELETE /nav/alerts/mock               — clear all mock alerts
POST /nav/alerts/pedway/{seg}/close   — manually close a pedway segment
POST /nav/alerts/pedway/{seg}/restore — restore a pedway segment
GET  /nav/alerts/map                  — GeoJSON FeatureCollection of alert locations

Data flow
─────────
  1. CTAService.get_snapshot() called (cached 60s in Redis)
  2. CTA Alerts API polled (3 retries, falls back to mock if unavailable)
  3. Elevator IDs matched to internal graph node identifiers
  4. ElevatorReliabilityTracker updates per-elevator outage history
  5. Pedway closure hours enforced (06:00–22:00 CST)
  6. Full AlertsSnapshot returned

SSE Stream protocol
───────────────────
  event: alert_update    — new or resolved alert
  event: elevator_update — elevator status change
  event: pedway_update   — pedway closure change
  event: ping            — keepalive every 30s
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional, Set

from fastapi import APIRouter, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from app.data.chicago_constants import CTA_LOOP_STATIONS, PEDWAY_NODES
from app.services.cta_service import (
    CTAAlert,
    AlertCategory,
    AlertSeverity,
    ElevatorStatus,
    PedwaySegment,
    ELEVATOR_IDS,
    LOOP_STATION_IDS,
    get_service,
)

log = logging.getLogger("loopnav.cta_api")

router = APIRouter(prefix="/nav", tags=["LoopNav CTA Alerts"])

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

SSE_ALERT_INTERVAL_S = 30
SSE_PING_INTERVAL_S  = 30
SSE_MAX_DURATION_S   = 3600
HISTORY_REDIS_KEY    = "nav:cta:history"
HISTORY_MAX_RECORDS  = 50

SEVERITY_COLORS = {
    AlertSeverity.CRITICAL: "#7B0000",
    AlertSeverity.MAJOR:    "#C0392B",
    AlertSeverity.MODERATE: "#E67E22",
    AlertSeverity.MINOR:    "#27AE60",
}

CATEGORY_ICONS = {
    AlertCategory.ELEVATOR:  "🛗",
    AlertCategory.ESCALATOR: "🪜",
    AlertCategory.TRAIN:     "🚆",
    AlertCategory.PEDWAY:    "🚇",
    AlertCategory.SIGNAL:    "🚦",
    AlertCategory.WEATHER:   "🌧",
    AlertCategory.OTHER:     "ℹ️",
}

RELIABILITY_LABELS = {
    (0.0, 0.50):  "poor",
    (0.50, 0.70): "degraded",
    (0.70, 0.85): "moderate",
    (0.85, 0.95): "good",
    (0.95, 1.01): "excellent",
}

ROUTE_NAMES = {
    "Brn": "Brown Line", "G":    "Green Line",
    "Org": "Orange Line", "P":   "Purple Line",
    "Pink": "Pink Line",  "Red": "Red Line",
    "Blue": "Blue Line",
}


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic schemas
# ─────────────────────────────────────────────────────────────────────────────

class MockAlertRequest(BaseModel):
    headline:          str       = Field(..., max_length=200)
    short_description: str       = Field(default="", max_length=500)
    category:          str       = Field(default="elevator")
    severity:          str       = Field(default="Minor")
    affected_routes:   List[str] = Field(default_factory=list)
    elevator_ids:      List[str] = Field(default_factory=list)


class PedwayClosureRequest(BaseModel):
    reason:      str           = Field(..., max_length=300)
    reopen_time: Optional[str] = Field(default=None)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_cta_svc(request: Request):
    svc = getattr(getattr(request, "app", None), "state", None)
    svc = getattr(svc, "cta_svc", None)
    return svc if svc is not None else get_service()


def _reliability_label(score: float) -> str:
    for (lo, hi), label in RELIABILITY_LABELS.items():
        if lo <= score < hi:
            return label
    return "unknown"


def _alert_to_dict(a: CTAAlert) -> Dict[str, Any]:
    return {
        **a.as_dict(),
        "severity_color": SEVERITY_COLORS.get(a.severity, "#888"),
        "category_icon":  CATEGORY_ICONS.get(a.category, "ℹ️"),
    }


def _elevator_to_dict(e: ElevatorStatus) -> Dict[str, Any]:
    return {
        **e.as_dict(),
        "reliability_label": _reliability_label(e.reliability),
        "status_text":       "Operational" if e.is_operational else "OUT OF SERVICE",
        "status_color":      "#27AE60" if e.is_operational else "#C0392B",
    }


def _pedway_to_dict(s: PedwaySegment) -> Dict[str, Any]:
    return {
        **s.as_dict(),
        "status_text":  "Open" if not s.is_closed else "CLOSED",
        "status_color": "#27AE60" if not s.is_closed else "#C0392B",
    }


async def _record_alert_history(request: Request, alert_ids: Set[str], snapshot) -> None:
    cache = getattr(getattr(request, "app", None), "state", None)
    cache = getattr(cache, "cache", None)
    if not cache:
        return
    record = {
        "ts":            datetime.now(timezone.utc).isoformat(),
        "alert_count":   snapshot.total_alerts,
        "elevator_down": snapshot.total_elevators_down,
        "pedway_closed": snapshot.total_pedway_closed,
        "any_critical":  snapshot.any_critical,
        "alert_ids":     list(alert_ids),
    }
    try:
        await cache.lpush(HISTORY_REDIS_KEY, record)
        await cache.ltrim(HISTORY_REDIS_KEY, 0, HISTORY_MAX_RECORDS - 1)
        await cache.expire(HISTORY_REDIS_KEY, 86400)
    except Exception:
        pass


async def _get_alert_history(request: Request) -> List[Dict]:
    cache = getattr(getattr(request, "app", None), "state", None)
    cache = getattr(cache, "cache", None)
    if not cache:
        return []
    try:
        return await cache.lrange(HISTORY_REDIS_KEY, 0, HISTORY_MAX_RECORDS - 1)
    except Exception:
        return []


async def _invalidate_alert_cache(request: Request) -> None:
    cache = getattr(getattr(request, "app", None), "state", None)
    cache = getattr(cache, "cache", None)
    if cache:
        try:
            await cache.delete("nav:cta:alerts")
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/alerts")
async def get_all_alerts(
    request:          Request,
    category:         Optional[str] = Query(default=None),
    severity:         Optional[str] = Query(default=None, description="Minimum: Minor/Moderate/Major"),
    elevator_only:    bool          = Query(default=False),
    include_pedway:   bool          = Query(default=True),
    include_stations: bool          = Query(default=False),
):
    """
    All active alerts for the Chicago Loop area.

    Combines CTA Customer Alerts API (service disruptions, elevator outages),
    pedway closure registry, and manually injected mock alerts.

    Results sorted: elevator/escalator outages first, then by severity descending.
    """
    t0  = time.monotonic()
    svc = _get_cta_svc(request)

    try:
        snapshot = await svc.get_snapshot()
    except Exception as exc:
        log.error("CTA service error: %s", exc)
        raise HTTPException(status_code=502, detail={
            "error":   "CTA_SERVICE_ERROR",
            "message": "Failed to fetch CTA alerts",
        })

    alerts = snapshot.alerts

    if elevator_only:
        alerts = [a for a in alerts if a.is_elevator_outage or a.is_escalator_outage]
    elif category:
        try:
            cat    = AlertCategory(category.lower())
            alerts = [a for a in alerts if a.category == cat]
        except ValueError:
            raise HTTPException(status_code=400, detail={
                "error": "INVALID_CATEGORY",
                "valid": [c.value for c in AlertCategory],
            })

    if severity:
        sev_order = {"Minor": 1, "Moderate": 2, "Major": 3, "Critical": 4}
        min_score = sev_order.get(severity, 0)
        alerts    = [a for a in alerts if a.severity_score >= min_score]

    pedway_alerts: List[Dict] = []
    if include_pedway:
        for seg in snapshot.pedway_closures:
            if seg.is_closed:
                pedway_alerts.append({
                    "alert_id":          f"PW-{seg.segment_id}",
                    "headline":          f"Pedway CLOSED: {seg.name}",
                    "short_description": seg.closure_reason or "Segment temporarily closed",
                    "category":          "pedway",
                    "severity":          "Moderate",
                    "severity_color":    "#E67E22",
                    "category_icon":     "🚇",
                    "is_elevator_outage": False,
                    "is_escalator_outage": False,
                    "alternative":       seg.alternative,
                    "reopen_time":       seg.reopen_time,
                    "fetched_at":        datetime.now(timezone.utc).isoformat(),
                })

    alert_ids = {a.alert_id for a in alerts}
    asyncio.create_task(_record_alert_history(request, alert_ids, snapshot))

    elapsed_ms = round((time.monotonic() - t0) * 1000, 2)

    response: Dict[str, Any] = {
        "alerts":              [_alert_to_dict(a) for a in alerts],
        "pedway_alerts":       pedway_alerts,
        "total_alerts":        len(alerts),
        "total_pedway_closed": len(pedway_alerts),
        "elevator_outages":    snapshot.total_elevators_down,
        "any_critical":        snapshot.any_critical,
        "source":              snapshot.source,
        "fetched_at":          snapshot.fetched_at,
        "elapsed_ms":          elapsed_ms,
    }
    if include_stations:
        response["elevator_statuses"] = [_elevator_to_dict(e) for e in snapshot.elevator_statuses]

    return JSONResponse(content=response)


@router.get("/alerts/elevators")
async def get_elevator_status_all(request: Request):
    """
    Comprehensive elevator and escalator status for all Loop stations.

    Returns operational status, reliability score (0–1), and outage alerts.
    The routing engine uses reliability to penalise unreliable elevators.
    Critical for ADA-compliant route planning.
    """
    svc      = _get_cta_svc(request)
    snapshot = await svc.get_snapshot()

    outaged_ids = {eid for a in snapshot.elevator_outages for eid in a.elevator_ids}
    statuses    = sorted(
        [_elevator_to_dict(e) for e in snapshot.elevator_statuses],
        key=lambda e: (int(e["is_operational"]), -e["reliability"]),
    )
    down_count  = sum(1 for s in snapshot.elevator_statuses if not s.is_operational)
    ada_impact  = "none" if down_count == 0 else ("low" if down_count == 1 else ("medium" if down_count < 5 else "high"))

    return {
        "elevator_statuses":  statuses,
        "total_elevators":    len(statuses),
        "elevators_down":     down_count,
        "elevators_up":       len(statuses) - down_count,
        "ada_routing_impact": ada_impact,
        "outage_alerts":      [_alert_to_dict(a) for a in snapshot.elevator_outages],
        "outaged_elevator_ids": list(outaged_ids),
        "message": (
            f"{down_count} elevator(s) out of service — accessible routes may be limited."
            if down_count > 0 else "All known elevators are operational."
        ),
        "fetched_at": snapshot.fetched_at,
    }


@router.get("/alerts/reliability")
async def get_elevator_reliability(request: Request):
    """
    Per-elevator reliability scores based on outage history (last 1 hour).

    Reliability → routing cost multiplier:
      0.95+  (excellent) → 1.05×  cost
      0.80   (good)      → 1.25×  cost
      0.65   (degraded)  → 1.54×  cost
      < 0.50 (poor)      → AVOID  in accessible mode

    Threshold: routing engine avoids elevators with reliability < 0.50.
    """
    svc      = _get_cta_svc(request)
    snapshot = await svc.get_snapshot()

    scores: List[Dict] = []
    for status in sorted(snapshot.elevator_statuses, key=lambda s: s.reliability):
        scores.append({
            "elevator_id":       status.elevator_id,
            "description":       status.description,
            "reliability":       status.reliability,
            "reliability_label": _reliability_label(status.reliability),
            "is_operational":    status.is_operational,
            "outage_count_1h":   status.outage_count,
            "routing_cost_mult": round(1.0 / max(status.reliability, 0.01), 2),
            "avoid_in_routing":  status.reliability < 0.50,
        })

    avg_rel    = sum(s["reliability"] for s in scores) / len(scores) if scores else 0
    poor_count = sum(1 for s in scores if s["reliability_label"] == "poor")

    return {
        "reliability_scores": scores,
        "total_elevators":    len(scores),
        "avg_reliability":    round(avg_rel, 3),
        "poor_elevators":     poor_count,
        "routing_threshold":  0.50,
        "note":               "Routing engine avoids elevators with reliability < 0.50 in accessible mode.",
        "fetched_at":         snapshot.fetched_at,
    }


@router.get("/alerts/pedway")
async def get_pedway_status(request: Request):
    """
    Status of all Chicago Pedway segments.

    Pedway operates 06:00–22:00 CST daily. Outside these hours all segments
    are automatically closed. Manual closures can be injected for demo purposes.
    """
    svc      = _get_cta_svc(request)
    snapshot = await svc.get_snapshot()

    segments    = [_pedway_to_dict(s) for s in snapshot.pedway_closures]
    open_segs   = [s for s in segments if not s["is_closed"]]
    closed_segs = [s for s in segments if s["is_closed"]]

    now_hour    = datetime.now(timezone.utc).hour - 6
    if now_hour < 0:
        now_hour += 24
    in_hours = 6 <= now_hour < 22

    return {
        "segments":           segments,
        "open_count":         len(open_segs),
        "closed_count":       len(closed_segs),
        "all_open":           not closed_segs,
        "operating_hours":    "06:00–22:00 CST",
        "currently_in_hours": in_hours,
        "open_segments":      open_segs,
        "closed_segments":    closed_segs,
        "note": (
            "All pedway segments open." if not closed_segs else
            f"{len(closed_segs)} segment(s) closed — routing engine will use street/mid level."
        ),
        "fetched_at": snapshot.fetched_at,
    }


@router.get("/alerts/pedway/{segment_id}")
async def get_pedway_segment(segment_id: str, request: Request):
    """Single pedway segment status by ID (PW001–PW006)."""
    svc      = _get_cta_svc(request)
    snapshot = await svc.get_snapshot()

    target = next((s for s in snapshot.pedway_closures if s.segment_id == segment_id), None)
    if not target:
        raise HTTPException(status_code=404, detail={
            "error":     "SEGMENT_NOT_FOUND",
            "message":   f"Pedway segment '{segment_id}' not found",
            "available": [s.segment_id for s in snapshot.pedway_closures],
        })
    return _pedway_to_dict(target)


@router.get("/alerts/stations")
async def get_loop_stations(request: Request):
    """
    Reference data for all CTA Loop stations with live elevator status.
    Also includes Pedway entry nodes.
    """
    svc      = _get_cta_svc(request)
    snapshot = await svc.get_snapshot()

    elev_by_station: Dict[str, List[ElevatorStatus]] = {}
    for status in snapshot.elevator_statuses:
        for station_name in LOOP_STATION_IDS:
            key = station_name.lower().split("/")[0].split("(")[0].strip()
            if key in status.elevator_id.lower():
                elev_by_station.setdefault(station_name, []).append(status)
                break

    stations = []
    for s in CTA_LOOP_STATIONS:
        elevators = elev_by_station.get(s.name, [])
        all_up    = all(e.is_operational for e in elevators)
        stations.append({
            "name":              s.name,
            "lat":               s.lat,
            "lon":               s.lng,
            "note":              s.note,
            "type":              "cta_station",
            "station_id":        LOOP_STATION_IDS.get(s.name, ""),
            "elevators":         [_elevator_to_dict(e) for e in elevators],
            "elevator_count":    len(elevators),
            "all_elevators_up":  all_up,
            "accessible":        all_up or len(elevators) == 0,
        })

    return {
        "stations":            stations,
        "pedway_nodes":        [{"name": p.name, "lat": p.lat, "lon": p.lng, "note": p.note} for p in PEDWAY_NODES],
        "station_count":       len(stations),
        "accessible_stations": sum(1 for s in stations if s["accessible"]),
        "fetched_at":          snapshot.fetched_at,
    }


@router.get("/alerts/affected-routes")
async def get_affected_routes(request: Request):
    """Summary of which CTA routes are impacted by current alerts."""
    svc      = _get_cta_svc(request)
    snapshot = await svc.get_snapshot()

    route_impacts: Dict[str, List[Dict]] = {}
    for alert in snapshot.alerts:
        for route in alert.affected_routes:
            route_impacts.setdefault(route, []).append({
                "alert_id":    alert.alert_id,
                "headline":    alert.headline,
                "severity":    alert.severity.value,
                "category":    alert.category.value,
                "is_elevator": alert.is_elevator_outage or alert.is_escalator_outage,
            })

    summary = []
    for route_id, alerts_list in sorted(route_impacts.items()):
        max_sev = max({"Minor": 1, "Moderate": 2, "Major": 3, "Critical": 4}.get(a["severity"], 0) for a in alerts_list)
        summary.append({
            "route_id":    route_id,
            "route_name":  ROUTE_NAMES.get(route_id, route_id),
            "alert_count": len(alerts_list),
            "max_severity": {1: "Minor", 2: "Moderate", 3: "Major", 4: "Critical"}.get(max_sev, "Unknown"),
            "alerts":      alerts_list,
        })

    return {
        "affected_routes": summary,
        "unaffected_routes": [
            {"route_id": rid, "route_name": name}
            for rid, name in ROUTE_NAMES.items() if rid not in route_impacts
        ],
        "total_alerts": snapshot.total_alerts,
        "fetched_at":   snapshot.fetched_at,
    }


@router.get("/alerts/map")
async def get_alerts_map(request: Request):
    """GeoJSON FeatureCollection of alert locations for map rendering."""
    svc      = _get_cta_svc(request)
    snapshot = await svc.get_snapshot()
    features = []

    for alert in snapshot.alerts:
        if not alert.elevator_ids:
            continue
        for station in CTA_LOOP_STATIONS:
            key = station.name.lower().split("/")[0].split("(")[0].strip()
            if any(key in eid.lower() for eid in alert.elevator_ids):
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [station.lng, station.lat]},
                    "properties": {
                        "alert_id":    alert.alert_id,
                        "headline":    alert.headline,
                        "category":    alert.category.value,
                        "severity":    alert.severity.value,
                        "color":       SEVERITY_COLORS.get(alert.severity, "#888"),
                        "icon":        CATEGORY_ICONS.get(alert.category, "ℹ️"),
                        "is_elevator": alert.is_elevator_outage,
                    },
                })
                break

    for seg in snapshot.pedway_closures:
        if seg.is_closed:
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [seg.lon, seg.lat]},
                "properties": {
                    "segment_id":    seg.segment_id,
                    "name":          seg.name,
                    "is_closed":     True,
                    "closure_reason": seg.closure_reason,
                    "category":      "pedway",
                    "color":         "#E67E22",
                    "icon":          "🚇",
                },
            })

    return JSONResponse(
        content={
            "type":     "FeatureCollection",
            "features": features,
            "_meta": {
                "alert_count":  len([f for f in features if "alert_id" in f["properties"]]),
                "pedway_count": len([f for f in features if "segment_id" in f["properties"]]),
                "fetched_at":   snapshot.fetched_at,
            },
        },
        media_type="application/geo+json",
    )


@router.get("/alerts/history")
async def get_alert_history(
    request: Request,
    limit:   int = Query(default=10, ge=1, le=HISTORY_MAX_RECORDS),
):
    """Recent alert snapshot history for trend analysis."""
    history = await _get_alert_history(request)
    return {
        "records":   history[:limit],
        "count":     len(history[:limit]),
        "total":     len(history),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/alerts/health")
async def cta_service_health(request: Request):
    """Service health check — API key, mock mode, cache state."""
    svc = _get_cta_svc(request)
    try:
        snapshot = await svc.get_snapshot()
        status   = "ok"
    except Exception as exc:
        snapshot = None
        status   = f"error: {exc}"

    return {
        "status":  status,
        "service": svc.as_dict(),
        "summary": {
            "total_alerts":     snapshot.total_alerts if snapshot else 0,
            "elevator_outages": snapshot.total_elevators_down if snapshot else 0,
            "pedway_closed":    snapshot.total_pedway_closed if snapshot else 0,
            "any_critical":     snapshot.any_critical if snapshot else False,
            "source":           snapshot.source if snapshot else "unknown",
        } if snapshot else None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/alerts/{alert_id}")
async def get_single_alert(alert_id: str, request: Request):
    """Full details for a single alert plus elevator reliability context."""
    svc      = _get_cta_svc(request)
    snapshot = await svc.get_snapshot()

    target = next((a for a in snapshot.alerts if a.alert_id == alert_id), None)
    if not target:
        raise HTTPException(status_code=404, detail={
            "error":   "ALERT_NOT_FOUND",
            "message": f"Alert '{alert_id}' not found",
            "note":    "Alerts expire when resolved — check /nav/alerts for current list",
        })

    elevator_context = []
    for eid in target.elevator_ids:
        rel = await svc.get_elevator_reliability(eid)
        elevator_context.append({
            "elevator_id": eid,
            "description": ELEVATOR_IDS.get(eid, eid),
            "reliability": rel,
            "label":       _reliability_label(rel),
        })

    return {
        **_alert_to_dict(target),
        "elevator_context": elevator_context,
        "routing_impact":   "avoid_accessible" if target.is_elevator_outage else "normal",
        "rerouting_advice": (
            f"Accessible routes using {', '.join(target.elevator_ids)} will be penalised by the routing engine."
            if target.elevator_ids else "No specific elevator rerouting required."
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Mock injection
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/alerts/mock")
async def inject_mock_alert(req: MockAlertRequest, request: Request):
    """
    Inject a custom mock alert for live demo.

    Perfect for demonstrating the routing engine response to elevator outages:
    1. POST this endpoint with category=elevator, elevator_ids=[...]
    2. Watch /nav/route switch to stairs/ramp for accessible mode
    3. DELETE /nav/alerts/mock/{id} to resolve the demo alert
    """
    svc = _get_cta_svc(request)
    try:
        cat = AlertCategory(req.category.lower())
    except ValueError:
        raise HTTPException(status_code=400, detail={
            "error": "INVALID_CATEGORY",
            "valid": [c.value for c in AlertCategory],
        })

    sev_map    = {"Minor": AlertSeverity.MINOR, "Moderate": AlertSeverity.MODERATE, "Major": AlertSeverity.MAJOR}
    severity   = sev_map.get(req.severity, AlertSeverity.MINOR)
    sev_scores = {"Minor": 2, "Moderate": 3, "Major": 4}
    alert_id   = f"MOCK-{str(uuid.uuid4())[:8].upper()}"

    alert = CTAAlert(
        alert_id            = alert_id,
        headline            = req.headline,
        short_description   = req.short_description,
        full_description    = req.short_description,
        impact              = "Manual mock for demo",
        severity            = severity,
        severity_score      = sev_scores.get(req.severity, 2),
        category            = cat,
        is_elevator_outage  = cat == AlertCategory.ELEVATOR,
        is_escalator_outage = cat == AlertCategory.ESCALATOR,
        affected_routes     = req.affected_routes,
        affected_stops      = [],
        elevator_ids        = req.elevator_ids,
        start_time          = datetime.now(timezone.utc).isoformat(),
        end_time            = None,
        source              = "mock_injected",
        fetched_at          = datetime.now(timezone.utc).isoformat(),
    )
    svc.mock_manager.add(alert)
    await _invalidate_alert_cache(request)

    return {
        "success":    True,
        "alert_id":   alert_id,
        "alert":      _alert_to_dict(alert),
        "message":    f"Mock alert '{alert_id}' injected.",
        "remove_url": f"/api/v1/nav/alerts/mock/{alert_id}",
    }


@router.delete("/alerts/mock/{alert_id}")
async def remove_mock_alert(alert_id: str, request: Request):
    """Remove a specific injected mock alert."""
    svc     = _get_cta_svc(request)
    removed = svc.mock_manager.remove(alert_id)
    if not removed:
        raise HTTPException(status_code=404, detail={
            "error":   "MOCK_NOT_FOUND",
            "message": f"Mock alert '{alert_id}' not found",
        })
    await _invalidate_alert_cache(request)
    return {
        "success":   True,
        "alert_id":  alert_id,
        "message":   f"Mock alert '{alert_id}' removed.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.delete("/alerts/mock")
async def clear_all_mock_alerts(request: Request):
    """Clear all injected mock alerts."""
    svc = _get_cta_svc(request)
    svc.mock_manager.clear()
    await _invalidate_alert_cache(request)
    return {
        "success":   True,
        "message":   "All mock alerts cleared.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Pedway management
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/alerts/pedway/{segment_id}/close")
async def close_pedway_segment(segment_id: str, req: PedwayClosureRequest, request: Request):
    """
    Manually close a pedway segment for demo or maintenance.
    Routing engine will automatically reroute around it.
    segment_id values: PW001–PW006
    """
    svc = _get_cta_svc(request)
    ok  = svc.pedway_store.close_segment(segment_id, req.reason, req.reopen_time)
    if not ok:
        raise HTTPException(status_code=404, detail={
            "error":   "SEGMENT_NOT_FOUND",
            "message": f"Pedway segment '{segment_id}' not found",
        })
    return {
        "success":     True,
        "segment_id":  segment_id,
        "closed":      True,
        "reason":      req.reason,
        "reopen_time": req.reopen_time,
        "message":     f"Pedway segment {segment_id} closed. Routing engine will avoid it.",
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }


@router.post("/alerts/pedway/{segment_id}/restore")
async def restore_pedway_segment(segment_id: str, request: Request):
    """Restore a manually closed pedway segment."""
    svc = _get_cta_svc(request)
    ok  = svc.pedway_store.restore_segment(segment_id)
    if not ok:
        raise HTTPException(status_code=404, detail={
            "error":   "SEGMENT_NOT_FOUND",
            "message": f"No manual closure found for '{segment_id}'",
        })
    return {
        "success":    True,
        "segment_id": segment_id,
        "closed":     False,
        "message":    f"Pedway segment {segment_id} restored.",
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SSE Stream
# ─────────────────────────────────────────────────────────────────────────────

async def _alert_sse_generator(request: Request) -> AsyncGenerator[str, None]:
    """SSE generator — pushes real-time alert change events."""
    svc              = _get_cta_svc(request)
    started_at       = time.monotonic()
    last_ping        = 0.0
    last_check       = 0.0
    prev_alert_ids:   Set[str] = set()
    prev_elev_down:   Set[str] = set()
    prev_pedway_closed: Set[str] = set()

    while True:
        if await request.is_disconnected():
            break
        if time.monotonic() - started_at > SSE_MAX_DURATION_S:
            yield "event: close\ndata: {\"reason\": \"max_duration\"}\n\n"
            break

        now = time.monotonic()

        if now - last_check >= SSE_ALERT_INTERVAL_S:
            try:
                snapshot = await svc.get_snapshot()
            except Exception as exc:
                log.warning("SSE CTA fetch error: %s", exc)
                await asyncio.sleep(5)
                continue

            curr_alert_ids     = {a.alert_id for a in snapshot.alerts}
            curr_elev_down     = {a.alert_id for a in snapshot.elevator_outages}
            curr_pedway_closed = {s.segment_id for s in snapshot.pedway_closures if s.is_closed}

            if curr_alert_ids != prev_alert_ids:
                data = {
                    "type":          "alert_update",
                    "new_alerts":    list(curr_alert_ids - prev_alert_ids),
                    "resolved":      list(prev_alert_ids - curr_alert_ids),
                    "total":         snapshot.total_alerts,
                    "elevator_down": snapshot.total_elevators_down,
                    "any_critical":  snapshot.any_critical,
                    "timestamp":     datetime.now(timezone.utc).isoformat(),
                }
                yield f"event: alert_update\ndata: {json.dumps(data)}\n\n"

            if curr_elev_down != prev_elev_down:
                data = {
                    "type":           "elevator_update",
                    "newly_outaged":  list(curr_elev_down - prev_elev_down),
                    "newly_restored": list(prev_elev_down - curr_elev_down),
                    "total_down":     snapshot.total_elevators_down,
                    "timestamp":      datetime.now(timezone.utc).isoformat(),
                }
                yield f"event: elevator_update\ndata: {json.dumps(data)}\n\n"

            if curr_pedway_closed != prev_pedway_closed:
                data = {
                    "type":           "pedway_update",
                    "newly_closed":   list(curr_pedway_closed - prev_pedway_closed),
                    "newly_opened":   list(prev_pedway_closed - curr_pedway_closed),
                    "total_closed":   snapshot.total_pedway_closed,
                    "timestamp":      datetime.now(timezone.utc).isoformat(),
                }
                yield f"event: pedway_update\ndata: {json.dumps(data)}\n\n"

            prev_alert_ids     = curr_alert_ids
            prev_elev_down     = curr_elev_down
            prev_pedway_closed = curr_pedway_closed
            last_check         = now

        if now - last_ping >= SSE_PING_INTERVAL_S:
            ping = {
                "type":      "ping",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "uptime_s":  round(now - started_at, 0),
            }
            yield f"event: ping\ndata: {json.dumps(ping)}\n\n"
            last_ping = now

        await asyncio.sleep(5)


@router.get("/alerts/stream")
async def cta_sse_stream(request: Request):
    """
    Server-Sent Events stream of real-time CTA alert changes.

    Events: alert_update, elevator_update, pedway_update, ping.

    Frontend usage:
        const es = new EventSource('/api/v1/nav/alerts/stream');
        es.addEventListener('elevator_update', e => {
            const data = JSON.parse(e.data);
            if (data.newly_outaged.length) showElevatorAlert(data);
        });
    """
    return StreamingResponse(
        _alert_sse_generator(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin": "*",
            "Connection":                  "keep-alive",
        },
    )
