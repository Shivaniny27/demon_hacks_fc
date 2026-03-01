"""
Routing endpoints — hazard-adjusted pedestrian routing for the Loop.

POST /api/v1/routing/route  — Compute a safe route between two Loop points
GET  /api/v1/routing/demo   — Pre-canned DePaul Commuter demo route
"""

from datetime import datetime, timezone
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from app.database import get_db
from app.models.report import Report, ReportStatus
from app.models.schemas import RouteRequest, RouteResponse
from app.services.routing_service import compute_route

router = APIRouter(prefix="/routing", tags=["Routing"])


async def _get_active_reports(db: AsyncSession) -> list[dict]:
    """Fetch active reports as plain dicts for the routing engine."""
    result = await db.execute(
        select(
            Report.latitude, Report.longitude,
            Report.severity, Report.ai_category, Report.summary_label,
            Report.near_death_corner, Report.near_bridge,
        ).where(
            and_(
                Report.status.in_([ReportStatus.ACTIVE, ReportStatus.VERIFIED]),
                Report.expires_at > datetime.now(timezone.utc),
            )
        )
    )
    return [
        {
            "latitude": r.latitude,
            "longitude": r.longitude,
            "severity": r.severity or 1,
            "ai_category": r.ai_category,
            "summary_label": r.summary_label,
            "near_death_corner": r.near_death_corner,
            "near_bridge": r.near_bridge,
        }
        for r in result.all()
    ]


@router.post("/route", response_model=RouteResponse)
async def get_route(
    body: RouteRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Compute a hazard-adjusted walking route through the Loop.

    The route cost model:
    - Base cost: Euclidean distance ÷ walking speed
    - Hazard multiplier: severity-based cost inflation within 150m of each hazard
    - Accessibility mode: bridges and elevator outages are effectively blocked

    Returns route segments with per-segment hazard scores and warnings.
    """
    active_reports = await _get_active_reports(db)
    return await compute_route(body, active_reports)


@router.get("/demo")
async def get_demo_route(db: AsyncSession = Depends(get_db)):
    """
    The DePaul Commuter demo route:
    Red Line at Lake/State → DePaul Center (1 E. Jackson Blvd)

    This is the route used in the pitch demo. It passes through
    several known hazard zones and demonstrates real-time rerouting.
    """
    request = RouteRequest(
        origin_lat=41.8860,   # Lake/State Red Line
        origin_lng=-87.6280,
        dest_lat=41.8788,     # DePaul Center, Jackson & State
        dest_lng=-87.6279,
        prefer_layer="surface",
        accessibility=False,
    )
    active_reports = await _get_active_reports(db)
    route = await compute_route(request, active_reports)

    return {
        "persona": "DePaul Commuter",
        "origin": "Lake/State Red Line Station",
        "destination": "DePaul Center (1 E. Jackson Blvd)",
        "route": route,
        "demo_note": "This route passes Willis Tower Plaza (Death Corner) and the State/Madison wind tunnel.",
    }
