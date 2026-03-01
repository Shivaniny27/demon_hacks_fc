"""
Hazard aggregation endpoints — feeds the Mapbox heatmap and stats panel.

GET /api/v1/hazards/heatmap   — Heatmap point cloud (lat/lng/weight)
GET /api/v1/hazards/stats     — Summary stats for dashboard
GET /api/v1/hazards/death-corners — Static wind tunnel corners with live risk
"""

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_

from app.database import get_db
from app.models.report import Report, ReportStatus
from app.models.schemas import HeatmapPoint, HeatmapResponse
from app.data.chicago_constants import DEATH_CORNERS
from app.services.weather_service import fetch_wind_data

router = APIRouter(prefix="/hazards", tags=["Hazards"])


@router.get("/heatmap", response_model=HeatmapResponse)
async def get_heatmap(
    min_severity: int = Query(1, ge=1, le=5, description="Minimum severity to include"),
    category: str = Query(None, description="Filter by category"),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns a heatmap-ready point cloud of all active hazards.
    Weight is normalized 0–1 (severity / 5).

    Designed to feed directly into Mapbox GL JS heatmap layer:
    map.addSource('hazards', { type: 'geojson', data: ... })
    """
    conditions = [
        Report.status.in_([ReportStatus.ACTIVE, ReportStatus.VERIFIED]),
        Report.expires_at > datetime.now(timezone.utc),
        Report.severity >= min_severity,
    ]
    if category:
        conditions.append(Report.ai_category == category.lower())

    result = await db.execute(
        select(Report.latitude, Report.longitude, Report.severity, Report.ai_category)
        .where(and_(*conditions))
    )
    rows = result.all()

    points = [
        HeatmapPoint(
            latitude=r.latitude,
            longitude=r.longitude,
            severity=r.severity or 1,
            category=r.ai_category,
            weight=round((r.severity or 1) / 5.0, 2),
        )
        for r in rows
    ]

    return HeatmapResponse(
        points=points,
        total=len(points),
        generated_at=datetime.now(timezone.utc),
    )


@router.get("/geojson")
async def get_geojson(
    min_severity: int = Query(1, ge=1, le=5),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns a GeoJSON FeatureCollection — alternative to heatmap for marker clusters.
    Drop this directly into Mapbox addSource as type: 'geojson'.
    """
    conditions = [
        Report.status.in_([ReportStatus.ACTIVE, ReportStatus.VERIFIED]),
        Report.expires_at > datetime.now(timezone.utc),
        Report.severity >= min_severity,
    ]
    result = await db.execute(
        select(Report).where(and_(*conditions)).order_by(Report.severity.desc())
    )
    reports = result.scalars().all()

    features = []
    for r in reports:
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [r.longitude, r.latitude],
            },
            "properties": {
                "id": str(r.id),
                "category": r.ai_category,
                "severity": r.severity,
                "summary_label": r.summary_label,
                "action": r.action,
                "layer": r.layer.value if r.layer else "surface",
                "near_death_corner": r.near_death_corner,
                "near_bridge": r.near_bridge,
                "near_pedway": r.near_pedway,
                "upvotes": r.upvotes,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            },
        })

    return {
        "type": "FeatureCollection",
        "features": features,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Dashboard summary stats — total reports, breakdown by category and severity."""
    now = datetime.now(timezone.utc)

    # Total active
    total_result = await db.execute(
        select(func.count(Report.id)).where(
            and_(
                Report.status.in_([ReportStatus.ACTIVE, ReportStatus.VERIFIED]),
                Report.expires_at > now,
            )
        )
    )
    total = total_result.scalar()

    # By category
    cat_result = await db.execute(
        select(Report.ai_category, func.count(Report.id))
        .where(and_(Report.status.in_([ReportStatus.ACTIVE, ReportStatus.VERIFIED]), Report.expires_at > now))
        .group_by(Report.ai_category)
    )
    by_category = {row[0] or "unknown": row[1] for row in cat_result.all()}

    # By severity
    sev_result = await db.execute(
        select(Report.severity, func.count(Report.id))
        .where(and_(Report.status.in_([ReportStatus.ACTIVE, ReportStatus.VERIFIED]), Report.expires_at > now))
        .group_by(Report.severity)
    )
    by_severity = {str(row[0] or 0): row[1] for row in sev_result.all()}

    # Critical (severity 5)
    critical_result = await db.execute(
        select(func.count(Report.id)).where(
            and_(
                Report.status.in_([ReportStatus.ACTIVE, ReportStatus.VERIFIED]),
                Report.expires_at > now,
                Report.severity == 5,
            )
        )
    )
    critical = critical_result.scalar()

    # Accessibility-specific count
    access_result = await db.execute(
        select(func.count(Report.id)).where(
            and_(
                Report.status.in_([ReportStatus.ACTIVE, ReportStatus.VERIFIED]),
                Report.expires_at > now,
                Report.ai_category == "accessibility",
            )
        )
    )
    accessibility_issues = access_result.scalar()

    return {
        "total_active": total,
        "critical_alerts": critical,
        "accessibility_issues": accessibility_issues,
        "by_category": by_category,
        "by_severity": by_severity,
        "generated_at": now.isoformat(),
    }


@router.get("/death-corners")
async def get_death_corners():
    """
    Return the known wind-tunnel Death Corners with live Venturi risk scores
    based on current O'Hare wind data.
    """
    wind_data = await fetch_wind_data()
    corner_risks = {c["name"]: c for c in wind_data.get("death_corner_risks", [])}

    result = []
    for corner in DEATH_CORNERS:
        risk = corner_risks.get(corner.name, {})
        result.append({
            "name": corner.name,
            "latitude": corner.lat,
            "longitude": corner.lng,
            "note": corner.note,
            "venturi_score": risk.get("venturi_score", 0.0),
            "estimated_gust_mph": risk.get("estimated_gust_mph", 0.0),
            "danger_level": risk.get("danger_level", "unknown"),
        })

    return {
        "corners": result,
        "wind_speed_mph": wind_data.get("wind_speed_mph"),
        "wind_direction": wind_data.get("direction_label"),
        "loop_danger_summary": wind_data.get("loop_danger_summary"),
        "recommend_pedway": wind_data.get("recommend_pedway", False),
    }
