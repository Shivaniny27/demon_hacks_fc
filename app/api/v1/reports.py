"""
Report endpoints — the core data pipeline of LoopSense.

POST /api/v1/reports   — Submit a new hazard report
GET  /api/v1/reports   — List active reports (with filters)
GET  /api/v1/reports/{id} — Get a single report
POST /api/v1/reports/{id}/upvote — Upvote (confirm) a report
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from app.database import get_db
from app.models.report import Report, ReportStatus
from app.models.schemas import ReportCreate, ReportOut
from app.config import settings
from app.data.chicago_constants import classify_location
from app.services.ai_agent import classify_report
from app.services.weather_service import get_wind_speed_mph
from app.api.websocket import manager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/reports", tags=["Reports"])


async def _run_ai_classification(report_id: UUID, db_url: str):
    """
    Background task: run AI classification and update the report in DB.
    Uses a fresh DB session to avoid sharing across async context.
    """
    from app.database import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        try:
            result = await session.execute(select(Report).where(Report.id == report_id))
            report = result.scalar_one_or_none()
            if not report:
                return

            # Get current wind speed to inject into AI context
            wind_speed = await get_wind_speed_mph()

            classification = await classify_report(
                description=report.description,
                raw_category=report.raw_category,
                address=report.address,
                layer=report.layer.value if report.layer else "surface",
                near_death_corner=report.near_death_corner,
                near_bridge=report.near_bridge,
                near_pedway=report.near_pedway,
                wind_speed_mph=wind_speed,
            )

            # Persist AI classification
            report.ai_category = classification.category.value
            report.severity = classification.severity
            report.summary_label = classification.summary_label
            report.action = classification.action
            report.ai_confidence = classification.confidence
            report.ai_reasoning = classification.reasoning
            report.status = ReportStatus.ACTIVE

            await session.commit()
            await session.refresh(report)

            # Broadcast to all WebSocket clients
            await manager.broadcast(
                "new_report",
                {
                    "id": str(report.id),
                    "latitude": report.latitude,
                    "longitude": report.longitude,
                    "address": report.address,
                    "ai_category": report.ai_category,
                    "severity": report.severity,
                    "summary_label": report.summary_label,
                    "action": report.action,
                    "layer": report.layer.value,
                    "near_death_corner": report.near_death_corner,
                    "near_bridge": report.near_bridge,
                    "near_pedway": report.near_pedway,
                    "created_at": report.created_at.isoformat(),
                },
            )
            logger.info("AI classified report %s → sev=%s cat=%s", report_id, report.severity, report.ai_category)

        except Exception as exc:
            logger.error("Background AI classification failed for %s: %s", report_id, exc)


@router.post("/", response_model=ReportOut, status_code=201)
async def submit_report(
    body: ReportCreate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """
    Submit a new hazard report.

    The report is immediately stored with PENDING status.
    AI classification runs in the background and broadcasts via WebSocket
    once complete (~1–2 seconds).
    """
    # Classify Chicago-specific location flags
    loc_flags = classify_location(body.latitude, body.longitude)

    expires_at = datetime.now(timezone.utc) + timedelta(hours=settings.REPORT_EXPIRY_HOURS)

    report = Report(
        latitude=body.latitude,
        longitude=body.longitude,
        address=body.address,
        description=body.description,
        raw_category=body.raw_category,
        layer=body.layer,
        reporter_id=body.reporter_id,
        status=ReportStatus.PENDING,
        expires_at=expires_at,
        **loc_flags,
    )
    db.add(report)
    await db.commit()
    await db.refresh(report)

    # Kick off AI classification asynchronously
    background_tasks.add_task(_run_ai_classification, report.id, settings.DATABASE_URL)

    logger.info("New report %s submitted at (%.4f, %.4f)", report.id, body.latitude, body.longitude)
    return report


@router.get("/", response_model=list[ReportOut])
async def list_reports(
    status: Optional[str] = Query(None, description="Filter by status: active, pending, verified"),
    category: Optional[str] = Query(None, description="Filter by AI category"),
    min_severity: int = Query(1, ge=1, le=5),
    layer: Optional[str] = Query(None, description="elevated | surface | pedway"),
    limit: int = Query(100, le=500),
    db: AsyncSession = Depends(get_db),
):
    """List active hazard reports with optional filters."""
    conditions = [
        Report.expires_at > datetime.now(timezone.utc),
    ]

    if status:
        try:
            conditions.append(Report.status == ReportStatus(status))
        except ValueError:
            raise HTTPException(400, f"Invalid status: {status}")
    else:
        # Default: only show active/verified (not pending/expired/dismissed)
        conditions.append(Report.status.in_([ReportStatus.ACTIVE, ReportStatus.VERIFIED]))

    if category:
        conditions.append(Report.ai_category == category.lower())

    if min_severity > 1:
        conditions.append(Report.severity >= min_severity)

    if layer:
        conditions.append(Report.layer == layer)

    result = await db.execute(
        select(Report)
        .where(and_(*conditions))
        .order_by(Report.severity.desc(), Report.created_at.desc())
        .limit(limit)
    )
    return result.scalars().all()


@router.get("/{report_id}", response_model=ReportOut)
async def get_report(report_id: UUID, db: AsyncSession = Depends(get_db)):
    """Fetch a single report by ID."""
    result = await db.execute(select(Report).where(Report.id == report_id))
    report = result.scalar_one_or_none()
    if not report:
        raise HTTPException(404, "Report not found.")
    return report


@router.post("/{report_id}/upvote", response_model=ReportOut)
async def upvote_report(report_id: UUID, db: AsyncSession = Depends(get_db)):
    """
    Upvote a report (community confirmation).
    Reports with ≥3 upvotes are automatically promoted to VERIFIED.
    """
    result = await db.execute(select(Report).where(Report.id == report_id))
    report = result.scalar_one_or_none()
    if not report:
        raise HTTPException(404, "Report not found.")

    report.upvotes += 1
    if report.upvotes >= 3 and report.status == ReportStatus.ACTIVE:
        report.status = ReportStatus.VERIFIED

    await db.commit()
    await db.refresh(report)

    # Broadcast upvote event
    await manager.broadcast("upvote", {"id": str(report_id), "upvotes": report.upvotes})
    return report
