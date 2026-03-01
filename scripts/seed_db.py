"""
Seed the LoopSense database with 30 realistic Chicago Loop hazard reports.

Usage:
    python -m scripts.seed_db

Requires DATABASE_URL to be set in .env or environment.
"""

import asyncio
import sys
import os

# Allow running from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone, timedelta
from uuid import uuid4

from sqlalchemy import text
from app.database import AsyncSessionLocal, engine, Base
from app.models.report import Report, ReportStatus, LoopLayer
from app.data.seed import SEED_REPORTS
from app.config import settings


async def seed():
    # Create tables
    async with engine.begin() as conn:
        try:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        except Exception:
            pass
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        # Check if already seeded
        from sqlalchemy import select, func
        result = await session.execute(select(func.count(Report.id)))
        count = result.scalar()
        if count > 0:
            print(f"Database already has {count} reports. Skipping seed.")
            print("To re-seed, run: DELETE FROM reports; and try again.")
            return

        expiry = datetime.now(timezone.utc) + timedelta(hours=settings.REPORT_EXPIRY_HOURS)

        reports = []
        for data in SEED_REPORTS:
            layer_val = data.get("layer", "surface")
            try:
                layer = LoopLayer(layer_val)
            except ValueError:
                layer = LoopLayer.SURFACE

            status_val = data.get("status", "active")
            try:
                status = ReportStatus(status_val)
            except ValueError:
                status = ReportStatus.ACTIVE

            report = Report(
                id=uuid4(),
                latitude=data["latitude"],
                longitude=data["longitude"],
                address=data.get("address"),
                description=data.get("description"),
                raw_category=data.get("raw_category"),
                ai_category=data.get("ai_category"),
                severity=data.get("severity"),
                summary_label=data.get("summary_label"),
                action=data.get("action"),
                ai_confidence=data.get("ai_confidence"),
                ai_reasoning="Pre-seeded demo data for LoopSense hackathon.",
                layer=layer,
                near_death_corner=data.get("near_death_corner", False),
                near_bridge=data.get("near_bridge", False),
                near_pedway=data.get("near_pedway", False),
                status=status,
                upvotes=0,
                created_at=datetime.now(timezone.utc),
                expires_at=expiry,
            )
            reports.append(report)

        session.add_all(reports)
        await session.commit()
        print(f"✅  Seeded {len(reports)} hazard reports into the database.")
        print(f"    Severity 5 (critical): {sum(1 for r in reports if r.severity == 5)}")
        print(f"    Accessibility issues:  {sum(1 for r in reports if r.ai_category == 'accessibility')}")
        print(f"    Near Death Corners:    {sum(1 for r in reports if r.near_death_corner)}")
        print(f"    Expires at:            {expiry.strftime('%Y-%m-%d %H:%M UTC')}")


if __name__ == "__main__":
    asyncio.run(seed())
