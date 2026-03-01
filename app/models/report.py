import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy import Column, String, Float, Integer, Boolean, DateTime, Text, Enum
from sqlalchemy.dialects.postgresql import UUID
from app.database import Base
import enum


class HazardCategory(str, enum.Enum):
    WIND = "wind"
    ICE = "ice"
    BLOCKED = "blocked"
    ACCESSIBILITY = "accessibility"
    CYCLING = "cycling"
    BRIDGE = "bridge"
    CROWD = "crowd"
    CONSTRUCTION = "construction"
    OTHER = "other"


class LoopLayer(str, enum.Enum):
    ELEVATED = "elevated"   # L train level
    SURFACE = "surface"     # Street level
    PEDWAY = "pedway"       # Underground


class ReportStatus(str, enum.Enum):
    PENDING = "pending"         # just submitted, AI not yet run
    ACTIVE = "active"           # AI classified, live on map
    VERIFIED = "verified"       # confirmed by multiple users
    EXPIRED = "expired"         # older than REPORT_EXPIRY_HOURS
    DISMISSED = "dismissed"     # false report


class Report(Base):
    __tablename__ = "reports"

    # Identity
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    reporter_id = Column(String(64), nullable=True)  # anonymous device fingerprint

    # Location
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    address = Column(String(255), nullable=True)

    # Raw user input
    description = Column(Text, nullable=True)          # free-text from user
    raw_category = Column(String(64), nullable=True)   # user-selected icon category

    # AI classification (filled after OpenAI call)
    ai_category = Column(String(64), nullable=True)
    severity = Column(Integer, nullable=True)           # 1 (minor) – 5 (critical)
    summary_label = Column(String(255), nullable=True)  # short AI-generated title
    action = Column(String(64), nullable=True)          # "Immediate Alert" | "Needs Verification"
    ai_confidence = Column(Float, nullable=True)        # 0.0 – 1.0
    ai_reasoning = Column(Text, nullable=True)          # debug / transparency

    # Chicago-specific metadata
    layer = Column(
        Enum(LoopLayer, name="loop_layer"),
        default=LoopLayer.SURFACE,
        nullable=False,
    )
    near_death_corner = Column(Boolean, default=False)  # known wind-tunnel intersection
    near_bridge = Column(Boolean, default=False)
    near_pedway = Column(Boolean, default=False)

    # Status & lifecycle
    status = Column(
        Enum(ReportStatus, name="report_status"),
        default=ReportStatus.PENDING,
        nullable=False,
    )
    upvotes = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), onupdate=lambda: datetime.now(timezone.utc))

    def __repr__(self) -> str:
        return f"<Report id={self.id} severity={self.severity} category={self.ai_category}>"
