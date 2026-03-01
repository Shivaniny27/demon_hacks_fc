from __future__ import annotations
from datetime import datetime
from typing import Optional
from uuid import UUID
from pydantic import BaseModel, Field, field_validator
from app.models.report import HazardCategory, LoopLayer, ReportStatus


# ── Incoming report from the mobile / web client ──────────────────────────────

class ReportCreate(BaseModel):
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    address: Optional[str] = None
    description: Optional[str] = Field(None, max_length=1000)
    raw_category: Optional[str] = None   # e.g. "wind", "ice", "blocked"
    layer: LoopLayer = LoopLayer.SURFACE
    reporter_id: Optional[str] = None

    @field_validator("raw_category")
    @classmethod
    def normalize_category(cls, v):
        if v:
            return v.lower().strip()
        return v


# ── AI classification result (internal) ───────────────────────────────────────

class AIClassification(BaseModel):
    category: HazardCategory
    severity: int = Field(..., ge=1, le=5)
    summary_label: str
    action: str   # "Immediate Alert" | "Needs Verification"
    confidence: float = Field(..., ge=0.0, le=1.0)
    reasoning: str


# ── Full report returned to clients ───────────────────────────────────────────

class ReportOut(BaseModel):
    id: UUID
    latitude: float
    longitude: float
    address: Optional[str]
    description: Optional[str]
    raw_category: Optional[str]
    ai_category: Optional[str]
    severity: Optional[int]
    summary_label: Optional[str]
    action: Optional[str]
    ai_confidence: Optional[float]
    layer: LoopLayer
    near_death_corner: bool
    near_bridge: bool
    near_pedway: bool
    status: ReportStatus
    upvotes: int
    created_at: datetime
    expires_at: Optional[datetime]

    model_config = {"from_attributes": True}


# ── Heatmap point (lightweight) ───────────────────────────────────────────────

class HeatmapPoint(BaseModel):
    latitude: float
    longitude: float
    severity: int
    category: Optional[str]
    weight: float   # normalized 0–1 for Mapbox heatmap


# ── Heatmap response ──────────────────────────────────────────────────────────

class HeatmapResponse(BaseModel):
    points: list[HeatmapPoint]
    total: int
    generated_at: datetime


# ── Route request ─────────────────────────────────────────────────────────────

class RouteRequest(BaseModel):
    origin_lat: float
    origin_lng: float
    dest_lat: float
    dest_lng: float
    prefer_layer: LoopLayer = LoopLayer.SURFACE
    accessibility: bool = False


# ── Segment in a route ────────────────────────────────────────────────────────

class RouteSegment(BaseModel):
    from_lat: float
    from_lng: float
    to_lat: float
    to_lng: float
    hazard_score: float   # 0.0 = clear, 1.0 = max danger
    layer: LoopLayer
    notes: list[str] = []


class RouteResponse(BaseModel):
    segments: list[RouteSegment]
    total_hazard_score: float
    warnings: list[str]
    estimated_minutes: float


# ── WebSocket broadcast payload ───────────────────────────────────────────────

class WSMessage(BaseModel):
    event: str   # "new_report" | "report_expired" | "upvote"
    payload: dict
