"""
LoopNav Pydantic Schemas.

Separate from the existing app/models/schemas.py (hazard reports).
These are the request/response models for the 3-layer navigation system.
"""

from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional


# ── Shared ────────────────────────────────────────────────────────────────────

class Coords(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lng: float = Field(..., ge=-180, le=180)


# ── Route request ─────────────────────────────────────────────────────────────

class NavRouteRequest(BaseModel):
    origin: Coords
    destination: Coords
    mode: str = Field(
        default="optimal",
        description="optimal | street_only | pedway_preferred | mid_preferred"
    )
    accessible: bool = False
    simulated_hour: Optional[int] = Field(
        default=None,
        ge=0, le=23,
        description="Override current hour for time-simulation slider (0-23)"
    )


# ── Route response ────────────────────────────────────────────────────────────

class RouteSegment(BaseModel):
    level: str                              # street | mid | pedway
    geometry: list[list[float]]             # [[lng, lat], [lng, lat], ...]
    time_s: int
    distance_m: float
    covered: bool
    instruction: str
    connector_type: Optional[str] = None    # stairs | elevator | escalator | null
    elevator_id: Optional[str] = None


class SingleRoute(BaseModel):
    segments: list[RouteSegment]
    total_time_s: int
    total_distance_m: float
    covered_pct: float                      # % of distance that is covered/sheltered
    level_changes: int
    route_summary: str                      # e.g. "Street → Pedway → Street"
    accessibility_score: int               # 0-100
    label: str                              # "Fastest" | "Most Covered" | "Fewest Changes"


class NavRouteResponse(BaseModel):
    routes: dict[str, SingleRoute]          # fastest, most_covered, fewest_changes
    recommended: str                        # key of the recommended route
    weather: dict
    elevator_alerts: list[dict]
    weather_recommendation: Optional[str]
    generated_at: str


# ── AI natural language route ─────────────────────────────────────────────────

class AIRouteRequest(BaseModel):
    text: str = Field(..., min_length=3, max_length=500)


class AIRouteResponse(BaseModel):
    origin: Optional[str]
    destination: Optional[str]
    mode: str
    accessible: bool
    reasoning: str


# ── Isochrone ─────────────────────────────────────────────────────────────────

class IsochroneRequest(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lng: float = Field(..., ge=-180, le=180)
    minutes: int = Field(default=10, ge=1, le=30)
    mode: str = "optimal"
