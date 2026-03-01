"""
LoopNav — Production Weather & Wind Intelligence Service.

Integrates with OpenWeatherMap for real-time Chicago Loop conditions.
Computes pedestrian routing impact (none/low/medium/high) based on:
  - Wind speed + gust at O'Hare (amplified for downtown canyons via Venturi model)
  - Temperature "feels like" (wind chill / heat index)
  - Precipitation type and intensity
  - Visibility (fog, heavy snow)
  - Time-of-day seasonal modifiers

Architecture
────────────
  WeatherService        — main class, singleton shared via app.state.weather
  OpenWeatherClient     — async HTTP client with retry + timeout
  WeatherCache          — Redis-backed + in-memory L1 cache
  WeatherImpactScorer   — converts raw data into routing impact level
  DeathCornerAnalyzer   — per-intersection Venturi amplification
  MockScenarioManager   — demo scenarios for offline/hackathon mode

Caching strategy
────────────────
  L1 (in-memory):  30-second TTL — eliminates duplicate requests within a spike
  L2 (Redis):       5-minute TTL — survives API server restart

Retry strategy
──────────────
  3 attempts with 0.5s / 1.0s / 2.0s back-off. On all failures: mock data.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.config import settings
from app.data.chicago_constants import (
    DEATH_CORNERS,
    LOOP_BRIDGES,
    PEDWAY_NODES,
    SUNNY_SIDES,
    Location,
)

log = logging.getLogger("loopnav.weather")

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

OPENWEATHER_CURRENT_URL  = "https://api.openweathermap.org/data/2.5/weather"
OPENWEATHER_FORECAST_URL = "https://api.openweathermap.org/data/2.5/forecast"

# O'Hare Airport — closest ASOS station to the Loop with reliable data.
# Wind is measured at 10m above grade; we apply canyon amplification below.
OHARE_LAT = 41.9742
OHARE_LNG = -87.9073

# Loop centroid used for city-level context
LOOP_LAT = 41.8827
LOOP_LNG = -87.6307

# Venturi amplification: canyons can double wind speed.
# We model it as a cosine function of canyon-alignment vs wind direction.
CANYON_AMP_MAX = 2.0    # maximum amplification at full canyon alignment

# Wind thresholds for pedestrian impact (mph)
WIND_NONE_MAX     = 10.0   # breezy is fine
WIND_LOW_MAX      = 20.0   # noticeable, no danger
WIND_MEDIUM_MAX   = 30.0   # umbrellas flip, bike lanes dangerous
WIND_HIGH_MIN     = 30.0   # 30+ mph = avoid street-level exposure

# Temperature thresholds (°F) for winter vs summer impact
TEMP_VERY_COLD    = 20.0   # frostbite possible, strongly prefer covered routes
TEMP_COLD         = 35.0   # cold, covered routes preferred
TEMP_HOT          = 90.0   # heat index; outdoor routes less desirable

# Visibility thresholds (metres)
VIS_FOG_SEVERE    = 500    # severe fog — all outdoor routes impacted
VIS_FOG_MODERATE  = 1000   # moderate fog

# Cache TTLs (seconds)
L1_TTL_S = 30     # in-memory
L2_TTL_S = 300    # Redis

# HTTP timeout / retry
HTTP_TIMEOUT_S = 6.0
RETRY_DELAYS   = [0.5, 1.0, 2.0]

# Mock scenario names
MOCK_BLIZZARD      = "blizzard"
MOCK_NORMAL_WINTER = "normal_winter"
MOCK_CLEAR_SUMMER  = "clear_summer"
MOCK_RAIN          = "rain"
MOCK_HEAT          = "heat"
MOCK_FOG           = "fog"

VALID_MOCK_SCENARIOS = {
    MOCK_BLIZZARD, MOCK_NORMAL_WINTER, MOCK_CLEAR_SUMMER,
    MOCK_RAIN, MOCK_HEAT, MOCK_FOG,
}

# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

class WeatherImpactLevel(str, Enum):
    NONE   = "none"
    LOW    = "low"
    MEDIUM = "medium"
    HIGH   = "high"


@dataclass
class CornerRisk:
    """Venturi risk for a single Death Corner intersection."""
    name:               str
    lat:                float
    lon:                float
    note:               str
    venturi_score:      float       # 0.0–1.0
    amplified_gust_mph: float
    danger_level:       str         # low / moderate / high / extreme
    canyon_alignment:   float       # 0–1 how aligned wind is with canyon axis
    canyon_axis_deg:    float       # the axis of the street canyon


@dataclass
class CurrentWeather:
    """Parsed response from OpenWeatherMap /weather."""
    # Core measurements
    temp_f:         float
    feels_like_f:   float
    humidity_pct:   float
    pressure_hpa:   float
    visibility_m:   float
    # Wind
    wind_speed_mph: float
    wind_gust_mph:  float
    wind_deg:       float
    wind_label:     str             # N / NE / E / SE / S / SW / W / NW
    # Conditions
    condition_id:   int             # OpenWeatherMap condition code
    condition_main: str             # "Rain", "Snow", "Clear", etc.
    condition_desc: str             # "light snow", "heavy rain", etc.
    # Derived
    is_raining:     bool
    is_snowing:     bool
    is_foggy:       bool
    precip_1h_mm:   float           # precipitation last 1h
    # Metadata
    source:         str             # "openweathermap" / "mock:{scenario}"
    fetched_at:     str             # ISO timestamp


@dataclass
class ForecastSlot:
    """Single 3-hour forecast slot."""
    dt_utc:         str
    temp_f:         float
    feels_like_f:   float
    wind_speed_mph: float
    wind_gust_mph:  float
    condition_main: str
    condition_desc: str
    pop:            float           # probability of precipitation 0–1
    precip_3h_mm:   float


@dataclass
class WeatherPayload:
    """
    Complete weather response returned to API layer.
    Includes current conditions + routing intelligence.
    """
    # Raw conditions
    current:            CurrentWeather
    # Derived routing context
    impact:             WeatherImpactLevel
    impact_score:       float               # 0.0–1.0 continuous score
    loop_danger_summary: str
    recommend_pedway:   bool
    recommend_covered:  bool
    routing_multiplier: float               # cost multiplier for uncovered edges
    recommendation:     str                 # human-readable routing suggestion
    # Death corner analysis
    death_corner_risks: List[CornerRisk]
    worst_corner:       Optional[CornerRisk]
    # Optional forecast (populated only when requested)
    forecast:           Optional[List[ForecastSlot]]
    # Routing-engine context (simplified, passed to graph weight functions)
    routing_context:    Dict[str, Any]
    # Extras
    sunny_sides:        List[Dict]
    bridge_warnings:    List[str]
    seasonal_note:      str


@dataclass
class RoutingWeatherContext:
    """
    Minimal weather context used by the routing engine.
    Passed as `weather` kwarg to get_route_alternatives().
    """
    impact:             str             # "none" / "low" / "medium" / "high"
    wind_speed_mph:     float
    wind_gust_mph:      float
    wind_deg:           float
    temp_f:             float
    feels_like_f:       float
    is_raining:         bool
    is_snowing:         bool
    recommend_pedway:   bool
    routing_multiplier: float
    fetched_at:         str


# ─────────────────────────────────────────────────────────────────────────────
# L1 In-memory cache
# ─────────────────────────────────────────────────────────────────────────────

class _L1Cache:
    """Simple timestamp-based in-memory cache for a single weather payload."""

    def __init__(self, ttl_s: int = L1_TTL_S):
        self._data: Optional[WeatherPayload] = None
        self._ts:   float                    = 0.0
        self._ttl:  int                      = ttl_s

    def get(self) -> Optional[WeatherPayload]:
        if self._data and (time.monotonic() - self._ts) < self._ttl:
            return self._data
        return None

    def set(self, payload: WeatherPayload) -> None:
        self._data = payload
        self._ts   = time.monotonic()

    def invalidate(self) -> None:
        self._data = None
        self._ts   = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# OpenWeatherMap HTTP client
# ─────────────────────────────────────────────────────────────────────────────

class OpenWeatherClient:
    """
    Thin async HTTP client wrapping OpenWeatherMap.
    Implements retry with exponential back-off and structured error logging.
    """

    def __init__(self, api_key: str):
        self._key     = api_key
        self._timeout = HTTP_TIMEOUT_S

    async def _get(self, url: str, params: Dict) -> Optional[Dict]:
        params["appid"] = self._key
        params["units"] = "imperial"    # °F, mph throughout the service

        for attempt, delay in enumerate(RETRY_DELAYS + [0], start=1):
            try:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    resp = await client.get(url, params=params)
                    resp.raise_for_status()
                    return resp.json()
            except httpx.TimeoutException:
                log.warning("OWM timeout (attempt %d/%d) — %s", attempt, len(RETRY_DELAYS) + 1, url)
            except httpx.HTTPStatusError as exc:
                log.warning("OWM HTTP %d (attempt %d) — %s", exc.response.status_code, attempt, url)
                if exc.response.status_code in (401, 403):
                    log.error("OWM API key invalid or quota exceeded")
                    return None
            except Exception as exc:
                log.warning("OWM error (attempt %d): %s", attempt, exc)

            if delay > 0:
                await asyncio.sleep(delay)

        return None

    async def fetch_current(self, lat: float, lon: float) -> Optional[Dict]:
        return await self._get(OPENWEATHER_CURRENT_URL, {"lat": lat, "lon": lon})

    async def fetch_forecast(self, lat: float, lon: float, cnt: int = 4) -> Optional[Dict]:
        """cnt = number of 3-hour slots to return (4 = 12 hours)."""
        return await self._get(OPENWEATHER_FORECAST_URL, {"lat": lat, "lon": lon, "cnt": cnt})


# ─────────────────────────────────────────────────────────────────────────────
# Wind direction helpers
# ─────────────────────────────────────────────────────────────────────────────

def _wind_label(deg: float) -> str:
    dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    return dirs[round(deg / 45) % 8]


def _wind_chill_f(temp_f: float, wind_mph: float) -> float:
    """NWS wind chill formula. Valid for temp ≤ 50°F and wind > 3 mph."""
    if temp_f > 50 or wind_mph <= 3:
        return temp_f
    return (
        35.74
        + 0.6215 * temp_f
        - 35.75 * (wind_mph ** 0.16)
        + 0.4275 * temp_f * (wind_mph ** 0.16)
    )


def _heat_index_f(temp_f: float, humidity: float) -> float:
    """Rothfusz heat index formula. Valid for temp ≥ 80°F and humidity > 40%."""
    if temp_f < 80 or humidity < 40:
        return temp_f
    hi = (
        -42.379
        + 2.04901523 * temp_f
        + 10.14333127 * humidity
        - 0.22475541 * temp_f * humidity
        - 0.00683783 * temp_f ** 2
        - 0.05481717 * humidity ** 2
        + 0.00122874 * temp_f ** 2 * humidity
        + 0.00085282 * temp_f * humidity ** 2
        - 0.00000199 * temp_f ** 2 * humidity ** 2
    )
    return round(hi, 1)


# ─────────────────────────────────────────────────────────────────────────────
# Death Corner Venturi analysis
# ─────────────────────────────────────────────────────────────────────────────

# Approximate street canyon axis per Death Corner (degrees from north)
_CANYON_AXES: Dict[str, float] = {
    "Wacker & Adams":      45.0,   # Wacker runs NE-SW
    "State & Madison":      0.0,   # State runs N-S
    "Willis Tower Plaza": 180.0,   # Open plaza — all directions
    "Michigan & Wacker":   90.0,   # Michigan N-S, Wacker E-W
    "Dearborn & Monroe":    0.0,   # Classic N-S canyon
}


class DeathCornerAnalyzer:
    """Computes Venturi amplification and danger level per Death Corner."""

    def analyze_all(
        self,
        wind_speed_mph: float,
        wind_gust_mph:  float,
        wind_deg:       float,
    ) -> Tuple[List[CornerRisk], Optional[CornerRisk]]:
        """
        Score every Death Corner and return (all_risks, worst_risk).
        """
        risks: List[CornerRisk] = []
        for corner in DEATH_CORNERS:
            risk = self._score_corner(corner, wind_speed_mph, wind_gust_mph, wind_deg)
            risks.append(risk)

        risks.sort(key=lambda r: -r.venturi_score)
        worst = risks[0] if risks else None
        return risks, worst

    def _score_corner(
        self,
        corner:         Location,
        wind_speed_mph: float,
        wind_gust_mph:  float,
        wind_deg:       float,
    ) -> CornerRisk:
        axis      = _CANYON_AXES.get(corner.name, 0.0)
        alignment = abs(math.cos(math.radians(wind_deg - axis)))
        speed_ms  = wind_speed_mph * 0.44704

        # Speed factor: normalised at 15 m/s ≈ 34 mph
        speed_factor = min(speed_ms / 15.0, 1.0)

        # Venturi score: 0–1
        venturi_score = round(speed_factor * (0.5 + 0.5 * alignment), 4)

        # Amplified gust
        amp_gust = round(wind_gust_mph * (1.0 + venturi_score * (CANYON_AMP_MAX - 1.0)), 1)

        if venturi_score > 0.75:
            danger = "extreme"
        elif venturi_score > 0.50:
            danger = "high"
        elif venturi_score > 0.25:
            danger = "moderate"
        else:
            danger = "low"

        return CornerRisk(
            name               = corner.name,
            lat                = corner.lat,
            lon                = corner.lng,
            note               = corner.note,
            venturi_score      = venturi_score,
            amplified_gust_mph = amp_gust,
            danger_level       = danger,
            canyon_alignment   = round(alignment, 4),
            canyon_axis_deg    = axis,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Impact scoring
# ─────────────────────────────────────────────────────────────────────────────

class WeatherImpactScorer:
    """
    Converts raw weather measurements into a routing impact level and multiplier.

    Scoring weights:
      Wind (gust):    40%
      Temperature:    30%
      Precipitation:  20%
      Visibility:     10%
    """

    def score(
        self,
        wind_gust_mph:  float,
        temp_f:         float,
        feels_like_f:   float,
        is_raining:     bool,
        is_snowing:     bool,
        is_foggy:       bool,
        precip_1h_mm:   float,
        visibility_m:   float,
        worst_venturi:  float,
    ) -> Tuple[WeatherImpactLevel, float, float]:
        """
        Returns (impact_level, impact_score 0–1, routing_multiplier 1.0–2.5).
        """

        # ── Wind score (0–1) ──────────────────────────────────────────────
        effective_gust = wind_gust_mph * (1.0 + worst_venturi * 0.8)
        if effective_gust >= WIND_HIGH_MIN:
            wind_score = min((effective_gust - WIND_HIGH_MIN) / 30.0 + 0.7, 1.0)
        elif effective_gust >= WIND_LOW_MAX:
            wind_score = 0.4 + (effective_gust - WIND_LOW_MAX) / (WIND_HIGH_MIN - WIND_LOW_MAX) * 0.3
        elif effective_gust >= WIND_NONE_MAX:
            wind_score = (effective_gust - WIND_NONE_MAX) / (WIND_LOW_MAX - WIND_NONE_MAX) * 0.4
        else:
            wind_score = 0.0

        # ── Temperature score (0–1) ───────────────────────────────────────
        if feels_like_f <= TEMP_VERY_COLD:
            temp_score = min((TEMP_VERY_COLD - feels_like_f) / 30.0 + 0.8, 1.0)
        elif feels_like_f <= TEMP_COLD:
            temp_score = 0.4 + (TEMP_COLD - feels_like_f) / (TEMP_COLD - TEMP_VERY_COLD) * 0.4
        elif feels_like_f >= TEMP_HOT:
            temp_score = min((feels_like_f - TEMP_HOT) / 15.0 + 0.3, 0.7)
        else:
            temp_score = 0.0

        # ── Precipitation score (0–1) ─────────────────────────────────────
        precip_score = 0.0
        if is_snowing:
            precip_score = min(0.5 + precip_1h_mm / 10.0, 1.0)
        elif is_raining:
            precip_score = min(0.3 + precip_1h_mm / 20.0, 0.9)
        elif is_foggy:
            precip_score = 0.2

        # ── Visibility score (0–1) ────────────────────────────────────────
        if visibility_m <= VIS_FOG_SEVERE:
            vis_score = 0.9
        elif visibility_m <= VIS_FOG_MODERATE:
            vis_score = 0.5
        elif visibility_m <= 3000:
            vis_score = 0.2
        else:
            vis_score = 0.0

        # ── Weighted aggregate ────────────────────────────────────────────
        impact_score = round(
            wind_score  * 0.40
            + temp_score  * 0.30
            + precip_score * 0.20
            + vis_score   * 0.10,
            4,
        )

        # ── Classify ─────────────────────────────────────────────────────
        if impact_score >= 0.65:
            level = WeatherImpactLevel.HIGH
        elif impact_score >= 0.40:
            level = WeatherImpactLevel.MEDIUM
        elif impact_score >= 0.15:
            level = WeatherImpactLevel.LOW
        else:
            level = WeatherImpactLevel.NONE

        # ── Routing cost multiplier ───────────────────────────────────────
        multiplier_map = {
            WeatherImpactLevel.HIGH:   2.5,
            WeatherImpactLevel.MEDIUM: 1.8,
            WeatherImpactLevel.LOW:    1.3,
            WeatherImpactLevel.NONE:   1.0,
        }
        multiplier = multiplier_map[level]

        return level, impact_score, multiplier


# ─────────────────────────────────────────────────────────────────────────────
# Recommendation builder
# ─────────────────────────────────────────────────────────────────────────────

class RecommendationBuilder:
    """Builds human-readable routing recommendations from weather context."""

    def build(
        self,
        impact:          WeatherImpactLevel,
        current:         CurrentWeather,
        worst_corner:    Optional[CornerRisk],
    ) -> str:
        parts: List[str] = []

        if impact == WeatherImpactLevel.HIGH:
            if current.is_snowing:
                parts.append("Heavy snow — use Pedway tunnels to avoid slippery sidewalks.")
            elif current.is_raining:
                parts.append("Heavy rain — choose covered routes through the Pedway or skywalk.")
            elif current.wind_gust_mph >= WIND_HIGH_MIN:
                if worst_corner:
                    parts.append(
                        f"Dangerous wind gusts near {worst_corner.name} "
                        f"({worst_corner.amplified_gust_mph:.0f} mph amplified). "
                        "Take the Pedway to avoid wind tunnels."
                    )
                else:
                    parts.append("Dangerous wind — use Pedway or sheltered routes.")
            elif current.feels_like_f <= TEMP_VERY_COLD:
                parts.append(
                    f"Wind chill {current.feels_like_f:.0f}°F — risk of frostbite. "
                    "Stay underground via the Pedway."
                )
            else:
                parts.append("Severe conditions — strongly prefer Pedway or covered skywalk routes.")

        elif impact == WeatherImpactLevel.MEDIUM:
            if current.is_snowing:
                parts.append("Light snow — covered routes recommended.")
            elif current.is_raining:
                parts.append("Rain — bring an umbrella or use covered routes.")
            elif current.wind_speed_mph >= WIND_LOW_MAX:
                parts.append(
                    f"Wind at {current.wind_speed_mph:.0f} mph — "
                    "avoid open plazas like Willis Tower and Millennium Park."
                )
            elif current.feels_like_f <= TEMP_COLD:
                parts.append(
                    f"Cold ({current.feels_like_f:.0f}°F feels like) — "
                    "Pedway or sheltered mid-level routes are warmer."
                )
            else:
                parts.append("Moderate conditions — covered routes preferred.")

        elif impact == WeatherImpactLevel.LOW:
            if current.temp_f >= TEMP_HOT:
                parts.append(
                    f"Hot ({current.temp_f:.0f}°F) — shaded routes or air-conditioned "
                    "Pedway passages keep you cooler."
                )
            elif current.wind_speed_mph >= WIND_NONE_MAX:
                parts.append("Breezy — stay on the sunny side of the street when possible.")
            else:
                parts.append("Mild conditions — all routes comfortable.")
        else:
            parts.append("Clear conditions — all routes open. Enjoy the walk!")

        # Sunny-side bonus hint in winter
        if current.temp_f < TEMP_COLD and impact != WeatherImpactLevel.HIGH:
            parts.append("Tip: walk the sunny south-facing side of Wacker Dr for extra warmth.")

        return " ".join(parts)

    def loop_danger_summary(self, impact: WeatherImpactLevel, wind_gust: float) -> str:
        if impact == WeatherImpactLevel.HIGH:
            return "Extreme — avoid outdoor travel; use Pedway tunnels"
        if impact == WeatherImpactLevel.MEDIUM:
            return "High — take Pedway or sheltered routes where possible"
        if impact == WeatherImpactLevel.LOW:
            return "Moderate — some discomfort; covered routes preferred"
        return "Low — normal pedestrian conditions"


# ─────────────────────────────────────────────────────────────────────────────
# Mock scenario factory
# ─────────────────────────────────────────────────────────────────────────────

class MockScenarioManager:
    """
    Provides realistic demo weather scenarios for hackathon offline mode.
    Active scenario can be set via POST /nav/weather/mock.
    """

    def __init__(self):
        self._active: Optional[str] = None

    @property
    def active(self) -> Optional[str]:
        return self._active

    def set_scenario(self, name: str) -> None:
        if name not in VALID_MOCK_SCENARIOS:
            raise ValueError(f"Unknown scenario '{name}'. Valid: {sorted(VALID_MOCK_SCENARIOS)}")
        self._active = name
        log.info("Weather mock scenario set: %s", name)

    def clear_scenario(self) -> None:
        self._active = None
        log.info("Weather mock scenario cleared — using real API")

    def build(self, scenario: Optional[str] = None) -> CurrentWeather:
        name = scenario or self._active or MOCK_NORMAL_WINTER
        builders = {
            MOCK_BLIZZARD:      self._blizzard,
            MOCK_NORMAL_WINTER: self._normal_winter,
            MOCK_CLEAR_SUMMER:  self._clear_summer,
            MOCK_RAIN:          self._rain,
            MOCK_HEAT:          self._heat,
            MOCK_FOG:           self._fog,
        }
        return builders.get(name, self._normal_winter)()

    # ── Scenario definitions ─────────────────────────────────────────────────

    def _blizzard(self) -> CurrentWeather:
        return CurrentWeather(
            temp_f=18.0, feels_like_f=2.0, humidity_pct=90.0,
            pressure_hpa=1005.0, visibility_m=300.0,
            wind_speed_mph=35.0, wind_gust_mph=52.0, wind_deg=315.0,
            wind_label="NW",
            condition_id=602, condition_main="Snow",
            condition_desc="heavy snow",
            is_raining=False, is_snowing=True, is_foggy=False,
            precip_1h_mm=8.0,
            source=f"mock:{MOCK_BLIZZARD}",
            fetched_at=datetime.now(timezone.utc).isoformat(),
        )

    def _normal_winter(self) -> CurrentWeather:
        return CurrentWeather(
            temp_f=28.0, feels_like_f=18.0, humidity_pct=65.0,
            pressure_hpa=1018.0, visibility_m=8000.0,
            wind_speed_mph=18.0, wind_gust_mph=28.0, wind_deg=315.0,
            wind_label="NW",
            condition_id=801, condition_main="Clouds",
            condition_desc="few clouds",
            is_raining=False, is_snowing=False, is_foggy=False,
            precip_1h_mm=0.0,
            source=f"mock:{MOCK_NORMAL_WINTER}",
            fetched_at=datetime.now(timezone.utc).isoformat(),
        )

    def _clear_summer(self) -> CurrentWeather:
        return CurrentWeather(
            temp_f=74.0, feels_like_f=76.0, humidity_pct=55.0,
            pressure_hpa=1015.0, visibility_m=16000.0,
            wind_speed_mph=8.0, wind_gust_mph=12.0, wind_deg=180.0,
            wind_label="S",
            condition_id=800, condition_main="Clear",
            condition_desc="clear sky",
            is_raining=False, is_snowing=False, is_foggy=False,
            precip_1h_mm=0.0,
            source=f"mock:{MOCK_CLEAR_SUMMER}",
            fetched_at=datetime.now(timezone.utc).isoformat(),
        )

    def _rain(self) -> CurrentWeather:
        return CurrentWeather(
            temp_f=45.0, feels_like_f=40.0, humidity_pct=95.0,
            pressure_hpa=1008.0, visibility_m=3000.0,
            wind_speed_mph=14.0, wind_gust_mph=22.0, wind_deg=135.0,
            wind_label="SE",
            condition_id=502, condition_main="Rain",
            condition_desc="heavy intensity rain",
            is_raining=True, is_snowing=False, is_foggy=False,
            precip_1h_mm=12.0,
            source=f"mock:{MOCK_RAIN}",
            fetched_at=datetime.now(timezone.utc).isoformat(),
        )

    def _heat(self) -> CurrentWeather:
        return CurrentWeather(
            temp_f=96.0, feels_like_f=105.0, humidity_pct=80.0,
            pressure_hpa=1010.0, visibility_m=12000.0,
            wind_speed_mph=5.0, wind_gust_mph=8.0, wind_deg=225.0,
            wind_label="SW",
            condition_id=800, condition_main="Clear",
            condition_desc="clear sky",
            is_raining=False, is_snowing=False, is_foggy=False,
            precip_1h_mm=0.0,
            source=f"mock:{MOCK_HEAT}",
            fetched_at=datetime.now(timezone.utc).isoformat(),
        )

    def _fog(self) -> CurrentWeather:
        return CurrentWeather(
            temp_f=38.0, feels_like_f=34.0, humidity_pct=98.0,
            pressure_hpa=1012.0, visibility_m=400.0,
            wind_speed_mph=4.0, wind_gust_mph=6.0, wind_deg=90.0,
            wind_label="E",
            condition_id=741, condition_main="Fog",
            condition_desc="fog",
            is_raining=False, is_snowing=False, is_foggy=True,
            precip_1h_mm=0.0,
            source=f"mock:{MOCK_FOG}",
            fetched_at=datetime.now(timezone.utc).isoformat(),
        )


# ─────────────────────────────────────────────────────────────────────────────
# OWM response parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_current(data: Dict) -> CurrentWeather:
    """Parse OpenWeatherMap /weather JSON into CurrentWeather."""
    wind   = data.get("wind", {})
    main   = data.get("main", {})
    cond   = data.get("weather", [{}])[0]
    rain   = data.get("rain", {})
    snow   = data.get("snow", {})

    temp_f       = float(main.get("temp", 32))
    humidity     = float(main.get("humidity", 50))
    wind_mph     = float(wind.get("speed", 0))
    wind_gust    = float(wind.get("gust", wind_mph))
    wind_deg     = float(wind.get("deg", 0))
    visibility_m = float(data.get("visibility", 10000))
    precip_mm    = float(rain.get("1h", snow.get("1h", 0)))
    cond_id      = int(cond.get("id", 800))
    cond_main    = cond.get("main", "Clear")
    cond_desc    = cond.get("description", "clear sky")

    is_raining = cond_main.lower() in ("rain", "drizzle", "thunderstorm")
    is_snowing = cond_main.lower() == "snow"
    is_foggy   = cond_main.lower() in ("fog", "mist", "haze", "smoke", "sand", "dust")

    # Compute feels_like
    if temp_f <= 50 and wind_mph > 3:
        feels_like = _wind_chill_f(temp_f, wind_mph)
    elif temp_f >= 80:
        feels_like = _heat_index_f(temp_f, humidity)
    else:
        feels_like = float(main.get("feels_like", temp_f))

    return CurrentWeather(
        temp_f         = round(temp_f, 1),
        feels_like_f   = round(feels_like, 1),
        humidity_pct   = humidity,
        pressure_hpa   = float(main.get("pressure", 1013)),
        visibility_m   = visibility_m,
        wind_speed_mph = round(wind_mph, 1),
        wind_gust_mph  = round(wind_gust, 1),
        wind_deg       = wind_deg,
        wind_label     = _wind_label(wind_deg),
        condition_id   = cond_id,
        condition_main = cond_main,
        condition_desc = cond_desc,
        is_raining     = is_raining,
        is_snowing     = is_snowing,
        is_foggy       = is_foggy,
        precip_1h_mm   = precip_mm,
        source         = "openweathermap",
        fetched_at     = datetime.now(timezone.utc).isoformat(),
    )


def _parse_forecast(data: Dict) -> List[ForecastSlot]:
    """Parse OWM /forecast JSON into ForecastSlot list."""
    slots = []
    for item in data.get("list", []):
        wind   = item.get("wind", {})
        main   = item.get("main", {})
        cond   = item.get("weather", [{}])[0]
        rain   = item.get("rain", {})
        snow   = item.get("snow", {})
        pop    = float(item.get("pop", 0))
        precip = float(rain.get("3h", snow.get("3h", 0)))

        slots.append(ForecastSlot(
            dt_utc         = datetime.utcfromtimestamp(item.get("dt", 0)).isoformat() + "Z",
            temp_f         = round(float(main.get("temp", 32)), 1),
            feels_like_f   = round(float(main.get("feels_like", 32)), 1),
            wind_speed_mph = round(float(wind.get("speed", 0)), 1),
            wind_gust_mph  = round(float(wind.get("gust", wind.get("speed", 0))), 1),
            condition_main = cond.get("main", "Clear"),
            condition_desc = cond.get("description", ""),
            pop            = pop,
            precip_3h_mm   = precip,
        ))
    return slots


# ─────────────────────────────────────────────────────────────────────────────
# Bridge warning builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_bridge_warnings(wind_speed_mph: float) -> List[str]:
    """Chicago River bridges become dangerous to cross in high wind."""
    warnings = []
    if wind_speed_mph >= 25:
        warnings.append(
            f"Wind at {wind_speed_mph:.0f} mph — Michigan Avenue Bridge and Wabash Ave "
            "Bridge are exposed to river wind. Use lower-level Wacker Drive instead."
        )
    if wind_speed_mph >= 35:
        warnings.append(
            "All Chicago River bridges are hazardous. Strongly prefer "
            "underground routes (Pedway level) for river crossings."
        )
    return warnings


def _build_seasonal_note(temp_f: float, month: int) -> str:
    """Contextual note about Chicago seasonality."""
    if month in (12, 1, 2):
        if temp_f < TEMP_VERY_COLD:
            return "Deep winter — The Hawk is out. Pedway is your best friend."
        return "Chicago winter — Pedway tunnels are heated to 70°F year-round."
    if month in (3, 4, 5):
        return "Spring in Chicago — weather changes rapidly. Check forecast before heading out."
    if month in (6, 7, 8):
        if temp_f > TEMP_HOT:
            return "Chicago summer heat — Pedway and AC-connected buildings provide relief."
        return "Chicago summer — enjoy Millennium Park and the lakefront path."
    return "Chicago fall — layers recommended; weather can swing 20°F in a day."


# ─────────────────────────────────────────────────────────────────────────────
# Payload assembler
# ─────────────────────────────────────────────────────────────────────────────

_corner_analyzer = DeathCornerAnalyzer()
_impact_scorer   = WeatherImpactScorer()
_rec_builder     = RecommendationBuilder()


def _assemble_payload(
    current:  CurrentWeather,
    forecast: Optional[List[ForecastSlot]] = None,
) -> WeatherPayload:
    """Convert raw CurrentWeather into a full WeatherPayload."""

    # Death corner analysis
    corner_risks, worst = _corner_analyzer.analyze_all(
        current.wind_speed_mph, current.wind_gust_mph, current.wind_deg
    )

    worst_venturi = worst.venturi_score if worst else 0.0

    # Impact scoring
    level, impact_score, multiplier = _impact_scorer.score(
        wind_gust_mph  = current.wind_gust_mph,
        temp_f         = current.temp_f,
        feels_like_f   = current.feels_like_f,
        is_raining     = current.is_raining,
        is_snowing     = current.is_snowing,
        is_foggy       = current.is_foggy,
        precip_1h_mm   = current.precip_1h_mm,
        visibility_m   = current.visibility_m,
        worst_venturi  = worst_venturi,
    )

    recommend_pedway  = level in (WeatherImpactLevel.HIGH, WeatherImpactLevel.MEDIUM)
    recommend_covered = level != WeatherImpactLevel.NONE

    recommendation    = _rec_builder.build(level, current, worst)
    danger_summary    = _rec_builder.loop_danger_summary(level, current.wind_gust_mph)
    bridge_warnings   = _build_bridge_warnings(current.wind_speed_mph)

    now              = datetime.now(timezone.utc)
    seasonal_note    = _build_seasonal_note(current.temp_f, now.month)

    routing_context: Dict[str, Any] = {
        "impact":             level.value,
        "wind_speed_mph":     current.wind_speed_mph,
        "wind_gust_mph":      current.wind_gust_mph,
        "wind_deg":           current.wind_deg,
        "temp_f":             current.temp_f,
        "feels_like_f":       current.feels_like_f,
        "is_raining":         current.is_raining,
        "is_snowing":         current.is_snowing,
        "recommend_pedway":   recommend_pedway,
        "routing_multiplier": multiplier,
        "fetched_at":         current.fetched_at,
    }

    return WeatherPayload(
        current             = current,
        impact              = level,
        impact_score        = impact_score,
        loop_danger_summary = danger_summary,
        recommend_pedway    = recommend_pedway,
        recommend_covered   = recommend_covered,
        routing_multiplier  = multiplier,
        recommendation      = recommendation,
        death_corner_risks  = corner_risks,
        worst_corner        = worst,
        forecast            = forecast,
        routing_context     = routing_context,
        sunny_sides         = SUNNY_SIDES,
        bridge_warnings     = bridge_warnings,
        seasonal_note       = seasonal_note,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main WeatherService
# ─────────────────────────────────────────────────────────────────────────────

class WeatherService:
    """
    Singleton weather service — attach to app.state.weather_svc in lifespan.

    Usage:
        payload = await app.state.weather_svc.get_full()
        ctx     = await app.state.weather_svc.get_routing_context()
    """

    def __init__(self, redis=None):
        self._client  = OpenWeatherClient(settings.OPENWEATHER_API_KEY) if settings.OPENWEATHER_API_KEY else None
        self._l1      = _L1Cache()
        self._redis   = redis
        self._mock    = MockScenarioManager()
        self._stats   = {
            "requests_total":  0,
            "cache_l1_hits":   0,
            "cache_l2_hits":   0,
            "api_calls":       0,
            "mock_responses":  0,
            "errors":          0,
        }

    @property
    def mock_manager(self) -> MockScenarioManager:
        return self._mock

    async def get_full(self, include_forecast: bool = False) -> WeatherPayload:
        """Primary entry point — returns full WeatherPayload."""
        self._stats["requests_total"] += 1

        # L1 cache check
        cached = self._l1.get()
        if cached and not include_forecast:
            self._stats["cache_l1_hits"] += 1
            return cached

        # L2 Redis cache check
        if self._redis and not include_forecast:
            try:
                raw = await self._redis.get("nav:weather:current")
                if raw:
                    self._stats["cache_l2_hits"] += 1
                    # Rebuild payload from cached routing_context
                    return self._rebuild_from_cache(raw)
            except Exception as exc:
                log.debug("Redis weather cache miss: %s", exc)

        # Fetch fresh data
        current  = await self._fetch_current()
        forecast = await self._fetch_forecast() if include_forecast else None
        payload  = _assemble_payload(current, forecast)

        # Store in L1
        self._l1.set(payload)

        # Store in L2 (Redis)
        if self._redis:
            try:
                await self._redis.set(
                    "nav:weather:current",
                    payload.routing_context,
                    ttl=L2_TTL_S,
                )
            except Exception as exc:
                log.debug("Redis weather cache write failed: %s", exc)

        return payload

    async def get_routing_context(self) -> RoutingWeatherContext:
        """Returns simplified routing context for the graph engine."""
        payload = await self.get_full()
        rc = payload.routing_context
        return RoutingWeatherContext(
            impact             = rc["impact"],
            wind_speed_mph     = rc["wind_speed_mph"],
            wind_gust_mph      = rc["wind_gust_mph"],
            wind_deg           = rc["wind_deg"],
            temp_f             = rc["temp_f"],
            feels_like_f       = rc["feels_like_f"],
            is_raining         = rc["is_raining"],
            is_snowing         = rc["is_snowing"],
            recommend_pedway   = rc["recommend_pedway"],
            routing_multiplier = rc["routing_multiplier"],
            fetched_at         = rc["fetched_at"],
        )

    async def get_impact_level(self) -> WeatherImpactLevel:
        """Quick accessor — no full payload construction overhead after L1 hit."""
        payload = await self.get_full()
        return payload.impact

    async def _fetch_current(self) -> CurrentWeather:
        """Fetch current weather. Falls back to mock on error or missing key."""
        # Mock scenario override
        if self._mock.active or not settings.OPENWEATHER_API_KEY:
            self._stats["mock_responses"] += 1
            return self._mock.build()

        self._stats["api_calls"] += 1
        data = await self._client.fetch_current(OHARE_LAT, OHARE_LNG)
        if data is None:
            self._stats["errors"] += 1
            log.warning("OWM current fetch failed — using mock")
            return self._mock.build()

        try:
            return _parse_current(data)
        except Exception as exc:
            self._stats["errors"] += 1
            log.error("OWM parse error: %s", exc)
            return self._mock.build()

    async def _fetch_forecast(self) -> Optional[List[ForecastSlot]]:
        """Fetch 12-hour forecast (4 × 3h slots)."""
        if self._mock.active or not settings.OPENWEATHER_API_KEY:
            return self._mock_forecast()

        data = await self._client.fetch_forecast(OHARE_LAT, OHARE_LNG, cnt=4)
        if data is None:
            return self._mock_forecast()
        try:
            return _parse_forecast(data)
        except Exception as exc:
            log.warning("Forecast parse error: %s", exc)
            return None

    def _mock_forecast(self) -> List[ForecastSlot]:
        """Simple repeating forecast based on current mock scenario."""
        current = self._mock.build()
        slots = []
        for i in range(4):
            slots.append(ForecastSlot(
                dt_utc         = datetime.utcnow().isoformat() + "Z",
                temp_f         = current.temp_f,
                feels_like_f   = current.feels_like_f,
                wind_speed_mph = current.wind_speed_mph,
                wind_gust_mph  = current.wind_gust_mph,
                condition_main = current.condition_main,
                condition_desc = current.condition_desc,
                pop            = 0.8 if current.is_raining or current.is_snowing else 0.1,
                precip_3h_mm   = current.precip_1h_mm * 3,
            ))
        return slots

    def _rebuild_from_cache(self, routing_ctx: Dict) -> WeatherPayload:
        """Reconstruct a WeatherPayload from cached routing context (lightweight)."""
        current = CurrentWeather(
            temp_f         = routing_ctx.get("temp_f", 32),
            feels_like_f   = routing_ctx.get("feels_like_f", 32),
            humidity_pct   = 0.0,
            pressure_hpa   = 1013.0,
            visibility_m   = 10000.0,
            wind_speed_mph = routing_ctx.get("wind_speed_mph", 0),
            wind_gust_mph  = routing_ctx.get("wind_gust_mph", 0),
            wind_deg       = routing_ctx.get("wind_deg", 0),
            wind_label     = _wind_label(routing_ctx.get("wind_deg", 0)),
            condition_id   = 800,
            condition_main = "Clear",
            condition_desc = "(from cache)",
            is_raining     = routing_ctx.get("is_raining", False),
            is_snowing     = routing_ctx.get("is_snowing", False),
            is_foggy       = False,
            precip_1h_mm   = 0.0,
            source         = "cache",
            fetched_at     = routing_ctx.get("fetched_at", datetime.now(timezone.utc).isoformat()),
        )
        return _assemble_payload(current)

    def invalidate_cache(self) -> None:
        self._l1.invalidate()

    def stats_snapshot(self) -> Dict[str, Any]:
        return dict(self._stats)

    def as_dict(self) -> Dict[str, Any]:
        """Service info for /health endpoint."""
        return {
            "api_key_set":    bool(settings.OPENWEATHER_API_KEY),
            "mock_active":    self._mock.active,
            "l1_cached":      self._l1.get() is not None,
            "stats":          self.stats_snapshot(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton + backward-compatible helpers
# ─────────────────────────────────────────────────────────────────────────────

# Shared service instance — created lazily, re-used by all requests.
_svc: Optional[WeatherService] = None


def get_service(redis=None) -> WeatherService:
    """
    Return (or create) the module-level WeatherService.
    Call with redis on first use (from lifespan); subsequent calls use cache.
    """
    global _svc
    if _svc is None:
        _svc = WeatherService(redis=redis)
    return _svc


# ── Backward-compatible helpers (used by existing LoopSense code) ────────────

async def fetch_wind_data() -> dict:
    """Legacy helper — returns flat dict compatible with original LoopSense format."""
    svc     = get_service()
    payload = await svc.get_full()
    c       = payload.current
    return {
        "source":               c.source,
        "wind_speed_mph":       c.wind_speed_mph,
        "gust_mph":             c.wind_gust_mph,
        "direction_deg":        c.wind_deg,
        "direction_label":      c.wind_label,
        "loop_danger_summary":  payload.loop_danger_summary,
        "death_corner_risks": [
            {
                "name":               r.name,
                "latitude":           r.lat,
                "longitude":          r.lon,
                "note":               r.note,
                "venturi_score":      r.venturi_score,
                "estimated_gust_mph": r.amplified_gust_mph,
                "danger_level":       r.danger_level,
            }
            for r in payload.death_corner_risks
        ],
        "fetched_at":           c.fetched_at,
        "recommend_pedway":     payload.recommend_pedway,
    }


async def get_wind_speed_mph() -> Optional[float]:
    """Legacy helper."""
    data = await fetch_wind_data()
    return data.get("wind_speed_mph")


async def fetch_routing_weather() -> Dict[str, Any]:
    """
    Returns a minimal dict used by nav_route.py in _fetch_live_data().
    Keys: impact, wind_speed_mph, wind_gust_mph, temp_f, recommend_pedway, routing_multiplier
    """
    svc = get_service()
    ctx = await svc.get_routing_context()
    return {
        "impact":             ctx.impact,
        "wind_speed_mph":     ctx.wind_speed_mph,
        "wind_gust_mph":      ctx.wind_gust_mph,
        "temp_f":             ctx.temp_f,
        "feels_like_f":       ctx.feels_like_f,
        "is_raining":         ctx.is_raining,
        "is_snowing":         ctx.is_snowing,
        "recommend_pedway":   ctx.recommend_pedway,
        "routing_multiplier": ctx.routing_multiplier,
        "fetched_at":         ctx.fetched_at,
    }
