"""
LoopNav — Production AI Natural Language Routing
=================================================
POST /api/v1/nav/ai-route          — Parse a natural language route request
POST /api/v1/nav/ai-route/classify — Classify intent without routing
GET  /api/v1/nav/ai-route/history/{session_id} — Conversation history
POST /api/v1/nav/ai-route/feedback — Submit AI response quality feedback

Architecture
------------
Claude claude-sonnet-4-6 is used as the primary parser. A deterministic
rule-based fallback is always available so the endpoint never returns 503
even if the Anthropic API is unavailable.

The system prompt is Chicago Loop–specific and context-enriched at
request time with live weather and time-of-day data so Claude reasons
as if it were a local guide, not a generic assistant.

Rate limiting: 30 AI requests per minute per IP (Claude API cost protection).
Caching: identical input text cached for 5 minutes to reduce API calls.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.config import settings
from app.models.nav_schemas import AIRouteResponse
from app.services.cache import Cache
from app.services.weather_service import fetch_wind_data

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/nav", tags=["LoopNav — AI Routing"])

# ── Configuration ─────────────────────────────────────────────────────────────

AI_CACHE_TTL_S       = 300   # 5 minutes — identical queries reuse response
AI_HISTORY_TTL_S     = 3600  # 1 hour session history retention
MAX_HISTORY_MESSAGES = 20    # cap conversation history per session
MAX_INPUT_LENGTH     = 500   # characters
MIN_INPUT_LENGTH     = 3

CLAUDE_MODEL = "claude-sonnet-4-6"
CLAUDE_MAX_TOKENS = 400
CLAUDE_TEMPERATURE = 0.1   # low — we want consistent structured output

# Known Loop locations for rule-based fallback
_LOCATION_MAP: dict[str, str] = {
    "ogilvie":              "Ogilvie Transportation Center",
    "ogilvie station":      "Ogilvie Transportation Center",
    "union station":        "Union Station",
    "union":                "Union Station",
    "millennium station":   "Millennium Station",
    "millennium":           "Millennium Station",
    "art institute":        "Art Institute of Chicago",
    "art institute of chicago": "Art Institute of Chicago",
    "city hall":            "Chicago City Hall",
    "chicago city hall":    "Chicago City Hall",
    "block 37":             "Block 37",
    "block37":              "Block 37",
    "macy's":               "Macy's on State Street",
    "macys":                "Macy's on State Street",
    "macy":                 "Macy's on State Street",
    "willis tower":         "Willis Tower",
    "willis":               "Willis Tower",
    "sears tower":          "Willis Tower",   # legacy name
    "chase tower":          "Chase Tower",
    "chase":                "Chase Tower",
    "daley center":         "Daley Center",
    "daley":                "Daley Center",
    "cultural center":      "Chicago Cultural Center",
    "chicago cultural center": "Chicago Cultural Center",
    "lake/state":           "Lake/State Red Line",
    "lake state":           "Lake/State Red Line",
    "quincy":               "Quincy/Wells (Brown Line)",
    "quincy wells":         "Quincy/Wells (Brown Line)",
    "millennium park":      "Millennium Park",
    "riverwalk":            "Chicago Riverwalk",
    "river walk":           "Chicago Riverwalk",
    "washington":           "Washington/Dearborn Blue Line",
    "washington dearborn":  "Washington/Dearborn Blue Line",
}

# Mode-triggering keywords for rule-based fallback
_MODE_KEYWORDS: dict[str, list[str]] = {
    "pedway_preferred": [
        "warm", "warmth", "dry", "cold", "rain", "snow", "raining", "snowing",
        "underground", "inside", "interior", "shelter", "sheltered", "covered",
        "stay dry", "weather", "storm", "blizzard", "freezing", "freeze",
        "polar vortex", "hawk", "wind",
    ],
    "street_only": [
        "outside", "outdoors", "fresh air", "scenic", "sightseeing",
        "walk outside", "street", "surface", "above ground", "enjoy",
        "explore", "wander", "open air",
    ],
    "mid_preferred": [
        "lower wacker", "riverwalk", "river walk", "wacker", "mid level",
        "mid-level", "tunnel",
    ],
    "optimal": [
        "fastest", "quickest", "quick", "fast", "hurry", "rush", "urgent",
        "shortest", "efficient", "direct",
    ],
}

_ACCESSIBLE_KEYWORDS = [
    "wheelchair", "accessible", "elevator", "ada", "mobility",
    "disability", "disabled", "handicap", "scooter", "walker",
    "crutch", "cane", "no stairs", "avoid stairs",
]

# Valid location names for the system prompt
_VALID_LOCATIONS = [
    "Ogilvie Transportation Center", "Union Station", "Millennium Station",
    "Art Institute of Chicago", "Chicago City Hall", "Block 37",
    "Macy's on State Street", "Willis Tower", "Chase Tower", "Daley Center",
    "Chicago Cultural Center", "Lake/State Red Line",
    "Washington/Dearborn Blue Line", "Quincy/Wells (Brown Line)",
    "Millennium Park", "Chicago Riverwalk",
]


# ── Additional models ──────────────────────────────────────────────────────────

class AIRouteRequest(BaseModel):
    text: str = Field(..., min_length=MIN_INPUT_LENGTH, max_length=MAX_INPUT_LENGTH)
    session_id:     Optional[str] = Field(
        default=None,
        description="Optional session ID to maintain conversation history."
    )
    include_context: bool = Field(
        default=True,
        description="If true, weather and time of day are injected into the prompt."
    )


class AIClassifyRequest(BaseModel):
    text: str = Field(..., min_length=MIN_INPUT_LENGTH, max_length=MAX_INPUT_LENGTH)


class AIClassifyResponse(BaseModel):
    intent:     str   # "route_request" | "question" | "feedback" | "unclear"
    confidence: float
    entities:   dict
    suggested_action: str


class AIFeedbackRequest(BaseModel):
    session_id: Optional[str] = None
    input_text: str
    ai_response: dict
    correct:    bool
    correction: Optional[dict] = None
    comment:    Optional[str] = None


class EnrichedAIResponse(BaseModel):
    origin:      Optional[str]
    destination: Optional[str]
    mode:        str
    accessible:  bool
    reasoning:   str
    confidence:  float
    source:      str   # "claude" | "rule_based" | "cache"
    context_used: dict
    processing_ms: float


# ── Rule-based fallback ────────────────────────────────────────────────────────

def _extract_location(text_lower: str) -> Optional[str]:
    """
    Scan for known location names in text.
    Longer matches win to avoid 'willis' matching inside 'willis tower'.
    """
    matches: list[tuple[int, str]] = []
    for key, name in _LOCATION_MAP.items():
        if key in text_lower:
            matches.append((len(key), name))
    if not matches:
        return None
    matches.sort(key=lambda x: x[0], reverse=True)
    return matches[0][1]


def _detect_mode(text_lower: str) -> str:
    scored: dict[str, int] = {mode: 0 for mode in _MODE_KEYWORDS}
    for mode, keywords in _MODE_KEYWORDS.items():
        for kw in keywords:
            if kw in text_lower:
                scored[mode] += 1
    best_mode  = max(scored, key=lambda m: scored[m])
    best_score = scored[best_mode]
    return best_mode if best_score > 0 else "optimal"


def _detect_accessible(text_lower: str) -> bool:
    return any(kw in text_lower for kw in _ACCESSIBLE_KEYWORDS)


def _rule_based_parse(text: str) -> dict:
    """
    Deterministic rule-based parser — never fails.
    Used when Claude is unavailable or as a confidence cross-check.
    """
    text_lower = text.lower()
    words      = text_lower.split()

    # Try to find origin and destination
    origin_raw      = None
    destination_raw = None

    # Pattern: "from X to Y"
    if " to " in text_lower and " from " in text_lower:
        from_idx = text_lower.index(" from ") + 6
        to_idx   = text_lower.index(" to ")
        if from_idx < to_idx:
            origin_raw      = text_lower[from_idx:to_idx].strip()
            destination_raw = text_lower[to_idx + 4:].strip()
        else:
            destination_raw = text_lower[text_lower.index(" to ") + 4:].strip()

    elif " to " in text_lower:
        to_idx          = text_lower.index(" to ")
        destination_raw = text_lower[to_idx + 4:].strip()

    # Map raw text to known locations
    origin      = _extract_location(origin_raw)      if origin_raw      else None
    destination = _extract_location(destination_raw) if destination_raw else _extract_location(text_lower)

    # If we only found one location, decide whether it's origin or destination
    if not origin and not destination:
        destination = _extract_location(text_lower)

    mode       = _detect_mode(text_lower)
    accessible = _detect_accessible(text_lower)

    # Reasoning string
    parts: list[str] = []
    if origin:
        parts.append(f"origin: {origin}")
    if destination:
        parts.append(f"destination: {destination}")
    parts.append(f"mode: {mode}")
    if accessible:
        parts.append("accessibility mode enabled")

    reasoning = (
        f"Rule-based extraction ({', '.join(parts) if parts else 'no entities found'})."
    )

    return {
        "origin":      origin,
        "destination": destination,
        "mode":        mode,
        "accessible":  accessible,
        "reasoning":   reasoning,
        "confidence":  0.55 if (origin or destination) else 0.20,
        "source":      "rule_based",
    }


# ── Prompt builders ────────────────────────────────────────────────────────────

def _build_system_prompt(weather_context: str, time_context: str) -> str:
    locations_str = "\n".join(f"  - {loc}" for loc in _VALID_LOCATIONS)
    return f"""You are a routing assistant for LoopNav, a Chicago Loop 3-layer navigation app.
The Chicago Loop has three walking levels: street (above ground), mid (Lower Wacker Drive / Riverwalk),
and pedway (underground network connecting 40+ buildings — warm, dry, and sheltered).

CURRENT CONDITIONS:
{time_context}
{weather_context}

VALID LOCATIONS (use the EXACT string from this list):
{locations_str}

AVAILABLE MODES:
  - optimal           Default. Fastest route, may cross all 3 layers.
  - pedway_preferred  Strongly prefers underground pedway. Best for bad weather.
  - street_only       Stays above ground. For sightseeing or fresh air.
  - mid_preferred     Prefers Lower Wacker / Riverwalk.

MODE SELECTION RULES:
  - "warm", "dry", "cold", "snow", "rain", "underground", "shelter", "stay inside" → pedway_preferred
  - "outside", "scenic", "fresh air", "sightseeing", "explore"                     → street_only
  - "lower wacker", "riverwalk", "tunnel"                                           → mid_preferred
  - "fastest", "quick", "hurry", "rush"                                             → optimal
  - Current weather is bad (wind > 20mph or temp < 32°F)                           → pedway_preferred

ACCESSIBILITY:
  Set accessible: true if the user mentions wheelchair, elevator, ADA, mobility, disability,
  no stairs, or similar terms.

CONFIDENCE:
  - 0.9+ if both origin and destination are clearly named and match the valid list
  - 0.7–0.9 if one location is clear, mode is inferable
  - 0.5–0.7 if locations are inferred or ambiguous
  - < 0.5 if the request is vague

OUTPUT: Valid JSON only. No markdown, no explanation outside the JSON.
{{
  "origin":      "<exact location name or null>",
  "destination": "<exact location name or null>",
  "mode":        "<one of the four modes>",
  "accessible":  <true or false>,
  "reasoning":   "<one sentence explaining your interpretation>",
  "confidence":  <float 0.0-1.0>
}}"""


def _build_weather_context(wind_data: dict) -> str:
    speed_mph        = wind_data.get("wind_speed_mph", 0)
    temp_f           = wind_data.get("temp_f", 45)
    recommend_pedway = wind_data.get("recommend_pedway", False)
    summary          = wind_data.get("loop_danger_summary", "")

    lines = [f"- Wind: {speed_mph:.0f} mph"]
    if temp_f:
        lines.append(f"- Temperature: {temp_f:.0f}°F")
    if summary:
        lines.append(f"- Loop conditions: {summary}")
    if recommend_pedway:
        lines.append("- RECOMMENDATION: pedway_preferred due to current conditions")

    return "Weather:\n" + "\n".join(lines)


def _build_time_context() -> str:
    now  = datetime.now(timezone.utc)
    hour = now.hour
    if 7 <= hour <= 9:
        period = "morning rush hour (7–9AM)"
    elif 11 <= hour <= 13:
        period = "midday (11AM–1PM)"
    elif 16 <= hour <= 18:
        period = "evening rush hour (4–6PM)"
    elif hour < 6 or hour >= 22:
        period = "late night — some pedway segments may be closed"
    else:
        period = "normal hours"

    return f"Time of day: {now.strftime('%H:%M UTC')} — {period}"


def _build_classify_prompt() -> str:
    return """Classify the intent of this message in the context of a Chicago Loop navigation app.

OUTPUT: Valid JSON only.
{
  "intent": "<one of: route_request | location_question | weather_question | feedback | unclear>",
  "confidence": <float 0.0-1.0>,
  "entities": {
    "has_origin": <bool>,
    "has_destination": <bool>,
    "has_mode_preference": <bool>,
    "has_accessibility": <bool>,
    "mentioned_locations": ["<location 1>", ...]
  },
  "suggested_action": "<what the frontend should do next>"
}"""


# ── Claude API calls ───────────────────────────────────────────────────────────

async def _call_claude(
    system_prompt: str,
    user_text: str,
    max_tokens: int = CLAUDE_MAX_TOKENS,
) -> str:
    """
    Makes a single Claude API call. Returns the raw text content.
    Raises RuntimeError on API failure so callers can fall back.
    """
    import anthropic

    client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

    message = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=max_tokens,
        system=system_prompt,
        messages=[{"role": "user", "content": user_text}],
    )

    raw = message.content[0].text.strip()

    # Strip markdown code fences if Claude wraps the JSON
    if raw.startswith("```"):
        lines = raw.split("\n")
        raw = "\n".join(
            line for line in lines[1:]
            if not line.strip().startswith("```")
        ).strip()

    return raw


def _parse_claude_json(raw: str, required_keys: list[str]) -> dict:
    """Parse and validate Claude's JSON response."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Claude returned invalid JSON: {e}\nRaw: {raw[:200]}")

    missing = [k for k in required_keys if k not in data]
    if missing:
        raise ValueError(f"Claude response missing keys: {missing}")

    return data


def _validate_location(name: Optional[str]) -> Optional[str]:
    """Return the location name if it's valid, else None."""
    if not name:
        return None
    for valid in _VALID_LOCATIONS:
        if name.strip().lower() == valid.lower():
            return valid
    # Fuzzy check — allow partial matches for robustness
    name_lower = name.strip().lower()
    for valid in _VALID_LOCATIONS:
        if name_lower in valid.lower() or valid.lower() in name_lower:
            return valid
    logger.warning("Claude returned unknown location: '%s' — setting to null", name)
    return None


# ── Conversation history ───────────────────────────────────────────────────────

async def _get_conversation_history(cache: Cache, session_id: str) -> list[dict]:
    history = await cache.get(f"nav:ai:history:{session_id}")
    return history or []


async def _append_to_history(
    cache: Cache, session_id: str, role: str, content: str
) -> None:
    history = await _get_conversation_history(cache, session_id)
    history.append({
        "role":      role,
        "content":   content,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    # Cap history length
    if len(history) > MAX_HISTORY_MESSAGES:
        history = history[-MAX_HISTORY_MESSAGES:]
    await cache.set(f"nav:ai:history:{session_id}", history, ttl=AI_HISTORY_TTL_S)


# ── Main parse function ────────────────────────────────────────────────────────

async def _parse_route_request(
    text: str,
    include_context: bool,
    cache: Cache,
    session_id: Optional[str],
) -> EnrichedAIResponse:
    """
    Full pipeline:
    1. Check cache for identical input
    2. Try Claude with enriched context
    3. Validate locations
    4. Fall back to rule-based if Claude fails
    5. Store in history if session_id provided
    """
    t0 = time.perf_counter()

    # Cache check (keyed on text hash, not session)
    text_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
    cache_key = f"nav:ai:parse:{text_hash}"

    if include_context:
        # Skip cache if context is included (weather changes)
        cached = None
    else:
        cached = await cache.get(cache_key)

    if cached:
        processing_ms = (time.perf_counter() - t0) * 1000
        cached["source"]        = "cache"
        cached["processing_ms"] = round(processing_ms, 1)
        return EnrichedAIResponse(**cached)

    # Build context
    context_used: dict = {}
    if include_context:
        try:
            wind_data          = await fetch_wind_data()
            weather_context    = _build_weather_context(wind_data)
            context_used["weather"] = {
                "wind_mph": wind_data.get("wind_speed_mph", 0),
                "temp_f":   wind_data.get("temp_f"),
                "recommend_pedway": wind_data.get("recommend_pedway", False),
            }
        except Exception:
            weather_context = "Weather: unavailable"
    else:
        weather_context    = "Weather: not included"
        wind_data          = {}

    time_context           = _build_time_context()
    context_used["hour"]   = datetime.now().hour

    # Try Claude
    source     = "claude"
    parsed     = None
    confidence = 0.0

    if settings.ANTHROPIC_API_KEY:
        try:
            system_prompt = _build_system_prompt(weather_context, time_context)
            raw           = await _call_claude(system_prompt, text)
            data          = _parse_claude_json(
                raw, ["origin", "destination", "mode", "accessible", "reasoning"]
            )

            # Validate and correct locations
            origin      = _validate_location(data.get("origin"))
            destination = _validate_location(data.get("destination"))
            mode        = data.get("mode", "optimal")
            accessible  = bool(data.get("accessible", False))
            reasoning   = str(data.get("reasoning", ""))
            confidence  = float(data.get("confidence", 0.75))

            if mode not in ("optimal", "street_only", "pedway_preferred", "mid_preferred"):
                logger.warning("Claude returned invalid mode '%s' — defaulting to optimal", mode)
                mode = "optimal"

            parsed = {
                "origin":      origin,
                "destination": destination,
                "mode":        mode,
                "accessible":  accessible,
                "reasoning":   reasoning,
                "confidence":  confidence,
                "source":      "claude",
            }

        except Exception as e:
            logger.warning("Claude API failed (%s) — falling back to rule-based parser", e)
            source = "rule_based"
    else:
        source = "rule_based"

    if parsed is None:
        parsed = _rule_based_parse(text)
        source = "rule_based"

    parsed["source"]       = source
    parsed["context_used"] = context_used
    parsed["processing_ms"] = round((time.perf_counter() - t0) * 1000, 1)

    # Cache if no live context was used
    if not include_context:
        await cache.set(cache_key, parsed, ttl=AI_CACHE_TTL_S)

    # Store in session history
    if session_id:
        await _append_to_history(cache, session_id, "user", text)
        await _append_to_history(
            cache, session_id, "assistant",
            json.dumps({"origin": parsed["origin"], "destination": parsed["destination"],
                        "mode": parsed["mode"], "accessible": parsed["accessible"]}),
        )

    # Analytics
    await cache.incr("nav:stats:ai_requests_total")
    await cache.incr(f"nav:stats:ai_source:{source}")
    if parsed.get("accessible"):
        await cache.incr("nav:stats:ai_accessible_requests")

    return EnrichedAIResponse(**parsed)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/ai-route", response_model=EnrichedAIResponse)
async def ai_route(req: AIRouteRequest, request: Request):
    """
    Parse a natural language route request using Claude claude-sonnet-4-6.

    Extracts origin, destination, routing mode, and accessibility preference
    from plain English. The frontend uses the response to auto-fill inputs
    and immediately trigger the /route endpoint.

    Falls back to a rule-based parser if the Anthropic API is unavailable.
    Identical queries are cached for 5 minutes (when include_context=false).

    **Example inputs:**
    - *"Take me from Ogilvie to the Art Institute while staying warm"*
    - *"Fastest way from Union Station to Willis Tower, I'm in a wheelchair"*
    - *"I want to walk outside from Millennium Park to Daley Center"*
    - *"Get me underground from Block 37 to City Hall"*
    """
    cache = Cache(request.app.state.redis)

    try:
        result = await _parse_route_request(
            text=req.text,
            include_context=req.include_context,
            cache=cache,
            session_id=req.session_id,
        )
    except Exception as e:
        logger.error("AI route parsing failed: %s", e, exc_info=True)
        # Always return something usable
        fallback = _rule_based_parse(req.text)
        return EnrichedAIResponse(
            **fallback,
            context_used={},
            processing_ms=0.0,
        )

    return result


@router.post("/ai-route/classify", response_model=AIClassifyResponse)
async def classify_intent(req: AIClassifyRequest, request: Request):
    """
    Classify the intent of a text message without performing full parsing.

    Useful for the frontend to decide whether to show the routing UI or
    a different response (e.g., a weather question should show weather card).

    Intents: **route_request** | **location_question** | **weather_question**
    | **feedback** | **unclear**
    """
    cache = Cache(request.app.state.redis)

    # Try Claude first
    if settings.ANTHROPIC_API_KEY:
        try:
            raw  = await _call_claude(_build_classify_prompt(), req.text, max_tokens=250)
            data = _parse_claude_json(
                raw, ["intent", "confidence", "entities", "suggested_action"]
            )
            await cache.incr("nav:stats:ai_classify_requests")
            return AIClassifyResponse(**data)
        except Exception as e:
            logger.warning("Claude classify failed (%s) — using rule-based", e)

    # Rule-based intent classification
    text_lower = req.text.lower()
    has_location = any(kw in text_lower for kw in _LOCATION_MAP)
    has_mode     = any(kw in text_lower for mode_kws in _MODE_KEYWORDS.values() for kw in mode_kws)
    has_acc      = _detect_accessible(text_lower)
    is_question  = "?" in req.text or any(
        text_lower.startswith(w) for w in ["what", "how", "where", "when", "is", "can"]
    )
    has_weather_kw = any(kw in text_lower for kw in ["weather", "wind", "cold", "rain", "snow"])

    if is_question and has_weather_kw:
        intent     = "weather_question"
        confidence = 0.75
        action     = "Show weather card"
    elif is_question and not has_location:
        intent     = "location_question"
        confidence = 0.65
        action     = "Show location search"
    elif has_location or has_mode:
        intent     = "route_request"
        confidence = 0.80 if has_location else 0.60
        action     = "Trigger route parsing"
    else:
        intent     = "unclear"
        confidence = 0.40
        action     = "Ask user to clarify origin and destination"

    mentioned_locs = [
        name for key, name in _LOCATION_MAP.items() if key in text_lower
    ]

    return AIClassifyResponse(
        intent=intent,
        confidence=confidence,
        entities={
            "has_origin":          False,
            "has_destination":     has_location,
            "has_mode_preference": has_mode,
            "has_accessibility":   has_acc,
            "mentioned_locations": list(dict.fromkeys(mentioned_locs)),
        },
        suggested_action=action,
    )


@router.get("/ai-route/history/{session_id}")
async def get_conversation_history(session_id: str, request: Request):
    """
    Return the conversation history for a session.

    History is stored in Redis for 1 hour. Each entry has:
    role (user/assistant), content, and timestamp.
    """
    cache   = Cache(request.app.state.redis)
    history = await _get_conversation_history(cache, session_id)

    return {
        "session_id": session_id,
        "messages":   history,
        "total":      len(history),
    }


@router.delete("/ai-route/history/{session_id}")
async def clear_conversation_history(session_id: str, request: Request):
    """Clear conversation history for a session."""
    cache = Cache(request.app.state.redis)
    await cache.delete(f"nav:ai:history:{session_id}")
    return {"message": f"History cleared for session {session_id}."}


@router.post("/ai-route/feedback")
async def ai_feedback(feedback: AIFeedbackRequest, request: Request):
    """
    Submit quality feedback on an AI route parsing response.

    Tracks correction patterns to identify systematic failures.
    Used to tune the system prompt and rule-based fallback.
    """
    cache = Cache(request.app.state.redis)

    import uuid as _uuid
    feedback_id = str(_uuid.uuid4())

    record = {
        "id":           feedback_id,
        "session_id":   feedback.session_id,
        "input_text":   feedback.input_text,
        "ai_response":  feedback.ai_response,
        "correct":      feedback.correct,
        "correction":   feedback.correction,
        "comment":      feedback.comment,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
    }

    await cache.set(f"nav:ai:feedback:{feedback_id}", record, ttl=86400 * 30)
    await cache.incr("nav:stats:ai_feedback_total")
    if not feedback.correct:
        await cache.incr("nav:stats:ai_feedback_incorrect")

    return {
        "message":     "Feedback recorded. Thank you.",
        "feedback_id": feedback_id,
    }
