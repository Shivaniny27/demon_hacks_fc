"""
LoopSense AI Agent — Powered by Llama 3.2 3B via Groq.

Takes a raw pedestrian hazard report and returns a structured classification
with severity, category, action, and a short human-readable summary.

The agent is given Chicago Loop–specific context so it reasons like a local,
not a generic AI.

Groq provides free, ultra-fast inference for Llama 3.2 3B.
API is OpenAI-compatible — no extra dependency required.
"""

import json
import logging
from typing import Optional

from openai import AsyncOpenAI

from app.config import settings
from app.models.report import HazardCategory
from app.models.schemas import AIClassification

logger = logging.getLogger(__name__)

GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GROQ_MODEL    = "llama-3.2-3b-preview"

# ── System prompt — Chicago-specific urban safety agent ───────────────────────
SYSTEM_PROMPT = """
You are LoopSense, an agentic urban safety AI embedded in the Chicago Loop
pedestrian intelligence system. You have deep knowledge of the Chicago Loop:

GEOGRAPHY & HAZARDS YOU KNOW WELL:
- The Loop has THREE vertical layers:
  1. ELEVATED ("elevated") — The L train tracks and platforms above street level.
     Wet grates, wind exposure, and gap hazards between trains and platforms.
  2. SURFACE ("surface") — Street level. The primary pedestrian realm.
  3. PEDWAY ("pedway") — The underground network connecting 40+ buildings.
     Usually warm and dry, but flooding, door jams, and lift failures occur.

- DEATH CORNERS — Wind-tunnel intersections where the Venturi effect between
  skyscrapers creates dangerous gusts (40+ mph). Key ones:
  Wacker & Adams, State & Madison, Willis Tower Plaza, Michigan & Wacker.

- CHICAGO RIVER BRIDGES — 20+ bascule bridges can be raised for boat traffic.
  A lifted bridge completely breaks accessible routes. Key bridges:
  Michigan Ave, Wabash Ave, State St, Dearborn St, Clark St, LaSalle St, Wells St.

- WINTER SPECIFICS — Black ice, freeze-thaw slush at curb cuts, frozen
  escalator machinery, platform ice from train door spray.

CATEGORIES:
- wind: Dangerous wind gusts, wind tunnel effects, blown debris.
- ice: Black ice, slippery surfaces, frozen stairs, slush pools.
- blocked: Path/route obstructions (construction, crowds, vehicles, events).
- accessibility: Elevator/escalator outages, ramp blockages, ADA route breaks.
- cycling: Bike lane hazards, bridge grating, potholes, debris.
- bridge: Bridge lift in progress or structural issues blocking pedestrian crossing.
- crowd: Dangerous crowd density, protests blocking routes.
- construction: Active construction, scaffold narrowing, utility work.
- other: Any real hazard that doesn't fit above categories.

SEVERITY SCALE:
1 — Minor inconvenience. Low urgency. Informational only.
2 — Noticeable hazard. Warrants awareness.
3 — Moderate hazard. Most pedestrians will be affected.
4 — Serious hazard. Rerouting recommended. Vulnerable users (elderly, wheelchair) MUST avoid.
5 — Critical / Life-threatening. Immediate city action required.

RULES:
- ANY elevator or ramp outage that blocks wheelchair access is MINIMUM severity 4.
- ANY lifted bridge is MINIMUM severity 4 (breaks accessible routes).
- ANY wind hazard at a known Death Corner is MINIMUM severity 4.
- ANY black ice on transit stairs or platforms is MINIMUM severity 5.
- If the report is vague or unverifiable, set action to "Needs Verification" and confidence ≤ 0.75.
- If the report is clearly dangerous (injury mentioned, multiple people affected), set action to "Immediate Alert".

Respond ONLY with a valid JSON object — no markdown, no explanation, just raw JSON:
{
  "category": "<one of the category strings above>",
  "severity": <integer 1-5>,
  "summary_label": "<max 80 chars — short, punchy, location-specific label>",
  "action": "<'Immediate Alert' or 'Needs Verification'>",
  "confidence": <float 0.0-1.0>,
  "reasoning": "<one sentence explaining your classification>"
}
"""


def _build_user_message(
    description: Optional[str],
    raw_category: Optional[str],
    address: Optional[str],
    layer: Optional[str],
    near_death_corner: bool,
    near_bridge: bool,
    near_pedway: bool,
    wind_speed_mph: Optional[float] = None,
) -> str:
    """Build the user message with all available context."""
    parts = []

    if address:
        parts.append(f"LOCATION: {address}")
    if layer:
        parts.append(f"LAYER: {layer}")
    if raw_category:
        parts.append(f"USER-SELECTED CATEGORY: {raw_category}")
    if description:
        parts.append(f"USER DESCRIPTION: {description}")

    flags = []
    if near_death_corner:
        flags.append("⚠️ This location is a known Death Corner wind-tunnel intersection.")
    if near_bridge:
        flags.append("🌉 This location is adjacent to a Chicago River bascule bridge.")
    if near_pedway:
        flags.append("🚇 This location is near a Pedway entrance — accessibility is critical.")
    if wind_speed_mph and wind_speed_mph > 20:
        flags.append(f"💨 Current O'Hare wind speed: {wind_speed_mph:.1f} mph (Venturi amplification likely).")

    if flags:
        parts.append("CONTEXT FLAGS:\n" + "\n".join(flags))

    return "\n".join(parts)


async def classify_report(
    description: Optional[str] = None,
    raw_category: Optional[str] = None,
    address: Optional[str] = None,
    layer: Optional[str] = "surface",
    near_death_corner: bool = False,
    near_bridge: bool = False,
    near_pedway: bool = False,
    wind_speed_mph: Optional[float] = None,
) -> AIClassification:
    """
    Send the report to Llama 3.2 3B (Groq) and return a structured classification.
    Falls back to a rule-based classifier if Groq is unavailable or key is not set.
    """
    if settings.GROQ_API_KEY:
        try:
            return await _classify_with_llama(
                description, raw_category, address, layer,
                near_death_corner, near_bridge, near_pedway, wind_speed_mph
            )
        except Exception as exc:
            logger.warning("Llama/Groq classification failed (%s) — falling back to rule-based.", exc)

    return _classify_rule_based(
        description, raw_category, address, layer,
        near_death_corner, near_bridge, near_pedway, wind_speed_mph
    )


async def _classify_with_llama(
    description, raw_category, address, layer,
    near_death_corner, near_bridge, near_pedway, wind_speed_mph
) -> AIClassification:
    client = AsyncOpenAI(
        api_key=settings.GROQ_API_KEY,
        base_url=GROQ_BASE_URL,
    )

    user_message = _build_user_message(
        description, raw_category, address, layer,
        near_death_corner, near_bridge, near_pedway, wind_speed_mph
    )

    response = await client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        response_format={"type": "json_object"},
        temperature=0.2,
        max_tokens=512,
    )

    raw_json = response.choices[0].message.content
    data = json.loads(raw_json)

    try:
        category = HazardCategory(data["category"])
    except ValueError:
        category = HazardCategory.OTHER

    return AIClassification(
        category=category,
        severity=max(1, min(5, int(data["severity"]))),
        summary_label=str(data["summary_label"])[:255],
        action=data.get("action", "Needs Verification"),
        confidence=float(data.get("confidence", 0.75)),
        reasoning=str(data.get("reasoning", "")),
    )


def _classify_rule_based(
    description, raw_category, address, layer,
    near_death_corner, near_bridge, near_pedway, wind_speed_mph
) -> AIClassification:
    """
    Deterministic fallback used when Groq is unavailable or key is not set.
    """
    cat_map = {
        "wind": HazardCategory.WIND,
        "ice": HazardCategory.ICE,
        "blocked": HazardCategory.BLOCKED,
        "accessibility": HazardCategory.ACCESSIBILITY,
        "cycling": HazardCategory.CYCLING,
        "bridge": HazardCategory.BRIDGE,
        "crowd": HazardCategory.CROWD,
        "construction": HazardCategory.CONSTRUCTION,
    }
    category = cat_map.get((raw_category or "").lower(), HazardCategory.OTHER)

    severity = 2
    if near_death_corner and category == HazardCategory.WIND:
        severity = 4
    if near_bridge and category == HazardCategory.BRIDGE:
        severity = 4
    if category == HazardCategory.ACCESSIBILITY:
        severity = 4
    if category == HazardCategory.ICE:
        severity = 3
    if wind_speed_mph and wind_speed_mph > 30:
        severity = max(severity, 4)

    action = "Immediate Alert" if severity >= 4 else "Needs Verification"
    label_parts = [category.value.capitalize()]
    if address:
        label_parts.append(f"— {address[:50]}")
    summary_label = " ".join(label_parts)

    return AIClassification(
        category=category,
        severity=severity,
        summary_label=summary_label,
        action=action,
        confidence=0.65,
        reasoning="Rule-based fallback classification (Groq unavailable).",
    )
