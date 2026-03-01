"""
LoopNav — Production Analytics & Stats API
==========================================
GET  /api/v1/nav/stats                     — Full stats snapshot
GET  /api/v1/nav/stats/stream              — Server-Sent Events live stream
GET  /api/v1/nav/stats/routes              — Route-level breakdown
GET  /api/v1/nav/stats/weather             — Weather impact analytics
GET  /api/v1/nav/stats/modes               — Routing mode distribution
GET  /api/v1/nav/stats/accessibility       — Accessibility usage stats
GET  /api/v1/nav/stats/ai                  — AI routing analytics
GET  /api/v1/nav/stats/feedback            — User feedback summary
GET  /api/v1/nav/stats/time-of-day         — Hourly usage breakdown
POST /api/v1/nav/stats/reset               — Reset all counters (demo use)
GET  /api/v1/nav/stats/export              — Export stats as JSON

All counters stored in Redis. Live stream pushes updates every 3 seconds
via Server-Sent Events for the analytics dashboard.

Redis key schema
----------------
nav:stats:routes_total           — INCR counter
nav:stats:popular_routes         — ZSET member=pair, score=count
nav:stats:times                  — LIST of route times (last 200)
nav:stats:mode:{mode}            — INCR counter per mode
nav:stats:accessible_requests    — INCR counter
nav:stats:ai_requests_total      — INCR counter
nav:stats:ai_source:{source}     — INCR per source (claude/rule_based/cache)
nav:stats:ai_classify_requests   — INCR counter
nav:stats:ai_feedback_total      — INCR counter
nav:stats:ai_feedback_incorrect  — INCR counter
nav:stats:feedback_total         — INCR counter
nav:stats:feedback_incorrect     — INCR counter (via rating)
nav:stats:rating:{1-5}           — INCR per star rating
nav:stats:issue:{type}           — INCR per reported issue
nav:stats:hour:{0-23}            — INCR per hour of day
nav:stats:weather_impact:{level} — INCR per weather impact level
nav:stats:session_start          — ISO timestamp of first request
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from app.services.cache import Cache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/nav", tags=["LoopNav — Analytics"])

# ── Configuration ──────────────────────────────────────────────────────────────

STREAM_INTERVAL_S = 3      # SSE push interval
STREET_BASELINE_S = 600    # baseline for "time saved" calculation (~800m at 1.4 m/s)
TOP_ROUTES_COUNT  = 5

VALID_MODES    = ("optimal", "street_only", "pedway_preferred", "mid_preferred")
VALID_ISSUES   = ("wrong_path", "missing_connector", "closed_pedway",
                  "inaccurate_time", "elevator_wrong", "other")
VALID_RATINGS  = (1, 2, 3, 4, 5)


# ── Redis helpers ──────────────────────────────────────────────────────────────

async def _safe_get_int(cache: Cache, key: str) -> int:
    try:
        return await cache.get_int(key)
    except Exception:
        return 0


async def _safe_zrevrange(cache: Cache, key: str, start: int, end: int) -> list:
    try:
        raw = await cache.zrevrange(key, start, end)
        result = []
        for i in range(0, len(raw) - 1, 2):
            member = raw[i]
            score  = raw[i + 1]
            if isinstance(member, bytes):
                member = member.decode()
            result.append({"route": member, "count": int(float(score))})
        return result
    except Exception:
        return []


async def _safe_lrange(cache: Cache, key: str, start: int, end: int) -> list[int]:
    try:
        raw = await cache.lrange(key, start, end)
        return [int(x) for x in raw if x]
    except Exception:
        return []


def _percentile(data: list[int], pct: float) -> int:
    if not data:
        return 0
    sorted_data = sorted(data)
    idx = max(0, int(len(sorted_data) * pct / 100) - 1)
    return sorted_data[idx]


def _avg(data: list[int]) -> int:
    return sum(data) // len(data) if data else 0


# ── Stats builders ─────────────────────────────────────────────────────────────

async def _build_route_stats(cache: Cache) -> dict:
    total      = await _safe_get_int(cache, "nav:stats:routes_total")
    times      = await _safe_lrange(cache, "nav:stats:times", 0, -1)
    top_routes = await _safe_zrevrange(cache, "nav:stats:popular_routes", 0, TOP_ROUTES_COUNT - 1)

    avg_time  = _avg(times)
    avg_saved = max(0, STREET_BASELINE_S - avg_time) if avg_time else 0

    percentiles: dict[str, int] = {}
    if times:
        percentiles = {
            "p50": _percentile(times, 50),
            "p75": _percentile(times, 75),
            "p90": _percentile(times, 90),
            "p95": _percentile(times, 95),
        }

    return {
        "routes_total":        total,
        "avg_time_s":          avg_time,
        "avg_time_min":        round(avg_time / 60, 1) if avg_time else 0,
        "avg_saved_s":         avg_saved,
        "avg_saved_min":       round(avg_saved / 60, 1) if avg_saved else 0,
        "min_time_s":          min(times) if times else 0,
        "max_time_s":          max(times) if times else 0,
        "percentiles_s":       percentiles,
        "top_routes":          top_routes,
        "sample_size":         len(times),
    }


async def _build_mode_stats(cache: Cache) -> dict:
    counts: dict[str, int] = {}
    total_modes = 0
    for mode in VALID_MODES:
        n = await _safe_get_int(cache, f"nav:stats:mode:{mode}")
        counts[mode]  = n
        total_modes  += n

    distributions: dict[str, float] = {}
    for mode, n in counts.items():
        distributions[mode] = round(n / total_modes * 100, 1) if total_modes else 0.0

    return {
        "counts":        counts,
        "distribution":  distributions,
        "total":         total_modes,
        "most_popular":  max(counts, key=lambda m: counts[m]) if counts else None,
    }


async def _build_accessibility_stats(cache: Cache) -> dict:
    total_routes = await _safe_get_int(cache, "nav:stats:routes_total")
    accessible   = await _safe_get_int(cache, "nav:stats:accessible_requests")
    ai_acc       = await _safe_get_int(cache, "nav:stats:ai_accessible_requests")

    return {
        "accessible_requests":         accessible,
        "accessible_pct":              round(accessible / total_routes * 100, 1) if total_routes else 0,
        "ai_accessible_requests":      ai_acc,
    }


async def _build_ai_stats(cache: Cache) -> dict:
    total    = await _safe_get_int(cache, "nav:stats:ai_requests_total")
    classify = await _safe_get_int(cache, "nav:stats:ai_classify_requests")
    fb_total = await _safe_get_int(cache, "nav:stats:ai_feedback_total")
    fb_wrong = await _safe_get_int(cache, "nav:stats:ai_feedback_incorrect")

    sources: dict[str, int] = {}
    for src in ("claude", "rule_based", "cache"):
        sources[src] = await _safe_get_int(cache, f"nav:stats:ai_source:{src}")

    claude_n    = sources.get("claude", 0)
    rule_n      = sources.get("rule_based", 0)
    cache_n     = sources.get("cache", 0)
    fallback_rt = round(rule_n / (claude_n + rule_n) * 100, 1) if (claude_n + rule_n) else 0

    return {
        "ai_requests_total":    total,
        "classify_requests":    classify,
        "sources":              sources,
        "fallback_rate_pct":    fallback_rt,
        "cache_hit_rate_pct":   round(cache_n / max(total, 1) * 100, 1),
        "feedback_total":       fb_total,
        "feedback_incorrect":   fb_wrong,
        "accuracy_pct":         round((1 - fb_wrong / max(fb_total, 1)) * 100, 1) if fb_total else None,
    }


async def _build_feedback_stats(cache: Cache) -> dict:
    total  = await _safe_get_int(cache, "nav:stats:feedback_total")
    ratings: dict[int, int] = {}
    for r in VALID_RATINGS:
        ratings[r] = await _safe_get_int(cache, f"nav:stats:rating:{r}")

    issues: dict[str, int] = {}
    for issue in VALID_ISSUES:
        issues[issue] = await _safe_get_int(cache, f"nav:stats:issue:{issue}")

    total_with_rating = sum(ratings.values())
    avg_rating = (
        sum(r * n for r, n in ratings.items()) / total_with_rating
        if total_with_rating else None
    )

    return {
        "feedback_total": total,
        "ratings":        ratings,
        "avg_rating":     round(avg_rating, 2) if avg_rating else None,
        "issues":         {k: v for k, v in issues.items() if v > 0},
        "top_issue":      max(issues, key=lambda i: issues[i]) if any(issues.values()) else None,
    }


async def _build_weather_impact_stats(cache: Cache) -> dict:
    impacts: dict[str, int] = {}
    for level in ("none", "low", "medium", "high"):
        impacts[level] = await _safe_get_int(cache, f"nav:stats:weather_impact:{level}")

    total = sum(impacts.values())
    return {
        "by_level":    impacts,
        "total":       total,
        "bad_weather_pct": round(
            (impacts.get("medium", 0) + impacts.get("high", 0)) / max(total, 1) * 100, 1
        ),
    }


async def _build_hourly_stats(cache: Cache) -> dict:
    hours: dict[int, int] = {}
    for h in range(24):
        hours[h] = await _safe_get_int(cache, f"nav:stats:hour:{h}")

    peak_hour = max(hours, key=lambda h: hours[h]) if hours else None
    peak_count = hours.get(peak_hour, 0) if peak_hour is not None else 0

    return {
        "by_hour":   hours,
        "peak_hour": peak_hour,
        "peak_count": peak_count,
        "rush_am_total": sum(hours.get(h, 0) for h in range(7, 10)),
        "rush_pm_total": sum(hours.get(h, 0) for h in range(16, 19)),
        "off_peak_total": sum(hours.get(h, 0) for h in list(range(0, 7)) + list(range(19, 24))),
    }


async def _build_full_snapshot(cache: Cache) -> dict:
    """
    Fetch all stat categories concurrently and merge into one dict.
    """
    (
        route_stats,
        mode_stats,
        acc_stats,
        ai_stats,
        feedback_stats,
        weather_stats,
        hourly_stats,
    ) = await asyncio.gather(
        _build_route_stats(cache),
        _build_mode_stats(cache),
        _build_accessibility_stats(cache),
        _build_ai_stats(cache),
        _build_feedback_stats(cache),
        _build_weather_impact_stats(cache),
        _build_hourly_stats(cache),
        return_exceptions=True,
    )

    def _safe(val: Any, fallback: dict) -> dict:
        return val if not isinstance(val, Exception) else fallback

    return {
        "routes":        _safe(route_stats,   {}),
        "modes":         _safe(mode_stats,    {}),
        "accessibility": _safe(acc_stats,     {}),
        "ai":            _safe(ai_stats,      {}),
        "feedback":      _safe(feedback_stats,{}),
        "weather":       _safe(weather_stats, {}),
        "hourly":        _safe(hourly_stats,  {}),
        "generated_at":  datetime.now(timezone.utc).isoformat(),
    }


async def _build_stream_snapshot(cache: Cache) -> dict:
    """
    Lightweight snapshot for the SSE stream — only the counters
    that change frequently enough to warrant 3-second updates.
    """
    route_stats = await _build_route_stats(cache)
    mode_stats  = await _build_mode_stats(cache)

    return {
        "routes_total":   route_stats["routes_total"],
        "avg_time_min":   route_stats["avg_time_min"],
        "avg_saved_min":  route_stats["avg_saved_min"],
        "top_routes":     route_stats["top_routes"][:3],
        "mode_counts":    mode_stats["counts"],
        "generated_at":   datetime.now(timezone.utc).isoformat(),
    }


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/stats")
async def stats_snapshot(request: Request):
    """
    Full analytics snapshot across all categories.

    Categories: routes, modes, accessibility, AI, feedback, weather impact, hourly.
    """
    cache = Cache(request.app.state.redis)
    return await _build_full_snapshot(cache)


@router.get("/stats/stream")
async def stats_stream(request: Request):
    """
    Server-Sent Events stream — pushes live route stats every 3 seconds.

    Frontend connects with:
      const es = new EventSource('/api/v1/nav/stats/stream')
      es.onmessage = e => setStats(JSON.parse(e.data))

    The stream sends a lightweight snapshot (not the full stats object)
    to minimise bandwidth. Disconnect handled gracefully.
    """
    cache = Cache(request.app.state.redis)

    async def event_generator():
        errors = 0
        while True:
            if await request.is_disconnected():
                logger.debug("SSE client disconnected from stats stream.")
                break
            try:
                snapshot = await _build_stream_snapshot(cache)
                yield f"data: {json.dumps(snapshot)}\n\n"
                errors = 0
            except Exception as e:
                errors += 1
                logger.warning("SSE stats error (%d): %s", errors, e)
                if errors >= 5:
                    yield f"data: {json.dumps({'error': 'stats_unavailable'})}\n\n"
                    break
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
            await asyncio.sleep(STREAM_INTERVAL_S)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


@router.get("/stats/routes")
async def route_stats(request: Request):
    """Detailed route timing statistics including percentiles and top pairs."""
    cache = Cache(request.app.state.redis)
    return await _build_route_stats(cache)


@router.get("/stats/modes")
async def mode_stats(request: Request):
    """Routing mode distribution — which mode users choose most."""
    cache = Cache(request.app.state.redis)
    return await _build_mode_stats(cache)


@router.get("/stats/accessibility")
async def accessibility_stats(request: Request):
    """Accessibility feature usage breakdown."""
    cache = Cache(request.app.state.redis)
    return await _build_accessibility_stats(cache)


@router.get("/stats/ai")
async def ai_stats(request: Request):
    """AI natural language routing analytics — Claude vs rule-based usage, accuracy."""
    cache = Cache(request.app.state.redis)
    return await _build_ai_stats(cache)


@router.get("/stats/feedback")
async def feedback_stats(request: Request):
    """User feedback summary — star ratings, reported issue types."""
    cache = Cache(request.app.state.redis)
    return await _build_feedback_stats(cache)


@router.get("/stats/weather")
async def weather_stats(request: Request):
    """Weather impact on routing — how many routes were triggered by bad weather."""
    cache = Cache(request.app.state.redis)
    return await _build_weather_impact_stats(cache)


@router.get("/stats/time-of-day")
async def time_of_day_stats(request: Request):
    """Hourly usage breakdown — identifies peak and off-peak periods."""
    cache = Cache(request.app.state.redis)
    return await _build_hourly_stats(cache)


@router.get("/stats/export")
async def export_stats(request: Request):
    """
    Export the complete stats snapshot as a downloadable JSON file.
    Useful for post-hackathon analysis.
    """
    cache    = Cache(request.app.state.redis)
    snapshot = await _build_full_snapshot(cache)

    from fastapi.responses import JSONResponse
    return JSONResponse(
        content=snapshot,
        headers={
            "Content-Disposition": (
                f"attachment; filename=loopnav-stats-"
                f"{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
            )
        },
    )


@router.post("/stats/reset")
async def reset_stats(request: Request):
    """
    Reset all analytics counters to zero.
    Use between demo runs to show clean metrics to judges.
    """
    r = request.app.state.redis

    keys_to_delete = [
        "nav:stats:routes_total",
        "nav:stats:popular_routes",
        "nav:stats:times",
        "nav:stats:accessible_requests",
        "nav:stats:ai_requests_total",
        "nav:stats:ai_classify_requests",
        "nav:stats:ai_feedback_total",
        "nav:stats:ai_feedback_incorrect",
        "nav:stats:feedback_total",
        "nav:stats:session_start",
    ]

    for mode in VALID_MODES:
        keys_to_delete.append(f"nav:stats:mode:{mode}")
    for src in ("claude", "rule_based", "cache"):
        keys_to_delete.append(f"nav:stats:ai_source:{src}")
    for r_val in VALID_RATINGS:
        keys_to_delete.append(f"nav:stats:rating:{r_val}")
    for issue in VALID_ISSUES:
        keys_to_delete.append(f"nav:stats:issue:{issue}")
    for h in range(24):
        keys_to_delete.append(f"nav:stats:hour:{h}")
    for level in ("none", "low", "medium", "high"):
        keys_to_delete.append(f"nav:stats:weather_impact:{level}")

    await r.delete(*keys_to_delete)

    # Record reset time
    cache = Cache(r)
    await cache.set(
        "nav:stats:session_start",
        datetime.now(timezone.utc).isoformat(),
        ttl=86400,
    )

    logger.info("Analytics counters reset at %s", datetime.now(timezone.utc).isoformat())

    return {
        "message":     "All analytics counters reset.",
        "reset_at":    datetime.now(timezone.utc).isoformat(),
        "keys_deleted": len(keys_to_delete),
    }
