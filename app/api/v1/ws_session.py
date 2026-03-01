"""
LoopNav — Production WebSocket Session Manager
===============================================
WS  /ws/session/{session_id}   — Live position tracking + turn-by-turn guidance
GET /ws/sessions/count         — Active session count (for dashboard)
GET /ws/sessions/health        — Session manager health

WebSocket Protocol
------------------
Client → Server messages (JSON):

  { "type": "init",
    "route": <SingleRoute as dict>,
    "destination": {"lat": float, "lng": float},
    "user_speed_ms": float  (optional, default 1.4) }

  { "type": "position_update",
    "lat": float, "lng": float,
    "accuracy_m": float,   (optional)
    "timestamp": str }     (optional ISO timestamp)

  { "type": "ping" }

  { "type": "reroute_ack",
    "accepted": bool }

Server → Client messages (JSON):

  { "type": "ready",
    "session_id": str,
    "message": str }

  { "type": "progress",
    "progress_pct": float,
    "segment_index": int,
    "current_level": str,
    "next_instruction": str,
    "next_level_change": str | null,
    "time_remaining_s": int,
    "distance_remaining_m": float,
    "deviation_m": float,
    "speed_ms": float | null }

  { "type": "approaching_connector",
    "connector_type": str,
    "in_seconds": int,
    "instruction": str }

  { "type": "level_changed",
    "from_level": str,
    "to_level":   str,
    "instruction": str }

  { "type": "off_route",
    "deviation_m": float,
    "message": str }

  { "type": "arrived",
    "total_time_s": int,
    "route_summary": str }

  { "type": "pong" }

  { "type": "error",
    "code": str,
    "message": str }
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import atan2, cos, radians, sin, sqrt
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, WebSocketException

logger = logging.getLogger(__name__)

router = APIRouter(tags=["LoopNav — WebSocket"])

# ── Configuration ─────────────────────────────────────────────────────────────

HEARTBEAT_INTERVAL_S    = 25      # send pong proactively to keep connection alive
OFF_ROUTE_THRESHOLD_M   = 80      # metres before declaring off-route
ARRIVAL_THRESHOLD_M     = 30      # metres from destination to declare arrival
CONNECTOR_WARNING_M     = 120     # metres ahead to warn about level change
MAX_POSITION_HISTORY    = 10      # positions kept for speed estimation
SESSION_IDLE_TIMEOUT_S  = 300     # 5 min without messages → close session
MAX_CONCURRENT_SESSIONS = 200     # hard cap — raise for production


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class Position:
    lat:        float
    lng:        float
    accuracy_m: float = 10.0
    timestamp:  float = field(default_factory=time.time)


@dataclass
class SessionState:
    session_id:       str
    websocket:        WebSocket
    route:            Optional[dict]         = None
    destination:      Optional[dict]         = None
    created_at:       float                  = field(default_factory=time.time)
    last_message_at:  float                  = field(default_factory=time.time)
    last_position:    Optional[Position]     = None
    position_history: deque                  = field(default_factory=lambda: deque(maxlen=MAX_POSITION_HISTORY))
    current_segment:  int                    = 0
    off_route_count:  int                    = 0
    total_positions:  int                    = 0
    reroute_pending:  bool                   = False
    user_speed_ms:    float                  = 1.4
    arrived:          bool                   = False
    # Connector approach tracking
    last_connector_warned: Optional[int]     = None


# ── Session store ──────────────────────────────────────────────────────────────

_sessions: dict[str, SessionState] = {}
_sessions_lock = asyncio.Lock()


# ── Geometry helpers ───────────────────────────────────────────────────────────

def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6_371_000
    φ1, φ2 = radians(lat1), radians(lat2)
    a = (sin((φ2 - φ1) / 2) ** 2
         + cos(φ1) * cos(φ2) * sin(radians(lng2 - lng1) / 2) ** 2)
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def _nearest_point_on_segment(
    lat: float, lng: float,
    seg_coords: list[list[float]],
) -> tuple[float, int]:
    """
    Returns (min_distance_m, index_of_nearest_coord) for a list of [lng, lat] coords.
    """
    min_dist  = float("inf")
    min_idx   = 0
    for i, coord in enumerate(seg_coords):
        d = _haversine_m(lat, lng, coord[1], coord[0])
        if d < min_dist:
            min_dist = d
            min_idx  = i
    return min_dist, min_idx


def _total_segment_distance(segments: list[dict]) -> float:
    return sum(s.get("distance_m", 0.0) for s in segments)


def _remaining_distance(seg_idx: int, segments: list[dict]) -> float:
    return sum(s.get("distance_m", 0.0) for s in segments[seg_idx:])


def _remaining_time(seg_idx: int, segments: list[dict]) -> int:
    return sum(s.get("time_s", 0) for s in segments[seg_idx:])


def _estimate_speed(history: deque) -> Optional[float]:
    """
    Estimate current walking speed from position history.
    Returns m/s or None if insufficient data.
    """
    if len(history) < 3:
        return None
    positions = list(history)
    total_dist = 0.0
    for i in range(1, len(positions)):
        p1, p2 = positions[i - 1], positions[i]
        total_dist += _haversine_m(p1.lat, p1.lng, p2.lat, p2.lng)
    elapsed = positions[-1].timestamp - positions[0].timestamp
    if elapsed < 1:
        return None
    return round(total_dist / elapsed, 2)


def _find_current_segment(
    lat: float, lng: float, segments: list[dict], hint_idx: int
) -> tuple[int, float]:
    """
    Efficient segment finder — searches forward from hint_idx first,
    then falls back to a full scan if deviation is large.

    Returns (segment_index, deviation_m).
    """
    # Forward scan (most common case — user is moving forward)
    best_dist = float("inf")
    best_seg  = hint_idx

    search_range = range(
        max(0, hint_idx - 1),
        min(len(segments), hint_idx + 4)
    )

    for i in search_range:
        seg = segments[i]
        d, _ = _nearest_point_on_segment(lat, lng, seg.get("geometry", []))
        if d < best_dist:
            best_dist = d
            best_seg  = i

    # If the nearest segment is still very far, do a full scan
    if best_dist > OFF_ROUTE_THRESHOLD_M * 0.5:
        for i, seg in enumerate(segments):
            d, _ = _nearest_point_on_segment(lat, lng, seg.get("geometry", []))
            if d < best_dist:
                best_dist = d
                best_seg  = i

    return best_seg, best_dist


def _next_connector(segments: list[dict], from_seg: int) -> Optional[dict]:
    """
    Looks ahead in the segment list for the next level-changing connector.
    Returns the connector segment dict or None.
    """
    for seg in segments[from_seg + 1:]:
        if seg.get("connector_type") is not None:
            return seg
    return None


def _distance_to_segment_end(
    lat: float, lng: float, seg: dict
) -> float:
    """Approximate distance from current position to end of a segment."""
    geometry = seg.get("geometry", [])
    if not geometry:
        return 0.0
    end_coord = geometry[-1]
    return _haversine_m(lat, lng, end_coord[1], end_coord[0])


def _compute_progress(seg_idx: int, total_segs: int) -> float:
    if total_segs <= 1:
        return 100.0
    return round(seg_idx / (total_segs - 1) * 100, 1)


def _format_connector_instruction(connector_type: str, to_level: str) -> str:
    level_labels = {"street": "street level", "mid": "Lower Wacker", "pedway": "the pedway"}
    label = level_labels.get(to_level, to_level)
    if connector_type == "elevator":
        return f"Take the elevator to {label}"
    if connector_type == "escalator":
        return f"Take the escalator to {label}"
    if connector_type == "stairs":
        return f"Take the stairs to {label}"
    return f"Transition to {label}"


def _check_arrival(lat: float, lng: float, destination: Optional[dict]) -> bool:
    if not destination:
        return False
    d = _haversine_m(lat, lng, destination["lat"], destination["lng"])
    return d <= ARRIVAL_THRESHOLD_M


def _send_error(code: str, message: str) -> dict:
    return {"type": "error", "code": code, "message": message}


# ── Message handlers ───────────────────────────────────────────────────────────

async def _handle_init(ws: WebSocket, session: SessionState, data: dict) -> dict:
    """Process an init message. Validates and stores the route."""
    route = data.get("route")
    if not route:
        return _send_error("MISSING_ROUTE", "init message must include a 'route' object.")

    segments = route.get("segments", [])
    if not segments:
        return _send_error("EMPTY_ROUTE", "Route has no segments.")

    # Validate basic segment structure
    for i, seg in enumerate(segments):
        if "geometry" not in seg or not seg["geometry"]:
            return _send_error(
                "INVALID_SEGMENT",
                f"Segment {i} is missing geometry."
            )

    destination = data.get("destination")
    if not destination and segments:
        # Infer destination from last segment's last coordinate
        last_seg    = segments[-1]
        last_coord  = last_seg["geometry"][-1]
        destination = {"lat": last_coord[1], "lng": last_coord[0]}

    user_speed = float(data.get("user_speed_ms", 1.4))
    user_speed = max(0.5, min(user_speed, 3.0))   # clamp to sane range

    session.route         = route
    session.destination   = destination
    session.user_speed_ms = user_speed
    session.current_segment = 0

    total_time_s = sum(s.get("time_s", 0) for s in segments)
    total_dist_m = sum(s.get("distance_m", 0.0) for s in segments)

    logger.info(
        "Session %s initialised | %d segments | %.0fm | %ds",
        session.session_id, len(segments), total_dist_m, total_time_s,
    )

    return {
        "type":       "ready",
        "session_id": session.session_id,
        "message":    "Session started. Send position_update messages to begin navigation.",
        "route_summary": route.get("route_summary", ""),
        "total_time_s":  total_time_s,
        "total_dist_m":  round(total_dist_m, 1),
        "segments":      len(segments),
    }


async def _handle_position_update(
    ws: WebSocket, session: SessionState, data: dict
) -> list[dict]:
    """
    Process a position update. Returns a list of messages to send back.
    May return multiple messages (e.g., progress + approaching_connector).
    """
    if not session.route:
        return [_send_error("NO_ROUTE", "No route loaded. Send an init message first.")]

    try:
        lat        = float(data["lat"])
        lng        = float(data["lng"])
        accuracy_m = float(data.get("accuracy_m", 10.0))
    except (KeyError, ValueError, TypeError):
        return [_send_error("INVALID_POSITION", "lat and lng must be valid floats.")]

    segments = session.route.get("segments", [])
    if not segments:
        return [_send_error("EMPTY_ROUTE", "Route has no segments.")]

    # Update position history for speed estimation
    pos = Position(lat=lat, lng=lng, accuracy_m=accuracy_m)
    session.position_history.append(pos)
    session.last_position   = pos
    session.total_positions += 1

    # Check arrival
    if _check_arrival(lat, lng, session.destination):
        if not session.arrived:
            session.arrived = True
            total_time = session.route.get("total_time_s", 0)
            return [{
                "type":         "arrived",
                "total_time_s": total_time,
                "route_summary": session.route.get("route_summary", ""),
                "message":      "You have arrived at your destination!",
            }]

    # Find current position on route
    seg_idx, deviation_m = _find_current_segment(
        lat, lng, segments, session.current_segment
    )

    # Update session segment index (only move forward, never backward)
    if seg_idx >= session.current_segment:
        prev_segment = session.current_segment
        session.current_segment = seg_idx

        # Detect level change crossing
        if prev_segment != seg_idx and seg_idx < len(segments):
            prev_seg = segments[prev_segment]
            curr_seg = segments[seg_idx]
            if prev_seg.get("level") != curr_seg.get("level"):
                pass  # level_changed message below

    # Off-route detection
    if deviation_m > OFF_ROUTE_THRESHOLD_M:
        session.off_route_count += 1
        logger.info(
            "Session %s off-route: %.1fm (count=%d)",
            session.session_id, deviation_m, session.off_route_count,
        )
        return [{
            "type":        "off_route",
            "deviation_m": round(deviation_m, 1),
            "message":     (
                "You are off the route. "
                "Tap 'Reroute' to calculate a new path from your current position."
            ),
        }]

    # Back on route — reset off-route counter
    session.off_route_count = 0

    progress         = _compute_progress(seg_idx, len(segments))
    time_remaining   = _remaining_time(seg_idx, segments)
    dist_remaining   = _remaining_distance(seg_idx, segments)
    estimated_speed  = _estimate_speed(session.position_history)

    current_seg  = segments[seg_idx]
    next_seg_idx = min(seg_idx + 1, len(segments) - 1)
    next_seg     = segments[next_seg_idx]
    next_instr   = next_seg.get("instruction", current_seg.get("instruction", "Continue on route"))

    # Find next level change for preview
    next_connector   = _next_connector(segments, seg_idx)
    next_level_change = None
    if next_connector:
        ct    = next_connector.get("connector_type", "connector")
        level = next_connector.get("level", "")
        next_level_change = f"Next: {ct} to {level}"

    messages: list[dict] = [{
        "type":                "progress",
        "progress_pct":        progress,
        "segment_index":       seg_idx,
        "current_level":       current_seg.get("level", "street"),
        "next_instruction":    next_instr,
        "next_level_change":   next_level_change,
        "time_remaining_s":    time_remaining,
        "distance_remaining_m": round(dist_remaining, 1),
        "deviation_m":         round(deviation_m, 1),
        "speed_ms":            estimated_speed,
        "accuracy_m":          accuracy_m,
    }]

    # Approaching connector warning (within CONNECTOR_WARNING_M)
    if next_connector and session.last_connector_warned != seg_idx:
        dist_to_end = _distance_to_segment_end(lat, lng, current_seg)
        if dist_to_end <= CONNECTOR_WARNING_M:
            ct      = next_connector.get("connector_type", "connector")
            to_lvl  = next_connector.get("level", "")
            eta_s   = int(dist_to_end / session.user_speed_ms)
            messages.append({
                "type":           "approaching_connector",
                "connector_type": ct,
                "in_seconds":     eta_s,
                "instruction":    _format_connector_instruction(ct, to_lvl),
            })
            session.last_connector_warned = seg_idx

    return messages


async def _handle_ping(session: SessionState) -> dict:
    return {
        "type":          "pong",
        "session_id":    session.session_id,
        "server_time":   datetime.now(timezone.utc).isoformat(),
        "uptime_s":      round(time.time() - session.created_at),
        "positions_rx":  session.total_positions,
    }


async def _heartbeat_loop(ws: WebSocket, session: SessionState) -> None:
    """
    Sends a periodic heartbeat to keep the WebSocket connection alive
    through proxies and load balancers that drop idle connections.
    """
    try:
        while not session.arrived:
            await asyncio.sleep(HEARTBEAT_INTERVAL_S)
            idle_s = time.time() - session.last_message_at
            if idle_s > SESSION_IDLE_TIMEOUT_S:
                logger.info(
                    "Session %s idle for %.0fs — closing.", session.session_id, idle_s
                )
                await ws.close(code=1000, reason="Session idle timeout")
                return
            try:
                await ws.send_json({
                    "type":       "heartbeat",
                    "session_id": session.session_id,
                    "idle_s":     round(idle_s),
                })
            except Exception:
                return
    except asyncio.CancelledError:
        pass


# ── WebSocket handler ─────────────────────────────────────────────────────────

@router.websocket("/ws/session/{session_id}")
async def route_session(websocket: WebSocket, session_id: str):
    """
    Live position tracking WebSocket for turn-by-turn navigation.

    Lifecycle:
    1. Client connects
    2. Client sends { type: "init", route: <route>, destination: {...} }
    3. Client sends periodic { type: "position_update", lat, lng }
    4. Server responds with progress/connector/off_route/arrived messages
    5. Connection closes on arrival or timeout
    """
    async with _sessions_lock:
        if len(_sessions) >= MAX_CONCURRENT_SESSIONS:
            await websocket.close(code=1013, reason="Server session limit reached")
            logger.warning("Session limit (%d) reached.", MAX_CONCURRENT_SESSIONS)
            return

    await websocket.accept()

    session = SessionState(
        session_id=session_id,
        websocket=websocket,
    )

    async with _sessions_lock:
        _sessions[session_id] = session

    logger.info(
        "WS session opened: %s  (total active: %d)",
        session_id, len(_sessions),
    )

    # Start heartbeat task
    heartbeat_task = asyncio.create_task(_heartbeat_loop(websocket, session))

    try:
        while True:
            try:
                raw = await websocket.receive_text()
            except WebSocketDisconnect:
                break

            session.last_message_at = time.time()

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json(_send_error(
                    "INVALID_JSON", "Message must be valid JSON."
                ))
                continue

            msg_type = data.get("type")

            if msg_type == "init":
                reply = await _handle_init(websocket, session, data)
                await websocket.send_json(reply)

            elif msg_type == "position_update":
                replies = await _handle_position_update(websocket, session, data)
                for reply in replies:
                    await websocket.send_json(reply)
                # If arrived, close gracefully after sending the message
                if session.arrived:
                    await asyncio.sleep(0.5)
                    break

            elif msg_type == "ping":
                await websocket.send_json(await _handle_ping(session))

            elif msg_type == "reroute_ack":
                accepted = data.get("accepted", True)
                if accepted:
                    session.reroute_pending = False
                    session.current_segment = 0
                    await websocket.send_json({
                        "type":    "reroute_accepted",
                        "message": "Rerouting from your current position.",
                    })
                else:
                    await websocket.send_json({
                        "type":    "reroute_declined",
                        "message": "Continuing on original route.",
                    })

            else:
                await websocket.send_json(_send_error(
                    "UNKNOWN_TYPE",
                    f"Unknown message type: '{msg_type}'. "
                    f"Valid types: init, position_update, ping, reroute_ack."
                ))

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error("WS session %s unexpected error: %s", session_id, e, exc_info=True)
        try:
            await websocket.send_json(_send_error("SERVER_ERROR", str(e)))
        except Exception:
            pass
    finally:
        heartbeat_task.cancel()
        async with _sessions_lock:
            _sessions.pop(session_id, None)
        logger.info(
            "WS session closed: %s  (total active: %d)",
            session_id, len(_sessions),
        )


# ── Session info endpoints ─────────────────────────────────────────────────────

@router.get("/ws/sessions/count")
async def active_session_count():
    """Return the count of currently active WebSocket sessions."""
    return {
        "active_sessions": len(_sessions),
        "max_sessions":    MAX_CONCURRENT_SESSIONS,
        "capacity_pct":    round(len(_sessions) / MAX_CONCURRENT_SESSIONS * 100, 1),
    }


@router.get("/ws/sessions/health")
async def session_health():
    """
    Return health stats for the session manager.
    Useful for ops monitoring and the analytics dashboard.
    """
    now      = time.time()
    sessions = list(_sessions.values())

    idle_counts = [now - s.last_message_at for s in sessions]
    pos_counts  = [s.total_positions for s in sessions]
    arrived     = sum(1 for s in sessions if s.arrived)

    return {
        "active_sessions":   len(sessions),
        "arrived_sessions":  arrived,
        "avg_idle_s":        round(sum(idle_counts) / max(len(idle_counts), 1), 1),
        "max_idle_s":        round(max(idle_counts, default=0), 1),
        "total_positions_rx": sum(pos_counts),
        "avg_positions_per_session": round(
            sum(pos_counts) / max(len(pos_counts), 1), 1
        ),
        "server_time":       datetime.now(timezone.utc).isoformat(),
    }
