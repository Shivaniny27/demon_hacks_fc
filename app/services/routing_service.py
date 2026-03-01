"""
LoopSense Smart Routing Service.

Implements hazard-weighted route adjustment for the Chicago Loop.

Strategy:
- Maintain a graph of key Loop waypoints (intersections, stations, Pedway nodes)
- Each edge has a base cost (travel time in seconds)
- Active hazard reports inflate edge costs proportional to severity
- Dijkstra's algorithm finds the safest route given current hazard state
- Returns a list of RouteSegments with per-segment hazard scores
"""

import heapq
import math
import logging
from typing import Optional

from app.data.chicago_constants import (
    DEATH_CORNERS, CTA_LOOP_STATIONS, PEDWAY_NODES, LOOP_BRIDGES,
    haversine_m, HAZARD_ROUTING_RADIUS_M,
)
from app.models.report import LoopLayer
from app.models.schemas import RouteRequest, RouteResponse, RouteSegment

logger = logging.getLogger(__name__)

# Walking speed assumptions
WALK_SPEED_M_S = 1.4       # comfortable pedestrian pace
PEDWAY_SPEED_M_S = 1.2     # slower due to turns, doors
SEVERITY_COST_MULTIPLIER = [1.0, 1.2, 1.5, 2.5, 4.0, 8.0]  # index = severity


# ── Node catalogue ────────────────────────────────────────────────────────────

def _build_node_list() -> list[dict]:
    """Combine all known Loop waypoints into a flat node list."""
    nodes = []
    for s in CTA_LOOP_STATIONS:
        nodes.append({"id": s.name, "lat": s.lat, "lng": s.lng, "layer": LoopLayer.SURFACE})
    for p in PEDWAY_NODES:
        nodes.append({"id": f"pedway:{p.name}", "lat": p.lat, "lng": p.lng, "layer": LoopLayer.PEDWAY})
    for dc in DEATH_CORNERS:
        nodes.append({"id": f"corner:{dc.name}", "lat": dc.lat, "lng": dc.lng, "layer": LoopLayer.SURFACE})
    return nodes


LOOP_NODES = _build_node_list()


def _nearest_node(lat: float, lng: float, layer: Optional[LoopLayer] = None) -> dict:
    """Return the closest node to the given coordinate, optionally filtered by layer."""
    candidates = LOOP_NODES
    if layer:
        layer_candidates = [n for n in LOOP_NODES if n["layer"] == layer]
        if layer_candidates:
            candidates = layer_candidates
    return min(candidates, key=lambda n: haversine_m(lat, lng, n["lat"], n["lng"]))


def _edge_base_cost(n1: dict, n2: dict) -> float:
    """Base traversal cost (seconds) for an edge between two nodes."""
    dist = haversine_m(n1["lat"], n1["lng"], n2["lat"], n2["lng"])
    speed = PEDWAY_SPEED_M_S if n1["layer"] == LoopLayer.PEDWAY else WALK_SPEED_M_S
    return dist / speed


def _hazard_cost_multiplier(
    n1: dict,
    n2: dict,
    active_reports: list[dict],
) -> tuple[float, float, list[str]]:
    """
    Return (multiplier, hazard_score 0-1, list_of_warning_strings) for an edge.
    hazard_score aggregates the severity influence of nearby reports.
    """
    mid_lat = (n1["lat"] + n2["lat"]) / 2
    mid_lng = (n1["lng"] + n2["lng"]) / 2

    multiplier = 1.0
    hazard_score = 0.0
    warnings = []

    for report in active_reports:
        dist = haversine_m(mid_lat, mid_lng, report["latitude"], report["longitude"])
        if dist <= HAZARD_ROUTING_RADIUS_M:
            sev = report.get("severity", 1)
            m = SEVERITY_COST_MULTIPLIER[min(sev, 5)]
            # Weight by proximity — closer = full weight
            proximity_weight = max(0.0, 1.0 - dist / HAZARD_ROUTING_RADIUS_M)
            edge_multiplier = 1.0 + (m - 1.0) * proximity_weight
            multiplier = max(multiplier, edge_multiplier)
            hazard_score = max(hazard_score, min(1.0, sev / 5.0 * proximity_weight))
            if sev >= 3:
                label = report.get("summary_label") or report.get("ai_category", "hazard")
                warnings.append(f"⚠️ {label} ({int(dist)}m away)")

    return multiplier, hazard_score, warnings


# ── Dijkstra ──────────────────────────────────────────────────────────────────

def _dijkstra(
    origin: dict,
    destination: dict,
    active_reports: list[dict],
    accessibility: bool = False,
) -> tuple[list[dict], float, list[str]]:
    """
    Run Dijkstra over the Loop node graph.

    Returns (path_nodes, total_hazard_score, all_warnings).
    """
    # Build adjacency: connect each node to its K nearest neighbours
    K_NEIGHBORS = 5
    adj: dict[str, list[tuple[float, dict, float, list[str]]]] = {n["id"]: [] for n in LOOP_NODES}

    for i, n1 in enumerate(LOOP_NODES):
        distances = sorted(
            [(haversine_m(n1["lat"], n1["lng"], n2["lat"], n2["lng"]), n2)
             for n2 in LOOP_NODES if n2["id"] != n1["id"]],
            key=lambda x: x[0],
        )
        for dist_m, n2 in distances[:K_NEIGHBORS]:
            base = dist_m / (PEDWAY_SPEED_M_S if n1["layer"] == LoopLayer.PEDWAY else WALK_SPEED_M_S)
            multiplier, hscore, warns = _hazard_cost_multiplier(n1, n2, active_reports)

            # Accessibility mode: hugely penalise edges near elevator outages or bridge lifts
            if accessibility:
                for report in active_reports:
                    mid_lat = (n1["lat"] + n2["lat"]) / 2
                    mid_lng = (n1["lng"] + n2["lng"]) / 2
                    dist = haversine_m(mid_lat, mid_lng, report["latitude"], report["longitude"])
                    if dist <= HAZARD_ROUTING_RADIUS_M and report.get("ai_category") in (
                        "accessibility", "bridge"
                    ):
                        multiplier = max(multiplier, 50.0)  # effectively block the edge

            cost = base * multiplier
            adj[n1["id"]].append((cost, n2, hscore, warns))

    # Dijkstra
    heap = [(0.0, origin["id"], [origin], 0.0, [])]
    visited = set()

    while heap:
        cost, node_id, path, h_score, all_warns = heapq.heappop(heap)
        if node_id in visited:
            continue
        visited.add(node_id)

        current = path[-1]
        if current["id"] == destination["id"]:
            return path, h_score, all_warns

        for edge_cost, neighbour, edge_h, edge_warns in adj.get(node_id, []):
            if neighbour["id"] not in visited:
                heapq.heappush(heap, (
                    cost + edge_cost,
                    neighbour["id"],
                    path + [neighbour],
                    max(h_score, edge_h),
                    all_warns + edge_warns,
                ))

    # No path found — return direct
    return [origin, destination], 0.0, ["⚠️ No safe route found — direct path used."]


# ── Public API ────────────────────────────────────────────────────────────────

async def compute_route(
    request: RouteRequest,
    active_reports: list[dict],
) -> RouteResponse:
    """
    Compute a hazard-adjusted route between two Loop points.

    active_reports should be a list of dicts with at least:
    latitude, longitude, severity, ai_category, summary_label
    """
    origin_node = _nearest_node(request.origin_lat, request.origin_lng)
    dest_node = _nearest_node(request.dest_lat, request.dest_lng)

    path_nodes, total_hazard_score, warnings = _dijkstra(
        origin_node, dest_node, active_reports, request.accessibility
    )

    # Build segments from consecutive node pairs
    segments: list[RouteSegment] = []
    for i in range(len(path_nodes) - 1):
        n1, n2 = path_nodes[i], path_nodes[i + 1]
        _, edge_h, edge_warns = _hazard_cost_multiplier(n1, n2, active_reports)
        dist = haversine_m(n1["lat"], n1["lng"], n2["lat"], n2["lng"])
        seg = RouteSegment(
            from_lat=n1["lat"],
            from_lng=n1["lng"],
            to_lat=n2["lat"],
            to_lng=n2["lng"],
            hazard_score=round(edge_h, 3),
            layer=n1["layer"],
            notes=list(set(edge_warns)),
        )
        segments.append(seg)

    # Total distance & estimated time
    total_dist = sum(
        haversine_m(s.from_lat, s.from_lng, s.to_lat, s.to_lng) for s in segments
    )
    estimated_minutes = total_dist / (WALK_SPEED_M_S * 60)

    # Deduplicate warnings
    unique_warnings = list(dict.fromkeys(warnings))

    if request.accessibility and any(
        r.get("ai_category") in ("accessibility", "bridge") for r in active_reports
    ):
        unique_warnings.insert(0, "♿ Some accessible routes are currently blocked — verify before travel.")

    return RouteResponse(
        segments=segments,
        total_hazard_score=round(total_hazard_score, 3),
        warnings=unique_warnings[:10],  # cap at 10 warnings
        estimated_minutes=round(estimated_minutes, 1),
    )
