"""
LoopNav Stub Routing Engine.

Dev2 uses these functions while Dev1 builds the real A* engine.
All functions match the exact same signature as the real engine
so swapping is a one-line change in nav_route.py.
"""

import networkx as nx
from math import radians, sin, cos, sqrt, atan2
from app.models.nav_schemas import RouteSegment, SingleRoute


# ── Utilities ─────────────────────────────────────────────────────────────────

def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371000
    φ1, φ2 = radians(lat1), radians(lat2)
    dφ = radians(lat2 - lat1)
    dλ = radians(lng2 - lng1)
    a = sin(dφ / 2) ** 2 + cos(φ1) * cos(φ2) * sin(dλ / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def snap_to_graph(lat: float, lng: float, G: nx.MultiDiGraph) -> str:
    """
    Return the nearest node ID in the graph to the given coordinate.
    Real engine uses SciPy KDTree. Stub uses linear scan — fine for small stub.
    """
    nodes = list(G.nodes(data=True))
    if not nodes:
        raise ValueError("ORIGIN_OUT_OF_BOUNDS: Graph has no nodes")

    nearest_id, nearest_data = min(
        nodes,
        key=lambda n: haversine_m(lat, lng, n[1].get("lat", 0), n[1].get("lng", 0))
    )

    # Reject if too far (> 500m from any node — outside Loop)
    dist = haversine_m(lat, lng, nearest_data.get("lat", 0), nearest_data.get("lng", 0))
    if dist > 500:
        raise ValueError(f"ORIGIN_OUT_OF_BOUNDS: Nearest node is {dist:.0f}m away")

    return nearest_id


def _make_segment(G: nx.MultiDiGraph, path: list[str], level_filter: str | None = None) -> list[RouteSegment]:
    """Build RouteSegment list from an ordered list of node IDs."""
    segments = []
    i = 0
    while i < len(path) - 1:
        u, v = path[i], path[i + 1]
        u_data = G.nodes[u]
        v_data = G.nodes[v]

        # Get edge data (first parallel edge)
        edge_data = dict(list(G[u][v].values())[0])

        level = edge_data.get("level", u_data.get("level", "street"))
        connector_type = edge_data.get("connector_type")

        # Build human-readable instruction
        if connector_type == "elevator":
            instruction = f"Take elevator ({u_data.get('building', '')} → {v_data.get('building', '')})"
        elif connector_type == "escalator":
            instruction = f"Take escalator to {level}"
        elif connector_type == "stairs":
            instruction = f"Take stairs to {level}"
        elif level == "pedway":
            instruction = f"Walk underground through pedway"
        elif level == "mid":
            instruction = f"Walk through Lower Wacker Drive"
        else:
            instruction = f"Walk along street-level path"

        segments.append(RouteSegment(
            level=level,
            geometry=[[u_data.get("lng", 0), u_data.get("lat", 0)],
                       [v_data.get("lng", 0), v_data.get("lat", 0)]],
            time_s=edge_data.get("time_s", 60),
            distance_m=edge_data.get("distance_m", 0.0),
            covered=edge_data.get("covered", False),
            instruction=instruction,
            connector_type=connector_type,
            elevator_id=edge_data.get("elevator_id"),
        ))
        i += 1

    return segments


def _build_single_route(segments: list[RouteSegment], label: str) -> SingleRoute:
    total_time = sum(s.time_s for s in segments)
    total_dist = sum(s.distance_m for s in segments)

    covered_dist = sum(s.distance_m for s in segments if s.covered)
    covered_pct = round((covered_dist / total_dist * 100) if total_dist > 0 else 0, 1)

    level_changes = sum(
        1 for i in range(1, len(segments))
        if segments[i].connector_type is not None
    )

    # Build summary string like "Street → Pedway → Street"
    level_labels = {"street": "Street", "mid": "Lower Wacker", "pedway": "Pedway"}
    seen_levels = []
    for s in segments:
        label_str = level_labels.get(s.level, s.level.title())
        if not seen_levels or seen_levels[-1] != label_str:
            seen_levels.append(label_str)
    route_summary = " → ".join(seen_levels)

    # Accessibility score (0–100)
    # Penalise: stairs, unreliable elevators, uncovered segments
    score = 100
    for s in segments:
        if s.connector_type == "stairs":
            score -= 30
        elif s.connector_type == "escalator":
            score -= 10
        if not s.covered and s.distance_m > 50:
            score -= 5
    score = max(0, score)

    return SingleRoute(
        segments=segments,
        total_time_s=total_time,
        total_distance_m=round(total_dist, 1),
        covered_pct=covered_pct,
        level_changes=level_changes,
        route_summary=route_summary,
        accessibility_score=score,
        label=label,
    )


def get_route_alternatives(
    origin_id: str,
    dest_id: str,
    G: nx.MultiDiGraph,
    weather: dict,
    hour: int,
    accessible: bool,
) -> dict[str, SingleRoute]:
    """
    Returns three route alternatives: fastest, most_covered, fewest_changes.
    Stub uses NetworkX shortest path with simple weight functions.
    Real engine uses custom A* with full cost modifiers.
    """

    def weight_fastest(u, v, data):
        d = dict(list(data.values())[0]) if isinstance(data, dict) and 0 in data else data
        base = d.get("time_s", 60)
        if accessible and d.get("connector_type") in ("stairs", "escalator"):
            return float("inf")
        # Penalise unreliable elevators
        rel = d.get("reliability", 1.0)
        if rel < 0.6:
            base += 120
        # Weather: penalise uncovered edges when raining/cold
        impact = weather.get("impact", "none")
        if impact in ("high", "medium") and not d.get("covered"):
            base *= 1.8 if impact == "high" else 1.4
        return base

    def weight_covered(u, v, data):
        d = dict(list(data.values())[0]) if isinstance(data, dict) and 0 in data else data
        base = d.get("time_s", 60)
        if accessible and d.get("connector_type") in ("stairs", "escalator"):
            return float("inf")
        if not d.get("covered"):
            base *= 3.0  # strongly prefer covered
        return base

    def weight_fewest_changes(u, v, data):
        d = dict(list(data.values())[0]) if isinstance(data, dict) and 0 in data else data
        base = d.get("time_s", 60)
        if accessible and d.get("connector_type") in ("stairs", "escalator"):
            return float("inf")
        if d.get("connector_type"):
            base += 500  # heavily penalise level changes
        return base

    results = {}
    configs = [
        ("fastest",        weight_fastest,        "Fastest"),
        ("most_covered",   weight_covered,        "Most Covered"),
        ("fewest_changes", weight_fewest_changes, "Fewest Changes"),
    ]

    for key, weight_fn, label in configs:
        try:
            path = nx.shortest_path(G, origin_id, dest_id, weight=weight_fn)
            segs = _make_segment(G, path)
            results[key] = _build_single_route(segs, label)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            # If no path found for this mode, fall back to fastest
            if "fastest" in results:
                results[key] = results["fastest"]
            else:
                raise ValueError("NO_ROUTE_FOUND")

    # Deduplicate: if routes are identical, collapse to fastest
    if results.get("most_covered") and results["most_covered"].route_summary == results["fastest"].route_summary:
        results["most_covered"] = results["fastest"]
    if results.get("fewest_changes") and results["fewest_changes"].route_summary == results["fastest"].route_summary:
        results["fewest_changes"] = results["fastest"]

    return results


def get_isochrone(
    lat: float,
    lng: float,
    minutes: int,
    mode: str,
    G: nx.MultiDiGraph,
    weather: dict,
) -> dict | None:
    """
    Returns a GeoJSON Polygon representing the reachable area from lat/lng
    within the given time budget. Stub returns a simple bounding box.
    Real engine uses Dijkstra with time cutoff + Shapely convex hull.
    """
    try:
        origin_id = snap_to_graph(lat, lng, G)
    except ValueError:
        return None

    # Time budget in seconds
    budget_s = minutes * 60

    # Simple approximation: walking ~1.4 m/s for budget seconds
    # 1 degree lat ≈ 111,000m, 1 degree lng ≈ 85,000m at Chicago latitude
    radius_m = 1.4 * budget_s
    if mode == "pedway_preferred":
        radius_m *= 1.2  # pedway bonus
    elif mode == "street_only":
        radius_m *= 0.85  # street penalty

    d_lat = radius_m / 111000
    d_lng = radius_m / 85000

    # Return as GeoJSON Polygon (simple box — stub only)
    return {
        "type": "Polygon",
        "coordinates": [[
            [lng - d_lng, lat - d_lat],
            [lng + d_lng, lat - d_lat],
            [lng + d_lng, lat + d_lat],
            [lng - d_lng, lat + d_lat],
            [lng - d_lng, lat - d_lat],
        ]]
    }
