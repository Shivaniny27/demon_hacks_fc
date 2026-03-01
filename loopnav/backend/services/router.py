"""
LoopNav Routing Engine — Phase 2
Pure Python A* with multi-layer cost functions, accessibility scoring,
isochrone generation, and Redis caching.
"""

import heapq
import json
import math
import hashlib
import time as time_mod
from datetime import datetime
from typing import Optional

import networkx as nx
import numpy as np
from scipy.spatial import KDTree
from shapely.geometry import MultiPoint
from shapely.ops import unary_union

try:
    import redis
    _redis = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    _redis.ping()
    REDIS_AVAILABLE = True
except Exception:
    _redis = None
    REDIS_AVAILABLE = False

# ── Constants ──────────────────────────────────────────────────────────────

WALK_SPEED = 1.4  # m/s
CONNECTOR_PENALTIES = {"stairs": 45, "elevator": 60, "escalator": 30}


# ── Helpers ────────────────────────────────────────────────────────────────

def haversine(lat1, lon1, lat2, lon2):
    """Distance in meters between two lat/lng points."""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _heuristic_seconds(lat1, lon1, lat2, lon2):
    """Haversine distance as walk time in seconds — A* admissible heuristic."""
    return haversine(lat1, lon1, lat2, lon2) / WALK_SPEED


# ── KDTree Node Snapper ───────────────────────────────────────────────────

class NodeSnapper:
    """Snaps arbitrary lat/lng to nearest graph node using KDTree."""

    def __init__(self, G):
        self.G = G
        self._nids = list(G.nodes)
        self._coords = np.array([
            [G.nodes[n]["lat"], G.nodes[n]["lng"]] for n in self._nids
        ])
        self._tree = KDTree(self._coords)

        # Per-level trees for targeted snapping
        self._level_trees = {}
        for level in ("street", "pedway", "mid"):
            mask = [i for i, n in enumerate(self._nids) if G.nodes[n]["level"] == level]
            if mask:
                nids = [self._nids[i] for i in mask]
                coords = self._coords[mask]
                self._level_trees[level] = (nids, coords, KDTree(coords))

    def snap(self, lat, lon, level=None):
        """Return nearest node ID. Optionally restrict to a specific level."""
        if level and level in self._level_trees:
            nids, coords, tree = self._level_trees[level]
            dist, idx = tree.query([lat, lon])
            return nids[idx]
        dist, idx = self._tree.query([lat, lon])
        return self._nids[idx]

    def snap_with_distance(self, lat, lon, level=None):
        """Return (node_id, distance_meters)."""
        nid = self.snap(lat, lon, level)
        nd = self.G.nodes[nid]
        dist = haversine(lat, lon, nd["lat"], nd["lng"])
        return nid, dist


# ── Cost Function ──────────────────────────────────────────────────────────

class CostFunction:
    """
    Configurable edge cost calculator with all modifiers:
    - Base walk time
    - Connector penalties (stairs/elevator/escalator)
    - Pedway hours enforcement
    - Elevator reliability penalty
    - Weather multipliers
    - Rush hour penalty near CTA stations
    - Mode bias (pedway_preferred, street_only, mid_preferred)
    """

    def __init__(self, G, mode="optimal", accessible=False, low_step=False,
                 weather=None, current_hour=None, depart_at=None,
                 elevator_alerts=None):
        self.G = G
        self.mode = mode
        self.accessible = accessible
        self.low_step = low_step

        # Weather: {"condition": "rain"|"heavy_snow"|"clear", "temp_f": 25}
        self.weather = weather or {"condition": "clear", "temp_f": 50}

        # Time of day
        if depart_at:
            self.hour = depart_at.hour
        elif current_hour is not None:
            self.hour = current_hour
        else:
            self.hour = datetime.now().hour

        # Set of elevator_ids that are currently out of service
        self.elevator_alerts = set(elevator_alerts or [])

        # Precompute CTA station node set for rush hour penalty
        self._cta_nodes = set()
        for n in G.nodes:
            if G.nodes[n].get("type") == "cta_station":
                self._cta_nodes.add(n)
                # Also add neighbors within 2 hops
                for neighbor in G.neighbors(n):
                    self._cta_nodes.add(neighbor)

    def edge_cost(self, u, v, key, edge_data):
        """
        Returns cost in seconds for traversing edge (u, v, key).
        Returns float('inf') to block the edge.
        """
        level = edge_data.get("level", "street")
        connector_type = edge_data.get("connector_type")
        is_connector = "->" in level

        # ── Mode blocking ──────────────────────────────────────────────
        if self.mode == "street_only":
            if is_connector:
                return float("inf")
            if level not in ("street",):
                return float("inf")

        # ── Accessibility blocking ─────────────────────────────────────
        if self.accessible:
            if connector_type == "stairs":
                return float("inf")
            if connector_type == "escalator":
                return float("inf")
            # Check elevator alerts
            elevator_id = edge_data.get("elevator_id")
            if connector_type == "elevator" and elevator_id in self.elevator_alerts:
                return float("inf")

        if self.low_step:
            if connector_type == "stairs":
                return float("inf")
            # Escalators OK in low-step mode

        # ── Elevator alert check (even for non-accessible mode) ────────
        if connector_type == "elevator":
            elevator_id = edge_data.get("elevator_id")
            if elevator_id in self.elevator_alerts:
                return float("inf")

        # ── Pedway hours enforcement ───────────────────────────────────
        open_hours = edge_data.get("open_hours")
        if open_hours and level == "pedway":
            if not self._is_within_hours(open_hours):
                return float("inf")

        # ── Base cost ──────────────────────────────────────────────────
        distance_m = edge_data.get("distance_m", 0)
        if isinstance(distance_m, str):
            distance_m = float(distance_m) if distance_m else 0
        base_time = distance_m / WALK_SPEED

        # ── Connector penalties ────────────────────────────────────────
        if connector_type:
            penalty = CONNECTOR_PENALTIES.get(connector_type, 0)
            base_time += penalty

        # ── Elevator reliability penalty ───────────────────────────────
        reliability = edge_data.get("reliability", 1.0)
        if isinstance(reliability, str):
            reliability = float(reliability) if reliability else 1.0
        if connector_type == "elevator" and reliability < 0.6:
            base_time += 120  # Unreliable elevator penalty

        # ── Weather multipliers (uncovered edges only) ─────────────────
        covered = edge_data.get("covered", False)
        if isinstance(covered, str):
            covered = covered.lower() == "true"
        sheltered = edge_data.get("sheltered", False)
        if isinstance(sheltered, str):
            sheltered = sheltered.lower() == "true"

        if not is_connector:
            weather_mult = self._weather_multiplier(covered, sheltered)
            base_time *= weather_mult

        # ── Rush hour penalty near CTA stations ────────────────────────
        if self._is_rush_hour() and (u in self._cta_nodes or v in self._cta_nodes):
            base_time *= 1.3

        # ── Mode bias ──────────────────────────────────────────────────
        if self.mode == "pedway_preferred" and level == "pedway":
            base_time *= 0.7
        elif self.mode == "mid_preferred" and level == "mid":
            base_time *= 0.75

        return base_time

    def _weather_multiplier(self, covered, sheltered):
        """Returns multiplier for weather impact on uncovered edges."""
        if covered:
            return 1.0

        condition = self.weather.get("condition", "clear")
        temp_f = self.weather.get("temp_f", 50)

        mult = 1.0

        if condition == "rain":
            mult = 1.8
        elif condition == "heavy_snow":
            mult = 2.0
        elif condition == "light_snow":
            mult = 1.5

        if temp_f <= 0:
            mult = max(mult, 2.0)
        elif temp_f <= 20:
            mult = max(mult, 1.5)

        if sheltered:
            mult = 1.0 + (mult - 1.0) * 0.3  # Sheltered reduces weather impact

        return mult

    def _is_rush_hour(self):
        return self.hour in (8, 17)  # 8-9AM or 5-6PM

    def _is_within_hours(self, open_hours_str):
        """Check if current hour is within 'HH:MM-HH:MM' range."""
        try:
            parts = open_hours_str.split("-")
            open_h = int(parts[0].split(":")[0])
            close_h = int(parts[1].split(":")[0])
            return open_h <= self.hour < close_h
        except (ValueError, IndexError):
            return True  # If parsing fails, assume open


# ── A* Router ──────────────────────────────────────────────────────────────

def astar(G, origin, destination, cost_fn):
    """
    Pure Python A* on MultiDiGraph.
    Returns (path_nodes, total_cost, edges_used) or (None, inf, []).
    """
    if origin == destination:
        return [origin], 0.0, []

    dest_node = G.nodes[destination]
    dest_lat, dest_lon = dest_node["lat"], dest_node["lng"]

    # Priority queue: (f_score, counter, node_id, g_value)
    counter = 0
    open_set = [(0, counter, origin, 0)]
    came_from = {}       # node -> (prev_node, edge_key, edge_data)
    g_score = {origin: 0}
    closed = set()

    while open_set:
        f, _, current, g_current = heapq.heappop(open_set)

        if current in closed:
            continue
        closed.add(current)

        if current == destination:
            # Reconstruct path
            path = [destination]
            edges = []
            node = destination
            while node in came_from:
                prev, ekey, edata = came_from[node]
                path.append(prev)
                edges.append({"from": prev, "to": node, "key": ekey, **edata})
                node = prev
            path.reverse()
            edges.reverse()
            return path, g_score[destination], edges

        cur_node = G.nodes[current]
        cur_lat, cur_lon = cur_node["lat"], cur_node["lng"]

        for _, neighbor, key, edge_data in G.edges(current, keys=True, data=True):
            cost = cost_fn.edge_cost(current, neighbor, key, edge_data)
            if cost == float("inf"):
                continue

            tentative_g = g_score[current] + cost

            if tentative_g < g_score.get(neighbor, float("inf")):
                g_score[neighbor] = tentative_g
                came_from[neighbor] = (current, key, dict(edge_data))

                n_node = G.nodes[neighbor]
                h = _heuristic_seconds(n_node["lat"], n_node["lng"], dest_lat, dest_lon)
                f_score = tentative_g + h

                counter += 1
                heapq.heappush(open_set, (f_score, counter, neighbor, tentative_g))

    return None, float("inf"), []


# ── Route Metrics ──────────────────────────────────────────────────────────

def _compute_route_metrics(G, path, edges):
    """Compute summary metrics for a route."""
    total_distance = 0
    total_time = 0
    covered_distance = 0
    level_changes = 0
    levels_used = set()
    prev_level = None
    elevator_ids = []

    for e in edges:
        dist = e.get("distance_m", 0)
        if isinstance(dist, str):
            dist = float(dist) if dist else 0
        t = e.get("time_s", 0)
        if isinstance(t, str):
            t = float(t) if t else 0
        total_distance += dist
        total_time += t

        covered = e.get("covered", False)
        if isinstance(covered, str):
            covered = covered.lower() == "true"
        if covered:
            covered_distance += dist

        level = e.get("level", "street")
        if "->" in level:
            level_changes += 1
            parts = level.split("->")
            levels_used.update(parts)
        else:
            levels_used.add(level)

        if e.get("elevator_id"):
            elevator_ids.append(e["elevator_id"])

    covered_pct = (covered_distance / total_distance * 100) if total_distance > 0 else 0

    return {
        "total_distance_m": round(total_distance, 1),
        "total_time_s": round(total_time, 1),
        "total_time_min": round(total_time / 60, 1),
        "covered_distance_m": round(covered_distance, 1),
        "covered_pct": round(covered_pct, 1),
        "level_changes": level_changes,
        "levels_used": sorted(levels_used),
        "elevator_ids": elevator_ids,
    }


# ── Accessibility Score ────────────────────────────────────────────────────

def _accessibility_score(G, path, edges):
    """
    Compute accessibility score 0-100 based on:
    - Surface quality
    - Elevator reliability
    - Number of connector types used
    - Covered percentage
    """
    if not edges:
        return 100

    score = 100.0
    total_dist = 0
    elevator_reliabilities = []

    for e in edges:
        dist = e.get("distance_m", 0)
        if isinstance(dist, str):
            dist = float(dist) if dist else 0
        total_dist += dist

        connector_type = e.get("connector_type")
        if connector_type == "stairs":
            score -= 15  # Stairs are bad for accessibility
        elif connector_type == "escalator":
            score -= 5

        reliability = e.get("reliability", 1.0)
        if isinstance(reliability, str):
            reliability = float(reliability) if reliability else 1.0
        if connector_type == "elevator":
            elevator_reliabilities.append(reliability)
            if reliability < 0.6:
                score -= 20
            elif reliability < 0.8:
                score -= 10

        surface = e.get("surface", "asphalt")
        if surface in ("gravel", "dirt", "cobblestone"):
            score -= 3

    # Bonus for covered routes
    covered_dist = sum(
        float(e.get("distance_m", 0)) if not isinstance(e.get("distance_m", 0), str)
        else (float(e["distance_m"]) if e.get("distance_m") else 0)
        for e in edges
        if (e.get("covered") is True or str(e.get("covered", "")).lower() == "true")
    )
    if total_dist > 0:
        covered_ratio = covered_dist / total_dist
        score += covered_ratio * 10  # Up to +10 for fully covered

    return max(0, min(100, round(score)))


# ── Turn-by-Turn Instructions ─────────────────────────────────────────────

def _generate_instructions(G, path, edges):
    """Generate step-by-step turn-by-turn instructions."""
    instructions = []

    for i, e in enumerate(edges):
        level = e.get("level", "street")
        connector_type = e.get("connector_type")
        dist = e.get("distance_m", 0)
        if isinstance(dist, str):
            dist = float(dist) if dist else 0

        from_node = G.nodes.get(e["from"], {})
        to_node = G.nodes.get(e["to"], {})

        if "->" in level:
            # Level change
            parts = level.split("->")
            if connector_type == "elevator":
                eid = e.get("elevator_id", "")
                instructions.append({
                    "type": "level_change",
                    "text": f"Take elevator from {parts[0]} to {parts[1]}",
                    "detail": f"Elevator {eid}" if eid else None,
                    "connector_type": "elevator",
                    "elevator_id": eid,
                    "reliability": e.get("reliability", 1.0),
                })
            elif connector_type == "escalator":
                direction = e.get("escalator_direction", "")
                instructions.append({
                    "type": "level_change",
                    "text": f"Take escalator {direction} from {parts[0]} to {parts[1]}",
                    "connector_type": "escalator",
                })
            elif connector_type == "stairs":
                instructions.append({
                    "type": "level_change",
                    "text": f"Take stairs from {parts[0]} to {parts[1]}",
                    "connector_type": "stairs",
                })
        else:
            # Walking segment
            building = to_node.get("building_name", "")
            if building:
                text = f"Walk {round(dist)}m through {building}"
            else:
                text = f"Walk {round(dist)}m along {level} level"

            # Merge short consecutive walking segments on same level
            if instructions and instructions[-1].get("type") == "walk" and instructions[-1].get("level") == level:
                prev_dist = instructions[-1].get("distance_m", 0)
                instructions[-1]["distance_m"] = round(prev_dist + dist)
                instructions[-1]["text"] = f"Walk {instructions[-1]['distance_m']}m along {level} level"
                continue

            instructions.append({
                "type": "walk",
                "text": text,
                "level": level,
                "distance_m": round(dist),
            })

    return instructions


# ── Path to GeoJSON ────────────────────────────────────────────────────────

def _path_to_geojson(G, path, edges):
    """Convert route path to GeoJSON for frontend rendering."""
    features = []
    current_segment = []
    current_level = None

    for node_id in path:
        nd = G.nodes[node_id]
        coord = [nd["lng"], nd["lat"]]

        # Determine level of this segment
        node_level = nd["level"]
        if current_level is None:
            current_level = node_level

        if node_level != current_level and current_segment:
            # Flush current segment
            features.append({
                "type": "Feature",
                "properties": {"level": current_level},
                "geometry": {"type": "LineString", "coordinates": list(current_segment)},
            })
            current_segment = [current_segment[-1]]  # Start new segment from last point
            current_level = node_level

        current_segment.append(coord)

    if len(current_segment) >= 2:
        features.append({
            "type": "Feature",
            "properties": {"level": current_level},
            "geometry": {"type": "LineString", "coordinates": current_segment},
        })

    # Add connector markers
    for e in edges:
        if e.get("connector_type"):
            to_nd = G.nodes.get(e["to"], {})
            features.append({
                "type": "Feature",
                "properties": {
                    "type": "connector",
                    "connector_type": e["connector_type"],
                    "elevator_id": e.get("elevator_id"),
                    "reliability": e.get("reliability"),
                    "level": e.get("level"),
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [to_nd.get("lng", 0), to_nd.get("lat", 0)],
                },
            })

    return {"type": "FeatureCollection", "features": features}


# ── Three Alternative Routes ───────────────────────────────────────────────

def compute_alternatives(G, snapper, origin_lat, origin_lon, dest_lat, dest_lon,
                         mode="optimal", accessible=False, low_step=False,
                         weather=None, current_hour=None, depart_at=None,
                         elevator_alerts=None):
    """
    Compute three alternative routes:
      1. Fastest (pure cost optimization)
      2. Most Covered (pedway_preferred bias)
      3. Fewest Level Changes (penalize connectors heavily)
    Deduplicates identical paths.
    """
    # Snap origin/destination — use street level for street_only mode
    if mode == "street_only":
        origin = snapper.snap(origin_lat, origin_lon, level="street")
        destination = snapper.snap(dest_lat, dest_lon, level="street")
    else:
        origin = snapper.snap(origin_lat, origin_lon)
        destination = snapper.snap(dest_lat, dest_lon)

    base_kwargs = dict(
        weather=weather,
        current_hour=current_hour,
        depart_at=depart_at,
        elevator_alerts=elevator_alerts,
    )

    results = []

    # Route 1: Fastest (requested mode)
    cf1 = CostFunction(G, mode=mode, accessible=accessible, low_step=low_step, **base_kwargs)
    path1, cost1, edges1 = astar(G, origin, destination, cf1)
    if path1:
        results.append(_build_route_result(G, "fastest", path1, cost1, edges1))

    # Route 2: Most Covered (force pedway_preferred unless street_only)
    if mode != "street_only":
        cf2 = CostFunction(G, mode="pedway_preferred", accessible=accessible, low_step=low_step, **base_kwargs)
        path2, cost2, edges2 = astar(G, origin, destination, cf2)
        if path2:
            results.append(_build_route_result(G, "most_covered", path2, cost2, edges2))

    # Route 3: Fewest Level Changes (heavy connector penalty)
    cf3 = CostFunctionFewestChanges(G, mode=mode, accessible=accessible, low_step=low_step, **base_kwargs)
    path3, cost3, edges3 = astar(G, origin, destination, cf3)
    if path3:
        results.append(_build_route_result(G, "fewest_changes", path3, cost3, edges3))

    # Deduplicate
    results = _deduplicate_routes(results)

    # If we have less than 2, add a street-only fallback
    if len(results) < 2 and mode != "street_only":
        cf_street = CostFunction(G, mode="street_only", accessible=accessible, low_step=low_step, **base_kwargs)
        path_s, cost_s, edges_s = astar(G, origin, destination, cf_street)
        if path_s:
            results.append(_build_route_result(G, "street_only", path_s, cost_s, edges_s))
            results = _deduplicate_routes(results)

    return results


def _build_route_result(G, label, path, cost, edges):
    """Package a single route into a result dict."""
    metrics = _compute_route_metrics(G, path, edges)
    return {
        "label": label,
        "path": path,
        "cost_s": round(cost, 1),
        "metrics": metrics,
        "accessibility_score": _accessibility_score(G, path, edges),
        "instructions": _generate_instructions(G, path, edges),
        "geojson": _path_to_geojson(G, path, edges),
    }


def _deduplicate_routes(routes):
    """Remove routes with identical paths."""
    seen = set()
    unique = []
    for r in routes:
        path_key = tuple(r["path"])
        if path_key not in seen:
            seen.add(path_key)
            unique.append(r)
    return unique


class CostFunctionFewestChanges(CostFunction):
    """Cost function variant that heavily penalizes level changes."""

    def edge_cost(self, u, v, key, edge_data):
        base = super().edge_cost(u, v, key, edge_data)
        if base == float("inf"):
            return base
        level = edge_data.get("level", "street")
        if "->" in level:
            base += 300  # 5 min penalty per level change
        return base


# ── Isochrone Generation ───────────────────────────────────────────────────

def compute_isochrone(G, snapper, origin_lat, origin_lon, minutes=10,
                      mode="optimal", accessible=False, weather=None,
                      current_hour=None, elevator_alerts=None):
    """
    Dijkstra from origin with time budget cutoff.
    Returns GeoJSON polygon of reachable area.
    Runs twice for optimal and street_only to show pedway advantage.
    """
    budget_s = minutes * 60

    def _reachable_polygon(cost_fn, label, start_node):
        # Dijkstra with cutoff
        visited = {}
        pq = [(0, start_node)]

        while pq:
            cost, node = heapq.heappop(pq)
            if node in visited:
                continue
            if cost > budget_s:
                break
            visited[node] = cost

            for _, neighbor, key, edge_data in G.edges(node, keys=True, data=True):
                if neighbor in visited:
                    continue
                edge_cost = cost_fn.edge_cost(node, neighbor, key, edge_data)
                if edge_cost == float("inf"):
                    continue
                new_cost = cost + edge_cost
                if new_cost <= budget_s:
                    heapq.heappush(pq, (new_cost, neighbor))

        if len(visited) < 3:
            return None

        points = []
        for n in visited:
            nd = G.nodes[n]
            points.append((nd["lng"], nd["lat"]))

        try:
            mp = MultiPoint(points)
            hull = mp.convex_hull
            if hull.geom_type == "Point" or hull.geom_type == "LineString":
                return None
            coords = list(hull.exterior.coords)
            return {
                "type": "Feature",
                "properties": {
                    "label": label,
                    "minutes": minutes,
                    "nodes_reached": len(visited),
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[c[0], c[1]] for c in coords]],
                },
            }
        except Exception:
            return None

    base_kwargs = dict(weather=weather, current_hour=current_hour, elevator_alerts=elevator_alerts)

    # Optimal isochrone (snap to any level)
    origin_opt = snapper.snap(origin_lat, origin_lon)
    cf_optimal = CostFunction(G, mode=mode, accessible=accessible, **base_kwargs)
    poly_optimal = _reachable_polygon(cf_optimal, "optimal", origin_opt)

    # Street-only isochrone (snap to street level)
    origin_street = snapper.snap(origin_lat, origin_lon, level="street")
    cf_street = CostFunction(G, mode="street_only", accessible=accessible, **base_kwargs)
    poly_street = _reachable_polygon(cf_street, "street_only", origin_street)

    features = []
    if poly_street:
        features.append(poly_street)
    if poly_optimal:
        features.append(poly_optimal)

    # Compute pedway advantage
    advantage = None
    if poly_optimal and poly_street:
        opt_nodes = poly_optimal["properties"]["nodes_reached"]
        str_nodes = poly_street["properties"]["nodes_reached"]
        if str_nodes > 0:
            advantage = round((opt_nodes - str_nodes) / str_nodes * 100, 1)

    return {
        "type": "FeatureCollection",
        "features": features,
        "pedway_advantage_pct": advantage,
    }


# ── Redis Caching ──────────────────────────────────────────────────────────

def _cache_key(origin_lat, origin_lon, dest_lat, dest_lon, mode, accessible, weather_condition, hour):
    """Generate deterministic cache key."""
    raw = f"{origin_lat:.5f},{origin_lon:.5f}|{dest_lat:.5f},{dest_lon:.5f}|{mode}|{accessible}|{weather_condition}|{hour}"
    return f"loopnav:route:{hashlib.md5(raw.encode()).hexdigest()}"


def cached_route(origin_lat, origin_lon, dest_lat, dest_lon, mode, accessible,
                 weather_condition="clear", hour=12):
    """Try to get route from Redis cache."""
    if not REDIS_AVAILABLE:
        return None
    try:
        key = _cache_key(origin_lat, origin_lon, dest_lat, dest_lon, mode, accessible, weather_condition, hour)
        data = _redis.get(key)
        if data:
            return json.loads(data)
    except Exception:
        pass
    return None


def cache_route(origin_lat, origin_lon, dest_lat, dest_lon, mode, accessible,
                weather_condition, hour, result):
    """Store route result in Redis with 180s TTL."""
    if not REDIS_AVAILABLE:
        return
    try:
        key = _cache_key(origin_lat, origin_lon, dest_lat, dest_lon, mode, accessible, weather_condition, hour)
        # Strip path node lists from cache (too large, only needed for dedup)
        cacheable = []
        for r in result:
            c = dict(r)
            c.pop("path", None)
            cacheable.append(c)
        _redis.setex(key, 180, json.dumps(cacheable))
    except Exception:
        pass


# ── Public API ─────────────────────────────────────────────────────────────

def route(G, snapper, origin_lat, origin_lon, dest_lat, dest_lon,
          mode="optimal", accessible=False, low_step=False,
          weather=None, current_hour=None, depart_at=None,
          elevator_alerts=None):
    """
    Main routing entry point.
    Returns list of alternative routes with metrics, instructions, GeoJSON.
    Uses Redis cache when available.
    """
    weather = weather or {"condition": "clear", "temp_f": 50}
    hour = current_hour if current_hour is not None else datetime.now().hour

    # Check cache
    cached = cached_route(origin_lat, origin_lon, dest_lat, dest_lon,
                          mode, accessible, weather.get("condition", "clear"), hour)
    if cached:
        return cached

    t0 = time_mod.time()

    # Compute routes
    results = compute_alternatives(
        G, snapper, origin_lat, origin_lon, dest_lat, dest_lon,
        mode=mode, accessible=accessible, low_step=low_step,
        weather=weather, current_hour=hour, depart_at=depart_at,
        elevator_alerts=elevator_alerts,
    )

    elapsed_ms = round((time_mod.time() - t0) * 1000)

    # Cache result
    cache_route(origin_lat, origin_lon, dest_lat, dest_lon,
                mode, accessible, weather.get("condition", "clear"), hour, results)

    return {
        "routes": results,
        "routing_time_ms": elapsed_ms,
        "origin_snapped": snapper.snap(origin_lat, origin_lon, level="street" if mode == "street_only" else None),
        "destination_snapped": snapper.snap(dest_lat, dest_lon, level="street" if mode == "street_only" else None),
        "mode": mode,
        "accessible": accessible,
        "weather": weather,
    }
