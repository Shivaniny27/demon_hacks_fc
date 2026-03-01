"""
LoopNav — Production Graph Loader Service.

Responsible for building, validating, modifying, and hot-reloading the
NetworkX MultiDiGraph that powers all LoopNav routing computations.

Architecture
────────────
  GraphLoader         — main class, attached to app.state on startup
  StubGraphLoader     — loads 16-node dev stub (USE_STUB_GRAPH=true)
  RealGraphLoader     — loads real graph from Dev 1's JSON export
  EdgeModifier        — applies live outages (elevators, pedway closures) to edge costs
  GraphValidator      — structural validation (connectivity, bounds, required attrs)
  GraphStats          — statistics aggregator
  HotReloadWatcher    — async file-watcher for live graph updates

Graph schema (per node):
  id:       str         — unique node ID
  lat:      float       — WGS-84 latitude
  lng:      float       — WGS-84 longitude
  level:    str         — "street" | "mid" | "pedway"
  name:     str         — human-readable label
  building: str         — building or POI name

Graph schema (per edge):
  distance_m:        float     — Euclidean distance in metres
  time_s:            float     — base traversal time in seconds
  level:             str       — "street" | "mid" | "pedway" | "connector"
  covered:           bool      — sheltered from weather
  sheltered:         bool      — has wind protection
  accessible:        bool      — wheelchair / mobility device compatible
  connector_type:    str|None  — "elevator" | "escalator" | "stairs" | None
  elevator_id:       str|None  — matches ElevatorReliabilityTracker IDs
  reliability:       float     — 0.0–1.0, elevator availability estimate
  open_hours:        str|None  — e.g. "06:00-22:00" (pedway only)
  outage:            bool      — set by EdgeModifier when live outage detected
  outage_multiplier: float     — cost inflation for outaged edges

Dev 1 JSON export format:
  {
    "nodes": [{"id": "...", "lat": ..., "lng": ..., "level": "...", ...}, ...],
    "edges": [{"u": "...", "v": "...", "key": 0, "data": {...}}, ...]
  }
  OR standard NetworkX JSON: graph.node_link_data() format.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

log = logging.getLogger("loopnav.graph")

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# Graph file locations
DEFAULT_GRAPH_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "graph.json"
)
DEFAULT_GRAPH_PATH = os.path.abspath(DEFAULT_GRAPH_PATH)

# Chicago Loop bounds (WGS-84) — used for node bounds validation
LOOP_BOUNDS = {
    "min_lat": 41.85, "max_lat": 41.92,
    "min_lon": -87.70, "max_lon": -87.60,
}

# Outage cost multipliers
OUTAGE_COST_INF   = 999_999.0   # effectively infinite — accessible mode
OUTAGE_COST_HIGH  = 200.0       # non-accessible: very expensive but not blocked

# Pedway hours
PEDWAY_OPEN_HOUR  = 6
PEDWAY_CLOSE_HOUR = 22

# Hot-reload polling interval (seconds)
HOT_RELOAD_INTERVAL = 10.0

# Required node attributes
REQUIRED_NODE_ATTRS = {"lat", "lng", "level"}

# Required edge attributes
REQUIRED_EDGE_ATTRS = {"distance_m", "time_s", "level"}

# Valid levels
VALID_LEVELS = {"street", "mid", "pedway"}


# ─────────────────────────────────────────────────────────────────────────────
# Haversine utility (no external dependency)
# ─────────────────────────────────────────────────────────────────────────────

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ  = math.radians(lat2 - lat1)
    dλ  = math.radians(lon2 - lon1)
    a   = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class NodeStats:
    level:  str
    count:  int
    bounds: Optional[Dict[str, float]]


@dataclass
class EdgeStats:
    level:         str
    count:         int
    covered_count: int
    total_dist_m:  float
    avg_time_s:    float


@dataclass
class GraphStats:
    """Statistical summary of the loaded graph."""
    total_nodes:        int
    total_edges:        int
    node_by_level:      Dict[str, int]
    edge_by_level:      Dict[str, int]
    covered_edges:      int
    accessible_edges:   int
    connector_edges:    int
    elevator_edges:     int
    total_distance_m:   float
    avg_edge_time_s:    float
    connected_components: int
    is_strongly_connected: bool
    bounds:             Optional[Dict[str, float]]
    graph_source:       str   # "stub" | "real" | "json"
    loaded_at:          str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "total_nodes":         self.total_nodes,
            "total_edges":         self.total_edges,
            "node_by_level":       self.node_by_level,
            "edge_by_level":       self.edge_by_level,
            "covered_edges":       self.covered_edges,
            "accessible_edges":    self.accessible_edges,
            "connector_edges":     self.connector_edges,
            "elevator_edges":      self.elevator_edges,
            "total_distance_m":    round(self.total_distance_m, 1),
            "avg_edge_time_s":     round(self.avg_edge_time_s, 2),
            "connected_components": self.connected_components,
            "is_strongly_connected": self.is_strongly_connected,
            "bounds":              self.bounds,
            "graph_source":        self.graph_source,
            "loaded_at":           self.loaded_at,
        }


@dataclass
class ValidationResult:
    """Result of graph structural validation."""
    valid:               bool
    errors:              List[str]
    warnings:            List[str]
    nodes_checked:       int
    edges_checked:       int
    nodes_out_of_bounds: int
    missing_attrs_nodes: int
    missing_attrs_edges: int
    isolated_nodes:      int
    dead_end_nodes:      int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "valid":                self.valid,
            "errors":               self.errors,
            "warnings":             self.warnings,
            "nodes_checked":        self.nodes_checked,
            "edges_checked":        self.edges_checked,
            "nodes_out_of_bounds":  self.nodes_out_of_bounds,
            "missing_attrs_nodes":  self.missing_attrs_nodes,
            "missing_attrs_edges":  self.missing_attrs_edges,
            "isolated_nodes":       self.isolated_nodes,
            "dead_end_nodes":       self.dead_end_nodes,
        }


@dataclass
class EdgeModification:
    """Record of a single edge modification (for rollback / audit)."""
    u:           str
    v:           str
    k:           int
    attr:        str
    old_value:   Any
    new_value:   Any
    reason:      str
    modified_at: float = field(default_factory=time.monotonic)


# ─────────────────────────────────────────────────────────────────────────────
# Graph Validator
# ─────────────────────────────────────────────────────────────────────────────

class GraphValidator:
    """
    Validates structural integrity of the NavigX MultiDiGraph.

    Checks:
      1. All nodes have required attributes (lat, lng, level)
      2. Node coordinates within Chicago Loop bounds
      3. All edges have required attributes
      4. No isolated nodes (no edges at all)
      5. Graph has at least one node per valid level
      6. Connected components check
    """

    def validate(self, G) -> ValidationResult:
        import networkx as nx

        errors:   List[str] = []
        warnings: List[str] = []

        nodes_oob    = 0
        miss_n_attrs = 0
        miss_e_attrs = 0
        isolated     = 0
        dead_ends    = 0

        # Node checks
        for node_id, data in G.nodes(data=True):
            # Missing attrs
            missing = REQUIRED_NODE_ATTRS - set(data.keys())
            if missing:
                miss_n_attrs += 1
                warnings.append(f"Node '{node_id}' missing attrs: {missing}")

            # Bounds
            lat = data.get("lat", 0)
            lon = data.get("lng", 0)
            if not (LOOP_BOUNDS["min_lat"] <= lat <= LOOP_BOUNDS["max_lat"]):
                nodes_oob += 1
                warnings.append(f"Node '{node_id}' lat={lat} out of Loop bounds")
            if not (LOOP_BOUNDS["min_lon"] <= lon <= LOOP_BOUNDS["max_lon"]):
                nodes_oob += 1

            # Level
            lvl = data.get("level", "")
            if lvl not in VALID_LEVELS:
                warnings.append(f"Node '{node_id}' unknown level '{lvl}'")

            # Isolation
            if G.degree(node_id) == 0:
                isolated += 1
                warnings.append(f"Node '{node_id}' is isolated (no edges)")

            # Dead-ends (single edge only)
            if G.degree(node_id) == 1:
                dead_ends += 1

        # Edge checks
        for u, v, k, data in G.edges(keys=True, data=True):
            missing = REQUIRED_EDGE_ATTRS - set(data.keys())
            if missing:
                miss_e_attrs += 1
                warnings.append(f"Edge ({u}→{v}) missing attrs: {missing}")

            # Sanity: time_s > 0
            t = data.get("time_s", 0)
            if t <= 0:
                warnings.append(f"Edge ({u}→{v}) has time_s={t} ≤ 0")

            # Level
            lvl = data.get("level", "")
            if lvl not in VALID_LEVELS | {"connector"}:
                warnings.append(f"Edge ({u}→{v}) unknown level '{lvl}'")

        # Connectivity
        try:
            components = nx.number_weakly_connected_components(G)
        except Exception:
            components = -1

        is_strongly = False
        try:
            is_strongly = nx.is_strongly_connected(G)
        except Exception:
            pass

        # Level coverage
        levels_present = set(data.get("level") for _, data in G.nodes(data=True))
        for lvl in VALID_LEVELS:
            if lvl not in levels_present:
                warnings.append(f"No nodes found for level '{lvl}'")

        # Errors: no nodes
        if G.number_of_nodes() == 0:
            errors.append("Graph has no nodes")
        if G.number_of_edges() == 0:
            errors.append("Graph has no edges")
        if components > 3:
            warnings.append(f"Graph has {components} weakly connected components — may have isolated sub-graphs")

        return ValidationResult(
            valid                = len(errors) == 0,
            errors               = errors,
            warnings             = warnings,
            nodes_checked        = G.number_of_nodes(),
            edges_checked        = G.number_of_edges(),
            nodes_out_of_bounds  = nodes_oob,
            missing_attrs_nodes  = miss_n_attrs,
            missing_attrs_edges  = miss_e_attrs,
            isolated_nodes       = isolated,
            dead_end_nodes       = dead_ends,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Graph Stats collector
# ─────────────────────────────────────────────────────────────────────────────

class GraphStatsCollector:
    """Aggregates statistics from a loaded graph."""

    def collect(self, G, source: str) -> GraphStats:
        import networkx as nx

        node_by_level: Dict[str, int] = {}
        all_lats, all_lons = [], []

        for node_id, data in G.nodes(data=True):
            lvl = data.get("level", "unknown")
            node_by_level[lvl] = node_by_level.get(lvl, 0) + 1
            lat = data.get("lat")
            lon = data.get("lng")
            if lat is not None:
                all_lats.append(lat)
            if lon is not None:
                all_lons.append(lon)

        edge_by_level:   Dict[str, int] = {}
        covered_cnt      = 0
        accessible_cnt   = 0
        connector_cnt    = 0
        elevator_cnt     = 0
        total_dist       = 0.0
        total_time       = 0.0
        edge_count       = 0

        for u, v, k, data in G.edges(keys=True, data=True):
            lvl = data.get("level", "unknown")
            edge_by_level[lvl] = edge_by_level.get(lvl, 0) + 1
            if data.get("covered"):
                covered_cnt += 1
            if data.get("accessible", True):
                accessible_cnt += 1
            if data.get("connector_type"):
                connector_cnt += 1
            if data.get("connector_type") == "elevator":
                elevator_cnt += 1
            total_dist += data.get("distance_m", 0)
            total_time += data.get("time_s", 0)
            edge_count += 1

        avg_time = total_time / edge_count if edge_count > 0 else 0

        try:
            components = nx.number_weakly_connected_components(G)
        except Exception:
            components = -1

        try:
            is_sc = nx.is_strongly_connected(G)
        except Exception:
            is_sc = False

        bounds = None
        if all_lats and all_lons:
            bounds = {
                "min_lat": min(all_lats), "max_lat": max(all_lats),
                "min_lon": min(all_lons), "max_lon": max(all_lons),
            }

        return GraphStats(
            total_nodes          = G.number_of_nodes(),
            total_edges          = edge_count,
            node_by_level        = node_by_level,
            edge_by_level        = edge_by_level,
            covered_edges        = covered_cnt,
            accessible_edges     = accessible_cnt,
            connector_edges      = connector_cnt,
            elevator_edges       = elevator_cnt,
            total_distance_m     = total_dist,
            avg_edge_time_s      = avg_time,
            connected_components = components,
            is_strongly_connected= is_sc,
            bounds               = bounds,
            graph_source         = source,
            loaded_at            = datetime.now(timezone.utc).isoformat(),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Stub Graph Loader
# ─────────────────────────────────────────────────────────────────────────────

class StubGraphLoader:
    """
    Loads the 16-node, 40-edge stub graph from app.graph.stub.
    Used when USE_STUB_GRAPH=true (dev/hackathon mode).
    """

    def load(self):
        from app.graph.stub import build_stub_graph
        G      = build_stub_graph()
        log.info("Stub graph loaded: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())
        return G

    def is_available(self) -> bool:
        try:
            from app.graph import stub  # noqa
            return True
        except ImportError:
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Real Graph Loader (Dev 1 JSON format)
# ─────────────────────────────────────────────────────────────────────────────

class RealGraphLoader:
    """
    Loads Dev 1's real graph from a JSON file.

    Supported formats:
      1. LoopNav native: {"nodes": [...], "edges": [...]}
      2. NetworkX node_link_data: {"directed": true, "multigraph": true, "nodes": [...], "links": [...]}
    """

    def __init__(self, path: str = DEFAULT_GRAPH_PATH):
        self._path = path

    def is_available(self) -> bool:
        return os.path.exists(self._path)

    def load(self):
        import networkx as nx

        if not os.path.exists(self._path):
            raise FileNotFoundError(f"Graph file not found: {self._path}")

        with open(self._path, encoding="utf-8") as f:
            data = json.load(f)

        # Detect format
        if "links" in data and "nodes" in data and "directed" in data:
            # NetworkX node_link format
            G = nx.node_link_graph(data, multigraph=True, directed=True)
            log.info("Loaded NetworkX node_link graph: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())
            return G

        if "nodes" in data and "edges" in data:
            return self._load_native(data)

        raise ValueError("Unknown graph JSON format. Expected 'nodes'+'edges' or NetworkX node_link format.")

    def _load_native(self, data: Dict):
        import networkx as nx

        G = nx.MultiDiGraph()

        # Add nodes
        for n in data["nodes"]:
            node_id = n.pop("id", None)
            if node_id is None:
                log.warning("Node missing 'id' field — skipping")
                continue
            G.add_node(node_id, **n)

        # Add edges
        for e in data["edges"]:
            u      = e.get("u") or e.get("source")
            v      = e.get("v") or e.get("target")
            key    = e.get("key", 0)
            e_data = e.get("data", {})

            if u not in G.nodes or v not in G.nodes:
                log.debug("Edge (%s→%s) references unknown node — skipping", u, v)
                continue

            G.add_edge(u, v, key=key, **e_data)

        log.info("Loaded native graph: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())
        return G

    def get_mtime(self) -> Optional[float]:
        try:
            return os.stat(self._path).st_mtime
        except Exception:
            return None


# ─────────────────────────────────────────────────────────────────────────────
# Edge Modifier
# ─────────────────────────────────────────────────────────────────────────────

class EdgeModifier:
    """
    Applies live operational data (outages, closures, weather) to graph edge costs.

    All modifications are tracked for rollback. The modifier never permanently
    changes base edge data — it only adds/updates overlay attributes:
      - "outage": bool
      - "outage_multiplier": float
      - "pedway_closed": bool
      - "weather_multiplier": float (applied at routing time, not stored)

    Usage:
        modifier = EdgeModifier(G)
        modifier.apply_elevator_outages(outaged_ids)
        modifier.apply_pedway_closures(closed_segment_ids)
        # routes are now computed with inflated costs for outaged equipment
        modifier.rollback_all()  # restore original costs
    """

    def __init__(self, G):
        self._G           = G
        self._mods:       List[EdgeModification] = []
        self._outaged:    Set[str]               = set()
        self._pw_closed:  Set[str]               = set()

    def apply_elevator_outages(
        self,
        outaged_elevator_ids: Set[str],
        accessible_mode:      bool = False,
    ) -> int:
        """
        Mark edges whose elevator_id is in the outage set.
        accessible_mode=True uses OUTAGE_COST_INF (route will avoid completely).
        Returns number of edges modified.
        """
        new_outages = outaged_elevator_ids - self._outaged
        cleared     = self._outaged - outaged_elevator_ids

        modified = 0

        # Apply new outages
        for u, v, k, data in self._G.edges(keys=True, data=True):
            eid = data.get("elevator_id")
            if not eid:
                continue

            if eid in new_outages:
                mult = OUTAGE_COST_INF if accessible_mode else OUTAGE_COST_HIGH
                self._mods.append(EdgeModification(
                    u=u, v=v, k=k, attr="outage",
                    old_value=data.get("outage", False),
                    new_value=True,
                    reason=f"elevator_outage:{eid}",
                ))
                self._G[u][v][k]["outage"]             = True
                self._G[u][v][k]["outage_multiplier"]  = mult
                modified += 1

            elif eid in cleared and data.get("outage"):
                self._G[u][v][k]["outage"]             = False
                self._G[u][v][k]["outage_multiplier"]  = 1.0
                modified += 1

        self._outaged = outaged_elevator_ids
        if modified:
            log.info("EdgeModifier: %d edges modified for elevator outages", modified)
        return modified

    def apply_pedway_closures(self, closed_segment_ids: Set[str]) -> int:
        """
        Mark pedway edges in closed_segment_ids as closed.
        Routing engine treats closed pedway edges as impassable.
        """
        new_closed = closed_segment_ids - self._pw_closed
        cleared    = self._pw_closed - closed_segment_ids
        modified   = 0

        for u, v, k, data in self._G.edges(keys=True, data=True):
            lvl   = data.get("level", "")
            seg_id = data.get("pedway_segment_id", "")
            if lvl != "pedway":
                continue

            if seg_id in new_closed:
                self._G[u][v][k]["pedway_closed"] = True
                modified += 1
            elif seg_id in cleared:
                self._G[u][v][k]["pedway_closed"] = False
                modified += 1

        self._pw_closed = closed_segment_ids
        return modified

    def apply_pedway_hours(self) -> int:
        """
        Open/close all pedway edges based on current UTC time.
        Pedway operates 06:00–22:00 Chicago time (approximate via UTC-6).
        Returns number of edges modified.
        """
        now_hour  = datetime.now(timezone.utc).hour - 6  # rough CST offset
        if now_hour < 0:
            now_hour += 24
        is_open   = PEDWAY_OPEN_HOUR <= now_hour < PEDWAY_CLOSE_HOUR
        modified  = 0

        for u, v, k, data in self._G.edges(keys=True, data=True):
            if data.get("level") != "pedway":
                continue
            if data.get("connector_type"):   # connectors use own hours
                continue
            current_closed = data.get("pedway_closed", False)
            should_close   = not is_open
            if current_closed != should_close:
                self._G[u][v][k]["pedway_closed"] = should_close
                modified += 1

        if modified:
            status = "CLOSED" if not is_open else "OPEN"
            log.info("Pedway hours: %s at hour %d CST — %d edges updated", status, now_hour, modified)
        return modified

    def rollback_all(self) -> int:
        """Restore all modified edge attributes to original values."""
        rolled = 0
        for mod in reversed(self._mods):
            try:
                self._G[mod.u][mod.v][mod.k][mod.attr] = mod.old_value
                rolled += 1
            except Exception:
                pass
        self._mods.clear()
        self._outaged.clear()
        self._pw_closed.clear()
        return rolled

    def get_modification_summary(self) -> Dict[str, Any]:
        return {
            "total_modifications": len(self._mods),
            "outaged_elevators":   list(self._outaged),
            "closed_pedway_segs":  list(self._pw_closed),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Hot-Reload Watcher
# ─────────────────────────────────────────────────────────────────────────────

class HotReloadWatcher:
    """
    Async task that polls the graph JSON file for changes.
    When a change is detected, calls the provided reload callback.

    Usage:
        watcher = HotReloadWatcher(path, on_change=loader.reload)
        asyncio.create_task(watcher.start())
    """

    def __init__(
        self,
        path:      str,
        on_change: Callable,
        interval:  float = HOT_RELOAD_INTERVAL,
    ):
        self._path      = path
        self._on_change = on_change
        self._interval  = interval
        self._last_mtime: Optional[float] = None
        self._running   = False

    async def start(self) -> None:
        self._running = True
        log.info("Hot-reload watcher started: %s (poll every %.0fs)", self._path, self._interval)
        while self._running:
            try:
                mtime = os.stat(self._path).st_mtime if os.path.exists(self._path) else None
                if mtime and mtime != self._last_mtime:
                    if self._last_mtime is not None:
                        log.info("Graph file changed — triggering hot reload")
                        await self._on_change()
                    self._last_mtime = mtime
            except Exception as exc:
                log.debug("Hot-reload watcher error: %s", exc)
            await asyncio.sleep(self._interval)

    def stop(self) -> None:
        self._running = False


# ─────────────────────────────────────────────────────────────────────────────
# KDTree spatial index (for fast nearest-node queries)
# ─────────────────────────────────────────────────────────────────────────────

class SpatialIndex:
    """
    Wraps scipy KDTree for fast nearest-node snapping.
    Falls back to linear scan if scipy is not available.
    """

    def __init__(self, G):
        self._G     = G
        self._nodes = list(G.nodes(data=True))
        self._tree  = None
        self._ids   = []
        self._build()

    def _build(self) -> None:
        try:
            from scipy.spatial import KDTree
            import numpy as np
            coords     = []
            self._ids  = []
            for nid, data in self._nodes:
                lat = data.get("lat")
                lon = data.get("lng")
                if lat is not None and lon is not None:
                    coords.append([lat, lon])
                    self._ids.append(nid)
            if coords:
                self._tree = KDTree(np.array(coords))
                log.debug("SpatialIndex: KDTree built with %d nodes", len(self._ids))
        except ImportError:
            log.debug("scipy not available — falling back to linear scan for snapping")

    def nearest(self, lat: float, lon: float, max_dist_m: float = 500.0) -> Optional[str]:
        """Return nearest node ID within max_dist_m. Returns None if too far."""
        if self._tree is not None:
            import numpy as np
            dist_deg, idx = self._tree.query([lat, lon])
            # Convert degrees to metres (rough)
            dist_m = dist_deg * 111_000
            if dist_m > max_dist_m:
                return None
            return self._ids[idx]

        # Linear fallback
        best_id   = None
        best_dist = float("inf")
        for nid, data in self._nodes:
            d = _haversine_m(lat, lon, data.get("lat", 0), data.get("lng", 0))
            if d < best_dist:
                best_dist = d
                best_id   = nid
        if best_dist > max_dist_m:
            return None
        return best_id

    def nodes_within(self, lat: float, lon: float, radius_m: float) -> List[Tuple[str, float]]:
        """Return (node_id, distance_m) for all nodes within radius."""
        results = []
        for nid, data in self._nodes:
            d = _haversine_m(lat, lon, data.get("lat", 0), data.get("lng", 0))
            if d <= radius_m:
                results.append((nid, d))
        results.sort(key=lambda x: x[1])
        return results


# ─────────────────────────────────────────────────────────────────────────────
# Main GraphLoader
# ─────────────────────────────────────────────────────────────────────────────

class GraphLoader:
    """
    Production graph loader — attach to app.state.graph_loader in lifespan.

    Responsibilities:
      1. Load graph (stub or real) on startup
      2. Validate graph integrity
      3. Build spatial index (KDTree)
      4. Apply live operational data (elevator outages, pedway closures)
      5. Hot-reload when graph file changes
      6. Expose stats and health check endpoints

    Usage:
        loader = GraphLoader(use_stub=True, redis=redis_conn)
        G      = await loader.load()
        stats  = loader.stats.as_dict()

        # Apply live outages (call from nav_route.py or a periodic task)
        await loader.apply_live_outages(cta_service)

        # Snap coordinates to graph node
        node_id = loader.snap(lat, lng)
    """

    def __init__(
        self,
        use_stub:     bool     = True,
        graph_path:   str      = DEFAULT_GRAPH_PATH,
        redis        = None,
    ):
        self._use_stub    = use_stub
        self._graph_path  = graph_path
        self._redis       = redis
        self._G           = None
        self._stats:      Optional[GraphStats]     = None
        self._validation: Optional[ValidationResult] = None
        self._modifier:   Optional[EdgeModifier]   = None
        self._index:      Optional[SpatialIndex]   = None
        self._watcher:    Optional[HotReloadWatcher] = None
        self._load_time   = 0.0
        self._reload_count = 0

    @property
    def graph(self):
        """Return the loaded NetworkX graph."""
        return self._G

    @property
    def stats(self) -> Optional[GraphStats]:
        return self._stats

    @property
    def validation(self) -> Optional[ValidationResult]:
        return self._validation

    @property
    def modifier(self) -> Optional[EdgeModifier]:
        return self._modifier

    @property
    def spatial_index(self) -> Optional[SpatialIndex]:
        return self._index

    async def load(self):
        """
        Load the graph synchronously (runs blocking I/O in thread pool for real graph).
        Returns the graph object.
        """
        t0 = time.monotonic()
        loop = asyncio.get_event_loop()

        if self._use_stub:
            G = await loop.run_in_executor(None, self._load_stub)
            source = "stub"
        else:
            G = await loop.run_in_executor(None, self._load_real)
            source = "real"

        self._G        = G
        self._modifier = EdgeModifier(G)
        self._index    = SpatialIndex(G)
        self._stats    = GraphStatsCollector().collect(G, source)
        self._validation = GraphValidator().validate(G)
        self._load_time  = time.monotonic() - t0
        self._reload_count += 1

        if not self._validation.valid:
            log.error("Graph validation FAILED: %s", self._validation.errors)
        if self._validation.warnings:
            for w in self._validation.warnings[:10]:
                log.warning("Graph warning: %s", w)

        log.info(
            "Graph loaded in %.2fs: %d nodes, %d edges [%s]",
            self._load_time, G.number_of_nodes(), G.number_of_edges(), source,
        )

        # Apply pedway hours on startup
        if self._modifier:
            self._modifier.apply_pedway_hours()

        return G

    def _load_stub(self):
        loader = StubGraphLoader()
        if not loader.is_available():
            raise RuntimeError("Stub graph not available — check app/graph/stub.py")
        return loader.load()

    def _load_real(self):
        loader = RealGraphLoader(self._graph_path)
        if not loader.is_available():
            log.warning("Real graph file not found at %s — falling back to stub", self._graph_path)
            return StubGraphLoader().load()
        return loader.load()

    async def reload(self) -> None:
        """Reload graph from disk (called by hot-reload watcher on file change)."""
        log.info("Reloading graph from disk...")
        old_G = self._G
        try:
            await self.load()
            log.info("Graph hot-reload complete")
        except Exception as exc:
            log.error("Graph hot-reload failed: %s — keeping old graph", exc)
            self._G = old_G

    def start_hot_reload(self) -> Optional[asyncio.Task]:
        """Start the async hot-reload watcher task. Returns the task."""
        if self._use_stub:
            log.debug("Hot-reload disabled for stub graph")
            return None
        self._watcher = HotReloadWatcher(self._graph_path, on_change=self.reload)
        return asyncio.create_task(self._watcher.start())

    def stop_hot_reload(self) -> None:
        if self._watcher:
            self._watcher.stop()

    # ── Live data application ────────────────────────────────────────────────

    async def apply_live_outages(self, cta_service) -> int:
        """Apply current CTA elevator outages to graph edge costs."""
        if not self._modifier or not self._G:
            return 0
        outaged_ids = await cta_service.get_outaged_elevator_ids()
        modified    = self._modifier.apply_elevator_outages(outaged_ids)
        if modified:
            log.info("Applied %d elevator outage(s) to graph", modified)
        return modified

    async def apply_pedway_closures_from_cta(self, cta_service) -> int:
        """Apply pedway closures from CTA service to graph."""
        if not self._modifier:
            return 0
        snapshot     = await cta_service.get_snapshot()
        closed_ids   = {s.segment_id for s in snapshot.pedway_closures if s.is_closed}
        return self._modifier.apply_pedway_closures(closed_ids)

    def apply_pedway_hours(self) -> int:
        """Enforce pedway operating hours on edge costs."""
        if not self._modifier:
            return 0
        return self._modifier.apply_pedway_hours()

    # ── Spatial snap ─────────────────────────────────────────────────────────

    def snap(self, lat: float, lon: float, max_dist_m: float = 500.0) -> Optional[str]:
        """Snap (lat, lon) to nearest graph node. Returns None if too far."""
        if not self._index:
            return None
        return self._index.nearest(lat, lon, max_dist_m=max_dist_m)

    def nodes_within_radius(self, lat: float, lon: float, radius_m: float) -> List[Tuple[str, float]]:
        """Return (node_id, dist_m) for nodes within radius_m."""
        if not self._index:
            return []
        return self._index.nodes_within(lat, lon, radius_m)

    # ── Graph queries ────────────────────────────────────────────────────────

    def get_node_data(self, node_id: str) -> Optional[Dict]:
        if not self._G or node_id not in self._G:
            return None
        return dict(self._G.nodes[node_id])

    def get_nodes_by_level(self, level: str) -> List[Tuple[str, Dict]]:
        if not self._G:
            return []
        return [(n, d) for n, d in self._G.nodes(data=True) if d.get("level") == level]

    def get_accessible_nodes(self) -> List[Tuple[str, Dict]]:
        """Return all nodes that have accessible elevator connectors."""
        if not self._G:
            return []
        accessible_set: Set[str] = set()
        for u, v, data in self._G.edges(data=True):
            if data.get("accessible") and data.get("connector_type") == "elevator":
                accessible_set.add(u)
                accessible_set.add(v)
        return [(n, self._G.nodes[n]) for n in accessible_set if n in self._G]

    def get_elevator_edges(self) -> List[Dict]:
        """Return all elevator edges with their current status."""
        if not self._G:
            return []
        edges = []
        for u, v, k, data in self._G.edges(keys=True, data=True):
            if data.get("connector_type") == "elevator":
                edges.append({
                    "u":          u,
                    "v":          v,
                    "k":          k,
                    "elevator_id": data.get("elevator_id"),
                    "outage":     data.get("outage", False),
                    "reliability": data.get("reliability", 0.95),
                    "accessible":  data.get("accessible", True),
                })
        return edges

    def find_path_between_levels(self, from_level: str, to_level: str) -> List[Tuple[str, str]]:
        """Return all connector edges between two levels."""
        if not self._G:
            return []
        result = []
        for u, v, data in self._G.edges(data=True):
            u_lvl = self._G.nodes[u].get("level", "")
            v_lvl = self._G.nodes[v].get("level", "")
            if {u_lvl, v_lvl} == {from_level, to_level}:
                result.append((u, v))
        return result

    # ── Export / health ───────────────────────────────────────────────────────

    def export_summary(self) -> Dict[str, Any]:
        """JSON summary for /health endpoint."""
        return {
            "loaded":          self._G is not None,
            "use_stub":        self._use_stub,
            "graph_path":      self._graph_path,
            "reload_count":    self._reload_count,
            "load_time_s":     round(self._load_time, 3),
            "hot_reload":      self._watcher is not None and self._watcher._running,
            "stats":           self._stats.as_dict() if self._stats else None,
            "validation":      {
                "valid":    self._validation.valid,
                "errors":   self._validation.errors,
                "warnings_count": len(self._validation.warnings) if self._validation else 0,
            } if self._validation else None,
            "modifier":        self._modifier.get_modification_summary() if self._modifier else None,
        }

    def health_check(self) -> Dict[str, Any]:
        """Quick health check — used by /health endpoint."""
        if not self._G:
            return {"status": "not_loaded", "nodes": 0, "edges": 0}
        return {
            "status":     "ok" if (self._validation and self._validation.valid) else "degraded",
            "nodes":      self._G.number_of_nodes(),
            "edges":      self._G.number_of_edges(),
            "source":     self._stats.graph_source if self._stats else "unknown",
            "use_stub":   self._use_stub,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Module-level helpers
# ─────────────────────────────────────────────────────────────────────────────

async def build_graph(use_stub: bool = True, graph_path: str = DEFAULT_GRAPH_PATH, redis=None):
    """
    Convenience function — build and return (loader, graph) tuple.
    Called from FastAPI lifespan:
        loader, G = await build_graph(use_stub=settings.USE_STUB_GRAPH)
        app.state.graph_loader = loader
        app.state.graph = G
    """
    loader = GraphLoader(use_stub=use_stub, graph_path=graph_path, redis=redis)
    G      = await loader.load()
    return loader, G


def get_graph_stats(G) -> Dict[str, Any]:
    """Quick stats snapshot — used when full GraphLoader not available."""
    collector = GraphStatsCollector()
    stats     = collector.collect(G, "unknown")
    return stats.as_dict()


def validate_graph(G) -> ValidationResult:
    """Validate graph without a full loader instance."""
    return GraphValidator().validate(G)
