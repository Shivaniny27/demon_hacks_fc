"""
LoopNav Graph Builder — Phase 1
Constructs a multi-level NetworkX graph from three GeoJSON layers:
  1. Street (OSM walkable ways)
  2. Pedway (underground tunnels)
  3. Mid (Lower Wacker + Riverwalk)
Then adds vertical connectors between layers and validates connectivity.
"""

import json
import math
import os
from collections import defaultdict

import networkx as nx
from scipy.spatial import KDTree
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)

# ── Helpers ────────────────────────────────────────────────────────────────

def haversine(lat1, lon1, lat2, lon2):
    """Distance in meters between two lat/lng points."""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def make_node_id(level, lat, lon):
    """Deterministic node ID from level + rounded coords."""
    return f"{level}_{lat:.6f}_{lon:.6f}"


def round_coord(val, precision=6):
    return round(val, precision)


# ── Node & Edge Schema ────────────────────────────────────────────────────

def make_node(node_id, lat, lng, level, node_type="intersection", building_name=None):
    """
    Node schema:
      id, lat, lng, level, type, building_name
    Types: intersection / building_entrance / connector / rest / cta_station
    """
    return {
        "id": node_id,
        "lat": lat,
        "lng": lng,
        "level": level,
        "type": node_type,
        "building_name": building_name,
    }


def make_edge_attrs(distance_m, level, covered=False, sheltered=False, surface="asphalt",
                    connector_type=None, accessible=True, escalator_direction=None,
                    elevator_id=None, time_penalty_s=0, open_hours=None, reliability=1.0):
    """
    Edge schema:
      distance_m, time_s, level, covered, sheltered, surface,
      connector_type (stairs/elevator/escalator/null),
      accessible, escalator_direction, elevator_id,
      time_penalty_s, open_hours, reliability (0-1)
    """
    walk_speed = 1.4  # m/s
    time_s = distance_m / walk_speed + time_penalty_s
    return {
        "distance_m": round(distance_m, 1),
        "time_s": round(time_s, 1),
        "level": level,
        "covered": covered,
        "sheltered": sheltered,
        "surface": surface,
        "connector_type": connector_type,
        "accessible": accessible,
        "escalator_direction": escalator_direction,
        "elevator_id": elevator_id,
        "time_penalty_s": time_penalty_s,
        "open_hours": open_hours,
        "reliability": reliability,
    }


# ── Layer Builders ─────────────────────────────────────────────────────────

def build_street_layer(G):
    """Load OSM streets into graph. Tag level=street, covered=false."""
    path = os.path.join(RAW_DIR, "streets_osm.geojson")
    with open(path) as f:
        data = json.load(f)

    node_count = 0
    edge_count = 0

    for feat in data["features"]:
        props = feat["properties"]
        coords = feat["geometry"]["coordinates"]

        level = "street"
        covered = bool(props.get("covered", False))
        sheltered = bool(props.get("sheltered", False))
        surface = props.get("surface", "asphalt")
        accessible = bool(props.get("accessible", True))

        prev_nid = None
        for lon, lat in coords:
            lat, lon = round_coord(lat), round_coord(lon)
            nid = make_node_id(level, lat, lon)

            if nid not in G:
                G.add_node(nid, **make_node(nid, lat, lon, level))
                node_count += 1

            if prev_nid and prev_nid != nid:
                prev = G.nodes[prev_nid]
                dist = haversine(prev["lat"], prev["lng"], lat, lon)
                attrs = make_edge_attrs(
                    distance_m=dist, level=level, covered=covered,
                    sheltered=sheltered, surface=surface, accessible=accessible,
                )
                G.add_edge(prev_nid, nid, **attrs)
                G.add_edge(nid, prev_nid, **attrs)  # bidirectional
                edge_count += 2

            prev_nid = nid

    print(f"  Street layer: {node_count} nodes, {edge_count} edges")
    return node_count, edge_count


def build_pedway_layer(G):
    """Load real Chicago Pedway routes from City of Chicago open data.
    Source: https://github.com/Chicago/osd-pedway-routes
    44 routes with MultiLineString geometry, 1079 coordinate points.
    """
    path = os.path.join(RAW_DIR, "pedway_real.geojson")
    with open(path) as f:
        data = json.load(f)

    node_count = 0
    edge_count = 0
    level = "pedway"

    for feat in data["features"]:
        props = feat["properties"]
        geom = feat["geometry"]
        route_name = props.get("PED_ROUTE", None)

        # MultiLineString: list of lines, each line is list of [lon, lat]
        lines = geom["coordinates"]

        for line in lines:
            prev_nid = None
            for point in line:
                lon, lat = point[0], point[1]
                lat, lon = round_coord(lat), round_coord(lon)
                nid = make_node_id(level, lat, lon)

                if nid not in G:
                    G.add_node(nid, **make_node(nid, lat, lon, level, "intersection", route_name))
                    node_count += 1

                if prev_nid and prev_nid != nid:
                    prev = G.nodes[prev_nid]
                    dist = haversine(prev["lat"], prev["lng"], lat, lon)
                    attrs = make_edge_attrs(
                        distance_m=dist, level=level, covered=True,
                        open_hours="06:00-22:00", reliability=0.95,
                    )
                    G.add_edge(prev_nid, nid, **attrs)
                    G.add_edge(nid, prev_nid, **attrs)
                    edge_count += 2

                prev_nid = nid

    print(f"  Pedway layer: {node_count} nodes, {edge_count} edges (from City of Chicago open data)")
    return node_count, edge_count


def build_mid_layer(G):
    """Hardcode Lower Wacker + Riverwalk segments. Tag level=mid, covered=true."""
    node_count = 0
    edge_count = 0

    mid_segments = [
        # Lower Wacker Drive (east-west, south of river)
        {"name": "Lower Wacker - Michigan to State",
         "coords": [[-87.6244, 41.8870], [-87.6260, 41.8870], [-87.6278, 41.8870]],
         "covered": True, "surface": "concrete"},
        {"name": "Lower Wacker - State to Dearborn",
         "coords": [[-87.6278, 41.8870], [-87.6295, 41.8870]],
         "covered": True, "surface": "concrete"},
        {"name": "Lower Wacker - Dearborn to Clark",
         "coords": [[-87.6295, 41.8870], [-87.6310, 41.8870]],
         "covered": True, "surface": "concrete"},
        {"name": "Lower Wacker - Clark to LaSalle",
         "coords": [[-87.6310, 41.8870], [-87.6320, 41.8870], [-87.6337, 41.8870]],
         "covered": True, "surface": "concrete"},
        {"name": "Lower Wacker - LaSalle to Wells",
         "coords": [[-87.6337, 41.8870], [-87.6360, 41.8870]],
         "covered": True, "surface": "concrete"},

        # Riverwalk segments (along south bank of river)
        {"name": "Riverwalk - Michigan to State",
         "coords": [[-87.6244, 41.8875], [-87.6260, 41.8875], [-87.6278, 41.8875]],
         "covered": False, "surface": "stone"},
        {"name": "Riverwalk - State to Dearborn",
         "coords": [[-87.6278, 41.8875], [-87.6295, 41.8875]],
         "covered": False, "surface": "stone"},
        {"name": "Riverwalk - Dearborn to Clark",
         "coords": [[-87.6295, 41.8875], [-87.6310, 41.8875]],
         "covered": False, "surface": "stone"},
        {"name": "Riverwalk - Clark to LaSalle",
         "coords": [[-87.6310, 41.8875], [-87.6320, 41.8875], [-87.6337, 41.8875]],
         "covered": False, "surface": "stone"},

        # Lower Wacker south spur (north-south under Wacker near Michigan)
        {"name": "Lower Wacker South - Michigan",
         "coords": [[-87.6244, 41.8870], [-87.6244, 41.8856], [-87.6244, 41.8843]],
         "covered": True, "surface": "concrete"},
    ]

    for seg in mid_segments:
        prev_nid = None
        for lon, lat in seg["coords"]:
            lat, lon = round_coord(lat), round_coord(lon)
            nid = make_node_id("mid", lat, lon)

            if nid not in G:
                G.add_node(nid, **make_node(nid, lat, lon, "mid", "intersection", seg["name"]))
                node_count += 1

            if prev_nid and prev_nid != nid:
                prev = G.nodes[prev_nid]
                dist = haversine(prev["lat"], prev["lng"], lat, lon)
                attrs = make_edge_attrs(
                    distance_m=dist, level="mid", covered=seg["covered"],
                    surface=seg["surface"], reliability=1.0,
                )
                G.add_edge(prev_nid, nid, **attrs)
                G.add_edge(nid, prev_nid, **attrs)
                edge_count += 2

            prev_nid = nid

    print(f"  Mid layer: {node_count} nodes, {edge_count} edges")
    return node_count, edge_count


# ── CTA Stations ───────────────────────────────────────────────────────────

def add_cta_stations(G):
    """Add CTA station nodes to street layer and snap to nearest street node."""
    path = os.path.join(RAW_DIR, "cta_stations.geojson")
    with open(path) as f:
        data = json.load(f)

    # Build KDTree from street nodes for snapping
    street_nodes = [(nid, G.nodes[nid]) for nid in G if G.nodes[nid]["level"] == "street"]
    if not street_nodes:
        print("  WARNING: No street nodes to snap CTA stations to")
        return

    nids = [n[0] for n in street_nodes]
    coords_arr = np.array([[n[1]["lat"], n[1]["lng"]] for n in street_nodes])
    tree = KDTree(coords_arr)

    added = 0
    for feat in data["features"]:
        props = feat["properties"]
        geom = feat["geometry"]
        lon, lat = geom["coordinates"][0], geom["coordinates"][1]

        station_name = props.get("station_name") or props.get("name", "CTA Station")

        # Add station node
        nid = make_node_id("street", round_coord(lat), round_coord(lon))
        if nid not in G:
            G.add_node(nid, **make_node(nid, lat, lon, "street", "cta_station", station_name))

        # Snap to nearest street node (connect with a short edge)
        dist_arr, idx = tree.query([lat, lon])
        nearest_nid = nids[idx]
        if nearest_nid != nid:
            dist = haversine(lat, lon, coords_arr[idx][0], coords_arr[idx][1])
            if dist < 200:  # only snap if within 200m
                attrs = make_edge_attrs(distance_m=dist, level="street")
                G.add_edge(nid, nearest_nid, **attrs)
                G.add_edge(nearest_nid, nid, **attrs)
                added += 1

    print(f"  CTA stations: {len(data['features'])} loaded, {added} snapped to street graph")


# ── Vertical Connectors ───────────────────────────────────────────────────

# Known vertical connector locations in the Chicago Loop
VERTICAL_CONNECTORS = [
    {
        "name": "Block 37",
        "lat": 41.8838, "lon": -87.6278,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "escalator", "direction": "down", "accessible": False},
            {"type": "escalator", "direction": "up", "accessible": False},
            {"type": "elevator", "accessible": True, "elevator_id": "CTA-BLK37-E1", "reliability": 0.85},
            {"type": "stairs", "accessible": False},
        ],
    },
    {
        "name": "Chase Tower",
        "lat": 41.8797, "lon": -87.6295,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "stairs", "accessible": False},
            {"type": "elevator", "accessible": True, "elevator_id": "CHASE-E1", "reliability": 0.90},
        ],
    },
    {
        "name": "Daley Center",
        "lat": 41.8839, "lon": -87.6320,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "stairs", "accessible": False},
            {"type": "elevator", "accessible": True, "elevator_id": "DALEY-E1", "reliability": 0.88},
        ],
    },
    {
        "name": "Millennium Station",
        "lat": 41.8843, "lon": -87.6244,
        "levels": ["street", "pedway", "mid"],
        "connectors": [
            {"type": "escalator", "direction": "down", "accessible": False},
            {"type": "escalator", "direction": "up", "accessible": False},
            {"type": "elevator", "accessible": True, "elevator_id": "CTA-MILL-E1", "reliability": 0.80},
            {"type": "stairs", "accessible": False},
        ],
    },
    {
        "name": "City Hall",
        "lat": 41.8833, "lon": -87.6320,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "stairs", "accessible": False},
            {"type": "elevator", "accessible": True, "elevator_id": "CITY-HALL-E1", "reliability": 0.92},
        ],
    },
    {
        "name": "Macy's (Marshall Field's)",
        "lat": 41.8838, "lon": -87.6260,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "escalator", "direction": "both", "accessible": False},
            {"type": "elevator", "accessible": True, "elevator_id": "MACYS-E1", "reliability": 0.75},
            {"type": "stairs", "accessible": False},
        ],
    },
    {
        "name": "Union Station Concourse",
        "lat": 41.8789, "lon": -87.6400,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "stairs", "accessible": False},
            {"type": "escalator", "direction": "up", "accessible": False},
            {"type": "elevator", "accessible": True, "elevator_id": "UNION-E1", "reliability": 0.82},
        ],
    },
    # Lower Wacker ramps (street <-> mid)
    {
        "name": "Lower Wacker Ramp - Michigan",
        "lat": 41.8870, "lon": -87.6244,
        "levels": ["street", "mid"],
        "connectors": [
            {"type": "stairs", "accessible": False},
            {"type": "elevator", "accessible": True, "elevator_id": "LW-MICH-E1", "reliability": 0.70},
        ],
    },
    {
        "name": "Lower Wacker Ramp - State",
        "lat": 41.8870, "lon": -87.6278,
        "levels": ["street", "mid"],
        "connectors": [
            {"type": "stairs", "accessible": False},
        ],
    },
    {
        "name": "Lower Wacker Ramp - Dearborn",
        "lat": 41.8870, "lon": -87.6295,
        "levels": ["street", "mid"],
        "connectors": [
            {"type": "stairs", "accessible": False},
        ],
    },
    {
        "name": "Lower Wacker Ramp - Clark",
        "lat": 41.8870, "lon": -87.6310,
        "levels": ["street", "mid"],
        "connectors": [
            {"type": "stairs", "accessible": False},
            {"type": "elevator", "accessible": True, "elevator_id": "LW-CLARK-E1", "reliability": 0.65},
        ],
    },
    {
        "name": "Lower Wacker Ramp - LaSalle",
        "lat": 41.8870, "lon": -87.6337,
        "levels": ["street", "mid"],
        "connectors": [
            {"type": "stairs", "accessible": False},
        ],
    },
    # Riverwalk access (street <-> mid)
    {
        "name": "Riverwalk Access - Michigan",
        "lat": 41.8875, "lon": -87.6244,
        "levels": ["street", "mid"],
        "connectors": [
            {"type": "stairs", "accessible": False},
        ],
    },
    # === Additional pedway exits for complete coverage ===
    {
        "name": "Aon Center / Prudential",
        "lat": 41.8820, "lon": -87.6244,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "stairs", "accessible": False},
            {"type": "elevator", "accessible": True, "elevator_id": "AON-E1", "reliability": 0.85},
        ],
    },
    {
        "name": "Monroe & Michigan",
        "lat": 41.8805, "lon": -87.6244,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "stairs", "accessible": False},
            {"type": "escalator", "direction": "both", "accessible": False},
        ],
    },
    {
        "name": "Adams & Michigan (Art Institute)",
        "lat": 41.8794, "lon": -87.6244,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "stairs", "accessible": False},
            {"type": "elevator", "accessible": True, "elevator_id": "ADAMS-MICH-E1", "reliability": 0.80},
        ],
    },
    {
        "name": "Monroe & Dearborn",
        "lat": 41.8805, "lon": -87.6295,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "stairs", "accessible": False},
        ],
    },
    {
        "name": "Monroe & Clark",
        "lat": 41.8805, "lon": -87.6310,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "stairs", "accessible": False},
        ],
    },
    {
        "name": "Monroe & LaSalle",
        "lat": 41.8805, "lon": -87.6320,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "stairs", "accessible": False},
            {"type": "elevator", "accessible": True, "elevator_id": "MON-LASALLE-E1", "reliability": 0.75},
        ],
    },
    {
        "name": "Madison & Wells",
        "lat": 41.8820, "lon": -87.6337,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "stairs", "accessible": False},
        ],
    },
    {
        "name": "Adams & Wells",
        "lat": 41.8789, "lon": -87.6337,
        "levels": ["street", "pedway"],
        "connectors": [
            {"type": "stairs", "accessible": False},
            {"type": "elevator", "accessible": True, "elevator_id": "ADAMS-WELLS-E1", "reliability": 0.82},
        ],
    },
]


def add_vertical_connectors(G):
    """
    Add vertical connector edges between layers.
    Each connector gets proper type, accessible flag, elevator ID.
    Snaps to nearest existing node in each layer via KDTree.
    """
    # Build KDTrees per level
    level_trees = {}
    for level in ("street", "pedway", "mid"):
        nodes = [(nid, G.nodes[nid]) for nid in G if G.nodes[nid]["level"] == level]
        if nodes:
            nids = [n[0] for n in nodes]
            coords = np.array([[n[1]["lat"], n[1]["lng"]] for n in nodes])
            level_trees[level] = (nids, coords, KDTree(coords))

    connector_count = 0
    SNAP_RADIUS = 0.003  # ~300m in degrees, generous snap for demo

    for vc in VERTICAL_CONNECTORS:
        lat, lon = vc["lat"], vc["lon"]
        name = vc["name"]
        levels = vc["levels"]

        # Find or create nodes in each level
        level_nodes = {}
        for level in levels:
            nid = make_node_id(level, round_coord(lat), round_coord(lon))

            if nid in G:
                level_nodes[level] = nid
            elif level in level_trees:
                # Snap to nearest existing node
                nids, coords, tree = level_trees[level]
                dist_arr, idx = tree.query([lat, lon])
                if dist_arr < SNAP_RADIUS:
                    level_nodes[level] = nids[idx]
                else:
                    # Create new node and connect to nearest
                    G.add_node(nid, **make_node(nid, lat, lon, level, "connector", name))
                    nearest_nid = nids[idx]
                    dist = haversine(lat, lon, coords[idx][0], coords[idx][1])
                    attrs = make_edge_attrs(distance_m=dist, level=level, covered=(level != "street"))
                    G.add_edge(nid, nearest_nid, **attrs)
                    G.add_edge(nearest_nid, nid, **attrs)
                    level_nodes[level] = nid
            else:
                # Layer has no nodes yet, create one
                G.add_node(nid, **make_node(nid, lat, lon, level, "connector", name))
                level_nodes[level] = nid

        # Add connector edges between each pair of levels
        for i, level_a in enumerate(levels):
            for level_b in levels[i + 1:]:
                if level_a not in level_nodes or level_b not in level_nodes:
                    continue
                nid_a = level_nodes[level_a]
                nid_b = level_nodes[level_b]

                for conn in vc["connectors"]:
                    ctype = conn["type"]
                    accessible = conn.get("accessible", False)
                    elevator_id = conn.get("elevator_id")
                    reliability = conn.get("reliability", 1.0)
                    esc_dir = conn.get("direction")

                    # Penalties by type
                    if ctype == "stairs":
                        penalty = 45
                    elif ctype == "elevator":
                        penalty = 60
                    elif ctype == "escalator":
                        penalty = 30
                    else:
                        penalty = 0

                    attrs = make_edge_attrs(
                        distance_m=5.0,  # vertical distance nominal
                        level=f"{level_a}->{level_b}",
                        covered=True,
                        connector_type=ctype,
                        accessible=accessible,
                        escalator_direction=esc_dir,
                        elevator_id=elevator_id,
                        time_penalty_s=penalty,
                        reliability=reliability,
                    )

                    # Bidirectional (except escalators with direction)
                    if ctype == "escalator" and esc_dir == "down":
                        # Down only: street -> pedway / street -> mid
                        G.add_edge(nid_a, nid_b, **attrs)
                        connector_count += 1
                    elif ctype == "escalator" and esc_dir == "up":
                        G.add_edge(nid_b, nid_a, **attrs)
                        connector_count += 1
                    else:
                        G.add_edge(nid_a, nid_b, **attrs)
                        G.add_edge(nid_b, nid_a, **attrs)
                        connector_count += 2

    print(f"  Vertical connectors: {connector_count} edges added across {len(VERTICAL_CONNECTORS)} junctions")
    return connector_count


# ── Graph Validation ───────────────────────────────────────────────────────

def validate_graph(G):
    """Check connectivity, layer counts, flag disconnected subgraphs."""
    print("\n=== Graph Validation ===")
    print(f"  Total nodes: {G.number_of_nodes()}")
    print(f"  Total edges: {G.number_of_edges()}")

    # Count by level
    level_counts = defaultdict(lambda: {"nodes": 0, "edges": 0})
    for nid in G.nodes:
        level = G.nodes[nid]["level"]
        level_counts[level]["nodes"] += 1

    for u, v, k in G.edges(keys=True):
        level = G.edges[u, v, k].get("level", "unknown")
        if "->" in level:
            level_counts["connector"]["edges"] += 1
        else:
            level_counts[level]["edges"] += 1

    for level, counts in sorted(level_counts.items()):
        print(f"    {level}: {counts['nodes']} nodes, {counts['edges']} edges")

    # Check connectivity
    undirected = G.to_undirected()
    components = list(nx.connected_components(undirected))
    print(f"\n  Connected components: {len(components)}")

    if len(components) > 1:
        # Sort by size descending
        components.sort(key=len, reverse=True)
        print(f"    Largest component: {len(components[0])} nodes")
        for i, comp in enumerate(components[1:], 2):
            if len(comp) > 5:
                sample = list(comp)[:3]
                levels = set(G.nodes[n]["level"] for n in comp)
                print(f"    Component {i}: {len(comp)} nodes (levels: {levels})")

    # Check cross-layer connectivity
    pedway_nodes = [n for n in G if G.nodes[n]["level"] == "pedway"]
    street_nodes = [n for n in G if G.nodes[n]["level"] == "street"]
    mid_nodes = [n for n in G if G.nodes[n]["level"] == "mid"]

    # Check if pedway is reachable from street
    if pedway_nodes and street_nodes:
        test_street = street_nodes[0]
        reachable = nx.single_source_shortest_path_length(G, test_street, cutoff=50)
        pedway_reachable = sum(1 for n in reachable if n in pedway_nodes or G.nodes.get(n, {}).get("level") == "pedway")
        print(f"\n  Cross-layer: {pedway_reachable} pedway nodes reachable from street (within 50 hops)")

    if mid_nodes and street_nodes:
        test_street = street_nodes[0]
        reachable = nx.single_source_shortest_path_length(G, test_street, cutoff=50)
        mid_reachable = sum(1 for n in reachable if G.nodes.get(n, {}).get("level") == "mid")
        print(f"  Cross-layer: {mid_reachable} mid nodes reachable from street (within 50 hops)")

    print("\n  [OK] Graph validation complete")
    return {
        "nodes": G.number_of_nodes(),
        "edges": G.number_of_edges(),
        "components": len(components),
        "level_counts": dict(level_counts),
    }


# ── Merge Disconnected Components ──────────────────────────────────────────

def _merge_disconnected(G):
    """Connect small disconnected street components to the main graph."""
    undirected = G.to_undirected()
    components = sorted(nx.connected_components(undirected), key=len, reverse=True)

    if len(components) <= 1:
        return

    main_comp = components[0]
    main_street = [n for n in main_comp if G.nodes[n]["level"] == "street"]
    if not main_street:
        return

    main_coords = np.array([[G.nodes[n]["lat"], G.nodes[n]["lng"]] for n in main_street])
    main_tree = KDTree(main_coords)

    merged = 0
    for comp in components[1:]:
        best_dist = float("inf")
        best_node = None
        best_main_idx = None

        for n in comp:
            nd = G.nodes[n]
            dist_arr, idx = main_tree.query([nd["lat"], nd["lng"]])
            if dist_arr < best_dist:
                best_dist = dist_arr
                best_node = n
                best_main_idx = idx

        if best_node and best_main_idx is not None:
            target = main_street[best_main_idx]
            nd = G.nodes[best_node]
            tg = G.nodes[target]
            dist = haversine(nd["lat"], nd["lng"], tg["lat"], tg["lng"])

            if dist < 500:
                attrs = make_edge_attrs(distance_m=dist, level="street")
                G.add_edge(best_node, target, **attrs)
                G.add_edge(target, best_node, **attrs)
                merged += 1

    print(f"  Merged {merged} disconnected components into main graph")


# ── Save/Load ──────────────────────────────────────────────────────────────

def _save_graph_json(G, path):
    """Save graph as JSON (nodes + edges). Handles None values."""
    def clean(d):
        return dict(d)  # json.dump handles None -> null natively

    data = {
        "nodes": {nid: clean(G.nodes[nid]) for nid in G.nodes},
        "edges": [
            {"source": u, "target": v, "key": k, **clean(G.edges[u, v, k])}
            for u, v, k in G.edges(keys=True)
        ],
    }
    with open(path, "w") as f:
        json.dump(data, f)
    size_kb = os.path.getsize(path) // 1024
    print(f"  Graph JSON: {size_kb} KB")


def load_graph_json(path=None):
    """Load graph from JSON. Returns NetworkX MultiDiGraph."""
    if path is None:
        path = os.path.join(PROCESSED_DIR, "loopnav_graph.json")
    with open(path) as f:
        data = json.load(f)

    G = nx.MultiDiGraph()
    for nid, attrs in data["nodes"].items():
        G.add_node(nid, **attrs)
    for edge in data["edges"]:
        e = dict(edge)
        u, v = e.pop("source"), e.pop("target")
        e.pop("key", None)
        G.add_edge(u, v, **e)

    return G


# ── Main Build ─────────────────────────────────────────────────────────────

def build_graph():
    """Build the complete multi-level graph. Returns NetworkX DiGraph."""
    print("=== Building LoopNav Graph ===\n")

    G = nx.MultiDiGraph()

    # Step 2: Build three layers
    print("Step 2: Building layers...")
    build_street_layer(G)
    build_pedway_layer(G)
    build_mid_layer(G)

    # Add CTA stations
    print("\nAdding CTA stations...")
    add_cta_stations(G)

    # Step 4: Vertical connectors
    print("\nStep 4: Adding vertical connectors...")
    add_vertical_connectors(G)

    # Merge small disconnected components into the main graph
    print("\nMerging disconnected components...")
    _merge_disconnected(G)

    # Step 5: Validation
    stats = validate_graph(G)

    # Save graph to disk as JSON (GraphML can't handle None values)
    graph_path = os.path.join(PROCESSED_DIR, "loopnav_graph.json")
    _save_graph_json(G, graph_path)
    print(f"\n  Graph saved to {graph_path}")

    return G, stats


if __name__ == "__main__":
    G, stats = build_graph()
    print(f"\nDone. {stats['nodes']} nodes, {stats['edges']} edges, {stats['components']} components")
