"""
LoopNav Stub Graph.

Used by Dev2 while Dev1 builds the real multi-layer graph.
Contains enough nodes and edges across all 3 layers (street, mid, pedway)
to test all API endpoints and routing logic end-to-end.

Swap to the real graph by setting USE_STUB_GRAPH=false in .env.
"""

import networkx as nx


def get_graph_stub() -> nx.MultiDiGraph:
    G = nx.MultiDiGraph()

    # ── Street layer nodes ────────────────────────────────────────────────────
    G.add_node("ogilvie",           lat=41.8827, lng=-87.6414, level="street",
               type="building_entrance", building="Ogilvie Transportation Center")
    G.add_node("union_station",     lat=41.8786, lng=-87.6399, level="street",
               type="building_entrance", building="Union Station")
    G.add_node("lasalle_madison",   lat=41.8820, lng=-87.6322, level="street",
               type="intersection")
    G.add_node("state_madison",     lat=41.8820, lng=-87.6278, level="street",
               type="intersection", near_cta_station=True)
    G.add_node("art_institute",     lat=41.8796, lng=-87.6237, level="street",
               type="building_entrance", building="Art Institute of Chicago")
    G.add_node("willis_tower",      lat=41.8789, lng=-87.6359, level="street",
               type="building_entrance", building="Willis Tower")
    G.add_node("daley_center",      lat=41.8838, lng=-87.6308, level="street",
               type="building_entrance", building="Daley Center")
    G.add_node("city_hall",         lat=41.8836, lng=-87.6323, level="street",
               type="building_entrance", building="Chicago City Hall")

    # ── Pedway layer nodes ────────────────────────────────────────────────────
    G.add_node("block37_pedway",    lat=41.8831, lng=-87.6278, level="pedway",
               type="connector", building="Block 37")
    G.add_node("chase_pedway",      lat=41.8818, lng=-87.6312, level="pedway",
               type="connector", building="Chase Tower")
    G.add_node("millennium_pedway", lat=41.8843, lng=-87.6243, level="pedway",
               type="building_entrance", building="Millennium Station")
    G.add_node("city_hall_pedway",  lat=41.8836, lng=-87.6323, level="pedway",
               type="connector", building="City Hall")
    G.add_node("daley_pedway",      lat=41.8838, lng=-87.6308, level="pedway",
               type="connector", building="Daley Center")

    # ── Mid layer nodes (Lower Wacker / Riverwalk) ────────────────────────────
    G.add_node("lower_wacker_lake", lat=41.8858, lng=-87.6380, level="mid",
               type="intersection", building=None)
    G.add_node("lower_wacker_mid",  lat=41.8830, lng=-87.6360, level="mid",
               type="intersection", building=None)
    G.add_node("lower_wacker_monroe", lat=41.8808, lng=-87.6350, level="mid",
               type="intersection", building=None)

    # ── Street edges ──────────────────────────────────────────────────────────
    def se(u, v, dist_m, covered=False, sheltered=False):
        G.add_edge(u, v,
            distance_m=dist_m, time_s=int(dist_m / 1.4),
            level="street", covered=covered, sheltered=sheltered,
            connector_type=None, accessible=True, reliability=1.0,
            open_hours=None)

    se("ogilvie",         "lasalle_madison",  550)
    se("lasalle_madison", "ogilvie",          550)
    se("union_station",   "lasalle_madison",  430)
    se("lasalle_madison", "union_station",    430)
    se("lasalle_madison", "state_madison",    340)
    se("state_madison",   "lasalle_madison",  340)
    se("state_madison",   "art_institute",    490)
    se("art_institute",   "state_madison",    490)
    se("lasalle_madison", "willis_tower",     360)
    se("willis_tower",    "lasalle_madison",  360)
    se("lasalle_madison", "city_hall",        160)
    se("city_hall",       "lasalle_madison",  160)
    se("lasalle_madison", "daley_center",     190)
    se("daley_center",    "lasalle_madison",  190)
    se("state_madison",   "millennium_pedway", 360, sheltered=True)
    se("millennium_pedway", "state_madison",   360, sheltered=True)

    # ── Pedway edges ──────────────────────────────────────────────────────────
    def pe(u, v, dist_m):
        G.add_edge(u, v,
            distance_m=dist_m, time_s=int(dist_m / 1.2),
            level="pedway", covered=True, sheltered=True,
            connector_type=None, accessible=True, reliability=1.0,
            open_hours=(6, 22))

    pe("block37_pedway",    "chase_pedway",       340)
    pe("chase_pedway",      "block37_pedway",      340)
    pe("chase_pedway",      "city_hall_pedway",   120)
    pe("city_hall_pedway",  "chase_pedway",        120)
    pe("city_hall_pedway",  "daley_pedway",        190)
    pe("daley_pedway",      "city_hall_pedway",    190)
    pe("block37_pedway",    "millennium_pedway",   590)
    pe("millennium_pedway", "block37_pedway",      590)

    # ── Mid / Lower Wacker edges ───────────────────────────────────────────────
    def me(u, v, dist_m):
        G.add_edge(u, v,
            distance_m=dist_m, time_s=int(dist_m / 1.4),
            level="mid", covered=True, sheltered=True,
            connector_type=None, accessible=False, reliability=1.0,
            open_hours=None)

    me("lower_wacker_lake",   "lower_wacker_mid",    320)
    me("lower_wacker_mid",    "lower_wacker_lake",   320)
    me("lower_wacker_mid",    "lower_wacker_monroe", 270)
    me("lower_wacker_monroe", "lower_wacker_mid",    270)

    # ── Vertical connectors (street ↔ pedway) ─────────────────────────────────
    def connector(u, v, ctype, accessible, elevator_id=None, reliability=1.0):
        penalty = {"elevator": 60, "escalator": 30, "stairs": 45}[ctype]
        G.add_edge(u, v,
            distance_m=0, time_s=penalty,
            level=G.nodes[u]["level"], covered=True, sheltered=True,
            connector_type=ctype, accessible=accessible,
            elevator_id=elevator_id, reliability=reliability,
            time_penalty_s=penalty, open_hours=None)

    connector("state_madison",   "block37_pedway",   "escalator", False)
    connector("block37_pedway",  "state_madison",    "escalator", False)

    connector("city_hall",       "city_hall_pedway", "elevator", True,
              elevator_id="CITYHALL_ELEV_01", reliability=0.90)
    connector("city_hall_pedway","city_hall",        "elevator", True,
              elevator_id="CITYHALL_ELEV_01", reliability=0.90)

    connector("daley_center",    "daley_pedway",     "stairs", False)
    connector("daley_pedway",    "daley_center",     "stairs", False)

    connector("chase_pedway",    "lasalle_madison",  "elevator", True,
              elevator_id="CHASE_ELEV_01", reliability=0.85)
    connector("lasalle_madison", "chase_pedway",     "elevator", True,
              elevator_id="CHASE_ELEV_01", reliability=0.85)

    # ── Vertical connectors (street ↔ mid) ────────────────────────────────────
    connector("ogilvie",         "lower_wacker_lake",  "stairs", False)
    connector("lower_wacker_lake","ogilvie",           "stairs", False)
    connector("willis_tower",    "lower_wacker_monroe","stairs", False)
    connector("lower_wacker_monroe","willis_tower",    "stairs", False)

    return G
