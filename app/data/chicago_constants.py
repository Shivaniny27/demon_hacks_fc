"""
Chicago Loop hard-coded geographic intelligence.

Coordinates use (latitude, longitude) tuples, WGS-84.
"""

from typing import NamedTuple


class Location(NamedTuple):
    lat: float
    lng: float
    name: str
    note: str = ""


# ── "Death Corners" — notorious wind tunnel intersections ─────────────────────
# The Venturi effect between skyscrapers creates dangerous gusts here.
DEATH_CORNERS: list[Location] = [
    Location(41.8792, -87.6358, "Wacker & Adams",
             "Funnels wind off the Chicago River — gusts exceed 40 mph in winter."),
    Location(41.8783, -87.6279, "State & Madison",
             "Busiest corner in the world; twin canyons amplify cross-winds."),
    Location(41.8788, -87.6357, "Willis Tower Plaza",
             "Open plaza accelerates north wind across the Sears Tower skirt."),
    Location(41.8856, -87.6270, "Michigan & Wacker",
             "River gap creates a direct wind channel from the lake."),
    Location(41.8800, -87.6316, "Dearborn & Monroe",
             "Classic canyon effect between mid-century towers."),
]

# ── CTA Loop L stations (surface + elevated) ─────────────────────────────────
CTA_LOOP_STATIONS: list[Location] = [
    Location(41.8858, -87.6256, "Millennium Station (Metra)"),
    Location(41.8832, -87.6282, "Washington/Wabash",
             "Key transfer — elevator reliability varies."),
    Location(41.8804, -87.6298, "Adams/Wabash"),
    Location(41.8790, -87.6294, "Harold Washington Library — State/Van Buren"),
    Location(41.8826, -87.6278, "Madison/Wabash"),
    Location(41.8860, -87.6280, "Lake/State (Red/Blue)"),
    Location(41.8857, -87.6317, "Clark/Lake (all lines)"),
    Location(41.8844, -87.6319, "Washington/Wells"),
    Location(41.8822, -87.6340, "Quincy/Wells"),
    Location(41.8795, -87.6339, "LaSalle/Van Buren"),
    Location(41.8791, -87.6321, "LaSalle (Blue)"),
]

# ── Pedway entry nodes ─────────────────────────────────────────────────────────
PEDWAY_NODES: list[Location] = [
    Location(41.8834, -87.6270, "Block 37 Tunnel",
             "Connects Washington/State to Block 37 mall."),
    Location(41.8839, -87.6295, "Daley Center Concourse",
             "Links City Hall to the Thompson Center via underground."),
    Location(41.8821, -87.6313, "Chase Tower Pedway",
             "One of the warmest underground stretches."),
    Location(41.8805, -87.6354, "Union Station Pedway",
             "Deep underground, connects to Canal St."),
    Location(41.8858, -87.6256, "Millennium Park Underground",
             "Connects Millennium Park garages to Michigan Ave."),
    Location(41.8843, -87.6285, "City Hall – Thompson Connector",
             "The classic Pedway segment, heated to 70°F year-round."),
]

# ── Chicago River bascule bridges (crossing points) ──────────────────────────
LOOP_BRIDGES: list[Location] = [
    Location(41.8875, -87.6270, "Michigan Avenue Bridge",
             "Most iconic — lifts for tall boats April–November."),
    Location(41.8868, -87.6313, "Wabash Avenue Bridge"),
    Location(41.8869, -87.6327, "State Street Bridge"),
    Location(41.8870, -87.6349, "Dearborn Street Bridge"),
    Location(41.8870, -87.6370, "Clark Street Bridge"),
    Location(41.8872, -87.6389, "LaSalle Street Bridge"),
    Location(41.8869, -87.6406, "Wells Street Bridge"),
    Location(41.8867, -87.6430, "Franklin-Orleans Street Bridge"),
]

# ── "Sunny side of the street" — south-facing stretches warmer in winter ─────
SUNNY_SIDES: list[dict] = [
    {
        "street": "Wells St (south of Lake)",
        "lat": 41.8840, "lng": -87.6339,
        "note": "East side of Wells St gets 2–3 hrs more winter sun.",
    },
    {
        "street": "Wacker Dr (upper)",
        "lat": 41.8872, "lng": -87.6360,
        "note": "Upper Wacker southbound is 8–12°F warmer than lower Wacker in January.",
    },
]

# ── Known alley / "rat-hole" maintenance corridors ────────────────────────────
LOOP_ALLEYS: list[Location] = [
    Location(41.8823, -87.6254, "Couch Place",
             "Historic alley from 1800s; tourists mistake it for a shortcut."),
    Location(41.8811, -87.6327, "Quincy Court",
             "Service alley behind the Rookery Building."),
    Location(41.8845, -87.6300, "City Hall Loading Alley",
             "Heavy truck traffic; no pedestrian accommodation."),
]

# ── Hazard radius thresholds (meters) ────────────────────────────────────────
DEATH_CORNER_RADIUS_M = 80      # flag report as "near death corner"
BRIDGE_RADIUS_M = 120           # flag report as "near bridge"
PEDWAY_RADIUS_M = 100           # flag report as "near pedway"
HAZARD_ROUTING_RADIUS_M = 150   # inflate route cost within this radius


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Return distance in metres between two WGS-84 coordinates."""
    import math
    R = 6_371_000
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ = math.radians(lat2 - lat1)
    dλ = math.radians(lng2 - lng1)
    a = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def classify_location(lat: float, lng: float) -> dict:
    """
    Given a lat/lng, return flags for near_death_corner, near_bridge, near_pedway.
    """
    near_dc = any(
        haversine_m(lat, lng, dc.lat, dc.lng) <= DEATH_CORNER_RADIUS_M
        for dc in DEATH_CORNERS
    )
    near_br = any(
        haversine_m(lat, lng, br.lat, br.lng) <= BRIDGE_RADIUS_M
        for br in LOOP_BRIDGES
    )
    near_pw = any(
        haversine_m(lat, lng, pw.lat, pw.lng) <= PEDWAY_RADIUS_M
        for pw in PEDWAY_NODES
    )
    return {
        "near_death_corner": near_dc,
        "near_bridge": near_br,
        "near_pedway": near_pw,
    }
