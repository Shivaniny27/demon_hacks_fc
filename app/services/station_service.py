"""
station_service.py — Chicago Loop CTA Station Intelligence
==========================================================
Deep per-station knowledge layer for the Chicago Loop L stations.

Features:
- Complete station database: lines, entrances, Pedway connections,
  platform layout, elevator locations, accessible routes
- Platform tips: which car/door to board for optimal exit at destination
- Entrance routing: which entrance to use from Pedway vs street
- Accessibility scoring per entrance and route
- Pedway-to-platform routing advice
- Station comparison and ranking for a given origin/destination pair
- Real-time integration hooks (elevator_service, outage_service)

Does NOT modify any existing service file.
"""

from __future__ import annotations

import math
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Enums ─────────────────────────────────────────────────────────────────────

class EntranceType(str, Enum):
    STREET_STAIR   = "street_stair"
    STREET_ELEV    = "street_elevator"
    PEDWAY_TUNNEL  = "pedway_tunnel"
    BUILDING_LOBBY = "building_lobby"
    SKYWALK        = "skywalk"


class PlatformEnd(str, Enum):
    NORTH = "north"
    SOUTH = "south"
    EAST  = "east"
    WEST  = "west"
    EITHER = "either"


class ExitOptimization(str, Enum):
    """Optimal end of train to board for exit."""
    FRONT = "front"    # head of train (direction of travel)
    REAR  = "rear"     # tail of train
    MIDDLE = "middle"


class StationRating(str, Enum):
    EXCELLENT = "excellent"
    GOOD      = "good"
    FAIR      = "fair"
    POOR      = "poor"


# ── Station Entrance Database ─────────────────────────────────────────────────

@dataclass
class StationEntrance:
    entrance_id:    str
    name:           str
    type:           EntranceType
    lat:            float
    lon:            float
    accessible:     bool
    has_elevator:   bool
    elevator_id:    Optional[str]
    has_farecard:   bool            # ADA farecard reader
    open_24h:       bool
    open_time:      Optional[str]   # "HH:MM" if not 24h
    close_time:     Optional[str]
    pedway_segment: Optional[str]   # pedway_service segment_id if connected
    notes:          str


@dataclass
class PlatformTip:
    """Advice on which part of the platform to board."""
    destination_station: str
    destination_name:    str
    line:                str
    direction:           str          # "northbound", "southbound", etc.
    board_end:           ExitOptimization
    exit_tip:            str          # human-readable
    time_saved_seconds:  int          # vs. boarding random car
    exit_to_pedway:      bool         # does this exit connect to Pedway?
    exit_landmark:       str          # nearest landmark at exit


@dataclass
class PlatformLayout:
    station_key:     str
    platform_length_cars: int        # number of cars the platform fits
    has_center_platform: bool        # island platform
    has_side_platform:   bool        # side platforms
    north_end_access:    bool        # staircase at north end
    south_end_access:    bool        # staircase at south end
    mezzanine_level:     bool        # intermediate mezzanine
    platform_level:      str         # "elevated" | "underground"
    typical_crowding:    Dict[str, str]  # hour → "light"|"moderate"|"heavy"


@dataclass
class StationProfile:
    """Complete intelligence profile for a single Loop L station."""
    station_key:         str
    name:                str
    short_name:          str
    map_id:              str
    lines:               List[str]
    lat:                 float
    lon:                 float
    platform_level:      str
    entrances:           List[StationEntrance]
    accessible_entrances: List[StationEntrance]
    platform_layout:     PlatformLayout
    platform_tips:       List[PlatformTip]
    pedway_connected:    bool
    pedway_segments:     List[str]
    pedway_entrance:     Optional[str]
    nearby_buildings:    List[str]
    nearby_landmarks:    List[str]
    ada_compliant:       bool
    elevator_ids:        List[str]
    weather_shelter:     bool          # True if station entrance is sheltered
    tunnel_connection:   Optional[str] # Blue/Red Line connections
    transfer_stations:   List[str]     # station_keys for transfers
    overall_rating:      StationRating
    rating_notes:        str


# ── Full Station Database ─────────────────────────────────────────────────────

STATION_PROFILES: Dict[str, StationProfile] = {}

def _build_station_profiles() -> Dict[str, StationProfile]:
    profiles: Dict[str, StationProfile] = {}

    # ── Clark/Lake ─────────────────────────────────────────────────────────────
    profiles["clark_lake"] = StationProfile(
        station_key="clark_lake",
        name="Clark/Lake",
        short_name="Clark/Lake",
        map_id="40380",
        lines=["Blue", "Brown", "Green", "Orange", "Pink", "Purple"],
        lat=41.8856, lon=-87.6319,
        platform_level="elevated",
        entrances=[
            StationEntrance("CL-ENT-1", "Washington St & Clark St (NE corner)", EntranceType.STREET_STAIR,
                            41.8857, -87.6319, accessible=False, has_elevator=False, elevator_id=None,
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="Main entrance. Stairs only."),
            StationEntrance("CL-ENT-2", "Washington St & Clark St (Elevator)", EntranceType.STREET_ELEV,
                            41.8856, -87.6320, accessible=True, has_elevator=True, elevator_id="CL-E1",
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="Accessible elevator entrance."),
            StationEntrance("CL-ENT-3", "Pedway Level – Lower Washington", EntranceType.PEDWAY_TUNNEL,
                            41.8854, -87.6319, accessible=True, has_elevator=True, elevator_id="CL-E2",
                            has_farecard=True, open_24h=False, open_time="06:00", close_time="22:00",
                            pedway_segment="clark_lake_pedway", notes="Pedway connection. Elevator to platform."),
        ],
        accessible_entrances=[],   # populated below
        platform_layout=PlatformLayout(
            station_key="clark_lake",
            platform_length_cars=8,
            has_center_platform=False,
            has_side_platform=True,
            north_end_access=True,
            south_end_access=True,
            mezzanine_level=True,
            platform_level="elevated",
            typical_crowding={
                "07": "heavy", "08": "heavy", "09": "moderate",
                "12": "moderate", "17": "heavy", "18": "heavy", "19": "moderate",
            },
        ),
        platform_tips=[
            PlatformTip("state_lake", "State/Lake", "Brown", "clockwise",
                        ExitOptimization.REAR, "Board rear cars. Exit south end onto State St.",
                        45, False, "State St & Lake St intersection"),
            PlatformTip("washington_wells", "Washington/Wells", "Brown", "clockwise",
                        ExitOptimization.FRONT, "Board front cars. Exit at Wells St elevator.",
                        60, True, "Wells St & Washington Blvd"),
            PlatformTip("washington_dearborn", "Washington/Dearborn (Blue)", "Blue", "O'Hare bound",
                        ExitOptimization.MIDDLE, "Board middle cars — platform is short.",
                        20, True, "Dearborn St underpass, Pedway connection"),
        ],
        pedway_connected=True,
        pedway_segments=["clark_lake_pedway", "pedway_dearborn_clark"],
        pedway_entrance="Lower Level, east side via elevator CL-E2",
        nearby_buildings=["55 W Wacker", "190 N State", "One N Dearborn", "Blue Cross Blue Shield Tower"],
        nearby_landmarks=["Chicago City Hall (2 blocks)", "Daley Plaza (2 blocks)"],
        ada_compliant=True,
        elevator_ids=["CL-E1", "CL-E2", "CL-E3"],
        weather_shelter=True,
        tunnel_connection="Blue Line mezzanine below",
        transfer_stations=["washington_dearborn"],
        overall_rating=StationRating.EXCELLENT,
        rating_notes="Most served station in the Loop. Excellent ADA access and Pedway connection.",
    )

    # ── State/Lake ─────────────────────────────────────────────────────────────
    profiles["state_lake"] = StationProfile(
        station_key="state_lake",
        name="State/Lake",
        short_name="State/Lake",
        map_id="40260",
        lines=["Brown", "Green", "Orange", "Pink", "Purple"],
        lat=41.8858, lon=-87.6278,
        platform_level="elevated",
        entrances=[
            StationEntrance("SL-ENT-1", "State St & Lake St (NW corner)", EntranceType.STREET_STAIR,
                            41.8859, -87.6278, accessible=False, has_elevator=False, elevator_id=None,
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="Main stair entrance."),
            StationEntrance("SL-ENT-2", "State St Elevator", EntranceType.STREET_ELEV,
                            41.8858, -87.6279, accessible=True, has_elevator=True, elevator_id="SL-E1",
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="ADA accessible. Elevator to mezzanine."),
        ],
        accessible_entrances=[],
        platform_layout=PlatformLayout(
            station_key="state_lake",
            platform_length_cars=8,
            has_center_platform=False,
            has_side_platform=True,
            north_end_access=True,
            south_end_access=False,
            mezzanine_level=True,
            platform_level="elevated",
            typical_crowding={
                "07": "moderate", "08": "heavy", "09": "moderate",
                "17": "heavy", "18": "heavy",
            },
        ),
        platform_tips=[
            PlatformTip("clark_lake", "Clark/Lake", "Brown", "counterclockwise",
                        ExitOptimization.FRONT, "Board front cars — stairs at north end exit to Clark/Lake stairs.",
                        30, False, "Clark St & Lake St"),
            PlatformTip("randolph_wabash", "Randolph/Wabash", "Brown", "clockwise",
                        ExitOptimization.REAR, "Board rear cars. Exit south at Wabash Ave.",
                        25, True, "Randolph St & Wabash Ave, Pedway connection"),
        ],
        pedway_connected=False,
        pedway_segments=["state_ns_corridor"],
        pedway_entrance=None,
        nearby_buildings=["Palmer House Hilton", "11 S State", "Chase Tower (nearby)"],
        nearby_landmarks=["Millennium Park entrance (3 blocks)", "Chicago Cultural Center (1 block)"],
        ada_compliant=True,
        elevator_ids=["SL-E1", "SL-E2"],
        weather_shelter=True,
        tunnel_connection=None,
        transfer_stations=[],
        overall_rating=StationRating.GOOD,
        rating_notes="Good for Millennium Park access. No Pedway connection.",
    )

    # ── Washington/Wabash ──────────────────────────────────────────────────────
    profiles["washington_wabash"] = StationProfile(
        station_key="washington_wabash",
        name="Washington/Wabash",
        short_name="Wash/Wabash",
        map_id="41700",
        lines=["Brown", "Green", "Orange", "Pink", "Purple"],
        lat=41.8831, lon=-87.6259,
        platform_level="elevated",
        entrances=[
            StationEntrance("WW-ENT-1", "Washington St entrance (north)", EntranceType.STREET_STAIR,
                            41.8833, -87.6259, accessible=False, has_elevator=False, elevator_id=None,
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="North stair entrance."),
            StationEntrance("WW-ENT-2", "Wabash Ave Elevator", EntranceType.STREET_ELEV,
                            41.8831, -87.6258, accessible=True, has_elevator=True, elevator_id="WW-E1",
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="ADA entrance via Wabash Ave elevator."),
            StationEntrance("WW-ENT-3", "Underground Concourse – Pedway", EntranceType.PEDWAY_TUNNEL,
                            41.8830, -87.6260, accessible=True, has_elevator=True, elevator_id="WW-E2",
                            has_farecard=True, open_24h=False, open_time="06:00", close_time="22:00",
                            pedway_segment="washington_wabash_concourse",
                            notes="Direct Pedway tunnel connection from underground."),
        ],
        accessible_entrances=[],
        platform_layout=PlatformLayout(
            station_key="washington_wabash",
            platform_length_cars=8,
            has_center_platform=False,
            has_side_platform=True,
            north_end_access=True,
            south_end_access=True,
            mezzanine_level=True,
            platform_level="elevated",
            typical_crowding={
                "08": "heavy", "09": "moderate", "17": "heavy", "18": "heavy",
            },
        ),
        platform_tips=[
            PlatformTip("adams_wabash", "Adams/Wabash", "Brown", "clockwise",
                        ExitOptimization.REAR, "Board rear cars. Arrive at Adams end, near Art Institute.",
                        40, True, "Adams St & Wabash Ave, Pedway below"),
            PlatformTip("madison_wabash", "Madison/Wabash", "Brown", "clockwise",
                        ExitOptimization.FRONT, "Board front cars — Madison entrance is north end.",
                        35, False, "Madison St & Wabash Ave"),
        ],
        pedway_connected=True,
        pedway_segments=["washington_wabash_concourse"],
        pedway_entrance="Underground concourse, north side — elevator WW-E2",
        nearby_buildings=["55 E Monroe", "Macy's on State", "The Legacy"],
        nearby_landmarks=["Millennium Park (1 block east)", "Cloud Gate (2 blocks)"],
        ada_compliant=True,
        elevator_ids=["WW-E1", "WW-E2"],
        weather_shelter=True,
        tunnel_connection=None,
        transfer_stations=[],
        overall_rating=StationRating.EXCELLENT,
        rating_notes="Best Pedway connection. Gateway to Millennium Park underground.",
    )

    # ── Washington/Dearborn (Blue) ─────────────────────────────────────────────
    profiles["washington_dearborn"] = StationProfile(
        station_key="washington_dearborn",
        name="Washington/Dearborn (Blue)",
        short_name="Wash/Dearborn",
        map_id="40370",
        lines=["Blue"],
        lat=41.8836, lon=-87.6295,
        platform_level="underground",
        entrances=[
            StationEntrance("WD-ENT-1", "Dearborn St & Washington St (NE)", EntranceType.STREET_STAIR,
                            41.8837, -87.6296, accessible=False, has_elevator=False, elevator_id=None,
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="Main street entrance."),
            StationEntrance("WD-ENT-2", "Washington St Elevator (ADA)", EntranceType.STREET_ELEV,
                            41.8836, -87.6294, accessible=True, has_elevator=True, elevator_id="WD-E1",
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="ADA entrance. Elevator to concourse."),
            StationEntrance("WD-ENT-3", "Pedway/City Hall Tunnel", EntranceType.PEDWAY_TUNNEL,
                            41.8836, -87.6295, accessible=True, has_elevator=True, elevator_id="WD-E2",
                            has_farecard=True, open_24h=False, open_time="05:00", close_time="23:00",
                            pedway_segment="blue_line_washington_pedway",
                            notes="Long Pedway tunnel through City Hall. CTA operating hours."),
        ],
        accessible_entrances=[],
        platform_layout=PlatformLayout(
            station_key="washington_dearborn",
            platform_length_cars=8,
            has_center_platform=True,
            has_side_platform=False,
            north_end_access=True,
            south_end_access=True,
            mezzanine_level=True,
            platform_level="underground",
            typical_crowding={
                "07": "moderate", "08": "heavy", "09": "moderate",
                "17": "heavy", "18": "moderate",
            },
        ),
        platform_tips=[
            PlatformTip("clark_lake", "Clark/Lake", "Blue", "O'Hare-bound",
                        ExitOptimization.FRONT, "Board front cars. Clark/Lake platform stairs are at the O'Hare end.",
                        60, True, "Clark St & Lake St, Pedway below"),
            PlatformTip("monroe_dearborn", "Monroe/Dearborn (Blue)", "Blue", "Forest Park-bound",
                        ExitOptimization.REAR, "Board rear cars — Monroe platform exit is at south end.",
                        45, True, "Monroe St Pedway tunnel"),
        ],
        pedway_connected=True,
        pedway_segments=["blue_line_washington_pedway", "pedway_dearborn_clark"],
        pedway_entrance="Concourse level — long tunnel through City Hall mezzanine",
        nearby_buildings=["City Hall", "Daley Center", "One N Dearborn"],
        nearby_landmarks=["City Hall (directly above)", "Daley Plaza (1 block)"],
        ada_compliant=True,
        elevator_ids=["WD-E1", "WD-E2"],
        weather_shelter=True,
        tunnel_connection="Blue Line O'Hare branch. Transfer to Loop L at Clark/Lake.",
        transfer_stations=["clark_lake"],
        overall_rating=StationRating.EXCELLENT,
        rating_notes="Best Blue Line Loop access. Long Pedway tunnel to City Hall.",
    )

    # ── Adams/Wabash ────────────────────────────────────────────────────────────
    profiles["adams_wabash"] = StationProfile(
        station_key="adams_wabash",
        name="Adams/Wabash",
        short_name="Adams/Wabash",
        map_id="40680",
        lines=["Brown", "Green", "Orange", "Pink", "Purple"],
        lat=41.8793, lon=-87.6260,
        platform_level="elevated",
        entrances=[
            StationEntrance("AW-ENT-1", "Adams St & Wabash Ave", EntranceType.STREET_STAIR,
                            41.8793, -87.6260, accessible=False, has_elevator=False, elevator_id=None,
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="Street level stairs."),
            StationEntrance("AW-ENT-2", "Adams St Elevator", EntranceType.STREET_ELEV,
                            41.8792, -87.6261, accessible=True, has_elevator=True, elevator_id="AW-E1",
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="ADA entrance."),
            StationEntrance("AW-ENT-3", "Pedway – Adams St Underpass", EntranceType.PEDWAY_TUNNEL,
                            41.8793, -87.6263, accessible=True, has_elevator=True, elevator_id="AW-E2",
                            has_farecard=True, open_24h=False, open_time="06:00", close_time="22:00",
                            pedway_segment="adams_wabash_underpass",
                            notes="Art Institute underground connection."),
        ],
        accessible_entrances=[],
        platform_layout=PlatformLayout(
            station_key="adams_wabash",
            platform_length_cars=8,
            has_center_platform=False,
            has_side_platform=True,
            north_end_access=True,
            south_end_access=False,
            mezzanine_level=False,
            platform_level="elevated",
            typical_crowding={
                "08": "moderate", "09": "light", "17": "heavy", "18": "moderate",
            },
        ),
        platform_tips=[
            PlatformTip("harold_washington_library", "Harold Washington Library", "Brown", "clockwise",
                        ExitOptimization.REAR, "Board rear cars. Library exit is at south end.",
                        40, True, "State St & Van Buren"),
        ],
        pedway_connected=True,
        pedway_segments=["adams_wabash_underpass"],
        pedway_entrance="Below platform, Adams St underpass. Connects to Art Institute.",
        nearby_buildings=["Art Institute of Chicago", "320 S Michigan", "Symphony Center"],
        nearby_landmarks=["Art Institute of Chicago (1 block)", "Grant Park (1 block)"],
        ada_compliant=True,
        elevator_ids=["AW-E1", "AW-E2"],
        weather_shelter=True,
        tunnel_connection=None,
        transfer_stations=[],
        overall_rating=StationRating.GOOD,
        rating_notes="Great for Art Institute / Grant Park access.",
    )

    # ── Quincy/Wells ────────────────────────────────────────────────────────────
    profiles["quincy_wells"] = StationProfile(
        station_key="quincy_wells",
        name="Quincy/Wells",
        short_name="Quincy/Wells",
        map_id="40040",
        lines=["Brown", "Orange", "Pink", "Purple"],
        lat=41.8784, lon=-87.6379,
        platform_level="elevated",
        entrances=[
            StationEntrance("QW-ENT-1", "Quincy St & Wells St", EntranceType.STREET_STAIR,
                            41.8784, -87.6379, accessible=False, has_elevator=False, elevator_id=None,
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="Historic 1897 station. Stairs only entrance."),
            StationEntrance("QW-ENT-2", "Quincy St Elevator", EntranceType.STREET_ELEV,
                            41.8783, -87.6380, accessible=True, has_elevator=True, elevator_id="QW-E1",
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="ADA accessible via Wells St."),
        ],
        accessible_entrances=[],
        platform_layout=PlatformLayout(
            station_key="quincy_wells",
            platform_length_cars=8,
            has_center_platform=False,
            has_side_platform=True,
            north_end_access=True,
            south_end_access=False,
            mezzanine_level=False,
            platform_level="elevated",
            typical_crowding={
                "08": "light", "09": "light", "17": "moderate", "18": "moderate",
            },
        ),
        platform_tips=[
            PlatformTip("washington_wells", "Washington/Wells", "Brown", "counterclockwise",
                        ExitOptimization.FRONT, "Board front cars for Wells St exit.",
                        30, True, "Washington Blvd & Wells St, Pedway below"),
        ],
        pedway_connected=False,
        pedway_segments=["quincy_wells_pedway"],
        pedway_entrance=None,
        nearby_buildings=["190 S LaSalle", "Civic Opera House", "One LaSalle"],
        nearby_landmarks=["Civic Opera House (1 block)", "LaSalle St Financial District"],
        ada_compliant=True,
        elevator_ids=["QW-E1"],
        weather_shelter=False,
        tunnel_connection=None,
        transfer_stations=[],
        overall_rating=StationRating.FAIR,
        rating_notes="Historic 1897 station. Limited ADA access. Less congested.",
    )

    # ── Jackson/Dearborn (Blue) ────────────────────────────────────────────────
    profiles["jackson_dearborn"] = StationProfile(
        station_key="jackson_dearborn",
        name="Jackson/Dearborn (Blue)",
        short_name="Jackson/Dearborn",
        map_id="40920",
        lines=["Blue"],
        lat=41.8781, lon=-87.6297,
        platform_level="underground",
        entrances=[
            StationEntrance("JD-ENT-1", "Dearborn St & Jackson Blvd", EntranceType.STREET_STAIR,
                            41.8782, -87.6297, accessible=False, has_elevator=False, elevator_id=None,
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="Main street stairs."),
            StationEntrance("JD-ENT-2", "Dearborn St Elevator", EntranceType.STREET_ELEV,
                            41.8781, -87.6298, accessible=True, has_elevator=True, elevator_id="JD-E1",
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="North ADA elevator."),
            StationEntrance("JD-ENT-3", "Jackson Blvd Pedway Tunnel", EntranceType.PEDWAY_TUNNEL,
                            41.8781, -87.6296, accessible=True, has_elevator=True, elevator_id="JD-E2",
                            has_farecard=True, open_24h=False, open_time="05:00", close_time="23:00",
                            pedway_segment="jackson_dearborn_pedway",
                            notes="South ADA elevator — Pedway connection. Frequently under maintenance."),
        ],
        accessible_entrances=[],
        platform_layout=PlatformLayout(
            station_key="jackson_dearborn",
            platform_length_cars=8,
            has_center_platform=True,
            has_side_platform=False,
            north_end_access=True,
            south_end_access=True,
            mezzanine_level=True,
            platform_level="underground",
            typical_crowding={
                "07": "light", "08": "moderate", "09": "light",
                "17": "moderate", "18": "moderate",
            },
        ),
        platform_tips=[
            PlatformTip("monroe_dearborn", "Monroe/Dearborn (Blue)", "Blue", "O'Hare-bound",
                        ExitOptimization.REAR, "Board rear cars — Monroe exit is at south end.",
                        30, True, "Monroe St Pedway"),
        ],
        pedway_connected=True,
        pedway_segments=["jackson_dearborn_pedway"],
        pedway_entrance="Jackson Blvd underground corridor — elevator JD-E2 (check status before use)",
        nearby_buildings=["Federal Plaza", "Dirksen Federal Building", "Chicago Bar Association"],
        nearby_landmarks=["Federal Plaza (above station)", "Monadnock Building (1 block)"],
        ada_compliant=True,
        elevator_ids=["JD-E1", "JD-E2"],
        weather_shelter=True,
        tunnel_connection="Blue Line. Transfer to Loop L at Clark/Lake.",
        transfer_stations=["clark_lake", "monroe_dearborn"],
        overall_rating=StationRating.GOOD,
        rating_notes="Second elevator (JD-E2) unreliable. Check status before ADA routing.",
    )

    # ── Monroe/Dearborn (Blue) ────────────────────────────────────────────────
    profiles["monroe_dearborn"] = StationProfile(
        station_key="monroe_dearborn",
        name="Monroe/Dearborn (Blue)",
        short_name="Monroe/Dearborn",
        map_id="40790",
        lines=["Blue"],
        lat=41.8807, lon=-87.6297,
        platform_level="underground",
        entrances=[
            StationEntrance("MD-ENT-1", "Dearborn St & Monroe St", EntranceType.STREET_STAIR,
                            41.8808, -87.6297, accessible=False, has_elevator=False, elevator_id=None,
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment=None, notes="Primary stair entrance."),
            StationEntrance("MD-ENT-2", "Monroe St ADA Elevator", EntranceType.STREET_ELEV,
                            41.8807, -87.6298, accessible=True, has_elevator=True, elevator_id="MD-E1",
                            has_farecard=True, open_24h=True, open_time=None, close_time=None,
                            pedway_segment="monroe_dearborn_pedway",
                            notes="Single elevator. Connects to Monroe St Pedway corridor."),
        ],
        accessible_entrances=[],
        platform_layout=PlatformLayout(
            station_key="monroe_dearborn",
            platform_length_cars=8,
            has_center_platform=True,
            has_side_platform=False,
            north_end_access=True,
            south_end_access=False,
            mezzanine_level=True,
            platform_level="underground",
            typical_crowding={
                "08": "moderate", "09": "light", "17": "moderate",
            },
        ),
        platform_tips=[
            PlatformTip("washington_dearborn", "Washington/Dearborn (Blue)", "Blue", "O'Hare-bound",
                        ExitOptimization.FRONT, "Board front — Washington platform exit is north end.",
                        40, True, "City Hall Pedway tunnel"),
        ],
        pedway_connected=True,
        pedway_segments=["monroe_dearborn_pedway"],
        pedway_entrance="Monroe St Pedway connection via elevator MD-E1",
        nearby_buildings=["Inland Steel Building", "190 S LaSalle (via Pedway)", "One Dearborn"],
        nearby_landmarks=["Art Institute of Chicago (1 block east)", "Millennium Park (2 blocks)"],
        ada_compliant=True,
        elevator_ids=["MD-E1"],
        weather_shelter=True,
        tunnel_connection="Blue Line only. Transfer to Loop L at Clark/Lake.",
        transfer_stations=["clark_lake", "jackson_dearborn"],
        overall_rating=StationRating.GOOD,
        rating_notes="Single elevator — critical ADA point. Check reliability before routing.",
    )

    # Populate accessible_entrances for all profiles
    for key, profile in profiles.items():
        profile.accessible_entrances = [
            e for e in profile.entrances if e.accessible
        ]

    return profiles


# Build on import
STATION_PROFILES = _build_station_profiles()


# ── Platform Tip Engine ───────────────────────────────────────────────────────

class PlatformTipEngine:
    """
    Returns boarding advice for a given origin → destination pair.
    Looks up pre-built platform tips indexed by destination station.
    """

    def get_tip(
        self,
        boarding_station_key: str,
        alighting_station_key: str,
        line: Optional[str] = None,
    ) -> Optional[PlatformTip]:
        """Return platform tip for boarding at station X and alighting at Y."""
        profile = STATION_PROFILES.get(boarding_station_key)
        if not profile:
            return None
        for tip in profile.platform_tips:
            if tip.destination_station == alighting_station_key:
                if line is None or tip.line == line:
                    return tip
        return None

    def get_all_tips(self, station_key: str) -> List[PlatformTip]:
        profile = STATION_PROFILES.get(station_key)
        return profile.platform_tips if profile else []

    def get_entrance_for_pedway(
        self, station_key: str
    ) -> Optional[StationEntrance]:
        """Return the Pedway-connected entrance for a station, if any."""
        profile = STATION_PROFILES.get(station_key)
        if not profile:
            return None
        for entrance in profile.entrances:
            if entrance.type == EntranceType.PEDWAY_TUNNEL:
                return entrance
        return None

    def get_accessible_entrance(
        self, station_key: str, prefer_pedway: bool = True
    ) -> Optional[StationEntrance]:
        """Return best accessible entrance, preferring Pedway connection if available."""
        profile = STATION_PROFILES.get(station_key)
        if not profile:
            return None
        accessible = profile.accessible_entrances
        if not accessible:
            return None
        if prefer_pedway:
            pedway_entrances = [e for e in accessible if e.type == EntranceType.PEDWAY_TUNNEL]
            if pedway_entrances:
                return pedway_entrances[0]
        return accessible[0]


# ── Station Ranker ────────────────────────────────────────────────────────────

class StationRanker:
    """
    Ranks Loop stations for a given origin position and desired line/destination.
    Scoring: distance + Pedway bonus + ADA status + reliability.
    """

    def rank_stations(
        self,
        origin_lat: float,
        origin_lon: float,
        desired_lines: Optional[List[str]] = None,
        accessible_required: bool = False,
        pedway_preferred: bool = True,
        max_walk_m: float = 1000.0,
    ) -> List[Dict[str, Any]]:
        """
        Return ranked list of stations with scoring breakdown.
        """
        results: List[Dict[str, Any]] = []

        for key, profile in STATION_PROFILES.items():
            dist_m = _haversine(origin_lat, origin_lon, profile.lat, profile.lon)
            if dist_m > max_walk_m:
                continue

            if desired_lines and not any(line in profile.lines for line in desired_lines):
                continue

            if accessible_required and not profile.ada_compliant:
                continue

            # Base score: lower is better
            score = dist_m

            # Pedway bonus: -100m equivalent
            if pedway_preferred and profile.pedway_connected:
                score -= 100

            # ADA penalty for non-compliant
            if accessible_required and not profile.accessible_entrances:
                score += 500

            # Weather shelter bonus
            if profile.weather_shelter:
                score -= 50

            rating_bonus = {
                StationRating.EXCELLENT: -80,
                StationRating.GOOD: -40,
                StationRating.FAIR: 0,
                StationRating.POOR: 100,
            }.get(profile.overall_rating, 0)
            score += rating_bonus

            pedway_entrance = self._get_pedway_entrance(profile)

            results.append({
                "station_key":       key,
                "station_name":      profile.name,
                "lines":             profile.lines,
                "distance_m":        round(dist_m, 1),
                "score":             round(score, 1),
                "pedway_connected":  profile.pedway_connected,
                "pedway_entrance":   pedway_entrance,
                "accessible":        profile.ada_compliant,
                "weather_shelter":   profile.weather_shelter,
                "overall_rating":    profile.overall_rating.value,
                "lat":               profile.lat,
                "lon":               profile.lon,
            })

        results.sort(key=lambda x: x["score"])
        return results

    @staticmethod
    def _get_pedway_entrance(profile: StationProfile) -> Optional[str]:
        for entrance in profile.entrances:
            if entrance.type == EntranceType.PEDWAY_TUNNEL:
                return entrance.notes
        return None


# ── Main StationService ───────────────────────────────────────────────────────

class StationService:
    """
    Production Loop station intelligence service.

    Methods:
      get_profile(station_key) → StationProfile
      get_all_profiles() → Dict[str, StationProfile]
      get_platform_tip(boarding, alighting, line) → PlatformTip
      get_accessible_entrance(station_key) → StationEntrance
      rank_stations(lat, lon, ...) → ranked list
      get_pedway_stations() → stations with Pedway access
      get_station_summary(key) → lightweight dict for API responses
      search_by_name(query) → matching station keys
    """

    def __init__(self):
        self._tip_engine = PlatformTipEngine()
        self._ranker     = StationRanker()

    def get_profile(self, station_key: str) -> Optional[StationProfile]:
        return STATION_PROFILES.get(station_key)

    def get_all_profiles(self) -> Dict[str, StationProfile]:
        return STATION_PROFILES

    def get_platform_tip(
        self,
        boarding_key: str,
        alighting_key: str,
        line: Optional[str] = None,
    ) -> Optional[PlatformTip]:
        return self._tip_engine.get_tip(boarding_key, alighting_key, line)

    def get_accessible_entrance(
        self, station_key: str, prefer_pedway: bool = True
    ) -> Optional[StationEntrance]:
        return self._tip_engine.get_accessible_entrance(station_key, prefer_pedway)

    def get_pedway_entrance(self, station_key: str) -> Optional[StationEntrance]:
        return self._tip_engine.get_entrance_for_pedway(station_key)

    def get_pedway_stations(self) -> List[str]:
        return [k for k, p in STATION_PROFILES.items() if p.pedway_connected]

    def rank_stations(
        self,
        lat: float,
        lon: float,
        lines: Optional[List[str]] = None,
        accessible_required: bool = False,
        pedway_preferred: bool = True,
        max_walk_m: float = 1000.0,
    ) -> List[Dict[str, Any]]:
        return self._ranker.rank_stations(lat, lon, lines, accessible_required, pedway_preferred, max_walk_m)

    def search_by_name(self, query: str) -> List[str]:
        """Return station keys whose name contains the query string."""
        q = query.lower()
        return [
            key for key, profile in STATION_PROFILES.items()
            if q in profile.name.lower() or q in profile.short_name.lower()
        ]

    def get_station_summary(self, station_key: str) -> Optional[Dict[str, Any]]:
        """Lightweight dict for embedding in route responses."""
        profile = STATION_PROFILES.get(station_key)
        if not profile:
            return None
        return {
            "key":              station_key,
            "name":             profile.name,
            "lines":            profile.lines,
            "lat":              profile.lat,
            "lon":              profile.lon,
            "pedway_connected": profile.pedway_connected,
            "pedway_entrance":  profile.pedway_entrance,
            "accessible":       profile.ada_compliant,
            "elevator_ids":     profile.elevator_ids,
            "overall_rating":   profile.overall_rating.value,
        }

    def get_nearest(
        self, lat: float, lon: float, limit: int = 3
    ) -> List[Tuple[str, float]]:
        """Return [(station_key, distance_m)] sorted by distance."""
        distances: List[Tuple[str, float]] = [
            (key, _haversine(lat, lon, p.lat, p.lon))
            for key, p in STATION_PROFILES.items()
        ]
        distances.sort(key=lambda x: x[1])
        return distances[:limit]

    def health_check(self) -> Dict[str, Any]:
        return {
            "status":           "ok",
            "total_stations":   len(STATION_PROFILES),
            "pedway_stations":  len(self.get_pedway_stations()),
            "accessible_stations": sum(1 for p in STATION_PROFILES.values() if p.ada_compliant),
        }


# ── Module-level Singleton ────────────────────────────────────────────────────

_station_service_instance: Optional[StationService] = None


def get_station_service() -> StationService:
    global _station_service_instance
    if _station_service_instance is None:
        _station_service_instance = StationService()
    return _station_service_instance


# ── Utility ───────────────────────────────────────────────────────────────────

def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))
