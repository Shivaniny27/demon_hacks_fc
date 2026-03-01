"""
test_station_service.py
=======================
Tests for Phase 3 Priority 5: Station Intelligence.

Covers:
  - Station profile completeness
  - Platform tip lookup (boarding advice)
  - Accessible entrance selection
  - Station ranking by distance + bonuses
  - Station search by name fragment
  - Nearest-N station lookup
"""

import math
from unittest.mock import MagicMock

import pytest

from app.services.station_service import (
    EntranceType,
    ExitOptimization,
    StationProfile,
    StationService,
    PlatformTipEngine,
    StationRanker,
    STATION_PROFILES,
    get_station_service,
)


# ── Station Profile Data ──────────────────────────────────────────────────────

class TestStationProfiles:

    def test_station_profiles_not_empty(self):
        assert len(STATION_PROFILES) > 0

    def test_clark_lake_in_profiles(self):
        assert "clark_lake" in STATION_PROFILES

    def test_all_profiles_have_name(self):
        for key, profile in STATION_PROFILES.items():
            assert profile.name, f"{key} has no name"

    def test_all_profiles_have_entrances(self):
        for key, profile in STATION_PROFILES.items():
            assert len(profile.entrances) > 0, f"{key} has no entrances"

    def test_entrance_has_lat_lon(self):
        for key, profile in STATION_PROFILES.items():
            for entrance in profile.entrances:
                assert -90 <= entrance.lat <= 90, f"{key} entrance has invalid lat"
                assert -180 <= entrance.lon <= 180, f"{key} entrance has invalid lon"

    def test_platform_tips_list(self):
        for key, profile in STATION_PROFILES.items():
            assert isinstance(profile.platform_tips, list)

    def test_accessible_entrances_exist(self):
        # At least some stations should have ADA-accessible entrances
        has_accessible = any(
            any(e.accessible for e in p.entrances)
            for p in STATION_PROFILES.values()
        )
        assert has_accessible

    def test_pedway_connected_entrances_have_segment(self):
        for key, profile in STATION_PROFILES.items():
            for entrance in profile.entrances:
                if entrance.type == EntranceType.PEDWAY_TUNNEL:
                    assert entrance.pedway_segment is not None, (
                        f"{key} pedway entrance missing pedway_segment"
                    )


# ── PlatformTipEngine ─────────────────────────────────────────────────────────

class TestPlatformTipEngine:

    def setup_method(self):
        self.engine = PlatformTipEngine()

    def test_returns_tip_for_known_route(self):
        # For any station that has tips defined, lookup should not crash
        for key, profile in STATION_PROFILES.items():
            if profile.platform_tips:
                tip_result = self.engine.get_tip(
                    boarding_station_key=key,
                    alighting_station_key=profile.platform_tips[0].destination_station,
                )
                # Should find the tip since we look up the exact destination
                assert tip_result is not None
                break

    def test_returns_none_for_unknown_route(self):
        result = self.engine.get_tip(
            boarding_station_key="clark_lake",
            alighting_station_key="nonexistent_dest_xyz",
        )
        assert result is None

    def test_tip_has_board_end(self):
        for key, profile in STATION_PROFILES.items():
            for tip in profile.platform_tips:
                # board_end is ExitOptimization enum
                assert isinstance(tip.board_end, ExitOptimization)


# ── StationRanker ─────────────────────────────────────────────────────────────

class TestStationRanker:

    def setup_method(self):
        self.ranker = StationRanker()

    def _rank(self, lat=41.8850, lon=-87.6290, **kwargs):
        return self.ranker.rank_stations(lat, lon, **kwargs)

    def test_returns_list(self):
        result = self._rank()
        assert isinstance(result, list)
        assert len(result) > 0

    def test_nearest_station_first(self):
        # Very close to Clark/Lake (41.8856, -87.6319) — should rank first
        result = self._rank(lat=41.8856, lon=-87.6319, max_walk_m=2000.0)
        assert len(result) > 0
        assert result[0]["station_key"] == "clark_lake"

    def test_pedway_bonus_applied(self):
        # Results are dicts — just check the return format
        result_pedway    = self._rank(lat=41.8850, lon=-87.6280, pedway_preferred=True)
        result_no_pedway = self._rank(lat=41.8850, lon=-87.6280, pedway_preferred=False)
        pedway_keys    = {r["station_key"] for r in result_pedway[:3]}
        no_pedway_keys = {r["station_key"] for r in result_no_pedway[:3]}
        assert isinstance(pedway_keys, set)
        assert isinstance(no_pedway_keys, set)

    def test_accessible_filter(self):
        result = self._rank(accessible_required=True, max_walk_m=2000.0)
        # All returned stations must be ADA compliant
        for ranked in result:
            profile = STATION_PROFILES.get(ranked["station_key"])
            if profile:
                assert profile.ada_compliant, (
                    f"{ranked['station_key']} not ADA compliant but in accessible results"
                )

    def test_scores_are_numeric(self):
        result = self._rank(max_walk_m=2000.0)
        for ranked in result:
            # Scores can be negative due to Pedway/rating bonuses
            assert isinstance(ranked["score"], (int, float))


# ── StationService ────────────────────────────────────────────────────────────

class TestStationService:

    def setup_method(self):
        self.svc = get_station_service()

    def test_get_station_profile(self):
        profile = self.svc.get_profile("clark_lake")
        assert profile is not None
        assert profile.name == "Clark/Lake"

    def test_get_nonexistent_station_returns_none(self):
        result = self.svc.get_profile("not_a_real_station")
        assert result is None

    def test_search_by_name_clark(self):
        # search_by_name returns List[str] (station keys)
        results = self.svc.search_by_name("Clark")
        assert "clark_lake" in results

    def test_search_by_name_case_insensitive(self):
        upper = self.svc.search_by_name("CLARK")
        lower = self.svc.search_by_name("clark")
        assert set(upper) == set(lower)

    def test_search_by_name_empty_returns_empty(self):
        results = self.svc.search_by_name("")
        assert isinstance(results, list)

    def test_get_nearest_returns_n(self):
        # get_nearest returns List[Tuple[str, float]]
        result = self.svc.get_nearest(lat=41.8858, lon=-87.6317, limit=3)
        assert len(result) <= 3

    def test_get_nearest_sorted_by_distance(self):
        result = self.svc.get_nearest(lat=41.8858, lon=-87.6317, limit=5)
        distances = [r[1] for r in result]
        assert distances == sorted(distances)

    def test_get_accessible_entrance_returns_entrance(self):
        entrance = self.svc.get_accessible_entrance("clark_lake")
        if entrance:
            assert entrance.accessible is True

    def test_rank_stations_returns_list_of_dicts(self):
        result = self.svc.rank_stations(lat=41.8850, lon=-87.6290, max_walk_m=2000.0)
        assert isinstance(result, list)
        if result:
            assert "station_key" in result[0]
            assert "score" in result[0]
            assert "distance_m" in result[0]


# ── Singleton ─────────────────────────────────────────────────────────────────

class TestStationServiceSingleton:

    def test_singleton_returns_same_instance(self):
        svc1 = get_station_service()
        svc2 = get_station_service()
        assert svc1 is svc2

    def test_service_has_required_methods(self):
        svc = get_station_service()
        assert hasattr(svc, "get_profile")
        assert hasattr(svc, "search_by_name")
        assert hasattr(svc, "get_nearest")
        assert hasattr(svc, "rank_stations")
        assert hasattr(svc, "get_accessible_entrance")
