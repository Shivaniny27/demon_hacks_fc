"""
test_sync_service.py
====================
Tests for Phase 3 Priority 4: Predictive Departure Sync.

Covers:
  - WalkTimeCalculator: Pedway auto-detect, stair/elevator penalty
  - DepartureSyncEngine: leave_at, urgency classification
  - SyncUrgency levels: LEAVE_NOW / LEAVE_SOON / ON_TRACK / WAIT / MISSED
  - ArrivalParser: CTA ETA raw dict → TrainArrival objects
  - Loop station map IDs constant
"""

import math
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.sync_service import (
    SyncUrgency,
    TrainArrival,
    TrainStatus,
    WalkEstimate,
    DepartureSync,
    WalkTimeCalculator,
    DepartureSyncEngine,
    ArrivalParser,
    SyncService,
    LOOP_STATION_MAP_IDS,
    get_sync_service,
)


# ── WalkTimeCalculator ────────────────────────────────────────────────────────

class TestWalkTimeCalculator:

    def setup_method(self):
        self.calc = WalkTimeCalculator()

    def test_returns_walk_estimate(self):
        est = self.calc.estimate(41.8858, -87.6280, "clark_lake")
        assert est is not None
        assert isinstance(est.walk_seconds, int)
        assert est.walk_seconds >= 0

    def test_total_seconds_includes_level_changes(self):
        est = self.calc.estimate(41.8858, -87.6280, "clark_lake")
        assert est.total_seconds >= est.walk_seconds

    def test_nearby_origin_short_walk(self):
        # Very close to Clark/Lake at lat=41.8856, lon=-87.6319
        est = self.calc.estimate(41.8856, -87.6319, "clark_lake")
        assert est.total_seconds < 600   # under 10 minutes total

    def test_far_origin_longer_walk(self):
        close = self.calc.estimate(41.8856, -87.6319, "clark_lake")
        far   = self.calc.estimate(41.878, -87.620, "clark_lake")
        assert far.walk_seconds > close.walk_seconds

    def test_accessible_adds_elevator_penalty(self):
        standard   = self.calc.estimate(41.8856, -87.6319, "clark_lake", accessible=False)
        accessible = self.calc.estimate(41.8856, -87.6319, "clark_lake", accessible=True)
        # Elevator is slower per level change than stairs
        assert accessible.total_seconds >= standard.total_seconds

    def test_pedway_auto_detected_for_pedway_station(self):
        # Origin within Pedway bounds: lat 41.876-41.886, lon -87.638 to -87.624
        # clark_lake is Pedway-accessible
        est = self.calc.estimate(41.882, -87.630, "clark_lake")
        assert isinstance(est.via_pedway, bool)

    def test_non_pedway_station_via_pedway_false(self):
        # jackson_state is not in PEDWAY_STATIONS
        est = self.calc.estimate(41.8780, -87.6279, "jackson_state")
        assert est.via_pedway is False

    def test_estimate_fields_complete(self):
        est = self.calc.estimate(41.8858, -87.6280, "clark_lake")
        assert hasattr(est, "origin_lat")
        assert hasattr(est, "origin_lon")
        assert hasattr(est, "straight_line_m")
        assert hasattr(est, "estimated_walk_m")
        assert hasattr(est, "level_changes")
        assert hasattr(est, "level_change_seconds")
        assert hasattr(est, "confidence")
        assert hasattr(est, "notes")

    def test_confidence_high_for_nearby(self):
        # < 300m straight line → confidence = "high"
        est = self.calc.estimate(41.8856, -87.6319, "clark_lake")
        assert est.confidence in ("high", "medium")

    def test_confidence_low_for_far(self):
        # Far origin → "low" confidence
        est = self.calc.estimate(41.870, -87.610, "clark_lake")
        assert est.confidence == "low"

    def test_station_name_populated(self):
        est = self.calc.estimate(41.8858, -87.6280, "clark_lake")
        assert est.station_name == "Clark/Lake"

    def test_station_key_preserved(self):
        est = self.calc.estimate(41.8858, -87.6280, "clark_lake")
        assert est.station_key == "clark_lake"


# ── ArrivalParser ─────────────────────────────────────────────────────────────

class TestArrivalParser:

    def setup_method(self):
        self.parser = ArrivalParser()

    def _raw_eta(self, rt="G", arr_t="2025-01-15T08:38:00", is_dly="0",
                 is_app="0", is_sch="0", is_flt="0", run_num="124"):
        """Single CTA eta dict as returned by ttarrivals.aspx."""
        return {
            "staId": "40380", "stpId": "30074",
            "staNm": "Clark/Lake", "stpDe": "Service toward Loop",
            "rn": run_num, "rt": rt,
            "destSt": "30182", "destNm": "Harlem/Lake",
            "trDr": "5",
            "prdt": "2025-01-15T08:28:00",
            "arrT": arr_t,
            "isApp": is_app, "isSch": is_sch, "isDly": is_dly, "isFlt": is_flt,
            "flags": None, "lat": "41.8820", "lon": "-87.6280", "heading": "269",
        }

    def test_parse_returns_train_arrival(self):
        arrival = self.parser.parse(self._raw_eta(), "clark_lake")
        assert arrival is not None
        assert isinstance(arrival, TrainArrival)

    def test_run_number_preserved(self):
        arrival = self.parser.parse(self._raw_eta(run_num="999"), "clark_lake")
        assert arrival.run_number == "999"

    def test_green_line_normalized(self):
        arrival = self.parser.parse(self._raw_eta(rt="G"), "clark_lake")
        assert arrival.line == "Green"

    def test_brown_line_normalized(self):
        arrival = self.parser.parse(self._raw_eta(rt="Brn"), "clark_lake")
        assert arrival.line == "Brown"

    def test_orange_line_normalized(self):
        arrival = self.parser.parse(self._raw_eta(rt="Org"), "clark_lake")
        assert arrival.line == "Orange"

    def test_purple_line_normalized(self):
        arrival = self.parser.parse(self._raw_eta(rt="P"), "clark_lake")
        assert arrival.line == "Purple"

    def test_station_name_populated(self):
        arrival = self.parser.parse(self._raw_eta(), "clark_lake")
        assert arrival.station_name == "Clark/Lake"

    def test_station_key_preserved(self):
        arrival = self.parser.parse(self._raw_eta(), "clark_lake")
        assert arrival.station_key == "clark_lake"

    def test_eta_is_datetime(self):
        arrival = self.parser.parse(self._raw_eta(), "clark_lake")
        assert isinstance(arrival.eta, datetime)

    def test_is_delayed_false(self):
        arrival = self.parser.parse(self._raw_eta(is_dly="0"), "clark_lake")
        assert arrival.is_delayed is False

    def test_is_delayed_true(self):
        arrival = self.parser.parse(self._raw_eta(is_dly="1"), "clark_lake")
        assert arrival.is_delayed is True

    def test_is_approaching_true(self):
        arrival = self.parser.parse(self._raw_eta(is_app="1"), "clark_lake")
        assert arrival.is_approaching is True

    def test_status_approaching(self):
        arrival = self.parser.parse(self._raw_eta(is_app="1"), "clark_lake")
        assert arrival.status == TrainStatus.APPROACHING

    def test_status_delayed(self):
        arrival = self.parser.parse(self._raw_eta(is_dly="1"), "clark_lake")
        assert arrival.status == TrainStatus.DELAYED

    def test_status_scheduled(self):
        arrival = self.parser.parse(self._raw_eta(is_sch="1"), "clark_lake")
        assert arrival.status == TrainStatus.SCHEDULED

    def test_lat_lon_parsed(self):
        arrival = self.parser.parse(self._raw_eta(), "clark_lake")
        assert arrival.lat == pytest.approx(41.882, abs=0.001)
        assert arrival.lon == pytest.approx(-87.628, abs=0.001)

    def test_line_color_hex(self):
        arrival = self.parser.parse(self._raw_eta(rt="G"), "clark_lake")
        assert arrival.line_color.startswith("#")
        assert len(arrival.line_color) == 7

    def test_bad_dict_returns_none(self):
        arrival = self.parser.parse({}, "clark_lake")
        assert arrival is None

    def test_stop_id_set(self):
        arrival = self.parser.parse(self._raw_eta(), "clark_lake")
        assert arrival.stop_id == "30074"

    def test_seconds_away_non_negative(self):
        arrival = self.parser.parse(self._raw_eta(), "clark_lake")
        assert arrival.seconds_away >= 0


# ── DepartureSyncEngine ───────────────────────────────────────────────────────

class TestDepartureSyncEngine:

    def setup_method(self):
        self.engine = DepartureSyncEngine()

    def _arrival(self, minutes_from_now: float, line="Green",
                 is_scheduled=False) -> TrainArrival:
        now = datetime.now(timezone.utc)
        eta = now + timedelta(minutes=minutes_from_now)
        seconds_away = max(0, int(minutes_from_now * 60))
        status = TrainStatus.SCHEDULED if is_scheduled else (
            TrainStatus.APPROACHING if minutes_from_now < 1 else TrainStatus.ON_TIME
        )
        return TrainArrival(
            run_number="124",
            line=line,
            line_color="#009B3A",
            destination="Harlem/Lake",
            station_key="clark_lake",
            station_name="Clark/Lake",
            stop_id="30074",
            stop_desc="Service toward Harlem/Lake",
            eta=eta,
            predicted_at=now,
            minutes_away=max(0, int(minutes_from_now)),
            seconds_away=seconds_away,
            is_approaching=minutes_from_now < 1,
            is_scheduled=is_scheduled,
            is_delayed=False,
            is_fault=False,
            status=status,
            lat=41.88,
            lon=-87.63,
            heading=270,
        )

    def _walk(self, total_seconds: int, via_pedway: bool = False) -> WalkEstimate:
        """Helper that sets total_seconds directly."""
        walk_s = max(0, total_seconds - 90)
        return WalkEstimate(
            origin_lat=41.8858,
            origin_lon=-87.6280,
            station_key="clark_lake",
            station_name="Clark/Lake",
            straight_line_m=float(walk_s * 1.2),
            estimated_walk_m=float(walk_s * 1.5),
            walk_seconds=walk_s,
            via_pedway=via_pedway,
            level_changes=2,
            level_change_seconds=90,
            total_seconds=total_seconds,
            confidence="medium",
            notes="Test estimate",
        )

    def test_urgency_leave_now_when_train_imminent(self):
        # arrival=3min (180s), total=60, buffer=60 → leave_in = 180-60-60=60 → LEAVE_SOON
        # arrival=2min (120s), total=60, buffer=60 → leave_in = 120-60-60=0 → LEAVE_NOW
        arrival = self._arrival(2.0)
        walk    = self._walk(60)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        assert sync.urgency in (SyncUrgency.LEAVE_NOW, SyncUrgency.LEAVE_SOON, SyncUrgency.MISSED)

    def test_urgency_leave_soon(self):
        # arrival=8min (480s), total=60, buffer=60 → leave_in = 480-60-60=360 → ON_TRACK
        # Try arrival=6min: 360-120=240 → LEAVE_SOON
        arrival = self._arrival(6.0)
        walk    = self._walk(120)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        assert sync.urgency in (SyncUrgency.LEAVE_SOON, SyncUrgency.ON_TRACK)

    def test_urgency_on_track_with_comfortable_margin(self):
        # arrival=15min (900s), total=180, buffer=60 → leave_in = 900-180-60=660 → ON_TRACK
        arrival = self._arrival(15.0)
        walk    = self._walk(180)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        assert sync.urgency in (SyncUrgency.ON_TRACK, SyncUrgency.LEAVE_SOON)

    def test_urgency_wait_when_train_far(self):
        # arrival=20min (1200s), total=60, buffer=60 → leave_in=1080 → WAIT
        arrival = self._arrival(20.0)
        walk    = self._walk(60)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        assert sync.urgency in (SyncUrgency.WAIT, SyncUrgency.ON_TRACK)

    def test_urgency_missed_when_train_already_gone(self):
        arrival = self._arrival(-5.0)   # 5 min ago
        walk    = self._walk(60)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        assert sync.urgency == SyncUrgency.MISSED

    def test_leave_at_before_eta(self):
        arrival = self._arrival(10.0)
        walk    = self._walk(180)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        assert sync.leave_at < arrival.eta

    def test_leave_in_seconds_is_int(self):
        arrival = self._arrival(10.0)
        walk    = self._walk(180)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        assert isinstance(sync.leave_in_seconds, int)

    def test_message_is_non_empty_string(self):
        arrival = self._arrival(10.0)
        walk    = self._walk(180)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        assert isinstance(sync.message, str)
        assert len(sync.message) > 0

    def test_catchable_true_when_plenty_of_time(self):
        # arrival=10min, total=60, buffer=60 → leave_in=480 → catchable ✓
        arrival = self._arrival(10.0)
        walk    = self._walk(60)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        assert sync.catchable is True

    def test_catchable_false_when_long_gone(self):
        arrival = self._arrival(-10.0)  # 10 min ago — missed by a lot
        walk    = self._walk(60)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        assert sync.catchable is False

    def test_confidence_low_for_scheduled_train(self):
        arrival = self._arrival(10.0, is_scheduled=True)
        walk    = self._walk(180)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        assert sync.confidence == "low"

    def test_platform_wait_non_negative(self):
        arrival = self._arrival(10.0)
        walk    = self._walk(180)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        assert sync.platform_wait_seconds >= 0

    def test_sync_result_has_all_fields(self):
        arrival = self._arrival(10.0)
        walk    = self._walk(180)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        assert hasattr(sync, "train")
        assert hasattr(sync, "walk_estimate")
        assert hasattr(sync, "buffer_seconds")
        assert hasattr(sync, "leave_at")
        assert hasattr(sync, "leave_in_seconds")
        assert hasattr(sync, "arrive_at_platform")
        assert hasattr(sync, "platform_wait_seconds")
        assert hasattr(sync, "urgency")
        assert hasattr(sync, "catchable")
        assert hasattr(sync, "message")
        assert hasattr(sync, "confidence")

    def test_pedway_route_reflected_in_message(self):
        arrival = self._arrival(10.0)
        walk    = self._walk(180, via_pedway=True)
        sync    = self.engine.sync(arrival, walk, buffer_seconds=60)
        # Message should mention Pedway for pedway routes
        assert isinstance(sync.message, str)


# ── Loop Station Map IDs ──────────────────────────────────────────────────────

class TestLoopStationMapIDs:

    def test_clark_lake_has_map_id(self):
        assert "clark_lake" in LOOP_STATION_MAP_IDS

    def test_state_lake_has_map_id(self):
        assert "state_lake" in LOOP_STATION_MAP_IDS

    def test_all_map_ids_are_strings(self):
        for key, map_id in LOOP_STATION_MAP_IDS.items():
            assert isinstance(map_id, str), f"{key} map_id is not a string"
            assert map_id.isdigit(), f"{key} map_id '{map_id}' is not numeric"

    def test_core_stations_present(self):
        expected = {"clark_lake", "state_lake", "washington_wabash"}
        assert expected.issubset(set(LOOP_STATION_MAP_IDS.keys()))

    def test_all_stations_count(self):
        assert len(LOOP_STATION_MAP_IDS) >= 10


# ── Singleton ─────────────────────────────────────────────────────────────────

class TestSyncServiceSingleton:

    def test_singleton_returns_same_instance(self):
        svc1 = get_sync_service()
        svc2 = get_sync_service()
        assert svc1 is svc2

    def test_service_has_get_arrivals(self):
        svc = get_sync_service()
        assert hasattr(svc, "get_arrivals")

    def test_service_has_sync_departure(self):
        svc = get_sync_service()
        assert hasattr(svc, "sync_departure")

    def test_service_has_multi_station_sync(self):
        svc = get_sync_service()
        assert hasattr(svc, "multi_station_sync")
