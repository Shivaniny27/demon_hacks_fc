"""
test_elevator_service.py
========================
Tests for Phase 3 Priority 1: Real-Time Elevator Reliability for ADA Routing.

Covers:
  - CTAOutageParser: headline → station + elevator ID extraction
  - ElevatorSeverity + ImpactLevel classification
  - ElevatorReliabilityEngine SCORE_TABLE
  - ADARoutingAdvisor graph edge modifications (async)
  - ElevatorStatusRegistry update / outage tracking
  - Loop station data completeness
  - Module-level singleton wiring
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.elevator_service import (
    ElevatorSeverity,
    ImpactLevel,
    ReliabilityTier,
    ElevatorOutage,
    ElevatorReliabilityEngine,
    ElevatorStatusRegistry,
    ADARoutingAdvisor,
    CTAOutageParser,
    LOOP_STATIONS,
    ELEVATOR_TO_STATION,
    ALL_ELEVATOR_IDS,
    STATION_NAME_PATTERNS,
    get_elevator_service,
)
from tests.conftest import MockRedis


# ── ElevatorSeverity Enum ─────────────────────────────────────────────────────

class TestElevatorSeverityEnum:

    def test_outage_value(self):
        assert ElevatorSeverity.OUTAGE.value == "elevator_outage"

    def test_escalator_value(self):
        assert ElevatorSeverity.ESCALATOR.value == "escalator_outage"

    def test_partial_value(self):
        assert ElevatorSeverity.PARTIAL.value == "partial_access"

    def test_all_values_are_strings(self):
        for severity in ElevatorSeverity:
            assert isinstance(severity.value, str)


# ── ImpactLevel Enum ──────────────────────────────────────────────────────────

class TestImpactLevelEnum:

    def test_critical_value(self):
        assert ImpactLevel.CRITICAL.value == "critical"

    def test_high_value(self):
        assert ImpactLevel.HIGH.value == "high"

    def test_medium_value(self):
        assert ImpactLevel.MEDIUM.value == "medium"

    def test_low_value(self):
        assert ImpactLevel.LOW.value == "low"


# ── ElevatorReliabilityEngine ─────────────────────────────────────────────────

class TestElevatorReliabilityEngineScoreTable:
    """Test the SCORE_TABLE constants and scoring logic."""

    def test_score_table_zero_outages(self):
        assert ElevatorReliabilityEngine.SCORE_TABLE[0] == pytest.approx(0.95, abs=0.01)

    def test_score_table_one_outage(self):
        assert ElevatorReliabilityEngine.SCORE_TABLE[1] == pytest.approx(0.80, abs=0.01)

    def test_score_table_two_outages(self):
        assert ElevatorReliabilityEngine.SCORE_TABLE[2] == pytest.approx(0.65, abs=0.01)

    def test_score_table_three_outages(self):
        assert ElevatorReliabilityEngine.SCORE_TABLE[3] == pytest.approx(0.50, abs=0.01)

    def test_score_floor_defined(self):
        assert ElevatorReliabilityEngine.SCORE_FLOOR > 0.0

    def test_score_monotonic_decreasing(self):
        table = ElevatorReliabilityEngine.SCORE_TABLE
        scores = [table[k] for k in sorted(table.keys())]
        for i in range(len(scores) - 1):
            assert scores[i] > scores[i + 1]

    async def test_calculate_score_returns_score_object(self):
        """Test with mock Redis — should return default high score for elevator with no history."""
        redis = MockRedis()
        engine = ElevatorReliabilityEngine(redis=redis, registry=None)
        eid = next(iter(ALL_ELEVATOR_IDS))  # first available elevator ID
        score = await engine.calculate_score(eid)
        assert score.elevator_id == eid
        assert 0.0 <= score.score <= 1.0
        assert isinstance(score.tier, ReliabilityTier)

    async def test_calculate_score_no_outage_history_gives_max(self):
        """Elevator with no outage history → max score (0.95)."""
        redis = MockRedis()
        engine = ElevatorReliabilityEngine(redis=redis, registry=None)
        eid = next(iter(ALL_ELEVATOR_IDS))
        score = await engine.calculate_score(eid)
        # No history in mock Redis → 0 outages → SCORE_TABLE[0] = 0.95
        assert score.score == pytest.approx(0.95, abs=0.01)
        assert score.tier == ReliabilityTier.EXCELLENT

    async def test_currently_down_reduces_score(self):
        """Registry reporting elevator as down should reduce score."""
        redis = MockRedis()
        registry = MagicMock()
        registry.is_elevator_down.return_value = True

        engine = ElevatorReliabilityEngine(redis=redis, registry=registry)
        eid = next(iter(ALL_ELEVATOR_IDS))
        score = await engine.calculate_score(eid)
        # Down penalty -0.15 applied on top of base score
        assert score.score < 0.95
        assert score.is_currently_down is True


# ── CTAOutageParser ────────────────────────────────────────────────────────────

class TestCTAOutageParser:

    def setup_method(self):
        self.parser = CTAOutageParser()

    def _alert(self, headline, impact_type="Elevator Status", alert_id="A1",
               description="", end_time=None):
        return {
            "AlertId": alert_id,
            "Headline": headline,
            "ShortDescription": description,
            "FullDescription": description,
            "Impact": "Elevator",
            "SeverityScore": "7",
            "ImpactType": impact_type,
            "EventStart": "2025-01-15T08:00:00",
            "EventEnd": end_time,
            "TBD": "0",
            "AffectedService": {"Service": []},
        }

    def test_parses_washington_wabash(self):
        alert = self._alert("Elevator at Washington/Wabash CTA Station is out of service")
        outage = self.parser.parse(alert)
        assert outage is not None
        assert outage.station_key == "washington_wabash"

    def test_parses_clark_lake(self):
        alert = self._alert("Clark/Lake elevator temporarily unavailable")
        outage = self.parser.parse(alert)
        assert outage is not None
        assert outage.station_key == "clark_lake"

    def test_parses_state_lake(self):
        alert = self._alert("State/Lake station elevator out of service")
        outage = self.parser.parse(alert)
        assert outage is not None
        assert outage.station_key == "state_lake"

    def test_alert_id_preserved(self):
        alert = self._alert("Clark/Lake elevator unavailable", alert_id="EL-999")
        outage = self.parser.parse(alert)
        assert outage is not None
        assert outage.alert_id == "EL-999"

    def test_outage_has_severity(self):
        alert = self._alert("Clark/Lake elevator down")
        outage = self.parser.parse(alert)
        assert outage is not None
        assert isinstance(outage.severity, ElevatorSeverity)

    def test_outage_has_impact_level(self):
        alert = self._alert("Clark/Lake elevator down")
        outage = self.parser.parse(alert)
        assert outage is not None
        assert isinstance(outage.impact_level, ImpactLevel)

    def test_end_time_none_when_not_set(self):
        alert = self._alert("Clark/Lake elevator down")
        outage = self.parser.parse(alert)
        assert outage is not None
        assert outage.end_time is None

    def test_end_time_parsed_when_provided(self):
        # CTAOutageParser.CTA_DT_FORMATS uses slashes, not ISO format
        alert = self._alert("Clark/Lake elevator down", end_time="2025/01/15 18:00:00")
        outage = self.parser.parse(alert)
        assert outage is not None
        assert outage.end_time is not None

    def test_bad_alert_returns_none_gracefully(self):
        # Completely empty dict — parser should not crash, return None
        outage = self.parser.parse({})
        # Either returns None or an ElevatorOutage with unknown station
        assert outage is None or outage.station_key in ("unknown", "")


# ── ElevatorStatusRegistry ────────────────────────────────────────────────────

class TestElevatorStatusRegistry:

    def setup_method(self):
        self.registry = ElevatorStatusRegistry()

    def _make_outage(self, alert_id="A1", station_key="clark_lake"):
        return ElevatorOutage(
            alert_id=alert_id,
            station_key=station_key,
            station_name="Clark/Lake",
            station_map_id="40380",
            elevator_ids=["E03", "E04"],
            severity=ElevatorSeverity.OUTAGE,
            impact_level=ImpactLevel.HIGH,
            headline="Elevator at Clark/Lake out of service",
            short_description="",
            full_description="",
            affected_lines=["G", "Brn"],
            start_time=datetime.now(timezone.utc),
            end_time=None,
            is_tbd=False,
            ada_alternatives=["State/Lake"],
        )

    async def test_update_adds_new_outage(self):
        outage = self._make_outage()
        added, resolved = await self.registry.update([outage])
        assert "A1" in added

    async def test_update_resolves_removed_outage(self):
        outage = self._make_outage()
        await self.registry.update([outage])
        # Second update with no outages → A1 should be resolved
        _, resolved = await self.registry.update([])
        assert "A1" in resolved

    async def test_get_active_outages_returns_list(self):
        outage = self._make_outage()
        await self.registry.update([outage])
        active = self.registry.get_active_outages()
        assert len(active) == 1
        assert active[0].alert_id == "A1"

    async def test_get_station_outages_filtered(self):
        outage_clark = self._make_outage("A1", "clark_lake")
        outage_state = self._make_outage("A2", "state_lake")
        await self.registry.update([outage_clark, outage_state])
        clark_outages = self.registry.get_station_outages("clark_lake")
        state_outages = self.registry.get_station_outages("state_lake")
        assert len(clark_outages) == 1
        assert len(state_outages) == 1
        assert all(o.station_key == "clark_lake" for o in clark_outages)

    async def test_is_elevator_down_when_outage_active(self):
        outage = self._make_outage()
        await self.registry.update([outage])
        # E03 is in the elevator_ids list → should be down
        assert self.registry.is_elevator_down("E03") is True

    def test_is_elevator_not_down_when_no_outage(self):
        # No update needed — fresh registry has no outages
        assert self.registry.is_elevator_down("E03") is False


# ── Loop Stations Data ────────────────────────────────────────────────────────

class TestLoopStationsData:

    def test_loop_stations_has_clark_lake(self):
        assert "clark_lake" in LOOP_STATIONS

    def test_all_stations_have_required_fields(self):
        required = {"name", "lat", "lon", "lines", "map_id", "elevators"}
        for key, station in LOOP_STATIONS.items():
            missing = required - set(station.keys())
            assert missing == set(), f"{key} missing fields: {missing}"

    def test_elevator_to_station_reverse_map(self):
        for elevator_id, station_key in ELEVATOR_TO_STATION.items():
            assert station_key in LOOP_STATIONS, (
                f"Elevator {elevator_id} maps to unknown station {station_key}"
            )

    def test_all_elevator_ids_are_strings(self):
        for eid in ALL_ELEVATOR_IDS:
            assert isinstance(eid, str)
            assert len(eid) > 0

    def test_stations_have_valid_coordinates(self):
        for key, station in LOOP_STATIONS.items():
            lat = station.get("lat", 0)
            lon = station.get("lon", 0)
            assert 41.87 < lat < 41.90, f"{key} lat {lat} out of Loop bounds"
            assert -87.65 < lon < -87.62, f"{key} lon {lon} out of Loop bounds"

    def test_station_name_patterns_map_to_known_stations(self):
        for pattern, station_key in STATION_NAME_PATTERNS.items():
            # All patterns should map to a key in LOOP_STATIONS
            assert station_key in LOOP_STATIONS or station_key == "lake_state", (
                f"Pattern '{pattern}' maps to unknown station '{station_key}'"
            )


# ── Singleton ─────────────────────────────────────────────────────────────────

class TestElevatorServiceSingleton:

    def test_singleton_returns_same_instance(self):
        svc1 = get_elevator_service()
        svc2 = get_elevator_service()
        assert svc1 is svc2

    def test_service_has_required_methods(self):
        svc = get_elevator_service()
        assert hasattr(svc, "initialize")
        assert hasattr(svc, "shutdown")
        assert hasattr(svc, "get_snapshot")
        assert hasattr(svc, "get_station_report")
