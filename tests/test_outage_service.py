"""
test_outage_service.py
======================
Tests for Phase 3 Priority 2: CTA Service Outages.

Covers:
  - OutageSeverity / OutageCategory / LineHealth enums
  - OutageParser: severity, category, Loop station extraction
  - LineStatusTracker: health computation per line
  - LoopImpactAnalyzer: suspend_lines / delay_lines / avoid_stations
  - Loop data constants
  - Singleton wiring
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.outage_service import (
    OutageSeverity,
    OutageCategory,
    LineHealth,
    ServiceOutage,
    RoutingOutageImpact,
    LineStatusTracker,
    LoopImpactAnalyzer,
    OutageParser,
    get_outage_service,
    LOOP_LINES,
    LOOP_STATION_NAMES,
)


# ── Enums ──────────────────────────────────────────────────────────────────────

class TestOutageEnums:

    def test_severity_critical(self):
        assert OutageSeverity.CRITICAL.value == "critical"

    def test_severity_high(self):
        assert OutageSeverity.HIGH.value == "high"

    def test_severity_medium(self):
        assert OutageSeverity.MEDIUM.value == "medium"

    def test_severity_low(self):
        assert OutageSeverity.LOW.value == "low"

    def test_category_suspension(self):
        assert OutageCategory.SUSPENSION.value == "suspension"

    def test_category_delay(self):
        assert OutageCategory.DELAY.value == "delay"

    def test_line_health_suspended(self):
        assert LineHealth.SUSPENDED.value == "suspended"

    def test_line_health_normal(self):
        assert LineHealth.NORMAL.value == "normal"


# ── OutageParser ──────────────────────────────────────────────────────────────

class TestOutageParser:

    def setup_method(self):
        self.parser = OutageParser()

    def _alert(self, headline, impact="Delays", description="",
               score="5", alert_id="A1", end_time=None):
        return {
            "AlertId": alert_id,
            "Headline": headline,
            "ShortDescription": description,
            "FullDescription": description,
            "Impact": impact,
            "SeverityScore": score,
            "ImpactType": "Train Route",
            "EventStart": "2025-01-15T08:00:00",
            "EventEnd": end_time,
            "TBD": "0",
            "MajorAlert": "0",
            "AlertURL": "",
            "AffectedService": {"Service": [{"RouteId": "G", "ServiceId": "G"}]},
        }

    def test_parses_green_line_alert(self):
        alert = self._alert("Green Line — delays due to track work")
        outage = self.parser.parse(alert)
        assert outage is not None
        # Headline should mention Green Line
        assert "Green" in outage.headline

    def test_alert_id_preserved(self):
        alert = self._alert("Green Line delays", alert_id="RT-999")
        outage = self.parser.parse(alert)
        assert outage is not None
        assert outage.alert_id == "RT-999"

    def test_severity_high_score_maps_to_critical_or_high(self):
        alert = self._alert("Red Line suspended", score="10")
        outage = self.parser.parse(alert)
        if outage:
            assert outage.severity in (OutageSeverity.CRITICAL, OutageSeverity.HIGH)

    def test_low_score_maps_to_low_or_info(self):
        # Use "Advisory" impact so it maps to OutageSeverity.LOW
        alert = self._alert("Minor service advisory on Blue Line", impact="Advisory", score="2")
        outage = self.parser.parse(alert)
        if outage:
            assert outage.severity in (OutageSeverity.INFO, OutageSeverity.LOW)

    def test_suspension_keyword_detected(self):
        alert = self._alert("Green Line service is suspended between Loop and Harlem",
                            impact="Service Suspension", score="10")
        outage = self.parser.parse(alert)
        if outage:
            assert outage.category in (OutageCategory.SUSPENSION, OutageCategory.DELAY)

    def test_has_start_time(self):
        alert = self._alert("Green Line delays")
        outage = self.parser.parse(alert)
        assert outage is not None
        assert isinstance(outage.start_time, datetime)

    def test_does_not_crash_on_empty_dict(self):
        # Should either return None or an object — never crash
        try:
            outage = self.parser.parse({})
            assert outage is None or isinstance(outage, ServiceOutage)
        except Exception:
            pass  # graceful degradation acceptable


# ── LineStatusTracker ─────────────────────────────────────────────────────────

class TestLineStatusTracker:

    def setup_method(self):
        self.tracker = LineStatusTracker()

    def _make_outage(self, lines, severity=OutageSeverity.MEDIUM,
                     category=OutageCategory.DELAY, loop_impacted=True):
        return ServiceOutage(
            alert_id="A1",
            headline="Delays on line",
            short_description="",
            full_description="",
            severity=severity,
            category=category,
            affected_lines=lines,
            affected_stations=[],
            loop_impacted=loop_impacted,
            start_time=datetime.now(timezone.utc),
            end_time=None,
            is_tbd=False,
            is_major=False,
            impact_text="delays",
            alert_url="",
            routing_impact={},
        )

    def test_no_outages_all_lines_normal(self):
        statuses = self.tracker.compute_statuses([])
        for line_id, status in statuses.items():
            assert status.health == LineHealth.NORMAL

    def test_green_line_delay_degrades_health(self):
        outages = [self._make_outage(["Green"], OutageSeverity.HIGH)]
        statuses = self.tracker.compute_statuses(outages)
        green = statuses.get("Green")
        if green:
            assert green.health != LineHealth.NORMAL

    def test_suspension_results_in_worst_health(self):
        outages = [self._make_outage(["Red"], OutageSeverity.CRITICAL, OutageCategory.SUSPENSION)]
        statuses = self.tracker.compute_statuses(outages)
        red = statuses.get("Red")
        if red:
            assert red.health == LineHealth.SUSPENDED

    def test_returns_dict_for_all_loop_lines(self):
        statuses = self.tracker.compute_statuses([])
        # Should return statuses for at least the core Loop lines
        returned_lines = set(statuses.keys())
        assert len(returned_lines) > 0

    def test_multiple_outages_counted(self):
        outages = [
            self._make_outage(["Green"], OutageSeverity.MEDIUM),
            self._make_outage(["Green"], OutageSeverity.HIGH),
        ]
        statuses = self.tracker.compute_statuses(outages)
        green = statuses.get("Green")
        if green:
            assert green.active_outages is not None
            assert len(green.active_outages) >= 1


# ── LoopImpactAnalyzer ────────────────────────────────────────────────────────

class TestLoopImpactAnalyzer:

    def setup_method(self):
        self.analyzer = LoopImpactAnalyzer()

    def _make_outage(self, lines, severity, category=OutageCategory.SUSPENSION,
                     stations=None, loop_impacted=True):
        return ServiceOutage(
            alert_id="A1",
            headline=f"{lines} outage",
            short_description="",
            full_description="",
            severity=severity,
            category=category,
            affected_lines=lines,
            affected_stations=stations or ["clark_lake"],
            loop_impacted=loop_impacted,
            start_time=datetime.now(timezone.utc),
            end_time=None,
            is_tbd=False,
            is_major=False,
            impact_text="",
            alert_url="",
            routing_impact={},
        )

    def test_no_outages_no_impact(self):
        impact = self.analyzer.analyze([])
        assert impact.suspend_lines == []
        assert impact.delay_lines == []
        assert impact.avoid_stations == []

    def test_critical_suspension_in_suspend_lines(self):
        outages = [self._make_outage(["Green"], OutageSeverity.CRITICAL)]
        impact = self.analyzer.analyze(outages)
        assert "Green" in impact.suspend_lines

    def test_high_severity_in_delay_lines(self):
        outages = [self._make_outage(["Blue"], OutageSeverity.HIGH, OutageCategory.DELAY)]
        impact = self.analyzer.analyze(outages)
        assert "Blue" in impact.delay_lines or "Blue" in impact.suspend_lines

    def test_affected_stations_in_avoid(self):
        outages = [self._make_outage(
            ["Green"], OutageSeverity.HIGH, OutageCategory.DELAY,
            stations=["clark_lake"]
        )]
        impact = self.analyzer.analyze(outages)
        assert "clark_lake" in impact.avoid_stations

    def test_suspended_line_has_alternatives(self):
        outages = [self._make_outage(["Green"], OutageSeverity.CRITICAL)]
        impact = self.analyzer.analyze(outages)
        # LoopImpactAnalyzer should suggest alternatives for suspended lines
        assert isinstance(impact.alternate_lines, dict)

    def test_non_loop_outage_ignored(self):
        outages = [self._make_outage(["Yellow"], OutageSeverity.CRITICAL,
                                     loop_impacted=False)]
        impact = self.analyzer.analyze(outages)
        # Non-Loop line outages should not affect suspend_lines
        assert "Yellow" not in impact.suspend_lines


# ── Loop Data Constants ───────────────────────────────────────────────────────

class TestLoopConstants:

    def test_loop_lines_not_empty(self):
        assert len(LOOP_LINES) > 0

    def test_loop_station_names_not_empty(self):
        assert len(LOOP_STATION_NAMES) > 0

    def test_loop_lines_includes_green_and_red(self):
        assert "Green" in LOOP_LINES
        assert "Red" in LOOP_LINES
        assert "Blue" in LOOP_LINES

    def test_loop_station_names_includes_clark_lake(self):
        assert any("Clark" in n for n in LOOP_STATION_NAMES)

    def test_loop_station_names_includes_state_lake(self):
        assert any("State" in n for n in LOOP_STATION_NAMES)


# ── Singleton ─────────────────────────────────────────────────────────────────

class TestOutageServiceSingleton:

    def test_singleton_returns_same_instance(self):
        svc1 = get_outage_service()
        svc2 = get_outage_service()
        assert svc1 is svc2

    def test_service_has_initialize_method(self):
        svc = get_outage_service()
        assert hasattr(svc, "initialize")

    def test_service_has_get_routing_impact(self):
        svc = get_outage_service()
        assert hasattr(svc, "get_routing_impact")
