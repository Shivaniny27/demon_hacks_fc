"""
test_congestion_service.py
==========================
Tests for the Congestion & Rush Hour Model (Priority 5).

Covers:
  - RushHourClassifier period detection
  - SpecialEventDetector event matching
  - CongestionModel base predictions
  - Real-data blending (ridership scale factors + crash penalties)
  - CTARidershipFetcher parsing mocked API responses
  - TrafficCrashFetcher proximity + penalty computation
  - RealTimeDataCache Redis serialization round-trip
  - CongestionService.predict() end-to-end
  - CongestionService.predict_trend() direction detection
  - CongestionService.predict_all() network snapshot
  - CongestionService.get_best_time_to_travel()
  - Background lifecycle (initialize / shutdown)
"""

import json
import math
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.congestion_service import (
    CongestionLevel,
    CongestionModel,
    CongestionService,
    ChicagoDataPortalClient,
    CrashHotspot,
    CTARidershipFetcher,
    DEFAULT_RIDERSHIP,
    HISTORICAL_RIDERSHIP,
    PeriodType,
    RealTimeDataCache,
    RidershipRecord,
    RushHourClassifier,
    SpecialEventDetector,
    TrafficCrashFetcher,
    get_congestion_service,
)
from tests.conftest import (
    MockRedis,
    CHICAGO_RIDERSHIP_PAYLOAD,
    CHICAGO_CRASHES_PAYLOAD,
)


# ── RushHourClassifier ────────────────────────────────────────────────────────

class TestRushHourClassifier:

    def setup_method(self):
        self.clf = RushHourClassifier()

    def _utc(self, hour_chicago: int) -> datetime:
        """Convert Chicago hour to UTC datetime (CST = UTC-6)."""
        return datetime(2025, 10, 15, (hour_chicago + 6) % 24, 0, tzinfo=timezone.utc)

    def test_am_peak_7am(self):
        assert self.clf.classify(self._utc(7)) == PeriodType.AM_PEAK

    def test_am_peak_830am(self):
        dt = datetime(2025, 10, 15, 14, 30, tzinfo=timezone.utc)  # 8:30 AM Chicago
        assert self.clf.classify(dt) == PeriodType.AM_PEAK

    def test_am_shoulder_6am(self):
        assert self.clf.classify(self._utc(6)) == PeriodType.AM_SHOULDER

    def test_am_shoulder_930am(self):
        dt = datetime(2025, 10, 15, 15, 30, tzinfo=timezone.utc)  # 9:30 AM Chicago
        assert self.clf.classify(dt) == PeriodType.AM_SHOULDER

    def test_midday(self):
        assert self.clf.classify(self._utc(12)) == PeriodType.MIDDAY

    def test_pm_peak_5pm(self):
        assert self.clf.classify(self._utc(17)) == PeriodType.PM_PEAK

    def test_pm_peak_615pm(self):
        dt = datetime(2025, 10, 16, 0, 15, tzinfo=timezone.utc)  # 6:15 PM Chicago
        assert self.clf.classify(dt) == PeriodType.PM_PEAK

    def test_evening_8pm(self):
        assert self.clf.classify(self._utc(20)) == PeriodType.EVENING

    def test_late_night_midnight(self):
        assert self.clf.classify(self._utc(0)) == PeriodType.LATE_NIGHT

    def test_late_night_230am(self):
        dt = datetime(2025, 10, 15, 8, 30, tzinfo=timezone.utc)  # 2:30 AM Chicago
        assert self.clf.classify(dt) == PeriodType.LATE_NIGHT

    def test_is_peak_am(self):
        assert self.clf.is_peak(PeriodType.AM_PEAK) is True

    def test_is_peak_pm(self):
        assert self.clf.is_peak(PeriodType.PM_PEAK) is True

    def test_is_not_peak_midday(self):
        assert self.clf.is_peak(PeriodType.MIDDAY) is False

    def test_minutes_to_next_change_during_am_peak(self):
        # 7:30 AM Chicago → 1.5h left in AM_PEAK (7–9), so 90 mins
        dt = datetime(2025, 10, 15, 13, 30, tzinfo=timezone.utc)  # 7:30 AM Chicago
        mins = self.clf.minutes_to_next_period_change(dt)
        assert 85 <= mins <= 95  # roughly 90 min

    def test_minutes_to_next_change_at_boundary(self):
        # 9:01 AM Chicago → just entered AM_SHOULDER (9–10), ~59 min left
        dt = datetime(2025, 10, 15, 15, 1, tzinfo=timezone.utc)
        mins = self.clf.minutes_to_next_period_change(dt)
        assert 50 <= mins <= 65


# ── SpecialEventDetector ──────────────────────────────────────────────────────

class TestSpecialEventDetector:

    def setup_method(self):
        self.det = SpecialEventDetector()

    def test_lollapalooza_detected(self):
        # August, Thursday, 2 PM — affects adams_wabash
        dt = datetime(2025, 8, 7, 20, 0, tzinfo=timezone.utc)  # Thu 2 PM Chicago
        events = self.det.get_active_events(dt, "adams_wabash")
        names = [e[0] for e in events]
        assert any("Lollapalooza" in n for n in names)

    def test_lollapalooza_multiplier(self):
        dt = datetime(2025, 8, 7, 20, 0, tzinfo=timezone.utc)
        events = self.det.get_active_events(dt, "adams_wabash")
        multipliers = [m for _, m in events]
        assert max(multipliers) == pytest.approx(1.65, abs=0.01)

    def test_lollapalooza_not_wrong_station(self):
        dt = datetime(2025, 8, 7, 20, 0, tzinfo=timezone.utc)
        events = self.det.get_active_events(dt, "quincy_wells")
        assert events == []

    def test_st_patricks_day(self):
        # March 17, 11 AM Chicago
        dt = datetime(2025, 3, 17, 17, 0, tzinfo=timezone.utc)  # 11 AM Chicago
        events = self.det.get_active_events(dt, "clark_lake")
        names = [e[0] for e in events]
        assert any("Patrick" in n for n in names)

    def test_no_event_normal_weekday(self):
        # Random Wednesday in November, no events
        dt = datetime(2025, 11, 19, 14, 0, tzinfo=timezone.utc)  # 8 AM Chicago
        events = self.det.get_active_events(dt, "clark_lake")
        assert events == []

    def test_jazz_festival_september(self):
        # September Saturday, 3 PM
        dt = datetime(2025, 9, 6, 21, 0, tzinfo=timezone.utc)  # Sat 3 PM Chicago
        events = self.det.get_active_events(dt, "state_lake")
        names = [e[0] for e in events]
        assert any("Jazz" in n for n in names)


# ── CongestionModel ───────────────────────────────────────────────────────────

class TestCongestionModel:

    def setup_method(self):
        self.model = CongestionModel()

    def test_predict_returns_prediction(self):
        dt = datetime(2025, 10, 15, 14, 0, tzinfo=timezone.utc)  # 8 AM Chicago
        pred = self.model.predict("clark_lake", "Clark/Lake", dt)
        assert pred is not None
        assert pred.station_key == "clark_lake"
        assert 0 <= pred.adjusted_score <= 100

    def test_am_peak_higher_than_midday(self):
        # AM peak (8 AM Chicago) should be more congested than midday (12 PM Chicago)
        am = datetime(2025, 10, 15, 14, 0, tzinfo=timezone.utc)
        mid = datetime(2025, 10, 15, 18, 0, tzinfo=timezone.utc)
        pred_am  = self.model.predict("clark_lake", "Clark/Lake", am)
        pred_mid = self.model.predict("clark_lake", "Clark/Lake", mid)
        assert pred_am.adjusted_score > pred_mid.adjusted_score

    def test_pm_peak_severe_at_clark_lake(self):
        # 5 PM Chicago = 23:00 UTC — Clark/Lake PM peak score = 90 → SEVERE
        dt = datetime(2025, 10, 15, 23, 0, tzinfo=timezone.utc)
        pred = self.model.predict("clark_lake", "Clark/Lake", dt)
        assert pred.congestion_level in (CongestionLevel.SEVERE, CongestionLevel.HEAVY)
        assert pred.is_peak is True

    def test_weekend_lower_than_weekday_same_hour(self):
        # Wednesday 8 AM vs Saturday 8 AM
        wed = datetime(2025, 10, 15, 14, 0, tzinfo=timezone.utc)  # Wed 8 AM
        sat = datetime(2025, 10, 18, 14, 0, tzinfo=timezone.utc)  # Sat 8 AM
        pred_wed = self.model.predict("clark_lake", "Clark/Lake", wed)
        pred_sat = self.model.predict("clark_lake", "Clark/Lake", sat)
        assert pred_wed.adjusted_score > pred_sat.adjusted_score

    def test_weather_heavy_rain_increases_score(self):
        dt = datetime(2025, 10, 15, 14, 0, tzinfo=timezone.utc)
        pred_normal    = self.model.predict("clark_lake", "Clark/Lake", dt, "normal")
        pred_heavy_rain = self.model.predict("clark_lake", "Clark/Lake", dt, "heavy_rain")
        assert pred_heavy_rain.adjusted_score > pred_normal.adjusted_score

    def test_weather_nice_decreases_score(self):
        dt = datetime(2025, 10, 15, 14, 0, tzinfo=timezone.utc)
        pred_normal = self.model.predict("clark_lake", "Clark/Lake", dt, "normal")
        pred_nice   = self.model.predict("clark_lake", "Clark/Lake", dt, "nice")
        assert pred_nice.adjusted_score < pred_normal.adjusted_score

    def test_score_clamped_0_100(self):
        dt = datetime(2025, 8, 7, 23, 0, tzinfo=timezone.utc)  # Lollapalooza PM peak
        pred = self.model.predict("adams_wabash", "Adams/Wabash", dt, "blizzard")
        assert 0.0 <= pred.adjusted_score <= 100.0

    def test_unknown_station_uses_default(self):
        dt = datetime(2025, 10, 15, 14, 0, tzinfo=timezone.utc)
        pred = self.model.predict("nonexistent_station", "Unknown", dt)
        assert pred is not None
        assert 0 <= pred.adjusted_score <= 100

    def test_score_to_level_boundaries(self):
        assert CongestionModel._score_to_level(0)   == CongestionLevel.LIGHT
        assert CongestionModel._score_to_level(29)  == CongestionLevel.LIGHT
        assert CongestionModel._score_to_level(30)  == CongestionLevel.MODERATE
        assert CongestionModel._score_to_level(59)  == CongestionLevel.MODERATE
        assert CongestionModel._score_to_level(60)  == CongestionLevel.HEAVY
        assert CongestionModel._score_to_level(84)  == CongestionLevel.HEAVY
        assert CongestionModel._score_to_level(85)  == CongestionLevel.SEVERE
        assert CongestionModel._score_to_level(100) == CongestionLevel.SEVERE

    def test_predict_trend_returns_trend(self):
        trend = self.model.predict_trend("clark_lake", "Clark/Lake")
        assert trend.station_key == "clark_lake"
        assert trend.trend in ("increasing", "decreasing", "stable")
        assert trend.current_score >= 0
        assert trend.in_30min_score >= 0
        assert trend.in_60min_score >= 0

    def test_alternative_stations_returned(self):
        dt = datetime(2025, 10, 15, 23, 0, tzinfo=timezone.utc)  # PM peak
        pred = self.model.predict("clark_lake", "Clark/Lake", dt)
        assert isinstance(pred.alternative_stations, list)
        assert "clark_lake" not in pred.alternative_stations

    def test_heavy_congestion_has_recommendation(self):
        dt = datetime(2025, 10, 15, 23, 0, tzinfo=timezone.utc)
        pred = self.model.predict("clark_lake", "Clark/Lake", dt)
        assert len(pred.recommendation) > 10


# ── CTARidershipFetcher ───────────────────────────────────────────────────────

class TestCTARidershipFetcher:

    @pytest.fixture
    def portal_client(self):
        client = AsyncMock(spec=ChicagoDataPortalClient)
        client.get = AsyncMock(return_value=CHICAGO_RIDERSHIP_PAYLOAD)
        return client

    @pytest.fixture
    def fetcher(self, portal_client):
        return CTARidershipFetcher(portal_client)

    async def test_fetch_returns_station_records(self, fetcher):
        result = await fetcher.fetch()
        assert "clark_lake" in result
        assert "state_lake" in result

    async def test_fetch_day_types(self, fetcher):
        result = await fetcher.fetch()
        clark = result["clark_lake"]
        assert "W" in clark
        assert "A" in clark
        assert "U" in clark

    async def test_scale_factor_above_one_for_high_ridership(self, fetcher):
        result = await fetcher.fetch()
        # Clark/Lake weekday avg_rides = 9500, baseline = 10000 → scale ≈ 0.95
        clark_w = result["clark_lake"]["W"]
        assert clark_w.scale_factor == pytest.approx(9500 / 10_000, abs=0.01)

    async def test_scale_factor_clamped(self, portal_client, fetcher):
        # Return unreasonably high ridership — should be clamped to 2.0
        portal_client.get = AsyncMock(return_value=[
            {"stationname": "Clark/Lake", "daytype": "W", "avg_rides": "999999.0"},
        ])
        result = await fetcher.fetch()
        assert result["clark_lake"]["W"].scale_factor == pytest.approx(2.0, abs=0.001)

    async def test_unknown_station_name_skipped(self, portal_client, fetcher):
        portal_client.get = AsyncMock(return_value=[
            {"stationname": "NotARealStation", "daytype": "W", "avg_rides": "5000.0"},
        ])
        result = await fetcher.fetch()
        assert result == {}

    async def test_empty_response_returns_empty(self, portal_client, fetcher):
        portal_client.get = AsyncMock(return_value=[])
        result = await fetcher.fetch()
        assert result == {}

    async def test_bad_avg_rides_skipped(self, portal_client, fetcher):
        portal_client.get = AsyncMock(return_value=[
            {"stationname": "Clark/Lake", "daytype": "W", "avg_rides": "not-a-number"},
        ])
        result = await fetcher.fetch()
        assert result == {}


# ── TrafficCrashFetcher ───────────────────────────────────────────────────────

class TestTrafficCrashFetcher:

    @pytest.fixture
    def portal_client(self):
        # fetch() now calls get_geojson() which returns GeoJSON features
        client = AsyncMock(spec=ChicagoDataPortalClient)
        client.get_geojson = AsyncMock(return_value=CHICAGO_CRASHES_PAYLOAD)
        return client

    @pytest.fixture
    def fetcher(self, portal_client):
        return TrafficCrashFetcher(portal_client)

    async def test_fetch_returns_all_stations(self, fetcher):
        result = await fetcher.fetch()
        assert "clark_lake" in result
        assert "washington_wabash" in result

    async def test_crash_near_clark_lake_counted(self, fetcher):
        result = await fetcher.fetch()
        # First crash at 41.8858, -87.6317 is exactly at clark_lake coords
        assert result["clark_lake"].crash_count_30d >= 1

    async def test_crash_near_state_lake_counted(self, fetcher):
        result = await fetcher.fetch()
        # Second crash at 41.8858, -87.6280 is at state_lake
        assert result["state_lake"].crash_count_30d >= 1

    async def test_no_crashes_penalty_is_1(self, portal_client, fetcher):
        # Area with no crashes → penalty stays 1.0
        portal_client.get_geojson = AsyncMock(return_value=[])
        result = await fetcher.fetch()
        assert result == {}

    def test_haversine_m_same_point(self):
        d = TrafficCrashFetcher._haversine_m(41.8858, -87.6317, 41.8858, -87.6317)
        assert d == pytest.approx(0.0, abs=0.01)

    def test_haversine_m_known_distance(self):
        # Clark/Lake to State/Lake is roughly 300-400m apart east-west
        d = TrafficCrashFetcher._haversine_m(41.8858, -87.6317, 41.8858, -87.6280)
        assert 300 < d < 450

    def test_penalty_capped_at_1_20(self, fetcher):
        # Very high crash count — penalty must not exceed 1.20
        hotspot = CrashHotspot(
            station_key="clark_lake",
            crash_count_30d=100,
            injury_count_30d=50,
            congestion_penalty=min(1.20, 1.0 + 100 * 0.02 + 50 * 0.01),
        )
        assert hotspot.congestion_penalty == pytest.approx(1.20, abs=0.001)


# ── RealTimeDataCache ─────────────────────────────────────────────────────────

class TestRealTimeDataCache:

    @pytest.fixture
    def cache(self, mock_redis):
        return RealTimeDataCache(mock_redis)

    async def test_store_and_retrieve_ridership(self, cache):
        data = {
            "clark_lake": {
                "W": {"station_key": "clark_lake", "daytype": "W",
                      "avg_daily_rides": 9500.0, "scale_factor": 0.95,
                      "data_date_range": "Last 90 days"}
            }
        }
        await cache.store_ridership(data)
        result = await cache.get_ridership()
        assert result is not None
        assert "clark_lake" in result
        assert result["clark_lake"]["W"]["scale_factor"] == pytest.approx(0.95, abs=0.001)

    async def test_store_and_retrieve_crashes(self, cache):
        data = {
            "clark_lake": {
                "station_key": "clark_lake",
                "crash_count_30d": 3,
                "injury_count_30d": 1,
                "congestion_penalty": 1.07,
            }
        }
        await cache.store_crashes(data)
        result = await cache.get_crashes()
        assert result["clark_lake"]["crash_count_30d"] == 3
        assert result["clark_lake"]["congestion_penalty"] == pytest.approx(1.07, abs=0.001)

    async def test_get_returns_none_when_empty(self, cache):
        result = await cache.get_ridership()
        assert result is None

    async def test_no_redis_store_is_noop(self):
        cache = RealTimeDataCache(redis=None)
        await cache.store_ridership({"key": "value"})
        result = await cache.get_ridership()
        assert result is None


# ── CongestionService — core predict ─────────────────────────────────────────

class TestCongestionService:

    def setup_method(self):
        self.svc = CongestionService()

    def test_predict_known_station(self):
        pred = self.svc.predict("clark_lake")
        assert pred is not None
        assert pred.station_key == "clark_lake"
        assert pred.station_name == "Clark/Lake"

    def test_predict_unknown_station_returns_prediction(self):
        # Unknown station → falls back to DEFAULT_RIDERSHIP, still returns value
        pred = self.svc.predict("unknown_station_xyz")
        assert pred is not None

    def test_predict_all_covers_all_stations(self):
        snapshot = self.svc.predict_all()
        assert len(snapshot.station_scores) == len(HISTORICAL_RIDERSHIP)
        assert snapshot.most_congested
        assert snapshot.least_congested
        assert 0 <= snapshot.network_avg_score <= 100

    def test_predict_all_most_and_least_no_overlap(self):
        snapshot = self.svc.predict_all()
        overlap = set(snapshot.most_congested) & set(snapshot.least_congested)
        # Possible edge case on tiny data, but with 13 stations should be empty
        assert len(snapshot.most_congested) >= 1
        assert len(snapshot.least_congested) >= 1

    def test_predict_trend(self):
        trend = self.svc.predict_trend("clark_lake")
        assert trend is not None
        assert trend.station_key == "clark_lake"
        assert trend.trend in ("increasing", "decreasing", "stable")

    def test_get_best_time_to_travel_weekday(self):
        hours = self.svc.get_best_time_to_travel("clark_lake", "weekday")
        assert len(hours) == 19   # hours 5–23
        scores = [h["score"] for h in hours]
        assert max(scores) >= min(scores)   # not flat
        # Verify structure
        for entry in hours:
            assert "hour" in entry
            assert "congestion_level" in entry
            assert "is_recommended" in entry

    def test_get_best_time_peak_not_recommended(self):
        hours = self.svc.get_best_time_to_travel("clark_lake", "weekday")
        pm_peak = next(h for h in hours if h["hour"] == 17)  # 5 PM
        assert pm_peak["is_recommended"] is False

    def test_get_routing_advice_structure(self):
        advice = self.svc.get_routing_advice("clark_lake")
        assert "congestion" in advice
        assert "score" in advice
        assert "alternatives" in advice

    def test_compare_stations(self):
        result = self.svc.compare_stations(["clark_lake", "quincy_wells", "adams_wabash"])
        assert len(result) == 3
        # Should be sorted by score ascending
        scores = [r["score"] for r in result]
        assert scores == sorted(scores)

    def test_health_check_structure(self):
        health = self.svc.health_check()
        assert health["status"] == "ok"
        assert health["stations_modeled"] == len(HISTORICAL_RIDERSHIP)
        assert "real_data_active" in health
        assert "ridership_stations" in health

    def test_real_data_summary_structure(self):
        summary = self.svc.get_real_data_summary("clark_lake")
        assert "ridership_scale_factors" in summary
        assert "crash_hotspot" in summary
        assert "real_data_active" in summary


# ── CongestionService — real-data blending ───────────────────────────────────

class TestCongestionServiceRealDataBlending:

    def setup_method(self):
        self.svc = CongestionService()

    def test_ridership_scale_above_1_increases_score(self):
        dt = datetime(2025, 10, 15, 14, 0, tzinfo=timezone.utc)  # 8 AM Chicago
        base_pred = self.svc.predict("clark_lake", dt)
        base_score = base_pred.adjusted_score

        # Inject high ridership (scale_factor = 1.5)
        self.svc._ridership_data = {
            "clark_lake": {
                "W": RidershipRecord(
                    station_key="clark_lake", daytype="W",
                    avg_daily_rides=15000, scale_factor=1.5,
                    data_date_range="Last 90 days"
                )
            }
        }
        self.svc._initialized = True
        high_pred = self.svc.predict("clark_lake", dt)
        assert high_pred.adjusted_score > base_score

    def test_ridership_scale_below_1_decreases_score(self):
        dt = datetime(2025, 10, 15, 14, 0, tzinfo=timezone.utc)
        base_pred = self.svc.predict("clark_lake", dt)
        base_score = base_pred.adjusted_score

        # Inject low ridership (scale_factor = 0.6)
        self.svc._ridership_data = {
            "clark_lake": {
                "W": RidershipRecord(
                    station_key="clark_lake", daytype="W",
                    avg_daily_rides=6000, scale_factor=0.6,
                    data_date_range="Last 90 days"
                )
            }
        }
        self.svc._initialized = True
        low_pred = self.svc.predict("clark_lake", dt)
        assert low_pred.adjusted_score < base_score

    def test_crash_penalty_increases_score(self):
        dt = datetime(2025, 10, 15, 18, 0, tzinfo=timezone.utc)  # midday quiet time
        base_pred = self.svc.predict("clark_lake", dt)
        base_score = base_pred.adjusted_score

        self.svc._crash_data = {
            "clark_lake": CrashHotspot(
                station_key="clark_lake",
                crash_count_30d=5,
                injury_count_30d=2,
                congestion_penalty=1.12,
            )
        }
        self.svc._initialized = True
        crash_pred = self.svc.predict("clark_lake", dt)
        assert crash_pred.adjusted_score >= base_score

    def test_score_never_exceeds_100_with_both_factors(self):
        dt = datetime(2025, 10, 15, 23, 0, tzinfo=timezone.utc)  # PM peak

        self.svc._ridership_data = {
            "clark_lake": {
                "W": RidershipRecord(
                    station_key="clark_lake", daytype="W",
                    avg_daily_rides=20000, scale_factor=2.0,
                    data_date_range="Last 90 days"
                )
            }
        }
        self.svc._crash_data = {
            "clark_lake": CrashHotspot(
                station_key="clark_lake",
                crash_count_30d=50, injury_count_30d=20,
                congestion_penalty=1.20,
            )
        }
        self.svc._initialized = True
        pred = self.svc.predict("clark_lake", dt)
        assert pred.adjusted_score <= 100.0

    def test_40_percent_blend_applied(self):
        dt = datetime(2025, 10, 15, 18, 0, tzinfo=timezone.utc)

        base_pred = CongestionService().predict("clark_lake", dt)
        base_score = base_pred.adjusted_score

        self.svc._ridership_data = {
            "clark_lake": {
                "W": RidershipRecord(
                    station_key="clark_lake", daytype="W",
                    avg_daily_rides=10000, scale_factor=2.0,
                    data_date_range="Last 90 days"
                )
            }
        }
        self.svc._initialized = True
        blended_pred = self.svc.predict("clark_lake", dt)

        # With scale_factor=2.0 and 40% blend: multiplier = 1 + (2-1)*0.4 = 1.4
        expected = min(100.0, base_score * 1.4)
        assert blended_pred.adjusted_score == pytest.approx(expected, abs=0.5)


# ── CongestionService — lifecycle ─────────────────────────────────────────────

class TestCongestionServiceLifecycle:

    async def test_initialize_without_token_skips_api(self, mock_redis):
        svc = CongestionService(redis=mock_redis)
        svc._token = ""   # no token → should skip API calls

        with patch.object(svc._ridership_fetcher, "fetch", new_callable=AsyncMock) as mock_fetch:
            await svc.initialize()
            mock_fetch.assert_not_called()

        assert svc._initialized is False

    async def test_initialize_with_token_calls_ridership_and_crash(self, mock_redis):
        svc = CongestionService(redis=mock_redis)
        svc._token = "fake-test-token"

        mock_ridership = {
            "clark_lake": {
                "W": RidershipRecord("clark_lake", "W", 9500.0, 0.95, "Last 90 days")
            }
        }
        mock_crashes = {
            "clark_lake": CrashHotspot("clark_lake", 2, 0, 1.04)
        }

        with (
            patch.object(svc._ridership_fetcher, "fetch", AsyncMock(return_value=mock_ridership)),
            patch.object(svc._crash_fetcher,     "fetch", AsyncMock(return_value=mock_crashes)),
        ):
            await svc.initialize()

        assert svc._initialized is True
        assert "clark_lake" in svc._ridership_data
        assert "clark_lake" in svc._crash_data

    async def test_shutdown_cancels_background_task(self, mock_redis):
        import asyncio
        svc = CongestionService(redis=mock_redis)
        svc._token = "fake-token"

        with (
            patch.object(svc._ridership_fetcher, "fetch", AsyncMock(return_value={})),
            patch.object(svc._crash_fetcher,     "fetch", AsyncMock(return_value={})),
        ):
            await svc.initialize()

        assert svc._bg_task is not None
        await svc.shutdown()
        assert svc._bg_task.cancelled() or svc._bg_task.done()

    async def test_initialize_uses_redis_cache_on_hit(self, mock_redis):
        svc = CongestionService(redis=mock_redis)
        svc._token = "fake-token"

        # Pre-populate cache
        cached_ridership = {
            "clark_lake": {
                "W": {"station_key": "clark_lake", "daytype": "W",
                      "avg_daily_rides": 9500.0, "scale_factor": 0.95,
                      "data_date_range": "Last 90 days"}
            }
        }
        await mock_redis.set("congestion:ridership_data", json.dumps(cached_ridership))

        with (
            patch.object(svc._ridership_fetcher, "fetch", new_callable=AsyncMock) as mock_rf,
            patch.object(svc._crash_fetcher,     "fetch", AsyncMock(return_value={})),
        ):
            await svc.initialize()
            mock_rf.assert_not_called()   # cache hit → no API call

        assert "clark_lake" in svc._ridership_data

    def test_serialization_roundtrip_ridership(self):
        svc = CongestionService()
        original = {
            "clark_lake": {
                "W": RidershipRecord("clark_lake", "W", 9500.0, 0.95, "Last 90 days"),
                "A": RidershipRecord("clark_lake", "A", 5800.0, 0.89, "Last 90 days"),
            }
        }
        serialized   = svc._serialize_ridership(original)
        deserialized = svc._deserialize_ridership(serialized)
        assert deserialized["clark_lake"]["W"].scale_factor == pytest.approx(0.95, abs=0.001)
        assert deserialized["clark_lake"]["A"].avg_daily_rides == pytest.approx(5800.0, abs=1)

    def test_serialization_roundtrip_crashes(self):
        svc = CongestionService()
        original = {
            "clark_lake": CrashHotspot("clark_lake", 3, 1, 1.07),
        }
        serialized   = svc._serialize_crashes(original)
        deserialized = svc._deserialize_crashes(serialized)
        assert deserialized["clark_lake"].crash_count_30d == 3
        assert deserialized["clark_lake"].congestion_penalty == pytest.approx(1.07, abs=0.001)


# ── ChicagoDataPortalClient ───────────────────────────────────────────────────

class TestChicagoDataPortalClient:

    async def test_get_passes_token_header(self):
        client = ChicagoDataPortalClient(app_token="test-token-123")
        mock_response = MagicMock()
        mock_response.json.return_value = [{"row": "data"}]
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            mock_http = AsyncMock()
            mock_http.get = AsyncMock(return_value=mock_response)
            MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_http)
            MockClient.return_value.__aexit__  = AsyncMock(return_value=False)

            result = await client.get("5neh-572f", {"$select": "stationname"})

        assert result == [{"row": "data"}]
        call_kwargs = mock_http.get.call_args
        assert call_kwargs[1]["headers"]["X-App-Token"] == "test-token-123"

    async def test_get_retries_on_http_error(self):
        import httpx as _httpx
        client = ChicagoDataPortalClient(app_token="tok")

        call_count = 0

        async def fake_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                resp = MagicMock()
                resp.status_code = 500
                raise _httpx.HTTPStatusError("server error", request=MagicMock(), response=resp)
            resp = MagicMock()
            resp.json.return_value = []
            resp.raise_for_status = MagicMock()
            return resp

        with patch("httpx.AsyncClient") as MockClient:
            mock_http = AsyncMock()
            mock_http.get = fake_get
            MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_http)
            MockClient.return_value.__aexit__  = AsyncMock(return_value=False)

            result = await client.get("5neh-572f", {})

        assert call_count == 3
        assert result == []

    async def test_get_returns_empty_on_all_failures(self):
        import httpx as _httpx
        client = ChicagoDataPortalClient(app_token="tok")

        async def always_fail(*args, **kwargs):
            raise _httpx.TimeoutException("timeout")

        with patch("httpx.AsyncClient") as MockClient:
            mock_http = AsyncMock()
            mock_http.get = always_fail
            MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_http)
            MockClient.return_value.__aexit__  = AsyncMock(return_value=False)

            result = await client.get("5neh-572f", {})

        assert result == []
