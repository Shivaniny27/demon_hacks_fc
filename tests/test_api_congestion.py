"""
test_api_congestion.py
======================
HTTP integration tests for /api/v1/nav/congestion/* endpoints.

Uses the real FastAPI router with a module-level CongestionService singleton
(historical model only — no live API calls). All external I/O is bypassed.
"""

import pytest
from fastapi.testclient import TestClient

from app.services.congestion_service import (
    CongestionService,
    HISTORICAL_RIDERSHIP,
    get_congestion_service,
)


@pytest.fixture(scope="module")
def client():
    """
    Minimal test client: patches DB init and Redis so the app starts cleanly.
    CongestionService uses historical model (no CHICAGO_APP_TOKEN).
    """
    import os
    from unittest.mock import AsyncMock, patch

    os.environ["USE_STUB_GRAPH"] = "true"
    os.environ["DEBUG"] = "true"

    with (
        patch("app.database.init_db", new_callable=AsyncMock),
        patch("redis.asyncio.from_url") as mock_redis_factory,
    ):
        # Make Redis ping fail → app.state.redis = None (graceful degradation)
        mock_r = AsyncMock()
        mock_r.ping = AsyncMock(side_effect=ConnectionRefusedError("no redis"))
        mock_redis_factory.return_value = mock_r

        from app.main import app
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c


KNOWN_STATION = "clark_lake"
UNKNOWN_STATION = "nonexistent_station_xyz"


# ── GET /nav/congestion/{station_key} ─────────────────────────────────────────

class TestCongestionPredict:

    def test_known_station_200(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}")
        assert resp.status_code == 200

    def test_response_has_required_fields(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}")
        data = resp.json()
        # Response wraps prediction under "prediction" key
        assert "prediction" in data
        pred = data["prediction"]
        required = {
            "station_key", "station_name", "congestion_level",
            "score", "percent_capacity", "is_peak", "period_type",
        }
        for field in required:
            assert field in pred, f"Missing field in prediction: {field}"

    def test_score_in_range(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}")
        data = resp.json()
        assert 0 <= data["prediction"]["score"] <= 100

    def test_congestion_level_valid_enum(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}")
        data = resp.json()
        assert data["prediction"]["congestion_level"] in ("light", "moderate", "heavy", "severe")

    def test_unknown_station_404(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{UNKNOWN_STATION}")
        assert resp.status_code == 404

    def test_weather_condition_query_param(self, client):
        resp_normal    = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}?weather_condition=normal")
        resp_blizzard  = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}?weather_condition=blizzard")
        assert resp_normal.status_code == 200
        assert resp_blizzard.status_code == 200
        normal_score   = resp_normal.json()["prediction"]["score"]
        blizzard_score = resp_blizzard.json()["prediction"]["score"]
        assert blizzard_score >= normal_score

    def test_all_loop_stations_return_200(self, client):
        for key in HISTORICAL_RIDERSHIP:
            resp = client.get(f"/api/v1/nav/congestion/{key}")
            assert resp.status_code == 200, f"Station {key} returned {resp.status_code}"


# ── GET /nav/congestion/{station_key}/trend ───────────────────────────────────

class TestCongestionTrend:

    def test_trend_200(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}/trend")
        assert resp.status_code == 200

    def test_trend_has_required_fields(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}/trend")
        data = resp.json()
        # Trend data is nested under "trend" key
        assert "trend" in data
        trend = data["trend"]
        required = {"current_score", "in_30min_score", "in_60min_score", "trend"}
        for field in required:
            assert field in trend, f"Missing: {field}"

    def test_trend_value_valid(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}/trend")
        data = resp.json()
        assert data["trend"]["trend"] in ("increasing", "decreasing", "stable")

    def test_unknown_station_404(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{UNKNOWN_STATION}/trend")
        assert resp.status_code == 404


# ── GET /nav/congestion/{station_key}/best-time ───────────────────────────────

class TestBestTime:

    def test_best_time_200(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}/best-time")
        assert resp.status_code == 200

    def test_best_time_has_hours(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}/best-time")
        data = resp.json()
        # Response key is "hourly_congestion" (hours 5–23 = 19 entries)
        assert "hourly_congestion" in data
        assert len(data["hourly_congestion"]) == 19

    def test_hour_entry_structure(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}/best-time")
        hours = resp.json()["hourly_congestion"]
        for entry in hours:
            assert "hour" in entry
            assert "congestion_level" in entry
            assert "is_recommended" in entry

    def test_weekend_day_type(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}/best-time?day_type=weekend")
        assert resp.status_code == 200

    def test_unknown_station_404(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{UNKNOWN_STATION}/best-time")
        assert resp.status_code == 404


# ── GET /nav/congestion/all ───────────────────────────────────────────────────

class TestCongestionAll:

    def test_all_200(self, client):
        resp = client.get("/api/v1/nav/congestion/all")
        assert resp.status_code == 200

    def test_all_has_stations(self, client):
        resp = client.get("/api/v1/nav/congestion/all")
        data = resp.json()
        assert "stations" in data
        assert len(data["stations"]) == len(HISTORICAL_RIDERSHIP)

    def test_all_sorted_by_score(self, client):
        resp = client.get("/api/v1/nav/congestion/all")
        stations = resp.json()["stations"]
        scores = [s["score"] for s in stations]
        assert scores == sorted(scores, reverse=True)

    def test_all_has_network_avg(self, client):
        resp = client.get("/api/v1/nav/congestion/all")
        assert "network_avg_score" in resp.json()


# ── GET /nav/congestion/worst ─────────────────────────────────────────────────

class TestCongestionWorst:

    def test_worst_200(self, client):
        resp = client.get("/api/v1/nav/congestion/worst")
        assert resp.status_code == 200

    def test_worst_has_stations(self, client):
        data = client.get("/api/v1/nav/congestion/worst").json()
        # Response key is "most_congested"
        assert "most_congested" in data
        assert len(data["most_congested"]) > 0

    def test_worst_top_n_param(self, client):
        # Query param is "limit", not "n"
        resp = client.get("/api/v1/nav/congestion/worst?limit=2")
        data = resp.json()
        assert len(data["most_congested"]) <= 2


# ── GET /nav/congestion/best ──────────────────────────────────────────────────

class TestCongestionBest:

    def test_best_200(self, client):
        resp = client.get("/api/v1/nav/congestion/best")
        assert resp.status_code == 200

    def test_best_less_congested_than_worst(self, client):
        best_score  = client.get("/api/v1/nav/congestion/best").json()["least_congested"][0]["score"]
        worst_score = client.get("/api/v1/nav/congestion/worst").json()["most_congested"][0]["score"]
        assert best_score <= worst_score


# ── POST /nav/congestion/compare ─────────────────────────────────────────────

class TestCongestionCompare:

    def test_compare_200(self, client):
        resp = client.post(
            "/api/v1/nav/congestion/compare",
            json={"station_keys": ["clark_lake", "quincy_wells", "adams_wabash"]},
        )
        assert resp.status_code == 200

    def test_compare_returns_ranked_list(self, client):
        resp = client.post(
            "/api/v1/nav/congestion/compare",
            json={"station_keys": ["clark_lake", "quincy_wells"]},
        )
        data = resp.json()
        # Response key is "ranked_least_to_most_congested"
        assert "ranked_least_to_most_congested" in data
        assert len(data["ranked_least_to_most_congested"]) == 2

    def test_compare_sorted_by_score(self, client):
        resp = client.post(
            "/api/v1/nav/congestion/compare",
            json={"station_keys": ["clark_lake", "quincy_wells", "harold_washington_library"]},
        )
        stations = resp.json()["ranked_least_to_most_congested"]
        scores = [s["score"] for s in stations]
        assert scores == sorted(scores)

    def test_compare_empty_list_422(self, client):
        resp = client.post("/api/v1/nav/congestion/compare", json={"station_keys": []})
        assert resp.status_code in (400, 422)


# ── GET /nav/congestion/routing-advice ───────────────────────────────────────

class TestRoutingAdvice:

    def test_routing_advice_200(self, client):
        resp = client.get(f"/api/v1/nav/congestion/routing-advice?station_key={KNOWN_STATION}")
        assert resp.status_code == 200

    def test_routing_advice_has_fields(self, client):
        resp = client.get(f"/api/v1/nav/congestion/routing-advice?station_key={KNOWN_STATION}")
        data = resp.json()
        assert "congestion" in data
        assert "score" in data


# ── GET /nav/congestion/events ────────────────────────────────────────────────

class TestCongestionEvents:

    def test_events_200(self, client):
        resp = client.get("/api/v1/nav/congestion/events")
        assert resp.status_code == 200

    def test_events_has_active_list(self, client):
        data = client.get("/api/v1/nav/congestion/events").json()
        assert "active_events" in data
        assert isinstance(data["active_events"], list)


# ── GET /nav/congestion/schedule ──────────────────────────────────────────────

class TestCongestionSchedule:

    def test_schedule_200(self, client):
        resp = client.get(f"/api/v1/nav/congestion/schedule?station_key={KNOWN_STATION}")
        assert resp.status_code == 200

    def test_schedule_has_13_hours(self, client):
        resp = client.get(f"/api/v1/nav/congestion/schedule?station_key={KNOWN_STATION}")
        data = resp.json()
        assert "forecast_hours" in data
        assert len(data["forecast_hours"]) == 13

    def test_schedule_unknown_station_404(self, client):
        resp = client.get(f"/api/v1/nav/congestion/schedule?station_key={UNKNOWN_STATION}")
        assert resp.status_code == 404


# ── GET /nav/congestion/health ────────────────────────────────────────────────

class TestCongestionHealth:

    def test_health_200(self, client):
        resp = client.get("/api/v1/nav/congestion/health")
        assert resp.status_code == 200

    def test_health_status_ok(self, client):
        data = client.get("/api/v1/nav/congestion/health").json()
        assert data["status"] == "ok"

    def test_health_has_real_data_fields(self, client):
        data = client.get("/api/v1/nav/congestion/health").json()
        assert "real_data_active" in data
        assert "chicago_token_set" in data
        assert "ridership_stations" in data


# ── GET /nav/congestion/{station_key}/real-data ───────────────────────────────

class TestRealDataSummary:

    def test_real_data_200(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}/real-data")
        assert resp.status_code == 200

    def test_real_data_has_fields(self, client):
        data = client.get(f"/api/v1/nav/congestion/{KNOWN_STATION}/real-data").json()
        assert "ridership_scale_factors" in data
        assert "crash_hotspot" in data
        assert "real_data_active" in data

    def test_real_data_unknown_station_404(self, client):
        resp = client.get(f"/api/v1/nav/congestion/{UNKNOWN_STATION}/real-data")
        assert resp.status_code == 404
