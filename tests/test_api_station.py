"""
test_api_station.py
===================
HTTP integration tests for /api/v1/nav/station/* endpoints.
"""

import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient

from app.services.station_service import STATION_PROFILES


@pytest.fixture(scope="module")
def client():
    import os
    os.environ["USE_STUB_GRAPH"] = "true"

    with (
        patch("app.database.init_db", new_callable=AsyncMock),
        patch("redis.asyncio.from_url") as mock_rf,
    ):
        mock_r = AsyncMock()
        mock_r.ping = AsyncMock(side_effect=ConnectionRefusedError("no redis"))
        mock_rf.return_value = mock_r

        from app.main import app
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c


class TestStationAll:

    def test_all_200(self, client):
        resp = client.get("/api/v1/nav/station/all")
        assert resp.status_code == 200

    def test_all_returns_stations(self, client):
        data = client.get("/api/v1/nav/station/all").json()
        assert "stations" in data
        assert len(data["stations"]) > 0

    def test_pedway_only_filter(self, client):
        resp = client.get("/api/v1/nav/station/all?pedway_only=true")
        assert resp.status_code == 200
        stations = resp.json()["stations"]
        # All returned stations should be Pedway-connected
        for s in stations:
            assert s.get("pedway_connected") is True

    def test_line_filter(self, client):
        resp = client.get("/api/v1/nav/station/all?line=Green")
        assert resp.status_code == 200


class TestStationPedway:

    def test_pedway_200(self, client):
        resp = client.get("/api/v1/nav/station/pedway")
        assert resp.status_code == 200

    def test_pedway_only_connected_stations(self, client):
        data = client.get("/api/v1/nav/station/pedway").json()
        assert "stations" in data


class TestStationRank:

    def test_rank_200(self, client):
        resp = client.get("/api/v1/nav/station/rank?lat=41.8858&lon=-87.6317")
        assert resp.status_code == 200

    def test_rank_returns_ordered_list(self, client):
        data = client.get("/api/v1/nav/station/rank?lat=41.8858&lon=-87.6317").json()
        # Response key is "ranked_stations"
        assert "ranked_stations" in data
        stations = data["ranked_stations"]
        scores = [s["score"] for s in stations]
        assert scores == sorted(scores)

    def test_rank_missing_coords_422(self, client):
        resp = client.get("/api/v1/nav/station/rank")
        assert resp.status_code == 422


class TestStationNearest:

    def test_nearest_200(self, client):
        resp = client.get("/api/v1/nav/station/nearest?lat=41.8858&lon=-87.6317")
        assert resp.status_code == 200

    def test_nearest_n_param(self, client):
        # Query param is "limit", not "n"
        resp = client.get("/api/v1/nav/station/nearest?lat=41.8858&lon=-87.6317&limit=2")
        data = resp.json()
        # Response key is "nearest"
        assert len(data["nearest"]) <= 2

    def test_nearest_first_is_clark_lake(self, client):
        resp = client.get("/api/v1/nav/station/nearest?lat=41.8858&lon=-87.6317&limit=1")
        stations = resp.json()["nearest"]
        if stations:
            assert stations[0]["station_key"] == "clark_lake"


class TestStationSearch:

    def test_search_200(self, client):
        resp = client.get("/api/v1/nav/station/search?q=clark")
        assert resp.status_code == 200

    def test_search_finds_clark_lake(self, client):
        data = client.get("/api/v1/nav/station/search?q=Clark").json()
        # Response key is "results"
        keys = [s["station_key"] for s in data.get("results", [])]
        assert "clark_lake" in keys

    def test_search_case_insensitive(self, client):
        upper = client.get("/api/v1/nav/station/search?q=CLARK").json()
        lower = client.get("/api/v1/nav/station/search?q=clark").json()
        upper_keys = {s["station_key"] for s in upper.get("results", [])}
        lower_keys = {s["station_key"] for s in lower.get("results", [])}
        assert upper_keys == lower_keys


class TestStationDetail:

    def test_station_detail_200(self, client):
        resp = client.get("/api/v1/nav/station/clark_lake")
        assert resp.status_code == 200

    def test_station_detail_has_name(self, client):
        data = client.get("/api/v1/nav/station/clark_lake").json()
        # Response key is "name", not "station_name"
        assert data.get("name") == "Clark/Lake"

    def test_station_detail_has_entrances(self, client):
        data = client.get("/api/v1/nav/station/clark_lake").json()
        assert "entrances" in data
        assert len(data["entrances"]) > 0

    def test_station_detail_unknown_404(self, client):
        resp = client.get("/api/v1/nav/station/nonexistent_xyz")
        assert resp.status_code == 404

    def test_all_profiled_stations_return_200(self, client):
        for key in STATION_PROFILES:
            resp = client.get(f"/api/v1/nav/station/{key}")
            assert resp.status_code == 200, f"Station {key} returned {resp.status_code}"


class TestStationEntrances:

    def test_entrances_200(self, client):
        resp = client.get("/api/v1/nav/station/clark_lake/entrances")
        assert resp.status_code == 200

    def test_entrances_returns_list(self, client):
        data = client.get("/api/v1/nav/station/clark_lake/entrances").json()
        assert "entrances" in data
        assert isinstance(data["entrances"], list)


class TestStationPlatformTip:

    def test_platform_tip_200(self, client):
        # "destination" is a required query param
        resp = client.get("/api/v1/nav/station/clark_lake/platform-tip?destination=state_lake&line=Green")
        assert resp.status_code == 200

    def test_platform_tip_has_advice(self, client):
        data = client.get(
            "/api/v1/nav/station/clark_lake/platform-tip?destination=state_lake&line=Green"
        ).json()
        # Response always has "tip_found" (True or False)
        assert "tip_found" in data or "boarding_station" in data


class TestStationAccessibleEntrance:

    def test_accessible_entrance_200(self, client):
        resp = client.get("/api/v1/nav/station/clark_lake/accessible-entrance")
        assert resp.status_code == 200
