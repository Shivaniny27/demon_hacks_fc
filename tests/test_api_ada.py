"""
test_api_ada.py
===============
HTTP integration tests for /api/v1/nav/ada/* endpoints (ADA / Elevator).

All CTA API calls are intercepted — no real API keys needed.
"""

import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient


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


class TestADAElevators:

    def test_elevators_200(self, client):
        resp = client.get("/api/v1/nav/ada/elevators")
        assert resp.status_code == 200

    def test_elevators_returns_list(self, client):
        data = client.get("/api/v1/nav/ada/elevators").json()
        # Response key is "outages" (not "elevators")
        assert "outages" in data
        assert isinstance(data["outages"], list)

    def test_elevators_severity_filter(self, client):
        resp = client.get("/api/v1/nav/ada/elevators?severity=major")
        assert resp.status_code == 200

    def test_elevators_line_filter(self, client):
        resp = client.get("/api/v1/nav/ada/elevators?line=G")
        assert resp.status_code == 200


class TestADAStations:

    def test_stations_200(self, client):
        resp = client.get("/api/v1/nav/ada/stations")
        assert resp.status_code == 200

    def test_stations_has_list(self, client):
        data = client.get("/api/v1/nav/ada/stations").json()
        assert "stations" in data
        assert isinstance(data["stations"], list)
        assert len(data["stations"]) > 0

    def test_single_station_200(self, client):
        resp = client.get("/api/v1/nav/ada/stations/clark_lake")
        assert resp.status_code == 200

    def test_unknown_station_404(self, client):
        resp = client.get("/api/v1/nav/ada/stations/nonexistent_xyz")
        assert resp.status_code == 404


class TestADAReliable:

    def test_reliable_200(self, client):
        resp = client.get("/api/v1/nav/ada/reliable")
        assert resp.status_code == 200

    def test_reliable_has_stations(self, client):
        data = client.get("/api/v1/nav/ada/reliable").json()
        assert "stations" in data


class TestADARoutingParams:

    def test_routing_params_200(self, client):
        resp = client.get("/api/v1/nav/ada/routing-params")
        assert resp.status_code == 200

    def test_routing_params_has_edge_mods(self, client):
        data = client.get("/api/v1/nav/ada/routing-params").json()
        assert "edge_modifications" in data
        assert isinstance(data["edge_modifications"], dict)


class TestADAReliability:

    def test_reliability_200(self, client):
        resp = client.get("/api/v1/nav/ada/reliability")
        assert resp.status_code == 200

    def test_reliability_has_scores(self, client):
        data = client.get("/api/v1/nav/ada/reliability").json()
        # Response key is "reliability_scores" (not "scores")
        assert "reliability_scores" in data


class TestADAChronic:

    def test_chronic_200(self, client):
        resp = client.get("/api/v1/nav/ada/chronic")
        assert resp.status_code == 200


class TestADAHealth:

    def test_route_warnings_200(self, client):
        resp = client.get("/api/v1/nav/ada/route-warnings?stations=clark_lake,state_lake")
        assert resp.status_code == 200
