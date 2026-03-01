"""
test_api_tracker.py
===================
HTTP integration tests for /api/v1/nav/tracker/* endpoints.

Train Tracker API calls are mocked so no CTA_TRAIN_TRACKER_KEY is needed.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

from tests.conftest import CTA_TRAIN_ARRIVALS_PAYLOAD

# Extract the ETA list that fetch_arrivals actually returns
_ETA_LIST = CTA_TRAIN_ARRIVALS_PAYLOAD["ctatt"]["eta"]


@pytest.fixture(scope="module")
def client():
    import os
    os.environ["USE_STUB_GRAPH"] = "true"

    with (
        patch("app.database.init_db", new_callable=AsyncMock),
        patch("redis.asyncio.from_url") as mock_rf,
        # Mock the CTA Train Tracker API call inside sync_service
        patch(
            "app.services.sync_service.TrainTrackerApiClient.fetch_arrivals",
            new_callable=AsyncMock,
            return_value=_ETA_LIST,
        ),
    ):
        mock_r = AsyncMock()
        mock_r.ping = AsyncMock(side_effect=ConnectionRefusedError("no redis"))
        mock_rf.return_value = mock_r

        from app.main import app
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c


KNOWN_STATION = "clark_lake"
UNKNOWN_STATION = "nonexistent_xyz"


class TestTrackerArrivals:

    def test_arrivals_known_station_200(self, client):
        resp = client.get(f"/api/v1/nav/tracker/arrivals/{KNOWN_STATION}")
        assert resp.status_code == 200

    def test_arrivals_has_trains_list(self, client):
        data = client.get(f"/api/v1/nav/tracker/arrivals/{KNOWN_STATION}").json()
        assert "arrivals" in data or "trains" in data or "eta" in data

    def test_arrivals_unknown_station_404(self, client):
        resp = client.get(f"/api/v1/nav/tracker/arrivals/{UNKNOWN_STATION}")
        assert resp.status_code == 404

    def test_arrivals_line_filter(self, client):
        resp = client.get(f"/api/v1/nav/tracker/arrivals/{KNOWN_STATION}?line=Green")
        assert resp.status_code == 200

    def test_arrivals_all_loop_200(self, client):
        resp = client.get("/api/v1/nav/tracker/arrivals")
        assert resp.status_code == 200


class TestTrackerSync:

    def test_sync_200(self, client):
        resp = client.post(
            "/api/v1/nav/tracker/sync",
            json={
                "lat": 41.8858,
                "lon": -87.6317,
                "station_key": "clark_lake",
                "line": "Green",
                "buffer_seconds": 60,
                "accessible": False,
            },
        )
        assert resp.status_code == 200

    def test_sync_has_leave_in_display(self, client):
        resp = client.post(
            "/api/v1/nav/tracker/sync",
            json={
                "lat": 41.8858, "lon": -87.6317,
                "station_key": "clark_lake", "line": "Green",
                "buffer_seconds": 60, "accessible": False,
            },
        )
        data = resp.json()
        # Departure info is nested under "recommended"
        assert "recommended" in data

    def test_sync_missing_coords_422(self, client):
        resp = client.post(
            "/api/v1/nav/tracker/sync",
            json={"station_key": "clark_lake"},
        )
        assert resp.status_code == 422

    def test_sync_unknown_station_404(self, client):
        resp = client.post(
            "/api/v1/nav/tracker/sync",
            json={
                "lat": 41.8858, "lon": -87.6317,
                "station_key": UNKNOWN_STATION,
                "line": "Green", "buffer_seconds": 60,
            },
        )
        assert resp.status_code in (404, 422)


class TestTrackerMultiSync:

    def test_multi_sync_200(self, client):
        resp = client.post(
            "/api/v1/nav/tracker/sync/multi",
            json={
                "lat": 41.8858,
                "lon": -87.6317,
                "max_walk_m": 800.0,
                "buffer_seconds": 60,
                "accessible": False,
            },
        )
        assert resp.status_code == 200

    def test_multi_sync_has_recommended(self, client):
        resp = client.post(
            "/api/v1/nav/tracker/sync/multi",
            json={
                "lat": 41.8858, "lon": -87.6317,
                "max_walk_m": 800.0,
                "buffer_seconds": 60,
            },
        )
        data = resp.json()
        assert "recommended" in data or "stations" in data


class TestTrackerWalk:

    def test_walk_200(self, client):
        resp = client.get(
            f"/api/v1/nav/tracker/walk/{KNOWN_STATION}?lat=41.8858&lon=-87.6317"
        )
        assert resp.status_code == 200

    def test_walk_has_walk_seconds(self, client):
        data = client.get(
            f"/api/v1/nav/tracker/walk/{KNOWN_STATION}?lat=41.8858&lon=-87.6317"
        ).json()
        assert "walk_seconds" in data

    def test_walk_seconds_positive(self, client):
        data = client.get(
            f"/api/v1/nav/tracker/walk/{KNOWN_STATION}?lat=41.8858&lon=-87.6317"
        ).json()
        assert data["walk_seconds"] > 0


class TestTrackerLines:

    def test_line_arrivals_200(self, client):
        resp = client.get("/api/v1/nav/tracker/lines/Green")
        assert resp.status_code == 200

    def test_line_arrivals_returns_stations(self, client):
        data = client.get("/api/v1/nav/tracker/lines/Green").json()
        assert "stations" in data or "arrivals" in data
