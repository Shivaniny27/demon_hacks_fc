"""
conftest.py — Shared fixtures for LoopSense + LoopNav test suite.

All external I/O (PostgreSQL, Redis, CTA APIs, Chicago Data Portal,
OpenWeather, Azure) is mocked so tests run without any real credentials
or running services.
"""

import json
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ── Redis mock ────────────────────────────────────────────────────────────────

class MockRedis:
    """In-memory async Redis substitute for unit/integration tests."""

    def __init__(self):
        self._store: Dict[str, Any] = {}

    async def get(self, key: str) -> Optional[str]:
        return self._store.get(key)

    async def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        self._store[key] = value
        return True

    async def delete(self, key: str) -> int:
        return 1 if self._store.pop(key, None) is not None else 0

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        pass

    async def zadd(self, key: str, mapping: Dict) -> int:
        if key not in self._store:
            self._store[key] = {}
        self._store[key].update(mapping)
        return len(mapping)

    async def zrangebyscore(self, key: str, min_score, max_score, withscores=False):
        zset = self._store.get(key, {})
        items = [
            (k, v) for k, v in zset.items()
            if (min_score == "-inf" or float(v) >= float(min_score))
            and (max_score == "+inf" or float(v) <= float(max_score))
        ]
        items.sort(key=lambda x: x[1])
        if withscores:
            return items
        return [k for k, _ in items]

    async def zcard(self, key: str) -> int:
        return len(self._store.get(key, {}))

    async def expire(self, key: str, seconds: int) -> bool:
        return key in self._store

    async def exists(self, key: str) -> int:
        return 1 if key in self._store else 0

    async def smembers(self, key: str):
        return self._store.get(key, set())

    async def sadd(self, key: str, *values) -> int:
        if key not in self._store:
            self._store[key] = set()
        before = len(self._store[key])
        self._store[key].update(values)
        return len(self._store[key]) - before

    async def lrange(self, key: str, start: int, end: int):
        lst = self._store.get(key, [])
        if end == -1:
            return lst[start:]
        return lst[start:end + 1]

    async def lpush(self, key: str, *values) -> int:
        if key not in self._store:
            self._store[key] = []
        for v in values:
            self._store[key].insert(0, v)
        return len(self._store[key])

    async def ltrim(self, key: str, start: int, end: int) -> bool:
        lst = self._store.get(key, [])
        self._store[key] = lst[start:end + 1] if end != -1 else lst[start:]
        return True

    def pipeline(self):
        return MockPipeline(self)


class MockPipeline:
    def __init__(self, redis: MockRedis):
        self._redis = redis
        self._cmds = []

    def zadd(self, key, mapping):
        self._cmds.append(("zadd", key, mapping))
        return self

    def expire(self, key, seconds):
        self._cmds.append(("expire", key, seconds))
        return self

    async def execute(self):
        results = []
        for cmd, *args in self._cmds:
            if cmd == "zadd":
                results.append(await self._redis.zadd(*args))
            elif cmd == "expire":
                results.append(await self._redis.expire(*args))
        return results


@pytest.fixture
def mock_redis() -> MockRedis:
    return MockRedis()


# ── CTA Alerts API mock payload ───────────────────────────────────────────────

CTA_ALERTS_PAYLOAD = {
    "CTAAlerts": {
        "TimeStamp": "2025-01-15T08:30:00",
        "ErrorCode": "0",
        "ErrorMessage": None,
        "Alert": [
            {
                "AlertId": "EL-001",
                "Headline": "Elevator at Washington/Wabash CTA Station is out of service",
                "ShortDescription": "Elevator #E18 serving Washington/Wabash is out of service.",
                "FullDescription": "Due to equipment failure, the elevator at Washington/Wabash is unavailable.",
                "SeverityScore": "7",
                "ImpactType": "Elevator Status",
                "EventStart": "2025-01-15T06:00:00",
                "EventEnd": None,
                "TBD": "0",
                "MajorAlert": "0",
                "AlertURL": "",
                "AffectedService": {"Service": []},
            },
            {
                "AlertId": "RTE-002",
                "Headline": "Green Line — Significant delays due to track maintenance",
                "ShortDescription": "Green Line trains are experiencing 15-minute delays.",
                "FullDescription": "Track work at Harlem/Lake causing significant delays on Green Line.",
                "SeverityScore": "5",
                "ImpactType": "Train Route",
                "EventStart": "2025-01-15T07:00:00",
                "EventEnd": "2025-01-15T09:00:00",
                "TBD": "0",
                "MajorAlert": "0",
                "AlertURL": "",
                "AffectedService": {"Service": [{"RouteId": "G", "ServiceId": "G"}]},
            },
        ],
    }
}

CTA_TRAIN_ARRIVALS_PAYLOAD = {
    "ctatt": {
        "tmst": "2025-01-15T08:30:00",
        "errCd": "0",
        "errNm": None,
        "eta": [
            {
                "staId": "40380",
                "stpId": "30074",
                "staNm": "Clark/Lake",
                "stpDe": "Service toward Loop",
                "rn": "124",
                "rt": "G",
                "destSt": "30182",
                "destNm": "Harlem/Lake",
                "trDr": "5",
                "prdt": "2025-01-15T08:28:00",
                "arrT": "2025-01-15T08:32:00",
                "isApp": "0",
                "isSch": "0",
                "isDly": "0",
                "isFlt": "0",
                "flags": None,
                "lat": "41.8820",
                "lon": "-87.6280",
                "heading": "269",
            },
            {
                "staId": "40380",
                "stpId": "30074",
                "staNm": "Clark/Lake",
                "stpDe": "Service toward Loop",
                "rn": "228",
                "rt": "Brn",
                "destSt": "30249",
                "destNm": "Kimball",
                "trDr": "1",
                "prdt": "2025-01-15T08:29:00",
                "arrT": "2025-01-15T08:38:00",
                "isApp": "0",
                "isSch": "0",
                "isDly": "0",
                "isFlt": "0",
                "flags": None,
                "lat": "41.8900",
                "lon": "-87.6317",
                "heading": "180",
            },
        ],
    }
}

CHICAGO_RIDERSHIP_PAYLOAD = [
    {"stationname": "Clark/Lake",       "daytype": "W", "avg_rides": "9500.0"},
    {"stationname": "Clark/Lake",       "daytype": "A", "avg_rides": "5800.0"},
    {"stationname": "Clark/Lake",       "daytype": "U", "avg_rides": "4600.0"},
    {"stationname": "State/Lake",       "daytype": "W", "avg_rides": "7800.0"},
    {"stationname": "Washington/Wabash","daytype": "W", "avg_rides": "7200.0"},
    {"stationname": "Adams/Wabash",     "daytype": "W", "avg_rides": "6800.0"},
]

# GeoJSON feature format (v3 API — coordinates are [lon, lat])
CHICAGO_CRASHES_PAYLOAD = [
    {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-87.6317, 41.8858]},
        "properties": {"crash_date": "2026-01-10T08:00:00", "injuries_total": "0"},
    },
    {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-87.6280, 41.8858]},
        "properties": {"crash_date": "2026-01-11T17:30:00", "injuries_total": "1"},
    },
    {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-87.6262, 41.8829]},
        "properties": {"crash_date": "2026-01-12T09:00:00", "injuries_total": "0"},
    },
]


@pytest.fixture
def mock_cta_alerts_response():
    return CTA_ALERTS_PAYLOAD


@pytest.fixture
def mock_ridership_response():
    return CHICAGO_RIDERSHIP_PAYLOAD


@pytest.fixture
def mock_crashes_response():
    return CHICAGO_CRASHES_PAYLOAD


# ── FastAPI TestClient ────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def test_client():
    """
    FastAPI TestClient with:
      - init_db mocked (no PostgreSQL required)
      - Redis mocked (no Redis required)
      - Phase 3 services mocked at lifespan level
      - USE_STUB_GRAPH=true (16-node stub graph)
    """
    import os
    os.environ.setdefault("USE_STUB_GRAPH", "true")
    os.environ.setdefault("DEBUG", "true")

    with (
        patch("app.database.init_db", new_callable=AsyncMock),
        patch("redis.asyncio.from_url", return_value=MockRedis()),
        patch("app.services.elevator_service.get_elevator_service") as mock_elev,
        patch("app.services.outage_service.get_outage_service") as mock_outage,
        patch("app.services.pedway_service.get_pedway_service") as mock_pedway,
        patch("app.services.congestion_service.get_congestion_service") as mock_cong,
    ):
        # Configure lightweight mock services for lifespan
        for mock_svc in (mock_elev, mock_outage, mock_pedway, mock_cong):
            svc = AsyncMock()
            svc.initialize = AsyncMock()
            svc.shutdown = AsyncMock()
            svc._initialized = True
            mock_svc.return_value = svc

        from app.main import app
        with TestClient(app, raise_server_exceptions=True) as client:
            yield client


# ── Time helpers ──────────────────────────────────────────────────────────────

@pytest.fixture
def am_peak_dt():
    """8:14 AM Chicago = 14:14 UTC (standard time)."""
    return datetime(2025, 10, 15, 14, 14, 0, tzinfo=timezone.utc)  # Wed, 8:14 AM CDT


@pytest.fixture
def pm_peak_dt():
    """5:30 PM Chicago = 23:30 UTC (standard time)."""
    return datetime(2025, 10, 15, 23, 30, 0, tzinfo=timezone.utc)  # Wed, 5:30 PM CDT


@pytest.fixture
def weekend_midday_dt():
    """Saturday 1:00 PM Chicago = 19:00 UTC."""
    return datetime(2025, 10, 18, 19, 0, 0, tzinfo=timezone.utc)  # Sat, 1:00 PM CDT
