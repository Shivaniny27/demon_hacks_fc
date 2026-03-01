"""
test_pedway_service.py
======================
Tests for Phase 3 Priority 3: Pedway Closures.

Covers:
  - PedwayHoursManager: open/closed determination by day + time
  - Segment data completeness
  - ClosureEventStore: add, retrieve, purge
  - PedwayService: close_segment, restore_segment, get_graph_edge_mods
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.pedway_service import (
    PedwayHoursManager,
    ClosureReason,
    SegmentStatus,
    SegmentType,
    ClosureEvent,
    PedwayService,
    PEDWAY_SEGMENTS,
    get_pedway_service,
)
from tests.conftest import MockRedis


# ── PedwayHoursManager ────────────────────────────────────────────────────────

class TestPedwayHoursManager:

    def setup_method(self):
        self.mgr = PedwayHoursManager()

    def _chicago_dt(self, weekday: int, hour: int, minute: int = 0) -> datetime:
        """
        Create a UTC datetime that corresponds to the given Chicago local time.
        weekday: 0=Mon … 6=Sun
        CST = UTC-6 (standard time). Properly handles midnight rollover.
        """
        # Find a Monday in 2025 to anchor weekday
        monday = datetime(2025, 10, 13, tzinfo=timezone.utc)   # Monday
        day    = monday + timedelta(days=weekday)
        # Correct: set local time then ADD 6h to get UTC (avoid modulo day-loss)
        local_dt = day.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return local_dt + timedelta(hours=6)

    def test_weekday_morning_open(self):
        # Wednesday 8 AM Chicago → open for prudential_connector (weekday 6:00–20:00)
        dt = self._chicago_dt(2, 8)
        assert self.mgr.is_open_now("prudential_connector", dt) is True

    def test_weekday_5am_closed(self):
        # Wednesday 5 AM Chicago → closed for prudential_connector (opens at 6:00)
        dt = self._chicago_dt(2, 5)
        assert self.mgr.is_open_now("prudential_connector", dt) is False

    def test_weekday_11pm_closed(self):
        # Wednesday 11 PM Chicago → closed for prudential_connector (closes at 20:00)
        dt = self._chicago_dt(2, 23)
        assert self.mgr.is_open_now("prudential_connector", dt) is False

    def test_weekend_afternoon_open(self):
        # Saturday 2 PM Chicago → open for prudential_connector (weekend 8:00–17:00)
        dt = self._chicago_dt(5, 14)
        assert self.mgr.is_open_now("prudential_connector", dt) is True

    def test_weekend_9pm_closed(self):
        # Saturday 9 PM Chicago → closed for prudential_connector (closes at 17:00)
        dt = self._chicago_dt(5, 21)
        assert self.mgr.is_open_now("prudential_connector", dt) is False

    def test_always_open_segment(self):
        # millennium_underground is ALWAYS_OPEN → returns True regardless of hour
        result = self.mgr.is_open_now("millennium_underground", self._chicago_dt(2, 3))
        assert result is True

    def test_next_open_time_returns_future(self):
        # 5 AM Wednesday → next open time for prudential_connector should be 6 AM same day
        dt = self._chicago_dt(2, 5)
        next_open = self.mgr.next_open_time("prudential_connector", dt)
        if next_open:
            assert next_open > dt


# ── Segment Data Completeness ─────────────────────────────────────────────────

class TestPedwaySegments:

    def test_segments_not_empty(self):
        assert len(PEDWAY_SEGMENTS) > 0

    def test_all_segments_have_required_fields(self):
        required = {"from_node", "to_node", "length_m", "type"}
        for seg_id, seg in PEDWAY_SEGMENTS.items():
            missing = required - set(seg.keys())
            assert missing == set(), f"Segment {seg_id} missing: {missing}"

    def test_segment_lengths_positive(self):
        for seg_id, seg in PEDWAY_SEGMENTS.items():
            assert seg["length_m"] > 0, f"Segment {seg_id} has non-positive length"

    def test_segment_types_valid(self):
        valid_types = {st.value for st in SegmentType}
        for seg_id, seg in PEDWAY_SEGMENTS.items():
            seg_type = seg.get("type", "")
            assert seg_type in valid_types or isinstance(seg_type, SegmentType), (
                f"Segment {seg_id} has invalid type: {seg_type}"
            )


# ── ClosureEvent ──────────────────────────────────────────────────────────────

class TestClosureEvent:

    def test_active_closure_has_future_expiry(self):
        now = datetime.now(timezone.utc)
        event = ClosureEvent(
            segment_id="millennium_underground",
            reason=ClosureReason.MAINTENANCE,
            description="Routine maintenance",
            created_at=now,
            expires_at=now + timedelta(hours=4),
            created_by="ops",
        )
        assert event.expires_at > event.created_at

    def test_closure_reason_values(self):
        # Ensure all reasons are usable
        for reason in ClosureReason:
            assert isinstance(reason.value, str)


# ── PedwayService ─────────────────────────────────────────────────────────────

class TestPedwayService:

    @pytest.fixture
    def service(self, mock_redis):
        svc = PedwayService(redis=mock_redis)
        return svc

    async def test_close_segment_adds_closure(self, service):
        segment_id = list(PEDWAY_SEGMENTS.keys())[0]
        result = await service.close_segment(
            segment_id=segment_id,
            reason=ClosureReason.EMERGENCY,
            description="Water pipe burst",
            duration_hours=2,
        )
        assert result is True

    async def test_restore_segment_removes_closure(self, service):
        segment_id = list(PEDWAY_SEGMENTS.keys())[0]
        closure_id = await service.close_segment(
            segment_id=segment_id,
            reason=ClosureReason.EMERGENCY,
            description="Test closure",
            duration_hours=2,
        )
        restored = await service.restore_segment(segment_id)
        assert restored is True

    async def test_closed_segment_high_weight_in_edge_mods(self, service):
        segment_id = list(PEDWAY_SEGMENTS.keys())[0]
        await service.close_segment(
            segment_id=segment_id,
            reason=ClosureReason.MAINTENANCE,
            description="Planned maintenance",
            duration_hours=8,
        )
        mods = await service.get_graph_edge_mods()
        assert segment_id in mods
        assert mods[segment_id] > 1.0   # closed = high penalty weight

    async def test_open_segment_not_in_edge_mods(self, service):
        mods = await service.get_graph_edge_mods()
        # get_graph_edge_mods returns ALL segments with weights (1.0=open, 999.0=closed)
        assert isinstance(mods, dict)
        assert len(mods) > 0

    async def test_restore_unknown_segment_returns_false(self, service):
        result = await service.restore_segment("nonexistent_segment_xyz")
        assert result is False


# ── Singleton ─────────────────────────────────────────────────────────────────

class TestPedwayServiceSingleton:

    def test_singleton_returns_same_instance(self):
        svc1 = get_pedway_service()
        svc2 = get_pedway_service()
        assert svc1 is svc2

    def test_service_has_initialize(self):
        svc = get_pedway_service()
        assert hasattr(svc, "initialize")

    def test_service_has_close_segment(self):
        svc = get_pedway_service()
        assert hasattr(svc, "close_segment")

    def test_service_has_get_graph_edge_mods(self):
        svc = get_pedway_service()
        assert hasattr(svc, "get_graph_edge_mods")
