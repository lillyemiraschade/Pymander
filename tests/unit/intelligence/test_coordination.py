"""Tests for the coordination detection engine."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from pymander.intelligence.coordination import (
    BatchCoordinationAnalyzer,
    RealtimeCoordinationDetector,
)
from pymander.schemas.coordination import CoordinationSignal
from pymander.schemas.enums import CoordinationSignalType


@pytest.fixture
def realtime_detector():
    redis = AsyncMock()
    redis.zadd = AsyncMock()
    redis.expire = AsyncMock()
    redis.zrangebyscore = AsyncMock(return_value=[])
    redis.set = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.lrange = AsyncMock(return_value=[])
    redis.lpush = AsyncMock()
    redis.ltrim = AsyncMock()
    neo4j = AsyncMock()
    neo4j.execute = AsyncMock(return_value=[{"edge_count": 0}])
    metrics = AsyncMock()
    metrics.increment = AsyncMock()
    return RealtimeCoordinationDetector(redis, neo4j, metrics)


@pytest.fixture
def batch_analyzer():
    redis = AsyncMock()
    redis.smembers = AsyncMock(return_value=set())
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock()
    redis.sadd = AsyncMock()
    redis.expire = AsyncMock()
    redis.zrangebyscore = AsyncMock(return_value=[])
    neo4j = AsyncMock()
    neo4j.execute = AsyncMock(return_value=[])
    neo4j.execute_write = AsyncMock()
    metrics = AsyncMock()
    metrics.increment = AsyncMock()
    return BatchCoordinationAnalyzer(neo4j, redis, metrics)


async def test_emit_signal(realtime_detector):
    signal = CoordinationSignal(
        type=CoordinationSignalType.TEMPORAL_BURST,
        accounts=["user1", "user2"],
        confidence=0.75,
        evidence={"test": True},
    )
    await realtime_detector.emit_signal(signal)
    realtime_detector.redis.set.assert_called_once()
    realtime_detector.redis.zadd.assert_called()


async def test_check_connection_density_empty(realtime_detector):
    density = await realtime_detector.check_connection_density(["a"])
    assert density == 0.0


async def test_check_connection_density_connected(realtime_detector):
    realtime_detector.neo4j.execute = AsyncMock(
        return_value=[{"edge_count": 3}]
    )
    density = await realtime_detector.check_connection_density(
        ["a", "b", "c"]
    )
    assert density == 1.0  # 3 edges / 3 possible = 1.0


async def test_temporal_burst_no_trigger(realtime_detector):
    """Below threshold doesn't trigger signal."""
    realtime_detector.redis.zrangebyscore = AsyncMock(
        return_value=[b"user1", b"user2", b"user3"]
    )
    record = {
        "narrative_ids": ["narr_1"],
        "actor": {"platform_id": "user1"},
        "platform": "reddit",
    }
    await realtime_detector.check_temporal_burst(record)
    # Should not emit signal (only 3 authors, threshold is 10)
    realtime_detector.redis.set.assert_not_called()


async def test_temporal_burst_triggers(realtime_detector):
    """Above threshold with low connection density triggers signal."""
    authors = [f"user_{i}".encode() for i in range(15)]
    realtime_detector.redis.zrangebyscore = AsyncMock(return_value=authors)
    realtime_detector.neo4j.execute = AsyncMock(
        return_value=[{"edge_count": 0}]
    )
    record = {
        "narrative_ids": ["narr_1"],
        "actor": {"platform_id": "user_0"},
        "platform": "reddit",
    }
    await realtime_detector.check_temporal_burst(record)
    realtime_detector.redis.set.assert_called()
    realtime_detector.metrics.increment.assert_any_call(
        "coordination.temporal_burst.detected"
    )


async def test_bot_indicators_short_history(realtime_detector):
    """Short history should not trigger any signals."""
    realtime_detector.redis.lrange = AsyncMock(
        return_value=[json.dumps({"timestamp": "2024-01-01T00:00:00"}).encode()]
    )
    record = {
        "actor": {"platform_id": "user1"},
        "platform": "reddit",
        "created_at": "2024-01-01T00:01:00",
        "content_type": "post",
    }
    await realtime_detector.check_bot_indicators(record)
    # No signals should be emitted for short history
    realtime_detector.redis.set.assert_not_called()


async def test_get_recent_signals_empty(batch_analyzer):
    signals = await batch_analyzer.get_recent_signals(hours=24)
    assert signals == []


async def test_aggregate_clusters_no_signals(batch_analyzer):
    clusters = await batch_analyzer.aggregate_into_clusters()
    assert clusters == []


async def test_estimate_cluster_reach(batch_analyzer):
    batch_analyzer.neo4j.execute = AsyncMock(
        return_value=[{"total_reach": 50000}]
    )
    reach = await batch_analyzer.estimate_cluster_reach({"a", "b", "c"})
    assert reach == 50000


async def test_stop(realtime_detector, batch_analyzer):
    realtime_detector.stop()
    assert realtime_detector._running is False
    batch_analyzer.stop()
    assert batch_analyzer._running is False
