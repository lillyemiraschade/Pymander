"""Tests for the Redis-based metrics collector."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from pymander.core.metrics import MetricsCollector


@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    pipe = MagicMock()
    pipe.incr = MagicMock(return_value=pipe)
    pipe.expire = MagicMock(return_value=pipe)
    pipe.get = MagicMock(return_value=pipe)
    pipe.execute = AsyncMock(return_value=[])
    redis.pipeline = MagicMock(return_value=pipe)
    return redis


@pytest.fixture
def metrics(mock_redis):
    return MetricsCollector(mock_redis)


class TestMetricsCollector:
    def test_key_generation(self, metrics):
        assert metrics._key("test.counter") == "metrics:test.counter"

    def test_key_generation_with_tags(self, metrics):
        key = metrics._key("test.counter", tags={"a": "1", "b": "2"})
        assert key == "metrics:test.counter:{a=1,b=2}"

    async def test_increment(self, metrics, mock_redis):
        await metrics.increment("test.counter")
        mock_redis.pipeline.assert_called()

    async def test_gauge(self, metrics, mock_redis):
        await metrics.gauge("test.gauge", 42.0)
        mock_redis.set.assert_called_once()
        args = mock_redis.set.call_args
        assert args[0][1] == 42.0

    async def test_timer(self, metrics, mock_redis):
        await metrics.timer("test.timer", 1.5)
        mock_redis.lpush.assert_called_once()

    async def test_get_counter(self, metrics, mock_redis):
        mock_redis.get.return_value = b"42"
        result = await metrics.get_counter("test.counter")
        assert result == 42

    async def test_get_counter_missing(self, metrics, mock_redis):
        mock_redis.get.return_value = None
        result = await metrics.get_counter("test.counter")
        assert result == 0

    async def test_get_gauge(self, metrics, mock_redis):
        mock_redis.get.return_value = b"3.14"
        result = await metrics.get_gauge("test.gauge")
        assert result == 3.14

    async def test_get_gauge_missing(self, metrics, mock_redis):
        mock_redis.get.return_value = None
        result = await metrics.get_gauge("test.gauge")
        assert result is None

    async def test_get_timings(self, metrics, mock_redis):
        mock_redis.lrange.return_value = [b"1.5", b"2.0", b"0.5"]
        result = await metrics.get_timings("test.timer")
        assert result == [1.5, 2.0, 0.5]
