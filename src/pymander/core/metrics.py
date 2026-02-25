"""Redis-based metrics collection for observability."""

from __future__ import annotations

import time
from datetime import date

import structlog
from redis.asyncio import Redis

logger = structlog.get_logger()


class MetricsCollector:
    """Simple metrics collection using Redis.

    Three types:
    - Counter: increment-only (records ingested, errors)
    - Gauge: set to current value (queue depth, active connections)
    - Timer: record duration of operations
    """

    def __init__(self, redis: Redis, prefix: str = "metrics") -> None:
        self.redis = redis
        self.prefix = prefix

    def _key(self, name: str, tags: dict | None = None) -> str:
        key = f"{self.prefix}:{name}"
        if tags:
            tag_str = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
            key = f"{key}:{{{tag_str}}}"
        return key

    async def increment(
        self, name: str, value: int = 1, tags: dict | None = None
    ) -> None:
        """Increment a counter with per-minute, per-hour, and per-day rollups."""
        key = self._key(name, tags)
        minute_key = f"{key}:minute:{int(time.time() / 60)}"
        hour_key = f"{key}:hour:{int(time.time() / 3600)}"
        day_key = f"{key}:day:{date.today().isoformat()}"

        pipe = self.redis.pipeline()
        pipe.incr(key)
        pipe.incr(minute_key)
        pipe.expire(minute_key, 3600)
        pipe.incr(hour_key)
        pipe.expire(hour_key, 604800)
        pipe.incr(day_key)
        pipe.expire(day_key, 2592000)
        await pipe.execute()

    async def gauge(
        self, name: str, value: float, tags: dict | None = None
    ) -> None:
        """Set a gauge to current value."""
        key = self._key(name, tags)
        await self.redis.set(key, value, ex=3600)

    async def timer(
        self, name: str, duration_seconds: float, tags: dict | None = None
    ) -> None:
        """Record a timing measurement."""
        key = f"{self._key(name, tags)}:timings"
        await self.redis.lpush(key, duration_seconds)
        await self.redis.ltrim(key, 0, 999)
        await self.redis.expire(key, 86400)

    async def get_counter(self, name: str, tags: dict | None = None) -> int:
        """Get current counter value."""
        val = await self.redis.get(self._key(name, tags))
        return int(val) if val else 0

    async def get_gauge(self, name: str, tags: dict | None = None) -> float | None:
        """Get current gauge value."""
        val = await self.redis.get(self._key(name, tags))
        return float(val) if val else None

    async def get_rate_per_minute(
        self, name: str, tags: dict | None = None
    ) -> float:
        """Get the rate per minute over the last 5 minutes."""
        now_minute = int(time.time() / 60)
        key = self._key(name, tags)
        pipe = self.redis.pipeline()
        for i in range(5):
            pipe.get(f"{key}:minute:{now_minute - i}")
        results = await pipe.execute()
        total = sum(int(v) for v in results if v)
        return total / 5.0

    async def get_counter_for_day(
        self, name: str, day: date | None = None, tags: dict | None = None
    ) -> int:
        """Get counter value for a specific day."""
        day = day or date.today()
        key = f"{self._key(name, tags)}:day:{day.isoformat()}"
        val = await self.redis.get(key)
        return int(val) if val else 0

    async def get_timings(
        self, name: str, tags: dict | None = None, count: int = 100
    ) -> list[float]:
        """Get recent timing measurements."""
        key = f"{self._key(name, tags)}:timings"
        vals = await self.redis.lrange(key, 0, count - 1)
        return [float(v) for v in vals]

    async def get_hourly_counts(
        self, name: str, hours: int = 24, tags: dict | None = None
    ) -> list[dict]:
        """Get hourly counter values for the last N hours."""
        now_hour = int(time.time() / 3600)
        key = self._key(name, tags)
        pipe = self.redis.pipeline()
        for i in range(hours):
            pipe.get(f"{key}:hour:{now_hour - i}")
        results = await pipe.execute()
        return [
            {
                "hour_offset": -i,
                "timestamp": (now_hour - i) * 3600,
                "count": int(v) if v else 0,
            }
            for i, v in enumerate(results)
        ]
