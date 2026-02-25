"""Engagement snapshot poller — re-polls content to build velocity curves."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime

import asyncpraw
import structlog
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.producer import KafkaProducerWrapper

logger = structlog.get_logger()

SNAPSHOT_INTERVALS = [
    300,      # 5 minutes
    900,      # 15 minutes
    3600,     # 1 hour
    21600,    # 6 hours
    86400,    # 24 hours
    259200,   # 72 hours
]

QUEUE_KEY = "engagement_poll_queue"


class EngagementPoller:
    """Re-polls already-ingested content to build engagement time series.

    Uses a Redis sorted set with next-poll timestamps as scores.
    """

    def __init__(
        self,
        redis: Redis,
        metrics: MetricsCollector,
        producer: KafkaProducerWrapper,
    ) -> None:
        self.redis = redis
        self.metrics = metrics
        self.producer = producer
        self._running = True
        self._reddit: asyncpraw.Reddit | None = None

    async def setup(self) -> None:
        settings = get_settings()
        self._reddit = asyncpraw.Reddit(
            client_id=settings.reddit.client_id,
            client_secret=settings.reddit.client_secret,
            user_agent=settings.reddit.user_agent,
        )

    async def schedule_snapshots(
        self,
        content_id: str,
        platform: str,
        platform_content_id: str,
        created_at: datetime,
    ) -> None:
        """Schedule all future engagement snapshots for a piece of content."""
        for interval in SNAPSHOT_INTERVALS:
            poll_at = created_at.timestamp() + interval
            item = json.dumps({
                "content_id": content_id,
                "platform": platform,
                "platform_id": platform_content_id,
                "interval": interval,
            })
            await self.redis.zadd(QUEUE_KEY, {item: poll_at})
        await self.metrics.increment(
            "engagement.snapshots.scheduled",
            value=len(SNAPSHOT_INTERVALS),
        )

    async def fetch_reddit_engagement(
        self, platform_id: str
    ) -> dict | None:
        """Fetch current engagement metrics for a Reddit submission."""
        if not self._reddit:
            return None
        try:
            submission = await self._reddit.submission(id=platform_id)
            return {
                "likes": submission.score,
                "replies": submission.num_comments,
                "quotes": getattr(submission, "num_crossposts", 0),
                "views": getattr(submission, "view_count", None),
                "upvote_ratio": getattr(submission, "upvote_ratio", None),
                "timestamp": datetime.now(UTC).isoformat(),
            }
        except Exception as e:
            logger.warning(
                "engagement_fetch_error",
                platform_id=platform_id,
                error=str(e),
            )
            return None

    async def fetch_engagement(
        self, platform: str, platform_id: str
    ) -> dict | None:
        if platform == "reddit":
            return await self.fetch_reddit_engagement(platform_id)
        return None

    async def store_snapshot(
        self, content_id: str, interval: int, snapshot: dict
    ) -> None:
        """Store engagement snapshot in Redis list."""
        key = f"engagement:snapshots:{content_id}"
        snapshot["interval"] = interval
        await self.redis.rpush(key, json.dumps(snapshot))
        await self.redis.expire(key, 604800)  # 7 days

    async def get_snapshots(self, content_id: str) -> list[dict]:
        """Get all snapshots for a content item."""
        key = f"engagement:snapshots:{content_id}"
        raw = await self.redis.lrange(key, 0, -1)
        return [json.loads(r) for r in raw]

    async def compute_velocity(self, content_id: str) -> dict | None:
        """Compute engagement velocity from snapshots."""
        snapshots = await self.get_snapshots(content_id)
        if len(snapshots) < 2:
            return None

        latest = snapshots[-1]
        previous = snapshots[-2]

        t_latest = datetime.fromisoformat(latest["timestamp"])
        t_previous = datetime.fromisoformat(previous["timestamp"])
        dt_hours = (t_latest - t_previous).total_seconds() / 3600

        if dt_hours == 0:
            return None

        velocity = {
            "likes_per_hour": (latest.get("likes", 0) - previous.get("likes", 0)) / dt_hours,
            "replies_per_hour": (
                latest.get("replies", 0) - previous.get("replies", 0)
            ) / dt_hours,
            "timestamp": latest["timestamp"],
            "content_id": content_id,
        }

        # Compute acceleration if 3+ snapshots
        if len(snapshots) >= 3:
            prev_prev = snapshots[-3]
            t_pp = datetime.fromisoformat(prev_prev["timestamp"])
            prev_dt = (t_previous - t_pp).total_seconds() / 3600
            if prev_dt > 0:
                prev_vel = (
                    previous.get("likes", 0) - prev_prev.get("likes", 0)
                ) / prev_dt
                velocity["acceleration"] = (
                    velocity["likes_per_hour"] - prev_vel
                ) / dt_hours

        # Store velocity
        vkey = f"engagement:velocity:{content_id}"
        await self.redis.set(vkey, json.dumps(velocity), ex=604800)

        # Flag anomaly if velocity is extremely high
        if velocity["likes_per_hour"] > 500:
            velocity["is_anomaly"] = True
            await self.producer.send(
                "alerts.velocity_anomaly",
                {
                    "content_id": content_id,
                    "velocity": velocity,
                    "severity": (
                        "high"
                        if velocity["likes_per_hour"] > 2000
                        else "medium"
                    ),
                },
            )
            await self.metrics.increment("engagement.anomalies.detected")

        return velocity

    async def poll_loop(self) -> None:
        """Continuously process due engagement polls."""
        logger.info("engagement_poller_started")
        while self._running:
            now = time.time()
            items = await self.redis.zrangebyscore(
                QUEUE_KEY, 0, now, start=0, num=100
            )
            if not items:
                await asyncio.sleep(1)
                continue

            for item_json in items:
                item = json.loads(item_json)
                try:
                    snapshot = await self.fetch_engagement(
                        item["platform"], item["platform_id"]
                    )
                    if snapshot:
                        await self.store_snapshot(
                            item["content_id"],
                            item["interval"],
                            snapshot,
                        )
                        await self.compute_velocity(item["content_id"])
                        await self.metrics.increment(
                            "engagement.snapshots.captured"
                        )
                except Exception as e:
                    logger.error(
                        "engagement_poll_error",
                        content_id=item["content_id"],
                        error=str(e),
                    )
                    await self.metrics.increment(
                        "engagement.snapshots.errors"
                    )

                await self.redis.zrem(QUEUE_KEY, item_json)

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    producer = KafkaProducerWrapper()
    await producer.start()

    poller = EngagementPoller(redis, metrics, producer)
    await poller.setup()
    try:
        await poller.poll_loop()
    except KeyboardInterrupt:
        poller.stop()
    finally:
        if poller._reddit:
            await poller._reddit.close()
        await producer.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
