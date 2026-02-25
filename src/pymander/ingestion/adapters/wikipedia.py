"""Wikipedia EventStreams adapter using SSE (aiohttp).

Connects to the Wikimedia EventStreams API to monitor real-time edits on
English Wikipedia.  High-signal pages are tracked for edit volume, revert
rates, and edit-war detection -- all useful for narrative detection.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import aiohttp
import structlog
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.adapters.base import AbstractSourceAdapter
from pymander.ingestion.producer import KafkaProducerWrapper
from pymander.ingestion.topics import RAW_WIKIPEDIA
from pymander.schemas.content import (
    ActorInfo,
    EngagementMetrics,
    UnifiedContentRecord,
)
from pymander.schemas.enums import ContentType, Platform

logger = structlog.get_logger()

SSE_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# High-signal pages / title prefixes whose edits matter for narrative tracking.
HIGH_SIGNAL_PREFIXES = [
    "2024", "2025", "2026",
    "Russo-Ukrainian War", "Russia", "Ukraine",
    "Israel", "Palestine", "Gaza",
    "COVID-19", "SARS-CoV-2",
    "Artificial intelligence", "Large language model",
    "Climate change", "Global warming",
    "Election", "Referendum",
    "Disinformation", "Misinformation",
    "NATO", "European Union",
    "United States", "China", "Taiwan",
    "Assassination", "Mass shooting",
    "Cryptocurrency", "Bitcoin",
]

# Categories whose pages are always considered high-signal.
HIGH_SIGNAL_CATEGORIES = {
    "Living people",
    "Current events",
    "Ongoing conflicts",
    "Politicians",
}

# Redis key TTLs.
DEDUP_TTL = 86400  # 1 day
EDIT_VOLUME_WINDOW = 3600  # 1 hour for rate tracking

# How long between reconnection attempts on stream failure.
RECONNECT_DELAY = 5


def _is_high_signal(title: str) -> bool:
    """Return True if the page title matches a high-signal prefix."""
    return any(prefix.lower() in title.lower() for prefix in HIGH_SIGNAL_PREFIXES)


def _change_to_record(change: dict) -> UnifiedContentRecord:
    """Map a Wikimedia RecentChange event to a UnifiedContentRecord."""
    title = change.get("title", "")
    user = change.get("user", "unknown")
    comment = change.get("comment", "")
    rev_id = change.get("revision", {}).get("new", 0)
    old_rev = change.get("revision", {}).get("old", 0)
    page_id = change.get("id", 0)
    namespace = change.get("namespace", 0)
    timestamp = change.get("timestamp", 0)

    diff_size = 0
    length = change.get("length", {})
    if isinstance(length, dict):
        new_len = length.get("new", 0) or 0
        old_len = length.get("old", 0) or 0
        diff_size = new_len - old_len

    created_at = (
        datetime.fromtimestamp(timestamp, tz=UTC)
        if timestamp
        else datetime.now(UTC)
    )

    url = f"https://en.wikipedia.org/w/index.php?diff={rev_id}&oldid={old_rev}"

    is_minor = change.get("minor", False)
    is_bot = change.get("bot", False)
    is_revert = "revert" in comment.lower() or "undo" in comment.lower()

    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=Platform.WIKIPEDIA,
        content_type=ContentType.EDIT,
        platform_content_id=f"wiki:{page_id}:{rev_id}",
        created_at=created_at,
        collected_at=datetime.now(UTC),
        text=comment if comment else None,
        title=title,
        url=url,
        language="en",
        actor=ActorInfo(
            platform_id=user,
            username=user,
            display_name=user,
            is_verified=not change.get("anon", False),
        ),
        engagement=EngagementMetrics(),
        hashtags=[],
        raw_payload={
            "namespace": namespace,
            "rev_id": rev_id,
            "old_rev": old_rev,
            "diff_size": diff_size,
            "is_minor": is_minor,
            "is_bot": is_bot,
            "is_revert": is_revert,
            "type": change.get("type", "edit"),
            "wiki": change.get("wiki", "enwiki"),
            "server_name": change.get("server_name", ""),
        },
    )


class WikipediaAdapter(AbstractSourceAdapter):
    """Wikipedia EventStreams source adapter (SSE)."""

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    async def connect(self) -> None:
        self._session = aiohttp.ClientSession()
        logger.info("wikipedia_adapter_connected")

    async def disconnect(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        logger.info("wikipedia_adapter_disconnected")

    async def fetch(self, **kwargs) -> AsyncGenerator[UnifiedContentRecord, None]:
        """Yield edit records from the Wikipedia EventStream.

        The stream is unbounded; callers can break out whenever they like.
        Only English Wikipedia edits on high-signal pages are yielded.
        """
        if not self._session:
            await self.connect()
        assert self._session is not None

        async with self._session.get(
            SSE_STREAM_URL, headers={"Accept": "text/event-stream"}
        ) as resp:
            async for line in resp.content:
                decoded = line.decode("utf-8", errors="replace").strip()
                if not decoded or not decoded.startswith("data:"):
                    continue
                try:
                    data = json.loads(decoded[5:].strip())
                except (json.JSONDecodeError, IndexError):
                    continue

                # Filter: English Wikipedia only.
                if data.get("wiki") != "enwiki":
                    continue
                # Filter: actual edits in article namespace.
                if data.get("type") != "edit" or data.get("namespace", -1) != 0:
                    continue
                title = data.get("title", "")
                if not _is_high_signal(title):
                    continue

                yield _change_to_record(data)


class WikipediaPoller:
    """Long-running poller that streams Wikipedia edits into Kafka.

    Tracks per-page edit volume, revert rates, and edit-war signals
    via Redis counters.
    """

    def __init__(
        self,
        redis: Redis,
        producer: KafkaProducerWrapper,
        metrics: MetricsCollector,
    ) -> None:
        self.redis = redis
        self.producer = producer
        self.metrics = metrics
        self._running = True
        self._adapter = WikipediaAdapter()

    # -- dedup helpers -------------------------------------------------------

    async def is_seen(self, key: str) -> bool:
        return bool(await self.redis.exists(key))

    async def mark_seen(self, key: str, ttl: int = DEDUP_TTL) -> None:
        await self.redis.set(key, "1", ex=ttl)

    # -- edit-war / revert tracking ------------------------------------------

    async def _track_edit_volume(self, title: str) -> int:
        """Increment and return hourly edit count for a page."""
        vol_key = f"wiki:edits:{title}:{int(datetime.now(UTC).timestamp() // 3600)}"
        count = await self.redis.incr(vol_key)
        await self.redis.expire(vol_key, EDIT_VOLUME_WINDOW * 2)
        return int(count)

    async def _track_revert(self, title: str) -> int:
        """Increment and return hourly revert count for a page."""
        rev_key = (
            f"wiki:reverts:{title}"
            f":{int(datetime.now(UTC).timestamp() // 3600)}"
        )
        count = await self.redis.incr(rev_key)
        await self.redis.expire(rev_key, EDIT_VOLUME_WINDOW * 2)
        return int(count)

    async def _check_edit_war(
        self, title: str, edits: int, reverts: int
    ) -> None:
        """Emit a metric when edit-war thresholds are crossed."""
        if edits >= 10 and reverts >= 3:
            logger.warning(
                "wikipedia_edit_war_detected",
                title=title,
                hourly_edits=edits,
                hourly_reverts=reverts,
            )
            await self.metrics.increment(
                "wikipedia.edit_wars", tags={"page": title[:80]}
            )

    # -- main loop -----------------------------------------------------------

    async def run(self) -> None:
        """Stream edits and publish to Kafka, reconnecting on failure."""
        logger.info("wikipedia_poller_starting")
        await self._adapter.connect()

        while self._running:
            try:
                async for record in self._adapter.fetch():
                    if not self._running:
                        break

                    dedup_key = f"wiki:seen:{record.platform_content_id}"
                    if await self.is_seen(dedup_key):
                        continue

                    # Publish to Kafka.
                    await self.producer.send(
                        RAW_WIKIPEDIA,
                        record.model_dump(mode="json"),
                        key=record.platform_content_id,
                    )
                    await self.mark_seen(dedup_key)
                    await self.metrics.increment("wikipedia.edits.ingested")

                    # Track volume / revert signals.
                    title = record.title or ""
                    edits = await self._track_edit_volume(title)
                    raw = record.raw_payload or {}
                    reverts = 0
                    if raw.get("is_revert"):
                        reverts = await self._track_revert(title)
                        await self.metrics.increment("wikipedia.reverts")

                    await self._check_edit_war(title, edits, reverts)

            except (TimeoutError, aiohttp.ClientError) as exc:
                logger.warning(
                    "wikipedia_stream_error",
                    error=str(exc),
                    reconnect_delay=RECONNECT_DELAY,
                )
                await self.metrics.increment("wikipedia.errors")
                await asyncio.sleep(RECONNECT_DELAY)
            except Exception:
                logger.exception("wikipedia_poller_unexpected_error")
                await self.metrics.increment("wikipedia.errors")
                await asyncio.sleep(RECONNECT_DELAY)

    def stop(self) -> None:
        self._running = False

    async def shutdown(self) -> None:
        self.stop()
        await self._adapter.disconnect()


async def main() -> None:
    """Entrypoint for running the Wikipedia ingestion service."""
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    producer = KafkaProducerWrapper()
    await producer.start()

    poller = WikipediaPoller(redis, producer, metrics)
    try:
        await poller.run()
    except KeyboardInterrupt:
        poller.stop()
    finally:
        await poller.shutdown()
        await producer.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
