"""Substack adapter using feedparser + trafilatura.

Monitors top newsletter RSS feeds, extracts full article text, and maps
content to UnifiedContentRecord.  Follows the same pattern as news.py.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import feedparser
import structlog
import trafilatura
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.adapters.base import AbstractSourceAdapter
from pymander.ingestion.producer import KafkaProducerWrapper
from pymander.ingestion.topics import RAW_SUBSTACK
from pymander.schemas.content import (
    ActorInfo,
    EngagementMetrics,
    UnifiedContentRecord,
)
from pymander.schemas.enums import ContentType, Platform

logger = structlog.get_logger()

# Default list of popular Substack newsletters to monitor.
SUBSTACK_FEEDS: dict[str, str] = {
    # Politics / policy
    "slowboring": "https://www.slowboring.com/feed",
    "popularinfo": "https://popular.info/feed",
    "heathercoxrichardson": "https://heathercoxrichardson.substack.com/feed",
    "thefp": "https://www.thefp.com/feed",
    "platformer": "https://www.platformer.news/feed",
    # Tech / AI
    "oneusefulthing": "https://www.oneusefulthingai.com/feed",
    "thealgorithm": "https://www.technologyreview.com/feed",
    "stratechery": "https://stratechery.com/feed",
    "lennyssletter": "https://www.lennysnewsletter.com/feed",
    # Culture / media
    "theintrinsicperspective": "https://www.theintrinsicperspective.com/feed",
    "commonreader": "https://thecommonreader.substack.com/feed",
    "charter": "https://www.charterworks.com/feed",
    # Finance / economics
    "noahpinion": "https://www.noahpinion.blog/feed",
    "thegeneralist": "https://www.generalist.com/feed",
    "constructionist": "https://constructionist.substack.com/feed",
    # National security / geopolitics
    "warontherocks": "https://warontherocks.com/feed",
    "foreignaffairs": "https://www.foreignaffairs.com/rss.xml",
}

POLL_INTERVAL = 600  # 10 minutes
DEDUP_TTL = 259200  # 3 days


def _extract_full_text(url: str) -> dict:
    """Download and extract full article text via trafilatura."""
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            result = trafilatura.extract(
                downloaded,
                include_comments=False,
                include_tables=True,
                include_links=True,
                output_format="json",
                with_metadata=True,
            )
            if result:
                parsed = json.loads(result)
                return {
                    "text": parsed.get("text", ""),
                    "title": parsed.get("title", ""),
                    "author": parsed.get("author", ""),
                    "date": parsed.get("date", ""),
                    "sitename": parsed.get("sitename", ""),
                    "description": parsed.get("description", ""),
                    "categories": parsed.get("categories", ""),
                    "tags": parsed.get("tags", ""),
                }
    except Exception as exc:
        logger.warning(
            "substack_extraction_failed", url=url, error=str(exc)
        )
    return {"text": "", "title": "", "error": "extraction_failed"}


def _parse_date(entry: dict) -> datetime:
    """Parse a date from an RSS entry, falling back to now."""
    for field in ("published_parsed", "updated_parsed"):
        parsed = entry.get(field)
        if parsed:
            try:
                from time import mktime

                return datetime.fromtimestamp(mktime(parsed), tz=UTC)
            except (OverflowError, OSError, ValueError):
                pass
    return datetime.now(UTC)


def _entry_to_record(
    entry: dict,
    feed_name: str,
    article: dict,
) -> UnifiedContentRecord:
    """Convert an RSS entry + extracted article to a UnifiedContentRecord."""
    title = article.get("title") or entry.get("title", "")
    text = article.get("text", "")
    link = entry.get("link", "")
    author = article.get("author") or entry.get("author", feed_name)
    sitename = article.get("sitename", feed_name)

    # Extract media from enclosures.
    media_urls: list[str] = []
    for enc in entry.get("enclosures", []):
        if enc.get("href"):
            media_urls.append(enc["href"])
    for ml in entry.get("media_content", []):
        if ml.get("url"):
            media_urls.append(ml["url"])

    # Build tag list from RSS categories.
    hashtags = [
        t.get("term", "")
        for t in entry.get("tags", [])
        if t.get("term")
    ]

    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=Platform.SUBSTACK,
        content_type=ContentType.ARTICLE,
        platform_content_id=entry.get("id", link),
        created_at=_parse_date(entry),
        collected_at=datetime.now(UTC),
        text=text if text else entry.get("summary", ""),
        title=title,
        url=link,
        media_urls=media_urls,
        language=entry.get("language") or article.get("language"),
        parent_id=None,
        root_id=None,
        conversation_id=None,
        actor=ActorInfo(
            platform_id=f"substack:{feed_name}",
            username=author,
            display_name=sitename,
            profile_url=link.rsplit("/", 1)[0] if "/" in link else link,
        ),
        engagement=EngagementMetrics(),
        hashtags=hashtags,
        raw_payload={
            "source_feed": feed_name,
            "sitename": sitename,
            "description": article.get("description", ""),
            "categories": article.get("categories", ""),
            "rss_tags": article.get("tags", ""),
        },
    )


class SubstackAdapter(AbstractSourceAdapter):
    """Substack RSS source adapter with full-text extraction."""

    def __init__(
        self, feeds: dict[str, str] | None = None
    ) -> None:
        self.feeds = feeds or SUBSTACK_FEEDS

    async def connect(self) -> None:
        logger.info("substack_adapter_connected", feeds=len(self.feeds))

    async def disconnect(self) -> None:
        logger.info("substack_adapter_disconnected")

    async def fetch(
        self, feed_name: str = "noahpinion", **kwargs
    ) -> AsyncGenerator[UnifiedContentRecord, None]:
        """Yield articles from a single Substack RSS feed."""
        url = self.feeds.get(feed_name)
        if not url:
            return

        feed = await asyncio.to_thread(feedparser.parse, url)
        for entry in feed.entries:
            link = entry.get("link", "")
            if link:
                article = await asyncio.to_thread(
                    _extract_full_text, link
                )
            else:
                article = {
                    "text": entry.get("summary", ""),
                    "title": entry.get("title", ""),
                }
            yield _entry_to_record(entry, feed_name, article)


class SubstackPoller:
    """Polls all configured Substack feeds on an interval."""

    def __init__(
        self,
        redis: Redis,
        producer: KafkaProducerWrapper,
        metrics: MetricsCollector,
        feeds: dict[str, str] | None = None,
    ) -> None:
        self.redis = redis
        self.producer = producer
        self.metrics = metrics
        self.feeds = feeds or SUBSTACK_FEEDS
        self._running = True

    # -- dedup ---------------------------------------------------------------

    async def is_seen(self, key: str) -> bool:
        return bool(await self.redis.exists(key))

    async def mark_seen(self, key: str, ttl: int = DEDUP_TTL) -> None:
        await self.redis.set(key, "1", ex=ttl)

    # -- polling logic -------------------------------------------------------

    async def poll_feed(self, name: str, url: str) -> int:
        """Poll a single Substack feed, return number of new items."""
        count = 0
        try:
            feed = await asyncio.to_thread(feedparser.parse, url)
            for entry in feed.entries:
                entry_id = entry.get("id", entry.get("link", ""))
                dedup_key = f"substack:{name}:{entry_id}"
                if await self.is_seen(dedup_key):
                    continue

                link = entry.get("link", "")
                if link:
                    article = await asyncio.to_thread(
                        _extract_full_text, link
                    )
                else:
                    article = {
                        "text": entry.get("summary", ""),
                        "title": entry.get("title", ""),
                    }

                record = _entry_to_record(entry, name, article)
                await self.producer.send(
                    RAW_SUBSTACK,
                    record.model_dump(mode="json"),
                    key=f"substack:{name}:{entry_id}",
                )
                await self.mark_seen(dedup_key)
                await self.metrics.increment(
                    "substack.articles.ingested",
                    tags={"feed": name},
                )
                count += 1

        except Exception as exc:
            logger.error(
                "substack_poll_error", feed=name, error=str(exc)
            )
            await self.metrics.increment(
                "substack.errors", tags={"feed": name}
            )
        return count

    async def run(self) -> None:
        """Poll all Substack feeds in a loop."""
        logger.info(
            "substack_poller_starting", feeds=len(self.feeds)
        )
        while self._running:
            total = 0
            for name, url in self.feeds.items():
                if not self._running:
                    break
                count = await self.poll_feed(name, url)
                total += count
                # Small delay between feeds.
                await asyncio.sleep(1)

            await self.metrics.gauge(
                "substack.feeds.monitored", len(self.feeds)
            )
            logger.info(
                "substack_poll_cycle_complete", new_articles=total
            )
            await asyncio.sleep(POLL_INTERVAL)

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    """Entrypoint for running the Substack ingestion service."""
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    producer = KafkaProducerWrapper()
    await producer.start()

    poller = SubstackPoller(redis, producer, metrics)
    try:
        await poller.run()
    except KeyboardInterrupt:
        poller.stop()
    finally:
        await producer.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
