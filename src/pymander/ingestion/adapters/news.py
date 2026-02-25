"""News/RSS ingestion adapter with full-text extraction via trafilatura."""

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
from pymander.ingestion.topics import RAW_RSS
from pymander.schemas.content import (
    ActorInfo,
    EngagementMetrics,
    UnifiedContentRecord,
)
from pymander.schemas.enums import ContentType, Platform

logger = structlog.get_logger()

NEWS_RSS_FEEDS = {
    # Top US outlets
    "nyt": "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
    "wapo": "https://feeds.washingtonpost.com/rss/world",
    "reuters": "https://www.rss.reuters.com/news/topNews",
    "ap": "https://rsshub.app/apnews/topics/apf-topnews",
    "bbc": "http://feeds.bbci.co.uk/news/rss.xml",
    "cnn": "http://rss.cnn.com/rss/edition.rss",
    "fox": "https://moxie.foxnews.com/google-publisher/latest.xml",
    "npr": "https://feeds.npr.org/1001/rss.xml",
    "politico": "https://www.politico.com/rss/politicopicks.xml",
    "thehill": "https://thehill.com/feed/",
    "axios": "https://api.axios.com/feed/",
    # Tech
    "techcrunch": "https://techcrunch.com/feed/",
    "verge": "https://www.theverge.com/rss/index.xml",
    "arstechnica": "https://feeds.arstechnica.com/arstechnica/index",
    "wired": "https://www.wired.com/feed/rss",
    # Finance
    "bloomberg": "https://feeds.bloomberg.com/markets/news.rss",
    "cnbc": "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    # International
    "guardian": "https://www.theguardian.com/world/rss",
    "aljazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "dw": "https://rss.dw.com/rdf/rss-en-all",
}

POLL_INTERVAL = 300  # 5 minutes


def extract_full_article(url: str) -> dict:
    """Extract full article text from URL using trafilatura."""
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
    except Exception as e:
        logger.warning("article_extraction_failed", url=url, error=str(e))

    return {"text": "", "title": "", "error": "extraction_failed"}


def _parse_date(entry: dict) -> datetime:
    """Parse date from RSS entry, falling back to now."""
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
    entry: dict, source_name: str, article: dict
) -> UnifiedContentRecord:
    """Convert an RSS entry + extracted article to UnifiedContentRecord."""
    title = article.get("title") or entry.get("title", "")
    text = article.get("text", "")
    link = entry.get("link", "")
    author = article.get("author") or entry.get("author", source_name)
    sitename = article.get("sitename", source_name)

    media_urls = []
    for enc in entry.get("enclosures", []):
        if enc.get("href"):
            media_urls.append(enc["href"])
    for ml in entry.get("media_content", []):
        if ml.get("url"):
            media_urls.append(ml["url"])

    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=Platform.RSS,
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
            platform_id=sitename,
            username=author,
            display_name=sitename,
        ),
        engagement=EngagementMetrics(),
        geo=None,
        hashtags=[],
        raw_payload={
            "source_feed": source_name,
            "categories": [
                t.get("term", "") for t in entry.get("tags", [])
            ],
            "description": article.get("description", ""),
        },
    )


class NewsAdapter(AbstractSourceAdapter):
    """RSS/News source adapter with full-text extraction."""

    def __init__(self, feeds: dict[str, str] | None = None) -> None:
        self.feeds = feeds or NEWS_RSS_FEEDS

    async def connect(self) -> None:
        logger.info("news_adapter_connected", feeds=len(self.feeds))

    async def disconnect(self) -> None:
        logger.info("news_adapter_disconnected")

    async def fetch(
        self, source_name: str = "bbc", **kwargs
    ) -> AsyncGenerator[UnifiedContentRecord, None]:
        url = self.feeds.get(source_name)
        if not url:
            return
        feed = await asyncio.to_thread(feedparser.parse, url)
        for entry in feed.entries:
            link = entry.get("link", "")
            if link:
                article = await asyncio.to_thread(extract_full_article, link)
            else:
                article = {"text": entry.get("summary", ""), "title": entry.get("title", "")}
            yield _entry_to_record(entry, source_name, article)


class NewsPoller:
    """Polls all configured RSS feeds on an interval."""

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
        self.feeds = feeds or NEWS_RSS_FEEDS
        self._running = True

    async def is_seen(self, key: str) -> bool:
        return bool(await self.redis.exists(key))

    async def mark_seen(self, key: str, ttl: int = 172800) -> None:
        await self.redis.set(key, "1", ex=ttl)

    async def poll_feed(self, name: str, url: str) -> int:
        """Poll a single feed, return number of new items."""
        count = 0
        try:
            feed = await asyncio.to_thread(feedparser.parse, url)
            for entry in feed.entries:
                entry_id = entry.get("id", entry.get("link", ""))
                dedup_key = f"news:{name}:{entry_id}"
                if await self.is_seen(dedup_key):
                    continue

                link = entry.get("link", "")
                if link:
                    article = await asyncio.to_thread(
                        extract_full_article, link
                    )
                else:
                    article = {
                        "text": entry.get("summary", ""),
                        "title": entry.get("title", ""),
                    }

                record = _entry_to_record(entry, name, article)
                await self.producer.send(
                    RAW_RSS,
                    record.model_dump(mode="json"),
                    key=f"news:{name}:{entry_id}",
                )
                await self.mark_seen(dedup_key)
                await self.metrics.increment("news.articles.ingested")
                count += 1

        except Exception as e:
            logger.error("news_poll_error", feed=name, error=str(e))
            await self.metrics.increment(
                "news.errors", tags={"feed": name}
            )
        return count

    async def run(self) -> None:
        """Poll all feeds in a loop."""
        logger.info("news_poller_starting", feeds=len(self.feeds))
        while self._running:
            total = 0
            for name, url in self.feeds.items():
                if not self._running:
                    break
                count = await self.poll_feed(name, url)
                total += count
            await self.metrics.gauge("news.feeds.monitored", len(self.feeds))
            logger.info("news_poll_cycle_complete", new_articles=total)
            await asyncio.sleep(POLL_INTERVAL)

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    """Entrypoint for running the news ingestion service."""
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    producer = KafkaProducerWrapper()
    await producer.start()

    poller = NewsPoller(redis, producer, metrics)
    try:
        await poller.run()
    except KeyboardInterrupt:
        poller.stop()
    finally:
        await producer.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
