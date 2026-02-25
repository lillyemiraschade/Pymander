"""Google Trends adapter using pytrends.

Polls trending searches and interest-over-time data on a configurable
interval per region.  Results are mapped to UnifiedContentRecord for
downstream narrative detection.
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import structlog
from pytrends.request import TrendReq
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.adapters.base import AbstractSourceAdapter
from pymander.ingestion.producer import KafkaProducerWrapper
from pymander.ingestion.topics import RAW_WEB
from pymander.schemas.content import (
    ActorInfo,
    EngagementMetrics,
    UnifiedContentRecord,
)
from pymander.schemas.enums import ContentType, Platform

logger = structlog.get_logger()

POLL_INTERVAL = 14400  # 4 hours
CACHE_TTL = 14400  # cache results for 4 hours to avoid rate-limits
RATE_LIMIT_DELAY = 60  # seconds to back off on 429


def _trend_hash(region: str, title: str, date_str: str) -> str:
    """Deterministic ID for a trending topic observation."""
    raw = f"gtrends:{region}:{title}:{date_str}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def _daily_trending_to_record(
    topic: dict,
    region: str,
) -> UnifiedContentRecord:
    """Map a single daily-trending topic dict to a UnifiedContentRecord."""
    title = topic.get("title", "")
    traffic = topic.get("formattedTraffic", "0")
    articles = topic.get("articles", [])
    related_queries = topic.get("relatedQueries", [])
    date_str = datetime.now(UTC).strftime("%Y-%m-%d")

    snippet_parts = []
    for art in articles[:3]:
        snippet_parts.append(
            f"- {art.get('title', '')} ({art.get('source', '')})"
        )

    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=Platform.WEB,
        content_type=ContentType.OTHER,
        platform_content_id=_trend_hash(region, title, date_str),
        created_at=datetime.now(UTC),
        collected_at=datetime.now(UTC),
        title=f"[Google Trends] {title}",
        text="\n".join(snippet_parts) if snippet_parts else None,
        url=f"https://trends.google.com/trends/explore?q={title}&geo={region}",
        language="en",
        actor=ActorInfo(
            platform_id="google_trends",
            username="google_trends",
            display_name=f"Google Trends ({region})",
        ),
        engagement=EngagementMetrics(
            views=_parse_traffic(traffic),
        ),
        hashtags=[],
        raw_payload={
            "region": region,
            "formatted_traffic": traffic,
            "related_queries": [
                q.get("query", "") if isinstance(q, dict) else str(q)
                for q in related_queries[:10]
            ],
            "articles": [
                {
                    "title": a.get("title", ""),
                    "source": a.get("source", ""),
                    "url": a.get("url", ""),
                }
                for a in articles[:5]
            ],
            "source_type": "daily_trending",
        },
    )


def _interest_to_record(
    keyword: str,
    region: str,
    interest_value: int,
    date_str: str,
) -> UnifiedContentRecord:
    """Map an interest-over-time data point to a UnifiedContentRecord."""
    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=Platform.WEB,
        content_type=ContentType.OTHER,
        platform_content_id=_trend_hash(region, keyword, date_str),
        created_at=datetime.now(UTC),
        collected_at=datetime.now(UTC),
        title=f"[Google Trends Interest] {keyword}",
        text=f"Interest score {interest_value}/100 for '{keyword}' in {region}",
        url=f"https://trends.google.com/trends/explore?q={keyword}&geo={region}",
        language="en",
        actor=ActorInfo(
            platform_id="google_trends",
            username="google_trends",
            display_name=f"Google Trends ({region})",
        ),
        engagement=EngagementMetrics(views=interest_value),
        hashtags=[],
        raw_payload={
            "region": region,
            "keyword": keyword,
            "interest_value": interest_value,
            "date": date_str,
            "source_type": "interest_over_time",
        },
    )


def _parse_traffic(formatted: str) -> int:
    """Parse '200K+' style traffic strings to an integer."""
    cleaned = formatted.replace("+", "").replace(",", "").strip()
    multiplier = 1
    if cleaned.upper().endswith("K"):
        multiplier = 1_000
        cleaned = cleaned[:-1]
    elif cleaned.upper().endswith("M"):
        multiplier = 1_000_000
        cleaned = cleaned[:-1]
    try:
        return int(float(cleaned) * multiplier)
    except (ValueError, TypeError):
        return 0


class GoogleTrendsAdapter(AbstractSourceAdapter):
    """Google Trends source adapter backed by pytrends."""

    def __init__(self, regions: list[str] | None = None) -> None:
        settings = get_settings()
        self.regions = regions or settings.google.trends_regions
        self._pytrends: TrendReq | None = None

    async def connect(self) -> None:
        self._pytrends = await asyncio.to_thread(
            TrendReq, hl="en-US", tz=0, retries=3, backoff_factor=1.0
        )
        logger.info("google_trends_adapter_connected", regions=self.regions)

    async def disconnect(self) -> None:
        self._pytrends = None
        logger.info("google_trends_adapter_disconnected")

    async def fetch(
        self, region: str = "US", **kwargs
    ) -> AsyncGenerator[UnifiedContentRecord, None]:
        """Yield trending topics for a given region."""
        if not self._pytrends:
            await self.connect()
        assert self._pytrends is not None

        try:
            trending = await asyncio.to_thread(
                self._pytrends.trending_searches, pn=region.lower()
            )
            for _, row in trending.iterrows():
                topic_title = str(row.iloc[0]) if len(row) > 0 else ""
                if topic_title:
                    yield _daily_trending_to_record(
                        {"title": topic_title}, region
                    )
        except Exception as exc:
            logger.warning(
                "google_trends_fetch_error", region=region, error=str(exc)
            )

    async def fetch_interest(
        self, keywords: list[str], region: str = "US"
    ) -> AsyncGenerator[UnifiedContentRecord, None]:
        """Yield interest-over-time records for narrative keywords."""
        if not self._pytrends:
            await self.connect()
        assert self._pytrends is not None

        try:
            await asyncio.to_thread(
                self._pytrends.build_payload,
                keywords[:5],
                cat=0,
                timeframe="now 7-d",
                geo=region,
            )
            iot = await asyncio.to_thread(
                self._pytrends.interest_over_time
            )
            if iot.empty:
                return
            # Emit latest data point per keyword.
            latest = iot.iloc[-1]
            date_str = str(iot.index[-1].date())
            for kw in keywords[:5]:
                if kw in iot.columns:
                    yield _interest_to_record(
                        kw, region, int(latest[kw]), date_str
                    )
        except Exception as exc:
            logger.warning(
                "google_trends_interest_error",
                region=region,
                keywords=keywords,
                error=str(exc),
            )

    async def fetch_related(
        self, keywords: list[str], region: str = "US"
    ) -> AsyncGenerator[UnifiedContentRecord, None]:
        """Yield related topics and rising queries for keywords."""
        if not self._pytrends:
            await self.connect()
        assert self._pytrends is not None

        try:
            await asyncio.to_thread(
                self._pytrends.build_payload,
                keywords[:5],
                cat=0,
                timeframe="now 7-d",
                geo=region,
            )
            related = await asyncio.to_thread(
                self._pytrends.related_queries
            )
            date_str = datetime.now(UTC).strftime("%Y-%m-%d")

            for kw, tables in related.items():
                rising = tables.get("rising")
                if rising is not None and not rising.empty:
                    for _, row in rising.head(5).iterrows():
                        query = str(row.get("query", ""))
                        value = int(row.get("value", 0))
                        yield _interest_to_record(
                            f"{kw} -> {query}", region, value, date_str
                        )
        except Exception as exc:
            logger.warning(
                "google_trends_related_error",
                keywords=keywords,
                error=str(exc),
            )


class GoogleTrendsPoller:
    """Polls Google Trends on a schedule and publishes to Kafka."""

    def __init__(
        self,
        redis: Redis,
        producer: KafkaProducerWrapper,
        metrics: MetricsCollector,
        regions: list[str] | None = None,
        narrative_keywords: list[str] | None = None,
    ) -> None:
        self.redis = redis
        self.producer = producer
        self.metrics = metrics
        settings = get_settings()
        self.regions = regions or settings.google.trends_regions
        self.narrative_keywords = narrative_keywords or []
        self._running = True
        self._adapter = GoogleTrendsAdapter(regions=self.regions)

    # -- Redis cache helpers -------------------------------------------------

    async def _is_cached(self, key: str) -> bool:
        return bool(await self.redis.exists(key))

    async def _set_cache(
        self, key: str, value: str = "1", ttl: int = CACHE_TTL
    ) -> None:
        await self.redis.set(key, value, ex=ttl)

    async def _get_cached(self, key: str) -> str | None:
        val = await self.redis.get(key)
        return val.decode() if isinstance(val, bytes) else val

    # -- polling logic -------------------------------------------------------

    async def _poll_region(self, region: str) -> int:
        """Poll trending topics for a single region, return new items."""
        count = 0
        try:
            async for record in self._adapter.fetch(region=region):
                cache_key = f"gtrends:seen:{record.platform_content_id}"
                if await self._is_cached(cache_key):
                    continue

                await self.producer.send(
                    RAW_WEB,
                    record.model_dump(mode="json"),
                    key=record.platform_content_id,
                )
                await self._set_cache(cache_key)
                await self.metrics.increment(
                    "google_trends.topics.ingested", tags={"region": region}
                )
                count += 1

            # Interest-over-time for narrative keywords.
            if self.narrative_keywords:
                async for record in self._adapter.fetch_interest(
                    self.narrative_keywords, region=region
                ):
                    cache_key = f"gtrends:iot:{record.platform_content_id}"
                    if await self._is_cached(cache_key):
                        continue
                    await self.producer.send(
                        RAW_WEB,
                        record.model_dump(mode="json"),
                        key=record.platform_content_id,
                    )
                    await self._set_cache(cache_key)
                    count += 1

                # Related / rising queries.
                async for record in self._adapter.fetch_related(
                    self.narrative_keywords, region=region
                ):
                    cache_key = f"gtrends:rel:{record.platform_content_id}"
                    if await self._is_cached(cache_key):
                        continue
                    await self.producer.send(
                        RAW_WEB,
                        record.model_dump(mode="json"),
                        key=record.platform_content_id,
                    )
                    await self._set_cache(cache_key)
                    count += 1

        except Exception as exc:
            logger.error(
                "google_trends_poll_error", region=region, error=str(exc)
            )
            await self.metrics.increment(
                "google_trends.errors", tags={"region": region}
            )
        return count

    async def run(self) -> None:
        """Poll all configured regions in a loop."""
        logger.info(
            "google_trends_poller_starting",
            regions=self.regions,
            keywords=self.narrative_keywords,
        )
        await self._adapter.connect()

        while self._running:
            total = 0
            for region in self.regions:
                if not self._running:
                    break
                count = await self._poll_region(region)
                total += count
                # Small delay between regions to be polite.
                await asyncio.sleep(2)

            await self.metrics.gauge(
                "google_trends.regions.monitored", len(self.regions)
            )
            logger.info(
                "google_trends_poll_cycle_complete", new_items=total
            )
            await asyncio.sleep(POLL_INTERVAL)

    def stop(self) -> None:
        self._running = False

    async def shutdown(self) -> None:
        self.stop()
        await self._adapter.disconnect()


async def main() -> None:
    """Entrypoint for running the Google Trends ingestion service."""
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    producer = KafkaProducerWrapper()
    await producer.start()

    poller = GoogleTrendsPoller(redis, producer, metrics)
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
