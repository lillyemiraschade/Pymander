"""YouTube ingestion adapter using google-api-python-client."""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from xml.etree import ElementTree

import aiohttp
import structlog
from googleapiclient.discovery import build as build_youtube
from redis.asyncio import Redis
from youtube_transcript_api import YouTubeTranscriptApi

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.adapters.base import AbstractSourceAdapter
from pymander.ingestion.producer import KafkaProducerWrapper
from pymander.ingestion.topics import RAW_YOUTUBE
from pymander.schemas.content import (
    ActorInfo,
    EngagementMetrics,
    UnifiedContentRecord,
)
from pymander.schemas.enums import ContentType, Platform

logger = structlog.get_logger()

# --- Configuration constants ---

TRENDING_REGIONS = ["US", "GB", "DE", "FR", "JP"]

# Channels to watch for uploads (populated from config / Redis at runtime)
DEFAULT_WATCHLIST_CHANNELS: list[str] = []

NARRATIVE_KEYWORDS = [
    "disinformation", "propaganda", "influence operation",
    "election interference", "information warfare", "psyop",
    "astroturfing", "bot network", "coordinated inauthentic",
]

# YouTube Data API v3 quota costs
QUOTA_COST_SEARCH = 100
QUOTA_COST_LIST_VIDEOS = 1  # per call (up to 50 IDs)
QUOTA_COST_LIST_CHANNELS = 1

MAX_IDS_PER_REQUEST = 50

# RSS feed URL for channel uploads (zero quota cost)
_CHANNEL_RSS_URL = (
    "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
)


# --- Quota tracker ---


class QuotaTracker:
    """Track daily YouTube API quota usage in Redis."""

    def __init__(self, redis: Redis, daily_limit: int = 10000) -> None:
        self.redis = redis
        self.daily_limit = daily_limit

    def _key(self) -> str:
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        return f"youtube:quota:{today}"

    async def consume(self, cost: int) -> bool:
        """Consume quota units. Returns True if within budget."""
        key = self._key()
        current = await self.redis.incrby(key, cost)
        if current == cost:
            # First usage today -- set expiry for 25 hours
            await self.redis.expire(key, 90000)
        if current > self.daily_limit:
            logger.warning(
                "youtube_quota_exceeded",
                used=current,
                limit=self.daily_limit,
            )
            return False
        return True

    async def remaining(self) -> int:
        val = await self.redis.get(self._key())
        used = int(val) if val else 0
        return max(0, self.daily_limit - used)

    async def used_today(self) -> int:
        val = await self.redis.get(self._key())
        return int(val) if val else 0


# --- Helper: fetch transcripts (free, no quota) ---


async def fetch_transcript(video_id: str) -> str | None:
    """Fetch auto-generated captions via youtube-transcript-api (no quota)."""
    try:
        segments = await asyncio.to_thread(
            YouTubeTranscriptApi.get_transcript, video_id,
        )
        return " ".join(seg["text"] for seg in segments)
    except Exception as e:
        logger.debug(
            "youtube_transcript_unavailable",
            video_id=video_id,
            error=str(e),
        )
        return None


# --- Helper: RSS feed for channel uploads (zero quota) ---


async def fetch_channel_uploads_rss(
    channel_id: str,
    session: aiohttp.ClientSession,
) -> list[dict]:
    """Parse a YouTube channel RSS feed to get recent video IDs and titles."""
    url = _CHANNEL_RSS_URL.format(channel_id=channel_id)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return []
            text = await resp.text()

        ns = {"atom": "http://www.w3.org/2005/Atom", "yt": "http://www.youtube.com/xml/schemas/2015"}
        root = ElementTree.fromstring(text)
        entries: list[dict] = []
        for entry in root.findall("atom:entry", ns):
            vid_el = entry.find("yt:videoId", ns)
            title_el = entry.find("atom:title", ns)
            published_el = entry.find("atom:published", ns)
            if vid_el is not None and vid_el.text:
                entries.append({
                    "video_id": vid_el.text,
                    "title": title_el.text if title_el is not None else "",
                    "published": published_el.text if published_el is not None else "",
                })
        return entries
    except Exception as e:
        logger.warning(
            "youtube_rss_error", channel_id=channel_id, error=str(e),
        )
        return []


# --- Mapping ---


def map_video_to_record(
    video: dict,
    transcript: str | None = None,
) -> UnifiedContentRecord:
    """Map a YouTube Data API v3 video resource to UnifiedContentRecord."""
    snippet = video.get("snippet", {})
    statistics = video.get("statistics", {})
    content_details = video.get("contentDetails", {})

    channel_id = snippet.get("channelId", "")
    channel_title = snippet.get("channelTitle", "")

    actor_info = ActorInfo(
        platform_id=channel_id,
        username=channel_title,
        display_name=channel_title,
        is_verified=False,
        profile_url=(
            f"https://www.youtube.com/channel/{channel_id}"
            if channel_id
            else None
        ),
    )

    engagement = EngagementMetrics(
        likes=int(statistics.get("likeCount", 0)),
        shares=0,
        replies=int(statistics.get("commentCount", 0)),
        views=int(statistics.get("viewCount", 0)) if statistics.get("viewCount") else None,
        quotes=0,
        bookmarks=int(statistics.get("favoriteCount", 0)),
    )

    video_id = video.get("id", "")
    if isinstance(video_id, dict):
        video_id = video_id.get("videoId", "")
    video_id = str(video_id)

    published = snippet.get("publishedAt", "")
    created_at = (
        datetime.fromisoformat(published.replace("Z", "+00:00"))
        if published
        else datetime.now(UTC)
    )

    description = snippet.get("description", "") or ""
    title = snippet.get("title", "") or ""

    # Build text: title + description, plus transcript if available
    text_parts = [title, description]
    if transcript:
        text_parts.append(f"\n[TRANSCRIPT]\n{transcript}")
    full_text = "\n\n".join(p for p in text_parts if p)

    # Extract tags as hashtags
    tags = snippet.get("tags") or []

    # Thumbnails as media URLs
    media_urls: list[str] = []
    thumbnails = snippet.get("thumbnails") or {}
    for quality in ("maxres", "high", "medium", "default"):
        thumb = thumbnails.get(quality)
        if thumb and thumb.get("url"):
            media_urls.append(thumb["url"])
            break

    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=Platform.YOUTUBE,
        content_type=ContentType.VIDEO,
        platform_content_id=video_id,
        created_at=created_at,
        collected_at=datetime.now(UTC),
        text=full_text,
        title=title,
        url=f"https://www.youtube.com/watch?v={video_id}",
        media_urls=media_urls,
        language=snippet.get("defaultAudioLanguage")
        or snippet.get("defaultLanguage"),
        parent_id=None,
        root_id=video_id,
        conversation_id=video_id,
        actor=actor_info,
        engagement=engagement,
        geo=None,
        hashtags=tags[:50],
        raw_payload={
            "category_id": snippet.get("categoryId"),
            "duration": content_details.get("duration"),
            "definition": content_details.get("definition"),
            "live_broadcast_content": snippet.get("liveBroadcastContent"),
            "has_transcript": transcript is not None,
        },
    )


# --- Adapter ---


class YouTubeAdapter(AbstractSourceAdapter):
    """YouTube source adapter implementing AbstractSourceAdapter."""

    def __init__(self) -> None:
        self._youtube = None
        self.settings = get_settings()

    async def connect(self) -> None:
        self._youtube = await asyncio.to_thread(
            build_youtube, "youtube", "v3",
            developerKey=self.settings.youtube.api_key,
        )
        logger.info("youtube_adapter_connected")

    async def disconnect(self) -> None:
        self._youtube = None
        logger.info("youtube_adapter_disconnected")

    async def fetch(
        self, video_ids: list[str] | None = None, **kwargs,
    ) -> AsyncGenerator[UnifiedContentRecord, None]:
        """Fetch videos by ID list (batched at 50 per request)."""
        if not self._youtube:
            raise RuntimeError("Adapter not connected")
        if not video_ids:
            return

        for i in range(0, len(video_ids), MAX_IDS_PER_REQUEST):
            batch = video_ids[i : i + MAX_IDS_PER_REQUEST]
            response = await asyncio.to_thread(
                self._youtube.videos()
                .list(
                    part="snippet,statistics,contentDetails",
                    id=",".join(batch),
                )
                .execute,
            )
            for item in response.get("items", []):
                transcript = await fetch_transcript(item["id"])
                yield map_video_to_record(item, transcript)


# --- Poller ---


class YouTubePoller:
    """Manages trending, channel-upload, and keyword-search polling."""

    TRENDING_INTERVAL = 3600  # 1 hour
    CHANNEL_INTERVAL = 900  # 15 minutes (RSS, no quota)
    KEYWORD_INTERVAL = 1800  # 30 minutes

    def __init__(
        self,
        api_key: str,
        redis: Redis,
        producer: KafkaProducerWrapper,
        metrics: MetricsCollector,
        daily_quota: int = 10000,
        watchlist_channels: list[str] | None = None,
        narrative_keywords: list[str] | None = None,
        trending_regions: list[str] | None = None,
    ) -> None:
        self.api_key = api_key
        self.redis = redis
        self.producer = producer
        self.metrics = metrics
        self.quota = QuotaTracker(redis, daily_limit=daily_quota)
        self.watchlist_channels = watchlist_channels or DEFAULT_WATCHLIST_CHANNELS
        self.narrative_keywords = narrative_keywords or NARRATIVE_KEYWORDS
        self.trending_regions = trending_regions or TRENDING_REGIONS
        self._youtube = None
        self._running = True

    async def _ensure_client(self):
        if not self._youtube:
            self._youtube = await asyncio.to_thread(
                build_youtube, "youtube", "v3",
                developerKey=self.api_key,
            )

    # --- dedup helpers ---

    async def is_seen(self, key: str) -> bool:
        return bool(await self.redis.exists(key))

    async def mark_seen(self, key: str, ttl: int = 172800) -> None:
        await self.redis.set(key, "1", ex=ttl)

    # --- Redis cache helper ---

    async def _cache_get(self, key: str) -> str | None:
        val = await self.redis.get(key)
        return val.decode() if val else None

    async def _cache_set(
        self, key: str, value: str, ttl: int = 3600,
    ) -> None:
        await self.redis.set(key, value, ex=ttl)

    # --- publish helper ---

    async def _publish(self, record: UnifiedContentRecord) -> None:
        dedup_key = f"youtube:video:{record.platform_content_id}"
        if await self.is_seen(dedup_key):
            return
        await self.producer.send(
            RAW_YOUTUBE,
            record.model_dump(mode="json"),
            key=f"youtube:{record.platform_content_id}",
        )
        await self.mark_seen(dedup_key)
        await self.metrics.increment("youtube.videos.ingested")

    # --- batch video detail lookup ---

    async def _fetch_video_details(
        self, video_ids: list[str],
    ) -> list[dict]:
        """Batch-fetch video details (max 50 IDs per request)."""
        await self._ensure_client()
        all_items: list[dict] = []
        for i in range(0, len(video_ids), MAX_IDS_PER_REQUEST):
            batch = video_ids[i : i + MAX_IDS_PER_REQUEST]
            if not await self.quota.consume(QUOTA_COST_LIST_VIDEOS):
                logger.warning("youtube_quota_skip_video_details")
                break
            response = await asyncio.to_thread(
                self._youtube.videos()
                .list(
                    part="snippet,statistics,contentDetails",
                    id=",".join(batch),
                )
                .execute,
            )
            all_items.extend(response.get("items", []))
        return all_items

    # --- trending videos ---

    async def _poll_trending(self) -> None:
        """Fetch trending videos per region."""
        while self._running:
            await self._ensure_client()
            for region in self.trending_regions:
                if not self._running:
                    break

                cache_key = f"youtube:trending:{region}"
                cached = await self._cache_get(cache_key)
                if cached:
                    logger.debug(
                        "youtube_trending_cache_hit", region=region,
                    )
                    continue

                if not await self.quota.consume(QUOTA_COST_LIST_VIDEOS):
                    logger.warning("youtube_quota_skip_trending")
                    break

                try:
                    response = await asyncio.to_thread(
                        self._youtube.videos()
                        .list(
                            part="snippet,statistics,contentDetails",
                            chart="mostPopular",
                            regionCode=region,
                            maxResults=50,
                        )
                        .execute,
                    )
                    items = response.get("items", [])
                    video_ids = [v["id"] for v in items]

                    # Cache the video IDs so we don't re-fetch this hour
                    await self._cache_set(
                        cache_key,
                        ",".join(video_ids),
                        ttl=self.TRENDING_INTERVAL,
                    )

                    for item in items:
                        transcript = await fetch_transcript(item["id"])
                        record = map_video_to_record(item, transcript)
                        await self._publish(record)

                except Exception as e:
                    logger.error(
                        "youtube_trending_error",
                        region=region,
                        error=str(e),
                    )
                    await self.metrics.increment(
                        "youtube.errors", tags={"source": "trending"},
                    )

            remaining = await self.quota.remaining()
            await self.metrics.gauge("youtube.quota.remaining", remaining)
            await asyncio.sleep(self.TRENDING_INTERVAL)

    # --- channel uploads via RSS (zero quota) ---

    async def _poll_channel_uploads(self) -> None:
        """Monitor watchlisted channels via RSS feeds (no quota cost)."""
        while self._running:
            if not self.watchlist_channels:
                await asyncio.sleep(self.CHANNEL_INTERVAL)
                continue

            async with aiohttp.ClientSession() as session:
                for channel_id in self.watchlist_channels:
                    if not self._running:
                        break
                    try:
                        entries = await fetch_channel_uploads_rss(
                            channel_id, session,
                        )
                        new_ids: list[str] = []
                        for entry in entries:
                            vid = entry["video_id"]
                            if not await self.is_seen(f"youtube:video:{vid}"):
                                new_ids.append(vid)

                        if new_ids:
                            items = await self._fetch_video_details(new_ids)
                            for item in items:
                                transcript = await fetch_transcript(
                                    item["id"],
                                )
                                record = map_video_to_record(
                                    item, transcript,
                                )
                                await self._publish(record)

                        logger.debug(
                            "youtube_channel_rss_polled",
                            channel_id=channel_id,
                            new_videos=len(new_ids),
                        )

                    except Exception as e:
                        logger.error(
                            "youtube_channel_poll_error",
                            channel_id=channel_id,
                            error=str(e),
                        )
                        await self.metrics.increment(
                            "youtube.errors",
                            tags={"source": "channel_upload"},
                        )

            await asyncio.sleep(self.CHANNEL_INTERVAL)

    # --- keyword search ---

    async def _poll_keyword_search(self) -> None:
        """Search for narrative keywords (expensive -- 100 units each)."""
        while self._running:
            await self._ensure_client()
            for keyword in self.narrative_keywords:
                if not self._running:
                    break

                # Cache check: hash the keyword for a stable key
                kw_hash = hashlib.md5(
                    keyword.encode(), usedforsecurity=False,
                ).hexdigest()[:12]
                cache_key = f"youtube:search:{kw_hash}"
                if await self._cache_get(cache_key):
                    continue

                if not await self.quota.consume(QUOTA_COST_SEARCH):
                    logger.warning("youtube_quota_skip_search")
                    break

                try:
                    response = await asyncio.to_thread(
                        self._youtube.search()
                        .list(
                            part="id",
                            q=keyword,
                            type="video",
                            order="date",
                            maxResults=25,
                            publishedAfter=(
                                datetime.now(UTC)
                                .replace(hour=0, minute=0, second=0)
                                .isoformat()
                            ),
                        )
                        .execute,
                    )
                    video_ids = [
                        item["id"]["videoId"]
                        for item in response.get("items", [])
                        if item.get("id", {}).get("videoId")
                    ]

                    await self._cache_set(
                        cache_key,
                        ",".join(video_ids),
                        ttl=self.KEYWORD_INTERVAL,
                    )

                    if video_ids:
                        items = await self._fetch_video_details(video_ids)
                        for item in items:
                            transcript = await fetch_transcript(item["id"])
                            record = map_video_to_record(item, transcript)
                            await self._publish(record)

                except Exception as e:
                    logger.error(
                        "youtube_keyword_search_error",
                        keyword=keyword,
                        error=str(e),
                    )
                    await self.metrics.increment(
                        "youtube.errors", tags={"source": "keyword_search"},
                    )

            used = await self.quota.used_today()
            await self.metrics.gauge("youtube.quota.used_today", used)
            await asyncio.sleep(self.KEYWORD_INTERVAL)

    # --- lifecycle ---

    async def run(self) -> None:
        """Start all polling loops concurrently."""
        tasks = [
            asyncio.create_task(self._poll_trending()),
            asyncio.create_task(self._poll_channel_uploads()),
            asyncio.create_task(self._poll_keyword_search()),
        ]
        logger.info(
            "youtube_poller_running",
            channels=len(self.watchlist_channels),
            keywords=len(self.narrative_keywords),
            regions=len(self.trending_regions),
        )
        await asyncio.gather(*tasks, return_exceptions=True)

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    """Entrypoint for running the YouTube ingestion service."""
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    producer = KafkaProducerWrapper()
    await producer.start()

    poller = YouTubePoller(
        api_key=settings.youtube.api_key,
        redis=redis,
        producer=producer,
        metrics=metrics,
        daily_quota=settings.youtube.daily_quota,
    )
    logger.info("youtube_poller_starting")
    try:
        await poller.run()
    except KeyboardInterrupt:
        poller.stop()
    finally:
        poller.stop()
        await producer.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
