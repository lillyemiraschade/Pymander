"""Bluesky adapter using AT Protocol via aiohttp (direct XRPC calls).

Authenticates with handle + app-password, monitors the public timeline,
and searches for narrative-relevant keywords.  Tracks reposts, likes,
replies, and quotes.
"""

from __future__ import annotations

import asyncio
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
from pymander.ingestion.topics import RAW_BLUESKY
from pymander.schemas.content import (
    ActorInfo,
    EngagementMetrics,
    UnifiedContentRecord,
)
from pymander.schemas.enums import ContentType, Platform

logger = structlog.get_logger()

BSKY_BASE = "https://bsky.social/xrpc"
POLL_INTERVAL = 60  # seconds between timeline polls
SEARCH_INTERVAL = 300  # seconds between keyword searches
DEDUP_TTL = 172800  # 2 days
AUTH_REFRESH_INTERVAL = 3600  # refresh session every hour


def _parse_bsky_datetime(dt_str: str | None) -> datetime:
    """Parse an AT Protocol datetime string to a timezone-aware datetime."""
    if not dt_str:
        return datetime.now(UTC)
    try:
        # AT Protocol uses ISO 8601.
        cleaned = dt_str.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned)
    except (ValueError, TypeError):
        return datetime.now(UTC)


def _detect_content_type(post: dict) -> ContentType:
    """Determine content type from post record structure."""
    record = post.get("record", {})
    if record.get("reply"):
        return ContentType.REPLY
    embed = record.get("embed", {})
    embed_type = embed.get("$type", "")
    if "app.bsky.embed.record" in embed_type:
        return ContentType.QUOTE
    return ContentType.POST


def _post_to_record(post: dict) -> UnifiedContentRecord:
    """Map a Bluesky post view to a UnifiedContentRecord."""
    record = post.get("record", {})
    author = post.get("author", {})
    uri = post.get("uri", "")
    cid = post.get("cid", "")
    text = record.get("text", "")
    created_str = record.get("createdAt", "")

    # Engagement metrics.
    like_count = post.get("likeCount", 0)
    repost_count = post.get("repostCount", 0)
    reply_count = post.get("replyCount", 0)
    quote_count = post.get("quoteCount", 0)

    # Extract handle-based profile URL.
    handle = author.get("handle", "")
    display_name = author.get("displayName", handle)
    did = author.get("did", "")

    # Build a web URL for the post.
    # URI format: at://did:plc:xxx/app.bsky.feed.post/rkey
    rkey = uri.rsplit("/", 1)[-1] if "/" in uri else cid
    post_url = f"https://bsky.app/profile/{handle}/post/{rkey}"

    # Extract reply threading.
    reply_ref = record.get("reply", {})
    parent_uri = reply_ref.get("parent", {}).get("uri")
    root_uri = reply_ref.get("root", {}).get("uri")

    # Extract hashtags from facets.
    hashtags = []
    for facet in record.get("facets", []):
        for feature in facet.get("features", []):
            if feature.get("$type") == "app.bsky.richtext.facet#tag":
                tag = feature.get("tag", "")
                if tag:
                    hashtags.append(tag)

    # Media URLs from embeds.
    media_urls: list[str] = []
    embed = record.get("embed", {})
    for img in embed.get("images", []):
        blob = img.get("image", {})
        ref = blob.get("ref", {}).get("$link", "")
        if ref and did:
            media_urls.append(
                f"https://cdn.bsky.app/img/feed_thumbnail/plain/{did}/{ref}@jpeg"
            )

    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=Platform.BLUESKY,
        content_type=_detect_content_type(post),
        platform_content_id=uri or cid,
        created_at=_parse_bsky_datetime(created_str),
        collected_at=datetime.now(UTC),
        text=text if text else None,
        title=None,
        url=post_url,
        media_urls=media_urls,
        language=record.get("langs", [None])[0] if record.get("langs") else None,
        parent_id=parent_uri,
        root_id=root_uri,
        conversation_id=root_uri,
        actor=ActorInfo(
            platform_id=did,
            username=handle,
            display_name=display_name,
            is_verified=bool(author.get("labels")),
            profile_url=f"https://bsky.app/profile/{handle}",
            bio=author.get("description"),
        ),
        engagement=EngagementMetrics(
            likes=like_count,
            shares=repost_count,
            replies=reply_count,
            quotes=quote_count,
        ),
        hashtags=hashtags,
        raw_payload={
            "uri": uri,
            "cid": cid,
            "author_did": did,
            "embed_type": embed.get("$type", ""),
            "labels": [
                lbl.get("val", "")
                for lbl in post.get("labels", [])
            ],
        },
    )


class BlueskyAdapter(AbstractSourceAdapter):
    """Bluesky AT Protocol source adapter via direct XRPC calls."""

    def __init__(self) -> None:
        settings = get_settings()
        self._handle = settings.bluesky.handle
        self._app_password = settings.bluesky.app_password
        self._session: aiohttp.ClientSession | None = None
        self._access_jwt: str | None = None
        self._refresh_jwt: str | None = None
        self._did: str | None = None

    # -- auth ----------------------------------------------------------------

    async def _create_session(self) -> None:
        """Authenticate with Bluesky and obtain JWTs."""
        assert self._session is not None
        resp = await self._session.post(
            f"{BSKY_BASE}/com.atproto.server.createSession",
            json={
                "identifier": self._handle,
                "password": self._app_password,
            },
        )
        resp.raise_for_status()
        data = await resp.json()
        self._access_jwt = data["accessJwt"]
        self._refresh_jwt = data["refreshJwt"]
        self._did = data["did"]
        logger.info("bluesky_session_created", did=self._did)

    async def _refresh_session(self) -> None:
        """Refresh the access JWT using the refresh token."""
        assert self._session is not None
        try:
            resp = await self._session.post(
                f"{BSKY_BASE}/com.atproto.server.refreshSession",
                headers={"Authorization": f"Bearer {self._refresh_jwt}"},
            )
            resp.raise_for_status()
            data = await resp.json()
            self._access_jwt = data["accessJwt"]
            self._refresh_jwt = data["refreshJwt"]
            logger.debug("bluesky_session_refreshed")
        except Exception:
            logger.warning("bluesky_refresh_failed_recreating")
            await self._create_session()

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._access_jwt}"}

    # -- lifecycle -----------------------------------------------------------

    async def connect(self) -> None:
        self._session = aiohttp.ClientSession()
        if self._handle and self._app_password:
            await self._create_session()
        logger.info("bluesky_adapter_connected")

    async def disconnect(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        self._access_jwt = None
        self._refresh_jwt = None
        logger.info("bluesky_adapter_disconnected")

    # -- data fetching -------------------------------------------------------

    async def fetch(self, **kwargs) -> AsyncGenerator[UnifiedContentRecord, None]:
        """Yield posts from the authenticated user's timeline."""
        if not self._session or not self._access_jwt:
            return

        cursor: str | None = kwargs.get("cursor")
        limit = min(kwargs.get("limit", 50), 100)

        params: dict = {"limit": limit}
        if cursor:
            params["cursor"] = cursor

        try:
            resp = await self._session.get(
                f"{BSKY_BASE}/app.bsky.feed.getTimeline",
                headers=self._auth_headers(),
                params=params,
            )
            resp.raise_for_status()
            data = await resp.json()
        except aiohttp.ClientResponseError as exc:
            if exc.status == 401:
                await self._refresh_session()
                return
            raise

        for item in data.get("feed", []):
            post = item.get("post", {})
            if post:
                yield _post_to_record(post)

    async def search(
        self, query: str, limit: int = 25
    ) -> AsyncGenerator[UnifiedContentRecord, None]:
        """Search Bluesky posts by keyword."""
        if not self._session or not self._access_jwt:
            return

        try:
            resp = await self._session.get(
                f"{BSKY_BASE}/app.bsky.feed.searchPosts",
                headers=self._auth_headers(),
                params={"q": query, "limit": min(limit, 100)},
            )
            resp.raise_for_status()
            data = await resp.json()
        except aiohttp.ClientResponseError as exc:
            if exc.status == 401:
                await self._refresh_session()
                return
            logger.warning(
                "bluesky_search_error", query=query, status=exc.status
            )
            return

        for post in data.get("posts", []):
            yield _post_to_record(post)


class BlueskyPoller:
    """Polls Bluesky timeline and keyword searches, publishes to Kafka."""

    def __init__(
        self,
        redis: Redis,
        producer: KafkaProducerWrapper,
        metrics: MetricsCollector,
        narrative_keywords: list[str] | None = None,
    ) -> None:
        self.redis = redis
        self.producer = producer
        self.metrics = metrics
        self.narrative_keywords = narrative_keywords or []
        self._running = True
        self._adapter = BlueskyAdapter()
        self._last_auth_refresh = 0.0

    # -- dedup ---------------------------------------------------------------

    async def is_seen(self, key: str) -> bool:
        return bool(await self.redis.exists(key))

    async def mark_seen(self, key: str, ttl: int = DEDUP_TTL) -> None:
        await self.redis.set(key, "1", ex=ttl)

    # -- ingest helpers ------------------------------------------------------

    async def _ingest_record(self, record: UnifiedContentRecord) -> bool:
        """Dedup, publish, track metrics.  Returns True if new."""
        dedup_key = f"bsky:seen:{record.platform_content_id}"
        if await self.is_seen(dedup_key):
            return False

        await self.producer.send(
            RAW_BLUESKY,
            record.model_dump(mode="json"),
            key=record.platform_content_id,
        )
        await self.mark_seen(dedup_key)
        await self.metrics.increment("bluesky.posts.ingested")

        # Track engagement sub-metrics.
        if record.engagement.shares > 0:
            await self.metrics.increment(
                "bluesky.reposts", tags={"count": str(record.engagement.shares)}
            )
        if record.engagement.quotes > 0:
            await self.metrics.increment("bluesky.quotes")
        return True

    # -- polling loops -------------------------------------------------------

    async def _poll_timeline(self) -> int:
        """Poll the authenticated timeline, return count of new posts."""
        count = 0
        try:
            async for record in self._adapter.fetch():
                if not self._running:
                    break
                if await self._ingest_record(record):
                    count += 1
        except Exception as exc:
            logger.error("bluesky_timeline_error", error=str(exc))
            await self.metrics.increment("bluesky.errors")
        return count

    async def _poll_searches(self) -> int:
        """Search for narrative keywords, return count of new posts."""
        count = 0
        for keyword in self.narrative_keywords:
            if not self._running:
                break
            try:
                async for record in self._adapter.search(keyword):
                    if await self._ingest_record(record):
                        count += 1
            except Exception as exc:
                logger.warning(
                    "bluesky_search_poll_error",
                    keyword=keyword,
                    error=str(exc),
                )
                await self.metrics.increment(
                    "bluesky.errors", tags={"source": "search"}
                )
            # Small delay between keyword searches.
            await asyncio.sleep(1)
        return count

    async def run(self) -> None:
        """Main polling loop."""
        logger.info(
            "bluesky_poller_starting",
            keywords=self.narrative_keywords,
        )
        await self._adapter.connect()

        search_counter = 0

        while self._running:
            # Timeline poll.
            tl_count = await self._poll_timeline()

            # Keyword search every SEARCH_INTERVAL / POLL_INTERVAL cycles.
            search_count = 0
            search_counter += 1
            cycles_per_search = max(SEARCH_INTERVAL // POLL_INTERVAL, 1)
            if (
                self.narrative_keywords
                and search_counter >= cycles_per_search
            ):
                search_count = await self._poll_searches()
                search_counter = 0

            # Periodic session refresh.
            now = asyncio.get_event_loop().time()
            if now - self._last_auth_refresh > AUTH_REFRESH_INTERVAL:
                try:
                    await self._adapter._refresh_session()
                    self._last_auth_refresh = now
                except Exception:
                    logger.warning("bluesky_auth_refresh_failed")

            await self.metrics.gauge(
                "bluesky.keywords.monitored", len(self.narrative_keywords)
            )
            logger.info(
                "bluesky_poll_cycle_complete",
                timeline_new=tl_count,
                search_new=search_count,
            )
            await asyncio.sleep(POLL_INTERVAL)

    def stop(self) -> None:
        self._running = False

    async def shutdown(self) -> None:
        self.stop()
        await self._adapter.disconnect()


async def main() -> None:
    """Entrypoint for running the Bluesky ingestion service."""
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    producer = KafkaProducerWrapper()
    await producer.start()

    poller = BlueskyPoller(redis, producer, metrics)
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
