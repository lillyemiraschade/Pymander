"""4chan ingestion adapter using aiohttp against the 4chan JSON API."""

from __future__ import annotations

import asyncio
import hashlib
import re
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
from pymander.ingestion.topics import RAW_FOURCHAN
from pymander.schemas.content import (
    ActorInfo,
    EngagementMetrics,
    GeoLocation,
    UnifiedContentRecord,
)
from pymander.schemas.enums import ContentType, Platform

logger = structlog.get_logger()

# --- Boards to monitor ---

MONITORED_BOARDS = ["pol", "news", "biz", "g", "int", "tv"]

BASE_URL = "https://a.4cdn.org"
IMAGE_BASE_URL = "https://i.4cdn.org"

# Rate-limit: 1 request per second
REQUEST_INTERVAL = 1.0

# Regex to extract backlinks like >>12345678
_BACKLINK_RE = re.compile(r">>(\d+)")

# Regex to strip basic HTML tags from 4chan post comments
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _clean_comment(raw_html: str | None) -> str:
    """Strip HTML tags and decode common entities from a 4chan comment."""
    if not raw_html:
        return ""
    text = raw_html.replace("<br>", "\n").replace("<br/>", "\n")
    text = _HTML_TAG_RE.sub("", text)
    text = (
        text.replace("&gt;", ">")
        .replace("&lt;", "<")
        .replace("&amp;", "&")
        .replace("&quot;", '"')
        .replace("&#039;", "'")
    )
    return text.strip()


def _extract_backlinks(comment: str) -> list[str]:
    """Extract >>post_number references from comment text."""
    return _BACKLINK_RE.findall(comment)


def _build_image_url(board: str, tim: int, ext: str) -> str:
    """Build the full image URL from a 4chan post's tim and ext fields."""
    return f"{IMAGE_BASE_URL}/{board}/{tim}{ext}"


def _post_to_record(
    post: dict,
    board: str,
    thread_no: int,
    is_op: bool,
) -> UnifiedContentRecord:
    """Map a single 4chan post dict to a UnifiedContentRecord."""
    post_no = post.get("no", 0)
    raw_comment = post.get("com", "")
    clean_text = _clean_comment(raw_comment)
    subject = post.get("sub", "")

    # Author handling: trip codes as username, otherwise "Anonymous"
    name = post.get("name", "Anonymous") or "Anonymous"
    trip = post.get("trip", "")
    username = trip if trip else name
    platform_id = trip if trip else hashlib.md5(
        f"{name}:{post.get('id', '')}".encode()
    ).hexdigest()[:16]

    actor = ActorInfo(
        platform_id=platform_id,
        username=username,
        display_name=name if name != "Anonymous" else None,
    )

    # Engagement: replies count is tracked at thread level, not per-post
    engagement = EngagementMetrics(
        likes=0,
        shares=0,
        replies=post.get("replies", 0) if is_op else 0,
        views=None,
        quotes=0,
        bookmarks=0,
    )

    # Country flag as geo data
    country_code = post.get("country")
    geo = (
        GeoLocation(country_code=country_code)
        if country_code
        else None
    )

    # Extract image URLs
    media_urls: list[str] = []
    if post.get("tim") and post.get("ext"):
        media_urls.append(
            _build_image_url(board, post["tim"], post["ext"])
        )

    # Determine parent_id from backlinks (first one is the direct reply target)
    backlinks = _extract_backlinks(clean_text)
    parent_id = backlinks[0] if backlinks else (
        str(thread_no) if not is_op else None
    )

    content_type = ContentType.POST if is_op else ContentType.COMMENT

    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=Platform.FOURCHAN,
        content_type=content_type,
        platform_content_id=str(post_no),
        created_at=datetime.fromtimestamp(post.get("time", 0), tz=UTC),
        collected_at=datetime.now(UTC),
        text=clean_text,
        title=subject if subject else None,
        url=f"https://boards.4chan.org/{board}/thread/{thread_no}#p{post_no}",
        media_urls=media_urls,
        language=None,
        parent_id=parent_id,
        root_id=str(thread_no),
        conversation_id=str(thread_no),
        actor=actor,
        engagement=engagement,
        geo=geo,
        hashtags=[],
        raw_payload={
            "board": board,
            "thread_no": thread_no,
            "post_no": post_no,
            "is_op": is_op,
            "trip": trip or None,
            "capcode": post.get("capcode"),
            "country": country_code,
            "country_name": post.get("country_name"),
            "poster_id": post.get("id"),
            "filename": post.get("filename"),
            "file_ext": post.get("ext"),
            "file_size": post.get("fsize"),
            "file_md5": post.get("md5"),
            "image_width": post.get("w"),
            "image_height": post.get("h"),
            "backlinks": backlinks,
            "sticky": bool(post.get("sticky")),
            "closed": bool(post.get("closed")),
            "archived": bool(post.get("archived")),
        },
    )


class FourChanAdapter(AbstractSourceAdapter):
    """4chan source adapter implementing AbstractSourceAdapter."""

    def __init__(self) -> None:
        self.session: aiohttp.ClientSession | None = None
        self.settings = get_settings()

    async def connect(self) -> None:
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "pymander/0.1.0"},
        )
        logger.info("fourchan_adapter_connected")

    async def disconnect(self) -> None:
        if self.session:
            await self.session.close()
            logger.info("fourchan_adapter_disconnected")

    async def fetch(
        self, board: str = "pol", **kwargs
    ) -> AsyncGenerator[UnifiedContentRecord, None]:
        """Fetch threads from a single board and yield records."""
        if not self.session:
            raise RuntimeError("Adapter not connected")

        threads_url = f"{BASE_URL}/{board}/threads.json"
        async with self.session.get(threads_url) as resp:
            if resp.status != 200:
                logger.error(
                    "fourchan_threads_fetch_failed",
                    board=board,
                    status=resp.status,
                )
                return
            pages = await resp.json()

        # Flatten thread list from all pages
        thread_ids = [
            t["no"]
            for page in pages
            for t in page.get("threads", [])
        ]

        for thread_id in thread_ids:
            await asyncio.sleep(REQUEST_INTERVAL)
            thread_url = f"{BASE_URL}/{board}/thread/{thread_id}.json"
            try:
                async with self.session.get(thread_url) as resp:
                    if resp.status == 404:
                        continue
                    if resp.status != 200:
                        logger.warning(
                            "fourchan_thread_fetch_failed",
                            board=board,
                            thread=thread_id,
                            status=resp.status,
                        )
                        continue
                    data = await resp.json()
            except Exception as exc:
                logger.warning(
                    "fourchan_thread_request_error",
                    board=board,
                    thread=thread_id,
                    error=str(exc),
                )
                continue

            posts = data.get("posts", [])
            for idx, post in enumerate(posts):
                yield _post_to_record(
                    post, board, thread_id, is_op=(idx == 0)
                )


class FourChanPoller:
    """Continuous poller for 4chan boards with Redis dedup and
    last-modified tracking to avoid redundant fetches."""

    POLL_INTERVAL = 60  # seconds between full board scans
    DEDUP_TTL = 86400  # 24h TTL for seen posts
    LAST_MODIFIED_TTL = 3600  # 1h TTL for last-modified timestamps

    def __init__(
        self,
        session: aiohttp.ClientSession,
        redis: Redis,
        producer: KafkaProducerWrapper,
        metrics: MetricsCollector,
    ) -> None:
        self.session = session
        self.redis = redis
        self.producer = producer
        self.metrics = metrics
        self._running = True

    async def _rate_limit(self) -> None:
        """Enforce 1 request/second rate limit."""
        await asyncio.sleep(REQUEST_INTERVAL)

    async def _get_last_modified(self, board: str, thread_no: int) -> str | None:
        """Retrieve cached Last-Modified header for a thread."""
        key = f"4chan:lastmod:{board}:{thread_no}"
        val = await self.redis.get(key)
        return val.decode() if val else None

    async def _set_last_modified(
        self, board: str, thread_no: int, last_modified: str
    ) -> None:
        """Cache the Last-Modified header for a thread."""
        key = f"4chan:lastmod:{board}:{thread_no}"
        await self.redis.set(key, last_modified, ex=self.LAST_MODIFIED_TTL)

    async def is_seen(self, key: str) -> bool:
        return bool(await self.redis.exists(key))

    async def mark_seen(self, key: str) -> None:
        await self.redis.set(key, "1", ex=self.DEDUP_TTL)

    async def _fetch_thread_list(self, board: str) -> list[dict]:
        """GET /{board}/threads.json and return flat list of thread stubs."""
        url = f"{BASE_URL}/{board}/threads.json"
        await self._rate_limit()
        try:
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    logger.error(
                        "fourchan_threads_list_error",
                        board=board,
                        status=resp.status,
                    )
                    return []
                pages = await resp.json()
                return [
                    t for page in pages for t in page.get("threads", [])
                ]
        except Exception as exc:
            logger.error(
                "fourchan_threads_list_exception",
                board=board,
                error=str(exc),
            )
            return []

    async def _fetch_thread(
        self, board: str, thread_no: int
    ) -> tuple[list[dict], str | None]:
        """GET /{board}/thread/{id}.json with If-Modified-Since.

        Returns (posts, last_modified_header). Empty list if 304 or error.
        """
        url = f"{BASE_URL}/{board}/thread/{thread_no}.json"
        headers: dict[str, str] = {}
        cached_lm = await self._get_last_modified(board, thread_no)
        if cached_lm:
            headers["If-Modified-Since"] = cached_lm

        await self._rate_limit()
        try:
            async with self.session.get(url, headers=headers) as resp:
                if resp.status == 304:
                    return [], None
                if resp.status == 404:
                    return [], None
                if resp.status != 200:
                    logger.warning(
                        "fourchan_thread_error",
                        board=board,
                        thread=thread_no,
                        status=resp.status,
                    )
                    return [], None
                last_modified = resp.headers.get("Last-Modified")
                data = await resp.json()
                return data.get("posts", []), last_modified
        except Exception as exc:
            logger.warning(
                "fourchan_thread_exception",
                board=board,
                thread=thread_no,
                error=str(exc),
            )
            return [], None

    async def poll_board(self, board: str) -> None:
        """Poll a single board continuously."""
        logger.info("fourchan_board_poll_start", board=board)

        while self._running:
            try:
                thread_stubs = await self._fetch_thread_list(board)
                ingested_count = 0

                for stub in thread_stubs:
                    if not self._running:
                        break

                    thread_no = stub["no"]
                    posts, last_modified = await self._fetch_thread(
                        board, thread_no
                    )

                    if not posts:
                        continue

                    if last_modified:
                        await self._set_last_modified(
                            board, thread_no, last_modified
                        )

                    for idx, post in enumerate(posts):
                        post_no = post.get("no", 0)
                        dedup_key = f"4chan:post:{board}:{post_no}"
                        if await self.is_seen(dedup_key):
                            continue

                        record = _post_to_record(
                            post, board, thread_no, is_op=(idx == 0)
                        )
                        await self.producer.send(
                            RAW_FOURCHAN,
                            record.model_dump(mode="json"),
                            key=f"4chan:{board}:{post_no}",
                        )
                        await self.mark_seen(dedup_key)
                        ingested_count += 1

                await self.metrics.increment(
                    "4chan.posts.ingested",
                    value=ingested_count,
                    tags={"board": board},
                )
                logger.info(
                    "fourchan_board_poll_cycle_complete",
                    board=board,
                    ingested=ingested_count,
                )

            except Exception as exc:
                logger.error(
                    "fourchan_board_poll_error",
                    board=board,
                    error=str(exc),
                )
                await self.metrics.increment(
                    "4chan.errors", tags={"board": board}
                )

            await asyncio.sleep(self.POLL_INTERVAL)

    async def run(self) -> None:
        """Start pollers for all monitored boards concurrently."""
        tasks = [
            asyncio.create_task(self.poll_board(board))
            for board in MONITORED_BOARDS
        ]
        await asyncio.gather(*tasks)

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    """Entrypoint for running the 4chan ingestion service."""
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    producer = KafkaProducerWrapper()
    await producer.start()

    session = aiohttp.ClientSession(
        headers={"User-Agent": "pymander/0.1.0"},
    )

    poller = FourChanPoller(session, redis, producer, metrics)
    logger.info(
        "fourchan_poller_starting", boards=MONITORED_BOARDS
    )
    try:
        await poller.run()
    except KeyboardInterrupt:
        poller.stop()
    finally:
        await session.close()
        await producer.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
