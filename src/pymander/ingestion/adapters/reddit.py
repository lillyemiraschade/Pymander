"""Reddit ingestion adapter using asyncpraw."""

from __future__ import annotations

import asyncio
import re
import time
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import asyncpraw
import structlog
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.adapters.base import AbstractSourceAdapter
from pymander.ingestion.producer import KafkaProducerWrapper
from pymander.ingestion.topics import RAW_REDDIT
from pymander.schemas.content import (
    ActorInfo,
    EngagementMetrics,
    UnifiedContentRecord,
)
from pymander.schemas.enums import ContentType, Platform

logger = structlog.get_logger()

# --- Subreddit tiers ---

TIER_1_SUBREDDITS = [
    "news", "worldnews", "politics", "technology", "science",
    "wallstreetbets", "cryptocurrency", "stocks", "economics",
    "conspiracy", "conservative", "liberal", "moderatepolitics",
    "outoftheloop", "bestof", "subredditdrama", "changemyview",
    "dataisbeautiful", "futurology", "collapse", "geopolitics",
    "business", "Entrepreneur", "startups",
]

TIER_2_SUBREDDITS = [
    "AskReddit", "explainlikeimfive", "todayilearned", "nottheonion",
    "UpliftingNews", "environment", "energy", "privacy", "netsec",
    "artificial", "MachineLearning", "singularity",
    "antiwork", "WorkReform", "LateStageCapitalism", "neoliberal",
    "PoliticalDiscussion", "NeutralPolitics", "law", "SupremeCourt",
    "ukraine", "CombatFootage",
    "technews", "programming", "cybersecurity",
    "personalfinance", "investing", "RealEstate",
    "climate", "ClimateActionPlan", "renewableenergy",
]

TIER_3_SUBREDDITS = [
    "movies", "gaming", "books", "television", "music",
    "space", "Documentaries", "history", "philosophy",
    "TrueReddit", "DepthHub", "FoodForThought",
    "Economics", "Finance", "CryptoCurrency",
    "Futurism", "transhumanism", "ArtificialInteligence",
]

TIER_INTERVALS = {
    1: 30,   # Tier 1: every 30 seconds
    2: 120,  # Tier 2: every 2 minutes
    3: 300,  # Tier 3: every 5 minutes
}

_MENTION_RE = re.compile(r"(?:^|[\s(])/?(u/\w+)", re.IGNORECASE)
_CASHTAG_RE = re.compile(r"\$([A-Z]{1,5})\b")


def _extract_mentions(text: str) -> list[str]:
    return _MENTION_RE.findall(text) if text else []


def _extract_cashtags(text: str) -> list[str]:
    return _CASHTAG_RE.findall(text) if text else []


def _extract_media_urls(submission) -> list[str]:
    """Extract media URLs from a Reddit submission."""
    urls: list[str] = []
    url = getattr(submission, "url", "") or ""

    # Direct image links
    if any(url.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp")):
        urls.append(url)

    # Gallery posts
    if getattr(submission, "is_gallery", False):
        meta = getattr(submission, "media_metadata", {}) or {}
        for item_data in meta.values():
            if item_data.get("status") == "valid":
                source = item_data.get("s", {})
                img_url = source.get("u", "").replace("&amp;", "&")
                if img_url:
                    urls.append(img_url)

    # Reddit-hosted video
    if getattr(submission, "is_video", False):
        media = getattr(submission, "media", None) or {}
        if isinstance(media, dict) and "reddit_video" in media:
            fallback = media["reddit_video"].get("fallback_url", "")
            if fallback:
                urls.append(fallback)

    return urls


async def process_submission(submission) -> UnifiedContentRecord:
    """Map a PRAW Submission to UnifiedContentRecord."""
    author = submission.author
    author_name = str(author) if author else "[deleted]"
    selftext = getattr(submission, "selftext", "") or ""
    title = getattr(submission, "title", "") or ""
    full_text = f"{title}\n\n{selftext}".strip() if selftext else title

    actor_info = ActorInfo(
        platform_id=author_name,
        username=author_name,
        display_name=None,
        follower_count=None,
        following_count=None,
        account_created_at=(
            datetime.fromtimestamp(author.created_utc, tz=UTC)
            if author and hasattr(author, "created_utc")
            else None
        ),
        is_verified=(
            getattr(author, "has_verified_email", False)
            if author
            else False
        ),
        bio=None,
        profile_url=(
            f"https://reddit.com/user/{author_name}"
            if author_name != "[deleted]"
            else None
        ),
    )

    engagement = EngagementMetrics(
        likes=getattr(submission, "score", 0),
        shares=0,
        replies=getattr(submission, "num_comments", 0),
        views=getattr(submission, "view_count", None),
        quotes=getattr(submission, "num_crossposts", 0),
        bookmarks=0,
    )

    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=Platform.REDDIT,
        content_type=ContentType.POST,
        platform_content_id=submission.id,
        created_at=datetime.fromtimestamp(
            submission.created_utc, tz=UTC
        ),
        collected_at=datetime.now(UTC),
        text=full_text,
        title=title,
        url=f"https://reddit.com{submission.permalink}",
        media_urls=_extract_media_urls(submission),
        language=None,
        parent_id=None,
        root_id=submission.id,
        conversation_id=submission.id,
        actor=actor_info,
        engagement=engagement,
        geo=None,
        hashtags=[],
        raw_payload={
            "subreddit": str(submission.subreddit),
            "upvote_ratio": getattr(submission, "upvote_ratio", None),
            "is_self": getattr(submission, "is_self", None),
            "link_flair_text": getattr(submission, "link_flair_text", None),
            "over_18": getattr(submission, "over_18", False),
            "spoiler": getattr(submission, "spoiler", False),
            "stickied": getattr(submission, "stickied", False),
            "domain": getattr(submission, "domain", None),
        },
    )


async def process_comment(comment, submission_id: str) -> UnifiedContentRecord:
    """Map a PRAW Comment to UnifiedContentRecord."""
    author = comment.author
    author_name = str(author) if author else "[deleted]"

    actor_info = ActorInfo(
        platform_id=author_name,
        username=author_name,
        display_name=None,
        is_verified=(
            getattr(author, "has_verified_email", False) if author else False
        ),
        profile_url=(
            f"https://reddit.com/user/{author_name}"
            if author_name != "[deleted]"
            else None
        ),
    )

    engagement = EngagementMetrics(
        likes=getattr(comment, "score", 0),
        replies=len(comment.replies) if hasattr(comment, "replies") and comment.replies else 0,
    )

    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=Platform.REDDIT,
        content_type=ContentType.COMMENT,
        platform_content_id=comment.id,
        created_at=datetime.fromtimestamp(comment.created_utc, tz=UTC),
        collected_at=datetime.now(UTC),
        text=getattr(comment, "body", "") or "",
        title=None,
        url=f"https://reddit.com{comment.permalink}",
        media_urls=[],
        language=None,
        parent_id=getattr(comment, "parent_id", None),
        root_id=submission_id,
        conversation_id=submission_id,
        actor=actor_info,
        engagement=engagement,
        geo=None,
        hashtags=[],
        raw_payload={
            "depth": getattr(comment, "depth", 0),
            "is_submitter": getattr(comment, "is_submitter", False),
            "controversiality": getattr(comment, "controversiality", 0),
            "distinguished": getattr(comment, "distinguished", None),
            "stickied": getattr(comment, "stickied", False),
        },
    )


class RedditRateLimiter:
    """Redis-backed rate limiter for Reddit API calls."""

    MAX_REQUESTS_PER_MINUTE = 85

    def __init__(self, redis: Redis, metrics: MetricsCollector) -> None:
        self.redis = redis
        self.metrics = metrics

    async def acquire(self) -> None:
        current_minute = int(time.time() / 60)
        key = f"reddit:ratelimit:{current_minute}"
        count = await self.redis.incr(key)
        if count == 1:
            await self.redis.expire(key, 120)
        if count > self.MAX_REQUESTS_PER_MINUTE:
            wait_time = 60 - (time.time() % 60) + 1
            logger.warning("reddit_rate_limit_pause", wait_seconds=wait_time)
            await self.metrics.increment("reddit.rate_limit.pauses")
            await asyncio.sleep(wait_time)


class RedditAdapter(AbstractSourceAdapter):
    """Reddit source adapter implementing AbstractSourceAdapter."""

    def __init__(self) -> None:
        self.reddit: asyncpraw.Reddit | None = None
        self.settings = get_settings()

    async def connect(self) -> None:
        self.reddit = asyncpraw.Reddit(
            client_id=self.settings.reddit.client_id,
            client_secret=self.settings.reddit.client_secret,
            user_agent=self.settings.reddit.user_agent,
        )
        logger.info("reddit_adapter_connected")

    async def disconnect(self) -> None:
        if self.reddit:
            await self.reddit.close()
            logger.info("reddit_adapter_disconnected")

    async def fetch(
        self, subreddit_name: str = "news", limit: int = 100, **kwargs
    ) -> AsyncGenerator[UnifiedContentRecord, None]:
        if not self.reddit:
            raise RuntimeError("Adapter not connected")
        subreddit = await self.reddit.subreddit(subreddit_name)
        async for submission in subreddit.new(limit=limit):
            yield await process_submission(submission)


class RedditPoller:
    """Manages polling across all subreddit tiers with dedup via Redis."""

    def __init__(
        self,
        reddit: asyncpraw.Reddit,
        redis: Redis,
        producer: KafkaProducerWrapper,
        metrics: MetricsCollector,
    ) -> None:
        self.reddit = reddit
        self.redis = redis
        self.producer = producer
        self.metrics = metrics
        self.rate_limiter = RedditRateLimiter(redis, metrics)
        self._running = True

    async def is_seen(self, key: str) -> bool:
        return bool(await self.redis.exists(key))

    async def mark_seen(self, key: str, ttl: int = 172800) -> None:
        await self.redis.set(key, "1", ex=ttl)

    async def poll_tier(
        self, subreddits: list[str], interval_seconds: int, tier: int
    ) -> None:
        logger.info(
            "reddit_tier_start",
            tier=tier,
            subreddits=len(subreddits),
            interval=interval_seconds,
        )
        while self._running:
            for subreddit_name in subreddits:
                if not self._running:
                    break
                try:
                    await self.rate_limiter.acquire()
                    subreddit = await self.reddit.subreddit(subreddit_name)
                    async for submission in subreddit.new(limit=100):
                        dedup_key = f"reddit:post:{submission.id}"
                        if await self.is_seen(dedup_key):
                            continue
                        record = await process_submission(submission)
                        await self.producer.send(
                            RAW_REDDIT,
                            record.model_dump(mode="json"),
                            key=f"reddit:{submission.id}",
                        )
                        await self.mark_seen(dedup_key)
                        await self.metrics.increment("reddit.posts.ingested")

                        # Ingest top-level comments (cap at 200)
                        try:
                            submission.comment_sort = "new"
                            await submission.comments.replace_more(limit=0)
                            for comment in submission.comments.list()[:200]:
                                ckey = f"reddit:comment:{comment.id}"
                                if await self.is_seen(ckey):
                                    continue
                                crec = await process_comment(
                                    comment, submission.id
                                )
                                await self.producer.send(
                                    RAW_REDDIT,
                                    crec.model_dump(mode="json"),
                                    key=f"reddit:{comment.id}",
                                )
                                await self.mark_seen(ckey)
                                await self.metrics.increment(
                                    "reddit.comments.ingested"
                                )
                        except Exception as ce:
                            logger.warning(
                                "reddit_comment_error",
                                submission=submission.id,
                                error=str(ce),
                            )

                except Exception as e:
                    logger.error(
                        "reddit_poll_error",
                        subreddit=subreddit_name,
                        error=str(e),
                    )
                    await self.metrics.increment(
                        "reddit.errors",
                        tags={"subreddit": subreddit_name},
                    )

            await self.metrics.gauge(
                "reddit.subreddits.monitored",
                len(TIER_1_SUBREDDITS)
                + len(TIER_2_SUBREDDITS)
                + len(TIER_3_SUBREDDITS),
            )
            await asyncio.sleep(interval_seconds)

    async def run(self) -> None:
        """Start all tier pollers concurrently."""
        tasks = [
            asyncio.create_task(
                self.poll_tier(TIER_1_SUBREDDITS, TIER_INTERVALS[1], 1)
            ),
            asyncio.create_task(
                self.poll_tier(TIER_2_SUBREDDITS, TIER_INTERVALS[2], 2)
            ),
            asyncio.create_task(
                self.poll_tier(TIER_3_SUBREDDITS, TIER_INTERVALS[3], 3)
            ),
        ]
        await asyncio.gather(*tasks)

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    """Entrypoint for running the Reddit ingestion service."""
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    producer = KafkaProducerWrapper()
    await producer.start()

    reddit = asyncpraw.Reddit(
        client_id=settings.reddit.client_id,
        client_secret=settings.reddit.client_secret,
        user_agent=settings.reddit.user_agent,
    )

    poller = RedditPoller(reddit, redis, producer, metrics)
    logger.info("reddit_poller_starting")
    try:
        await poller.run()
    except KeyboardInterrupt:
        poller.stop()
    finally:
        await reddit.close()
        await producer.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
