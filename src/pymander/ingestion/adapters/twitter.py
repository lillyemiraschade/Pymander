"""Twitter/X ingestion adapter using tweepy."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import structlog
import tweepy
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.adapters.base import AbstractSourceAdapter
from pymander.ingestion.producer import KafkaProducerWrapper
from pymander.ingestion.topics import RAW_TWITTER
from pymander.schemas.content import (
    ActorInfo,
    EngagementMetrics,
    UnifiedContentRecord,
)
from pymander.schemas.enums import ContentType, Platform

logger = structlog.get_logger()

# --- Tracked tweet fields and expansions ---

TWEET_FIELDS = [
    "id", "text", "created_at", "author_id", "conversation_id",
    "in_reply_to_user_id", "referenced_tweets", "attachments",
    "public_metrics", "entities", "lang", "geo", "source",
]

USER_FIELDS = [
    "id", "name", "username", "created_at", "verified",
    "public_metrics", "description", "profile_image_url",
]

MEDIA_FIELDS = ["media_key", "type", "url", "preview_image_url"]

EXPANSIONS = [
    "author_id",
    "referenced_tweets.id",
    "referenced_tweets.id.author_id",
    "attachments.media_keys",
    "in_reply_to_user_id",
    "entities.mentions.username",
]

# --- Search queries for backfill ---

DEFAULT_SEARCH_QUERIES = [
    "politics OR geopolitics OR election lang:en -is:retweet",
    "disinformation OR misinformation OR propaganda lang:en -is:retweet",
    "breaking news OR crisis OR conflict lang:en -is:retweet",
]


def _determine_content_type(tweet_data: dict) -> ContentType:
    """Determine content type from referenced_tweets."""
    refs = tweet_data.get("referenced_tweets") or []
    for ref in refs:
        ref_type = ref.get("type", "") if isinstance(ref, dict) else getattr(ref, "type", "")
        if ref_type == "replied_to":
            return ContentType.REPLY
        if ref_type == "retweeted":
            return ContentType.REPOST
        if ref_type == "quoted":
            return ContentType.QUOTE
    return ContentType.POST


def _extract_hashtags(tweet_data: dict) -> list[str]:
    """Extract hashtag texts from tweet entities."""
    entities = tweet_data.get("entities") or {}
    hashtags = entities.get("hashtags") or []
    return [h.get("tag", "") for h in hashtags if h.get("tag")]


def _extract_mentioned_users(tweet_data: dict) -> list[str]:
    """Extract mentioned usernames from tweet entities."""
    entities = tweet_data.get("entities") or {}
    mentions = entities.get("mentions") or []
    return [m.get("username", "") for m in mentions if m.get("username")]


def _extract_media_urls(
    tweet_data: dict, includes: dict | None = None,
) -> list[str]:
    """Extract media URLs from attachments and includes."""
    urls: list[str] = []

    # Media from includes lookup
    if includes:
        media_map = {
            m.get("media_key"): m for m in (includes.get("media") or [])
        }
        attachments = tweet_data.get("attachments") or {}
        for key in attachments.get("media_keys") or []:
            media = media_map.get(key, {})
            url = media.get("url") or media.get("preview_image_url")
            if url:
                urls.append(url)

    # URLs from entities
    entities = tweet_data.get("entities") or {}
    for u in entities.get("urls") or []:
        expanded = u.get("expanded_url", "")
        if expanded and any(
            ext in expanded for ext in (".jpg", ".png", ".gif", ".mp4", ".webp")
        ):
            urls.append(expanded)

    return urls


def _get_parent_id(tweet_data: dict) -> str | None:
    """Get parent tweet ID for replies."""
    refs = tweet_data.get("referenced_tweets") or []
    for ref in refs:
        ref_type = ref.get("type", "") if isinstance(ref, dict) else getattr(ref, "type", "")
        ref_id = ref.get("id", "") if isinstance(ref, dict) else getattr(ref, "id", "")
        if ref_type == "replied_to" and ref_id:
            return str(ref_id)
    return None


def _build_user_lookup(includes: dict | None) -> dict[str, dict]:
    """Build author_id -> user data lookup from includes."""
    if not includes:
        return {}
    return {
        str(u.get("id", "")): u
        for u in (includes.get("users") or [])
        if u.get("id")
    }


def map_tweet_to_record(
    tweet_data: dict,
    includes: dict | None = None,
) -> UnifiedContentRecord:
    """Map a Twitter API v2 tweet object to UnifiedContentRecord."""
    author_id = str(tweet_data.get("author_id", ""))
    user_lookup = _build_user_lookup(includes)
    user = user_lookup.get(author_id, {})
    user_metrics = user.get("public_metrics") or {}

    actor_info = ActorInfo(
        platform_id=author_id,
        username=user.get("username", author_id),
        display_name=user.get("name"),
        follower_count=user_metrics.get("followers_count"),
        following_count=user_metrics.get("following_count"),
        account_created_at=(
            datetime.fromisoformat(user["created_at"].replace("Z", "+00:00"))
            if user.get("created_at")
            else None
        ),
        is_verified=user.get("verified", False),
        bio=user.get("description"),
        profile_url=(
            f"https://x.com/{user['username']}"
            if user.get("username")
            else None
        ),
    )

    pub_metrics = tweet_data.get("public_metrics") or {}
    engagement = EngagementMetrics(
        likes=pub_metrics.get("like_count", 0),
        shares=pub_metrics.get("retweet_count", 0),
        replies=pub_metrics.get("reply_count", 0),
        views=pub_metrics.get("impression_count"),
        quotes=pub_metrics.get("quote_count", 0),
        bookmarks=pub_metrics.get("bookmark_count", 0),
    )

    tweet_id = str(tweet_data.get("id", ""))
    created_str = tweet_data.get("created_at", "")
    created_at = (
        datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        if created_str
        else datetime.now(UTC)
    )

    mentioned_users = _extract_mentioned_users(tweet_data)

    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=Platform.TWITTER,
        content_type=_determine_content_type(tweet_data),
        platform_content_id=tweet_id,
        created_at=created_at,
        collected_at=datetime.now(UTC),
        text=tweet_data.get("text", ""),
        title=None,
        url=f"https://x.com/i/status/{tweet_id}",
        media_urls=_extract_media_urls(tweet_data, includes),
        language=tweet_data.get("lang"),
        parent_id=_get_parent_id(tweet_data),
        root_id=str(tweet_data.get("conversation_id", tweet_id)),
        conversation_id=str(tweet_data.get("conversation_id", tweet_id)),
        actor=actor_info,
        engagement=engagement,
        geo=None,
        hashtags=_extract_hashtags(tweet_data),
        raw_payload={
            "source": tweet_data.get("source"),
            "referenced_tweets": tweet_data.get("referenced_tweets"),
            "in_reply_to_user_id": tweet_data.get("in_reply_to_user_id"),
            "mentioned_users": mentioned_users,
            "geo": tweet_data.get("geo"),
        },
    )


# --- Circuit breaker for rate limiting ---


class TwitterCircuitBreaker:
    """Redis-backed circuit breaker for Twitter API rate limits.

    States:
      CLOSED   - requests flow normally
      OPEN     - requests are blocked; reset after cooldown
      HALF_OPEN - one probe request allowed to test recovery
    """

    STATE_CLOSED = "closed"
    STATE_OPEN = "open"
    STATE_HALF_OPEN = "half_open"

    FAILURE_THRESHOLD = 5
    COOLDOWN_SECONDS = 300  # 5 minutes

    def __init__(self, redis: Redis, metrics: MetricsCollector) -> None:
        self.redis = redis
        self.metrics = metrics
        self._prefix = "twitter:circuit"

    async def _get_state(self) -> str:
        state = await self.redis.get(f"{self._prefix}:state")
        return state.decode() if state else self.STATE_CLOSED

    async def _set_state(self, state: str, ttl: int | None = None) -> None:
        if ttl:
            await self.redis.set(f"{self._prefix}:state", state, ex=ttl)
        else:
            await self.redis.set(f"{self._prefix}:state", state)

    async def _get_failure_count(self) -> int:
        count = await self.redis.get(f"{self._prefix}:failures")
        return int(count) if count else 0

    async def _incr_failures(self) -> int:
        key = f"{self._prefix}:failures"
        count = await self.redis.incr(key)
        if count == 1:
            await self.redis.expire(key, self.COOLDOWN_SECONDS)
        return count

    async def _reset_failures(self) -> None:
        await self.redis.delete(f"{self._prefix}:failures")

    async def can_request(self) -> bool:
        """Check whether a request is allowed under the circuit breaker."""
        state = await self._get_state()
        if state == self.STATE_CLOSED:
            return True
        if state == self.STATE_HALF_OPEN:
            return True
        # OPEN - check if cooldown has expired (key TTL)
        ttl = await self.redis.ttl(f"{self._prefix}:state")
        if ttl <= 0:
            await self._set_state(self.STATE_HALF_OPEN)
            return True
        return False

    async def record_success(self) -> None:
        await self._reset_failures()
        await self._set_state(self.STATE_CLOSED)

    async def record_failure(self) -> None:
        count = await self._incr_failures()
        if count >= self.FAILURE_THRESHOLD:
            await self._set_state(
                self.STATE_OPEN, ttl=self.COOLDOWN_SECONDS,
            )
            await self.metrics.increment("twitter.circuit_breaker.opened")
            logger.warning(
                "twitter_circuit_breaker_opened",
                failures=count,
                cooldown=self.COOLDOWN_SECONDS,
            )


# --- Streaming client (real-time) ---


class _PymStreamClient(tweepy.StreamingClient):
    """Tweepy StreamingClient subclass that pushes tweets into an asyncio queue."""

    def __init__(
        self,
        bearer_token: str,
        queue: asyncio.Queue,
        loop: asyncio.AbstractEventLoop,
        **kwargs,
    ) -> None:
        super().__init__(bearer_token, **kwargs)
        self._queue = queue
        self._loop = loop

    def on_response(self, response: tweepy.StreamResponse) -> None:
        """Called by the streaming thread for each incoming tweet."""
        tweet_data = response.data.data if response.data else {}
        includes_raw: dict = {}
        if response.includes:
            includes_raw = {
                "users": [
                    u.data for u in (response.includes.get("users") or [])
                ],
                "media": [
                    m.data for m in (response.includes.get("media") or [])
                ],
            }
        self._loop.call_soon_threadsafe(
            self._queue.put_nowait, (tweet_data, includes_raw),
        )

    def on_errors(self, errors) -> None:
        logger.error("twitter_stream_error", errors=errors)

    def on_connection_error(self) -> None:
        logger.warning("twitter_stream_connection_error")


# --- Adapter + Poller ---


class TwitterAdapter(AbstractSourceAdapter):
    """Twitter source adapter implementing AbstractSourceAdapter."""

    def __init__(self) -> None:
        self.client: tweepy.Client | None = None
        self.settings = get_settings()

    async def connect(self) -> None:
        self.client = tweepy.Client(
            bearer_token=self.settings.twitter.bearer_token,
            wait_on_rate_limit=True,
        )
        logger.info("twitter_adapter_connected")

    async def disconnect(self) -> None:
        self.client = None
        logger.info("twitter_adapter_disconnected")

    async def fetch(
        self, query: str = "news", max_results: int = 100, **kwargs,
    ) -> AsyncGenerator[UnifiedContentRecord, None]:
        """Backfill via search_recent_tweets."""
        if not self.client:
            raise RuntimeError("Adapter not connected")

        response = await asyncio.to_thread(
            self.client.search_recent_tweets,
            query=query,
            max_results=min(max_results, 100),
            tweet_fields=TWEET_FIELDS,
            user_fields=USER_FIELDS,
            media_fields=MEDIA_FIELDS,
            expansions=EXPANSIONS,
        )
        if not response or not response.data:
            return

        includes = {}
        if response.includes:
            includes = {
                "users": [
                    u.data for u in (response.includes.get("users") or [])
                ],
                "media": [
                    m.data for m in (response.includes.get("media") or [])
                ],
            }

        for tweet in response.data:
            yield map_tweet_to_record(tweet.data, includes)


class TwitterPoller:
    """Manages real-time streaming and periodic backfill with dedup via Redis."""

    BACKFILL_INTERVAL = 120  # seconds between search-based backfill passes
    STREAM_DRAIN_INTERVAL = 0.1  # seconds between stream queue drains

    def __init__(
        self,
        bearer_token: str,
        redis: Redis,
        producer: KafkaProducerWrapper,
        metrics: MetricsCollector,
        search_queries: list[str] | None = None,
        stream_rules: list[str] | None = None,
    ) -> None:
        self.bearer_token = bearer_token
        self.redis = redis
        self.producer = producer
        self.metrics = metrics
        self.search_queries = search_queries or DEFAULT_SEARCH_QUERIES
        self.stream_rules = stream_rules or []
        self.circuit_breaker = TwitterCircuitBreaker(redis, metrics)
        self._running = True
        self._stream_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)
        self._stream_client: _PymStreamClient | None = None
        self._search_client: tweepy.Client | None = None

    # --- dedup helpers ---

    async def is_seen(self, key: str) -> bool:
        return bool(await self.redis.exists(key))

    async def mark_seen(self, key: str, ttl: int = 172800) -> None:
        await self.redis.set(key, "1", ex=ttl)

    # --- content mapping for graph builder ---

    async def store_content_mapping(self, record: UnifiedContentRecord) -> None:
        """Store tweet -> author and conversation mappings in Redis for graph lookups."""
        tweet_id = record.platform_content_id
        mapping_key = f"twitter:map:{tweet_id}"
        mapping = {
            "author_id": record.actor.platform_id,
            "conversation_id": record.conversation_id or "",
            "parent_id": record.parent_id or "",
            "content_type": record.content_type.value,
        }
        await self.redis.hset(mapping_key, mapping=mapping)
        await self.redis.expire(mapping_key, 604800)  # 7 days

    # --- publish helper ---

    async def _publish(self, record: UnifiedContentRecord) -> None:
        dedup_key = f"twitter:tweet:{record.platform_content_id}"
        if await self.is_seen(dedup_key):
            return
        await self.producer.send(
            RAW_TWITTER,
            record.model_dump(mode="json"),
            key=f"twitter:{record.platform_content_id}",
        )
        await self.mark_seen(dedup_key)
        await self.store_content_mapping(record)
        await self.metrics.increment("twitter.tweets.ingested")

    # --- streaming ---

    async def _setup_stream_rules(self) -> None:
        """Sync stream filter rules with the API."""
        if not self._stream_client:
            return
        existing = await asyncio.to_thread(self._stream_client.get_rules)
        if existing and existing.data:
            ids = [r.id for r in existing.data]
            await asyncio.to_thread(
                self._stream_client.delete_rules, ids,
            )
            logger.info("twitter_stream_rules_cleared", count=len(ids))

        if self.stream_rules:
            add_rules = [
                tweepy.StreamRule(value=rule) for rule in self.stream_rules
            ]
            await asyncio.to_thread(
                self._stream_client.add_rules, add_rules,
            )
            logger.info(
                "twitter_stream_rules_added", count=len(self.stream_rules),
            )

    async def _run_stream(self) -> None:
        """Start the filtered stream in a background thread."""
        loop = asyncio.get_running_loop()
        self._stream_client = _PymStreamClient(
            self.bearer_token,
            self._stream_queue,
            loop,
            wait_on_rate_limit=True,
        )
        await self._setup_stream_rules()

        # filter() blocks, so run in a thread
        await asyncio.to_thread(
            self._stream_client.filter,
            tweet_fields=TWEET_FIELDS,
            user_fields=USER_FIELDS,
            media_fields=MEDIA_FIELDS,
            expansions=EXPANSIONS,
            threaded=False,
        )

    async def _drain_stream(self) -> None:
        """Continuously drain the stream queue and publish records."""
        while self._running:
            try:
                tweet_data, includes = await asyncio.wait_for(
                    self._stream_queue.get(), timeout=self.STREAM_DRAIN_INTERVAL,
                )
                record = map_tweet_to_record(tweet_data, includes)
                await self._publish(record)
                await self.circuit_breaker.record_success()
            except TimeoutError:
                continue
            except Exception as e:
                logger.error("twitter_stream_drain_error", error=str(e))
                await self.circuit_breaker.record_failure()
                await self.metrics.increment("twitter.errors")

    # --- backfill via search ---

    async def _backfill_loop(self) -> None:
        """Periodically search recent tweets for each configured query."""
        self._search_client = tweepy.Client(
            bearer_token=self.bearer_token,
            wait_on_rate_limit=True,
        )

        while self._running:
            if not await self.circuit_breaker.can_request():
                logger.warning("twitter_circuit_breaker_blocking_backfill")
                await asyncio.sleep(30)
                continue

            for query in self.search_queries:
                if not self._running:
                    break
                try:
                    response = await asyncio.to_thread(
                        self._search_client.search_recent_tweets,
                        query=query,
                        max_results=100,
                        tweet_fields=TWEET_FIELDS,
                        user_fields=USER_FIELDS,
                        media_fields=MEDIA_FIELDS,
                        expansions=EXPANSIONS,
                    )
                    if not response or not response.data:
                        continue

                    includes = {}
                    if response.includes:
                        includes = {
                            "users": [
                                u.data
                                for u in (response.includes.get("users") or [])
                            ],
                            "media": [
                                m.data
                                for m in (response.includes.get("media") or [])
                            ],
                        }

                    for tweet in response.data:
                        record = map_tweet_to_record(tweet.data, includes)
                        await self._publish(record)

                    await self.circuit_breaker.record_success()

                except tweepy.TooManyRequests:
                    logger.warning(
                        "twitter_rate_limited", query=query,
                    )
                    await self.circuit_breaker.record_failure()
                    await self.metrics.increment("twitter.rate_limit.hits")
                    await asyncio.sleep(60)

                except Exception as e:
                    logger.error(
                        "twitter_backfill_error",
                        query=query,
                        error=str(e),
                    )
                    await self.circuit_breaker.record_failure()
                    await self.metrics.increment("twitter.errors")

            await self.metrics.gauge(
                "twitter.search_queries.active",
                len(self.search_queries),
            )
            await asyncio.sleep(self.BACKFILL_INTERVAL)

    # --- lifecycle ---

    async def run(self) -> None:
        """Start stream consumer and backfill loop concurrently."""
        tasks = [
            asyncio.create_task(self._run_stream()),
            asyncio.create_task(self._drain_stream()),
            asyncio.create_task(self._backfill_loop()),
        ]
        logger.info(
            "twitter_poller_running",
            stream_rules=len(self.stream_rules),
            search_queries=len(self.search_queries),
        )
        await asyncio.gather(*tasks, return_exceptions=True)

    def stop(self) -> None:
        self._running = False
        if self._stream_client:
            self._stream_client.disconnect()


async def main() -> None:
    """Entrypoint for running the Twitter ingestion service."""
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    producer = KafkaProducerWrapper()
    await producer.start()

    poller = TwitterPoller(
        bearer_token=settings.twitter.bearer_token,
        redis=redis,
        producer=producer,
        metrics=metrics,
        search_queries=DEFAULT_SEARCH_QUERIES,
        stream_rules=settings.twitter.stream_rules,
    )
    logger.info("twitter_poller_starting")
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
