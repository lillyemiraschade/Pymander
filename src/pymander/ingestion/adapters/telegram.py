"""Telegram ingestion adapter using telethon for public channels/supergroups."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import structlog
from redis.asyncio import Redis
from telethon import TelegramClient
from telethon.tl.types import (
    Channel,
    Message,
    MessageReplyHeader,
    PeerChannel,
)

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.adapters.base import AbstractSourceAdapter
from pymander.ingestion.producer import KafkaProducerWrapper
from pymander.ingestion.topics import RAW_TELEGRAM
from pymander.schemas.content import (
    ActorInfo,
    EngagementMetrics,
    UnifiedContentRecord,
)
from pymander.schemas.enums import ContentType, Platform

logger = structlog.get_logger()

# Default poll interval in seconds
POLL_INTERVAL = 30

# How many messages to fetch per channel per poll cycle
MESSAGES_PER_POLL = 100

# Redis TTL for dedup keys (48 hours)
DEDUP_TTL = 172800


def _determine_content_type(message: Message) -> ContentType:
    """Determine the ContentType based on message characteristics."""
    if message.forward:
        return ContentType.FORWARD
    if message.reply_to and isinstance(
        message.reply_to, MessageReplyHeader
    ):
        return ContentType.REPLY
    return ContentType.POST


def _extract_media_urls(message: Message) -> list[str]:
    """Extract identifiable media references from a Telegram message.

    Telegram does not expose direct CDN URLs; we store file references
    that can be downloaded via the client later.
    """
    urls: list[str] = []
    if message.photo:
        # Use photo file_id as a reference
        urls.append(f"tg://photo/{message.id}")
    if message.document:
        urls.append(f"tg://document/{message.id}")
    if message.web_preview and message.web_preview.url:
        urls.append(message.web_preview.url)
    return urls


def _extract_reactions(message: Message) -> dict[str, int]:
    """Extract reaction counts from a message, if available."""
    reactions: dict[str, int] = {}
    if not hasattr(message, "reactions") or not message.reactions:
        return reactions
    results = getattr(message.reactions, "results", None)
    if not results:
        return reactions
    for r in results:
        emoticon = getattr(r.reaction, "emoticon", None)
        if emoticon:
            reactions[emoticon] = r.count
    return reactions


def _build_record(
    message: Message,
    channel_username: str,
    channel_title: str,
    channel_id: int,
) -> UnifiedContentRecord:
    """Map a Telegram Message to a UnifiedContentRecord."""
    content_type = _determine_content_type(message)
    text = message.text or message.message or ""

    # Author info: for channels the "author" is the channel itself
    sender_id = str(message.sender_id or channel_id)
    sender_name = channel_title

    # If post has a post_author signature (channels can show author names)
    post_author = getattr(message, "post_author", None)
    if post_author:
        sender_name = post_author

    actor = ActorInfo(
        platform_id=str(sender_id),
        username=channel_username or str(channel_id),
        display_name=sender_name,
        profile_url=(
            f"https://t.me/{channel_username}"
            if channel_username
            else None
        ),
    )

    # Engagement metrics
    views = getattr(message, "views", None)
    forwards_count = getattr(message, "forwards", None) or 0
    reactions_map = _extract_reactions(message)
    total_reactions = sum(reactions_map.values())

    engagement = EngagementMetrics(
        likes=total_reactions,
        shares=forwards_count,
        replies=getattr(message, "replies", None)
        and getattr(message.replies, "replies", 0)
        or 0,
        views=views,
        quotes=0,
        bookmarks=0,
    )

    # Threading: reply_to for supergroup discussions
    parent_id: str | None = None
    if (
        message.reply_to
        and isinstance(message.reply_to, MessageReplyHeader)
    ):
        parent_id = str(message.reply_to.reply_to_msg_id)

    # Forward chain tracking -- critical for Neo4j FORWARDED_FROM edges
    raw_payload: dict = {
        "channel_id": channel_id,
        "channel_username": channel_username,
        "reactions": reactions_map,
        "grouped_id": getattr(message, "grouped_id", None),
        "edit_date": (
            message.edit_date.isoformat()
            if message.edit_date
            else None
        ),
        "post_author": post_author,
    }

    if message.forward:
        fwd = message.forward
        fwd_channel_id = None
        if fwd.chat_id:
            fwd_channel_id = fwd.chat_id
        elif fwd.from_id and isinstance(fwd.from_id, PeerChannel):
            fwd_channel_id = fwd.from_id.channel_id

        raw_payload["forwarded_from_channel_id"] = fwd_channel_id
        raw_payload["forwarded_from_name"] = fwd.from_name
        raw_payload["forwarded_from_date"] = (
            fwd.date.isoformat() if fwd.date else None
        )
        raw_payload["forwarded_from_post_id"] = fwd.channel_post

    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=Platform.TELEGRAM,
        content_type=content_type,
        platform_content_id=f"{channel_id}:{message.id}",
        created_at=(
            message.date.replace(tzinfo=UTC)
            if message.date and message.date.tzinfo is None
            else message.date or datetime.now(UTC)
        ),
        collected_at=datetime.now(UTC),
        text=text,
        title=None,
        url=(
            f"https://t.me/{channel_username}/{message.id}"
            if channel_username
            else None
        ),
        media_urls=_extract_media_urls(message),
        language=None,
        parent_id=parent_id,
        root_id=str(channel_id),
        conversation_id=(
            f"{channel_id}:{parent_id}"
            if parent_id
            else f"{channel_id}:{message.id}"
        ),
        actor=actor,
        engagement=engagement,
        geo=None,
        hashtags=[],
        raw_payload=raw_payload,
    )


class TelegramAdapter(AbstractSourceAdapter):
    """Telegram source adapter implementing AbstractSourceAdapter."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.client: TelegramClient | None = None

    async def connect(self) -> None:
        tg = self.settings.telegram
        self.client = TelegramClient(
            tg.session_name, tg.api_id, tg.api_hash
        )
        await self.client.start()
        logger.info("telegram_adapter_connected")

    async def disconnect(self) -> None:
        if self.client:
            await self.client.disconnect()
            logger.info("telegram_adapter_disconnected")

    async def fetch(
        self,
        channel: str | None = None,
        limit: int = MESSAGES_PER_POLL,
        **kwargs,
    ) -> AsyncGenerator[UnifiedContentRecord, None]:
        """Fetch recent messages from a single channel."""
        if not self.client:
            raise RuntimeError("Adapter not connected")

        channels = (
            [channel] if channel else self.settings.telegram.channels
        )

        for ch in channels:
            try:
                entity = await self.client.get_entity(ch)
                if not isinstance(entity, Channel):
                    logger.warning(
                        "telegram_not_a_channel",
                        channel=ch,
                    )
                    continue

                channel_username = getattr(entity, "username", "") or ""
                channel_title = getattr(entity, "title", ch) or ch
                channel_id = entity.id

                async for message in self.client.iter_messages(
                    entity, limit=limit
                ):
                    if not isinstance(message, Message):
                        continue
                    yield _build_record(
                        message,
                        channel_username,
                        channel_title,
                        channel_id,
                    )
            except Exception as exc:
                logger.error(
                    "telegram_channel_fetch_error",
                    channel=ch,
                    error=str(exc),
                )


class TelegramPoller:
    """Continuous poller for Telegram public channels with Redis dedup."""

    def __init__(
        self,
        client: TelegramClient,
        redis: Redis,
        producer: KafkaProducerWrapper,
        metrics: MetricsCollector,
    ) -> None:
        self.client = client
        self.redis = redis
        self.producer = producer
        self.metrics = metrics
        self.settings = get_settings()
        self._running = True

    async def is_seen(self, key: str) -> bool:
        return bool(await self.redis.exists(key))

    async def mark_seen(self, key: str) -> None:
        await self.redis.set(key, "1", ex=DEDUP_TTL)

    async def _get_min_id(self, channel_id: int) -> int:
        """Get the last processed message ID for offset-based polling."""
        key = f"telegram:min_id:{channel_id}"
        val = await self.redis.get(key)
        return int(val) if val else 0

    async def _set_min_id(self, channel_id: int, msg_id: int) -> None:
        """Store the latest message ID we have processed."""
        key = f"telegram:min_id:{channel_id}"
        await self.redis.set(key, str(msg_id), ex=604800)  # 7 day TTL

    async def poll_channel(self, channel_name: str) -> None:
        """Poll a single channel continuously."""
        logger.info("telegram_channel_poll_start", channel=channel_name)

        # Resolve entity once
        try:
            entity = await self.client.get_entity(channel_name)
        except Exception as exc:
            logger.error(
                "telegram_entity_resolve_failed",
                channel=channel_name,
                error=str(exc),
            )
            return

        if not isinstance(entity, Channel):
            logger.warning(
                "telegram_not_a_channel",
                channel=channel_name,
                entity_type=type(entity).__name__,
            )
            return

        channel_username = getattr(entity, "username", "") or ""
        channel_title = getattr(entity, "title", channel_name) or channel_name
        channel_id = entity.id

        while self._running:
            try:
                min_id = await self._get_min_id(channel_id)
                max_seen_id = min_id
                ingested_count = 0

                async for message in self.client.iter_messages(
                    entity,
                    limit=MESSAGES_PER_POLL,
                    min_id=min_id,
                ):
                    if not isinstance(message, Message):
                        continue

                    dedup_key = (
                        f"telegram:msg:{channel_id}:{message.id}"
                    )
                    if await self.is_seen(dedup_key):
                        continue

                    record = _build_record(
                        message,
                        channel_username,
                        channel_title,
                        channel_id,
                    )
                    await self.producer.send(
                        RAW_TELEGRAM,
                        record.model_dump(mode="json"),
                        key=f"telegram:{channel_id}:{message.id}",
                    )
                    await self.mark_seen(dedup_key)
                    ingested_count += 1

                    if message.id > max_seen_id:
                        max_seen_id = message.id

                if max_seen_id > min_id:
                    await self._set_min_id(channel_id, max_seen_id)

                await self.metrics.increment(
                    "telegram.messages.ingested",
                    value=ingested_count,
                    tags={"channel": channel_username or str(channel_id)},
                )

                if ingested_count > 0:
                    logger.info(
                        "telegram_poll_cycle_complete",
                        channel=channel_name,
                        ingested=ingested_count,
                    )

            except Exception as exc:
                logger.error(
                    "telegram_poll_error",
                    channel=channel_name,
                    error=str(exc),
                )
                await self.metrics.increment(
                    "telegram.errors",
                    tags={
                        "channel": channel_username or str(channel_id)
                    },
                )

            await asyncio.sleep(POLL_INTERVAL)

    async def run(self) -> None:
        """Start pollers for all configured channels concurrently."""
        channels = self.settings.telegram.channels
        if not channels:
            logger.warning("telegram_no_channels_configured")
            return

        await self.metrics.gauge(
            "telegram.channels.monitored", len(channels)
        )

        tasks = [
            asyncio.create_task(self.poll_channel(ch))
            for ch in channels
        ]
        await asyncio.gather(*tasks)

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    """Entrypoint for running the Telegram ingestion service."""
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    producer = KafkaProducerWrapper()
    await producer.start()

    tg = settings.telegram
    client = TelegramClient(tg.session_name, tg.api_id, tg.api_hash)
    await client.start()

    poller = TelegramPoller(client, redis, producer, metrics)
    logger.info(
        "telegram_poller_starting",
        channels=settings.telegram.channels,
    )
    try:
        await poller.run()
    except KeyboardInterrupt:
        poller.stop()
    finally:
        await client.disconnect()
        await producer.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
