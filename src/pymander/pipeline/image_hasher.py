"""Perceptual image hashing for cross-platform visual tracking."""

from __future__ import annotations

import asyncio
from io import BytesIO

import aiohttp
import imagehash
import structlog
from PIL import Image
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.consumer import KafkaConsumerWrapper

logger = structlog.get_logger()

HAMMING_THRESHOLD = 10  # max hamming distance for a "match" (out of 64 bits)
HASH_TTL = 2592000  # 30 days


class ImageHasher:
    """Downloads images and computes perceptual hashes for cross-platform tracking."""

    def __init__(
        self, redis: Redis, metrics: MetricsCollector
    ) -> None:
        self.redis = redis
        self.metrics = metrics
        self._running = True

    async def process_image(
        self, url: str, content_id: str, platform: str
    ) -> str | None:
        """Download image, compute pHash, check for cross-platform matches."""
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp,
            ):
                if resp.status != 200:
                    return None
                data = await resp.read()

            img = await asyncio.to_thread(Image.open, BytesIO(data))
            phash = await asyncio.to_thread(imagehash.phash, img)
            hash_str = str(phash)

            # Store hash -> content mapping
            await self.redis.sadd(
                f"imagehash:{hash_str}", f"{platform}:{content_id}"
            )
            await self.redis.expire(
                f"imagehash:{hash_str}", HASH_TTL
            )

            # Store content -> hash mapping
            await self.redis.hset(
                f"content:hashes:{content_id}", url, hash_str
            )
            await self.redis.expire(
                f"content:hashes:{content_id}", HASH_TTL
            )

            # Check for cross-platform matches
            members = await self.redis.smembers(f"imagehash:{hash_str}")
            cross_platform = [
                m.decode()
                for m in members
                if not m.decode().startswith(f"{platform}:")
            ]

            if cross_platform:
                logger.info(
                    "cross_platform_image_detected",
                    content_id=content_id,
                    platform=platform,
                    matches=cross_platform,
                    hash=hash_str,
                )
                await self.metrics.increment(
                    "images.cross_platform_detected"
                )
                # Store cross-platform link
                await self.redis.sadd(
                    f"crossplatform:{content_id}",
                    *cross_platform,
                )
                await self.redis.expire(
                    f"crossplatform:{content_id}", HASH_TTL
                )

            await self.metrics.increment("images.hashed")
            return hash_str

        except Exception as e:
            logger.warning(
                "image_hash_failed",
                url=url,
                content_id=content_id,
                error=str(e),
            )
            await self.metrics.increment("images.hash_errors")
            return None

    async def run(self) -> None:
        """Consume media.to_hash topic and process images."""
        consumer = KafkaConsumerWrapper(
            "media.to_hash", group_id="pymander-image-hasher"
        )
        await consumer.start()
        logger.info("image_hasher_started")

        try:
            async for msg in consumer.messages():
                if not self._running:
                    break
                url = msg.get("url", "")
                content_id = msg.get("content_id", "")
                platform = msg.get("platform", "")
                if url and content_id:
                    await self.process_image(url, content_id, platform)
        finally:
            await consumer.stop()

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)

    hasher = ImageHasher(redis, metrics)
    try:
        await hasher.run()
    except KeyboardInterrupt:
        hasher.stop()
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
