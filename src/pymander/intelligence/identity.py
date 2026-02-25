"""Cross-platform identity resolution — probabilistic linking of accounts."""

from __future__ import annotations

import asyncio
import json
import re

import numpy as np
import structlog
from Levenshtein import distance as levenshtein_distance
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.network.neo4j_client import Neo4jClient
from pymander.schemas.coordination import IdentityLink

logger = structlog.get_logger()

LINK_CONFIDENCE_THRESHOLD = 0.70
HIGH_CONFIDENCE_THRESHOLD = 0.90

PLATFORM_BIO_PATTERNS = {
    "twitter": [r"twitter\.com/(\w+)", r"@(\w+)", r"x\.com/(\w+)"],
    "reddit": [r"reddit\.com/u/(\w+)", r"\bu/(\w+)"],
    "youtube": [r"youtube\.com/(@?\w+)", r"youtube\.com/c/(\w+)"],
    "telegram": [r"t\.me/(\w+)"],
    "bluesky": [r"bsky\.app/profile/(\S+)"],
}


class CrossPlatformLinker:
    """Probabilistic cross-platform identity linking using public data only.

    Each method produces a confidence score. Multiple methods compound:
    Final confidence = 1 - product(1 - method_confidence)
    """

    def __init__(self, neo4j: Neo4jClient, redis: Redis,
                 metrics: MetricsCollector) -> None:
        self.neo4j = neo4j
        self.redis = redis
        self.metrics = metrics

    async def method_username_matching(
        self, username_a: str, username_b: str,
    ) -> float:
        """Exact or fuzzy username match across platforms."""
        if not username_a or not username_b:
            return 0.0
        a_lower = username_a.lower().strip("@")
        b_lower = username_b.lower().strip("@")
        if a_lower == b_lower:
            return 0.60
        # Check for common patterns: user_123 vs user123 (higher signal)
        a_stripped = re.sub(r"[_\-.]", "", a_lower)
        b_stripped = re.sub(r"[_\-.]", "", b_lower)
        if a_stripped == b_stripped:
            return 0.45
        if levenshtein_distance(a_lower, b_lower) <= 2:
            return 0.35
        return 0.0

    async def method_bio_cross_reference(
        self, bio_a: str | None, platform_b: str, username_b: str,
    ) -> float:
        """Check if account A's bio references account B's platform."""
        if not bio_a or not username_b:
            return 0.0
        patterns = PLATFORM_BIO_PATTERNS.get(platform_b, [])
        for pattern in patterns:
            match = re.search(pattern, bio_a, re.IGNORECASE)
            if match and match.group(1).lower().strip("@") == username_b.lower().strip("@"):
                return 0.90
        return 0.0

    async def method_content_fingerprinting(
        self, uuid_a: str, uuid_b: str,
    ) -> float:
        """Check if both accounts posted near-identical content within short time windows.

        Uses cached embedding similarity from Redis.
        """
        # Get recent content hashes for both accounts
        key_a = f"identity:content_hashes:{uuid_a}"
        key_b = f"identity:content_hashes:{uuid_b}"
        hashes_a = await self.redis.lrange(key_a, 0, 49)
        hashes_b = await self.redis.lrange(key_b, 0, 49)

        if not hashes_a or not hashes_b:
            return 0.0

        # Check for matching content hashes (pre-computed by embedding pipeline)
        set_a = {h.decode() if isinstance(h, bytes) else h for h in hashes_a}
        set_b = {h.decode() if isinstance(h, bytes) else h for h in hashes_b}
        overlap = set_a & set_b

        if overlap:
            return min(0.80, 0.40 + len(overlap) * 0.10)
        return 0.0

    async def method_behavioral_fingerprinting(
        self, actor_a: dict, actor_b: dict,
    ) -> float:
        """Compare behavioral fingerprints: posting patterns, topics, language."""
        score = 0.0
        total_weight = 0.0

        # Posting hour overlap
        hours_a = actor_a.get("active_hours_utc", [])
        hours_b = actor_b.get("active_hours_utc", [])
        if hours_a and hours_b:
            set_a = set(hours_a)
            set_b = set(hours_b)
            union = set_a | set_b
            intersection = set_a & set_b
            hour_overlap = len(intersection) / len(union) if union else 0
            score += hour_overlap * 0.4
            total_weight += 0.4

        # Topic overlap
        topics_a = actor_a.get("primary_topics", [])
        topics_b = actor_b.get("primary_topics", [])
        if topics_a and topics_b:
            set_a = set(topics_a)
            set_b = set(topics_b)
            union = set_a | set_b
            intersection = set_a & set_b
            topic_overlap = len(intersection) / len(union) if union else 0
            score += topic_overlap * 0.6
            total_weight += 0.6

        if total_weight == 0:
            return 0.0
        return (score / total_weight) * 0.4  # Max 0.4 from behavioral alone

    async def resolve_identity(
        self, actor_a: dict, actor_b: dict,
    ) -> IdentityLink | None:
        """Run all methods and compute aggregate confidence."""
        if actor_a.get("primary_platform") == actor_b.get("primary_platform"):
            return None  # Same platform = different accounts by definition

        confidences: list[tuple[str, float]] = []

        # Method 1: Username matching
        conf = await self.method_username_matching(
            actor_a.get("username", ""), actor_b.get("username", ""),
        )
        if conf > 0:
            confidences.append(("username_matching", conf))

        # Method 2: Bio cross-reference (both directions)
        conf_ab = await self.method_bio_cross_reference(
            actor_a.get("bio"), actor_b.get("primary_platform", ""),
            actor_b.get("username", ""),
        )
        conf_ba = await self.method_bio_cross_reference(
            actor_b.get("bio"), actor_a.get("primary_platform", ""),
            actor_a.get("username", ""),
        )
        conf = max(conf_ab, conf_ba)
        if conf > 0:
            confidences.append(("bio_cross_reference", conf))

        # Method 3: Content fingerprinting
        conf = await self.method_content_fingerprinting(
            actor_a.get("internal_uuid", ""), actor_b.get("internal_uuid", ""),
        )
        if conf > 0:
            confidences.append(("content_fingerprinting", conf))

        # Method 4: Behavioral fingerprinting
        conf = await self.method_behavioral_fingerprinting(actor_a, actor_b)
        if conf > 0:
            confidences.append(("behavioral_fingerprinting", conf))

        if not confidences:
            return None

        aggregate = 1.0 - float(np.prod([1.0 - c for _, c in confidences]))

        if aggregate >= LINK_CONFIDENCE_THRESHOLD:
            return IdentityLink(
                account_a_uuid=actor_a.get("internal_uuid", ""),
                account_b_uuid=actor_b.get("internal_uuid", ""),
                account_a_platform=actor_a.get("primary_platform", ""),
                account_b_platform=actor_b.get("primary_platform", ""),
                confidence=round(aggregate, 4),
                methods=[
                    {"method": name, "confidence": round(c, 4)}
                    for name, c in confidences
                ],
            )
        return None


class IdentityResolutionRunner:
    """Daily batch job to discover and maintain cross-platform identity links."""

    def __init__(self, linker: CrossPlatformLinker, neo4j: Neo4jClient,
                 redis: Redis, metrics: MetricsCollector) -> None:
        self.linker = linker
        self.neo4j = neo4j
        self.redis = redis
        self.metrics = metrics
        self._running = True

    async def get_candidates_by_username(self) -> list[tuple[dict, dict]]:
        """Find candidate pairs by matching/similar usernames across platforms."""
        results = await self.neo4j.execute("""
            MATCH (a:Actor), (b:Actor)
            WHERE a.primary_platform <> b.primary_platform
              AND a.internal_uuid < b.internal_uuid
              AND toLower(a.username) = toLower(b.username)
              AND NOT (a)-[:SAME_PERSON]-(b)
            RETURN a {.*} AS actor_a, b {.*} AS actor_b
            LIMIT 1000
        """)
        return [(r["actor_a"], r["actor_b"]) for r in results]

    async def get_candidates_by_bio(self) -> list[tuple[dict, dict]]:
        """Find candidates whose bios reference other platforms."""
        results = await self.neo4j.execute("""
            MATCH (a:Actor)
            WHERE a.bio IS NOT NULL AND a.bio <> ''
            WITH a
            MATCH (b:Actor)
            WHERE b.primary_platform <> a.primary_platform
              AND (
                toLower(a.bio) CONTAINS toLower(b.username)
                OR toLower(a.bio) CONTAINS toLower(b.primary_platform)
              )
              AND NOT (a)-[:SAME_PERSON]-(b)
            RETURN a {.*} AS actor_a, b {.*} AS actor_b
            LIMIT 500
        """)
        return [(r["actor_a"], r["actor_b"]) for r in results]

    async def create_link(self, link: IdentityLink) -> None:
        """Create SAME_PERSON edge in Neo4j."""
        await self.neo4j.execute_write(
            """
            MATCH (a:Actor {internal_uuid: $uuid_a})
            MATCH (b:Actor {internal_uuid: $uuid_b})
            MERGE (a)-[r:SAME_PERSON]->(b)
            SET r.confidence = $confidence,
                r.methods = $methods,
                r.created_at = datetime(),
                r.updated_at = datetime()
            """,
            uuid_a=link.account_a_uuid,
            uuid_b=link.account_b_uuid,
            confidence=link.confidence,
            methods=json.dumps(link.methods),
        )

        # Store in Redis for fast lookup
        link_data = link.model_dump(mode="json")
        await self.redis.set(
            f"identity:link:{link.id}", json.dumps(link_data), ex=2592000,
        )
        await self.metrics.increment("identity.links.created")

    async def run_once(self) -> None:
        """Single resolution pass."""
        logger.info("identity_resolution_starting")

        # Get candidates
        username_candidates = await self.get_candidates_by_username()
        bio_candidates = await self.get_candidates_by_bio()

        all_candidates = username_candidates + bio_candidates
        # Deduplicate
        seen = set()
        unique_candidates = []
        for a, b in all_candidates:
            key = tuple(sorted([a.get("internal_uuid", ""), b.get("internal_uuid", "")]))
            if key not in seen:
                seen.add(key)
                unique_candidates.append((a, b))

        links_created = 0
        for actor_a, actor_b in unique_candidates:
            link = await self.linker.resolve_identity(actor_a, actor_b)
            if link:
                await self.create_link(link)
                links_created += 1
                logger.info(
                    "identity_link_created",
                    uuid_a=link.account_a_uuid,
                    uuid_b=link.account_b_uuid,
                    confidence=link.confidence,
                )

        logger.info(
            "identity_resolution_complete",
            candidates=len(unique_candidates),
            links_created=links_created,
        )

    async def run(self, interval_seconds: int = 86400) -> None:
        """Run identity resolution on a daily schedule."""
        logger.info("identity_resolution_runner_started")
        while self._running:
            try:
                await self.run_once()
            except Exception as e:
                logger.error("identity_resolution_error", error=str(e))
                await self.metrics.increment("identity.errors")
            await asyncio.sleep(interval_seconds)

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    settings = get_settings()
    from pymander.core.logging import setup_logging
    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    neo4j = Neo4jClient()
    await neo4j.connect()

    linker = CrossPlatformLinker(neo4j, redis, metrics)
    runner = IdentityResolutionRunner(linker, neo4j, redis, metrics)
    try:
        await runner.run()
    except KeyboardInterrupt:
        runner.stop()
    finally:
        await neo4j.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
