"""Graph construction pipeline — consumes content and builds Neo4j behavioral graph."""

from __future__ import annotations

import asyncio
import uuid

import structlog
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.consumer import KafkaConsumerWrapper
from pymander.ingestion.topics import RAW_FOURCHAN, RAW_REDDIT, RAW_RSS, RAW_TELEGRAM, RAW_TWITTER
from pymander.network.neo4j_client import Neo4jClient
from pymander.schemas.content import UnifiedContentRecord

logger = structlog.get_logger()

ALL_RAW_TOPICS = [RAW_REDDIT, RAW_RSS, RAW_TWITTER, RAW_TELEGRAM, RAW_FOURCHAN]


class GraphBuilder:
    """Consumes content from Kafka and builds the behavioral graph in Neo4j.

    For every piece of content:
    1. Upsert the author node
    2. For every interaction signal, create/strengthen edges
    """

    def __init__(self, neo4j: Neo4jClient, redis: Redis,
                 metrics: MetricsCollector) -> None:
        self.neo4j = neo4j
        self.redis = redis
        self.metrics = metrics
        self._running = True

    async def upsert_actor(self, actor_data: dict, platform: str) -> str:
        """Create or update an actor node. Returns internal_uuid."""
        platform_id = actor_data.get("platform_id", "unknown")
        platform_key = f"{platform}:{platform_id}"
        internal_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, platform_key))

        query = """
        MERGE (a:Actor {platform_key: $platform_key})
        ON CREATE SET
            a.internal_uuid = $uuid,
            a.primary_platform = $platform,
            a.username = $username,
            a.display_name = $display_name,
            a.follower_count = $followers,
            a.is_verified = $verified,
            a.bio = $bio,
            a.first_seen = datetime(),
            a.last_seen = datetime(),
            a.total_content_count = 1,
            a.influence_score = 0.0,
            a.amplification_score = 0.0,
            a.bridge_score = 0.0,
            a.bot_probability = 0.0,
            a.created_at = datetime()
        ON MATCH SET
            a.last_seen = datetime(),
            a.total_content_count = a.total_content_count + 1,
            a.follower_count = CASE WHEN $followers IS NOT NULL
                THEN $followers ELSE a.follower_count END,
            a.display_name = CASE WHEN $display_name IS NOT NULL
                THEN $display_name ELSE a.display_name END,
            a.updated_at = datetime()
        RETURN a.internal_uuid AS uuid
        """
        results = await self.neo4j.execute_write(
            query,
            platform_key=platform_key,
            uuid=internal_uuid,
            platform=platform,
            username=actor_data.get("username", platform_id),
            display_name=actor_data.get("display_name"),
            followers=actor_data.get("follower_count"),
            verified=actor_data.get("is_verified", False),
            bio=actor_data.get("bio"),
        )
        return results[0]["uuid"] if results else internal_uuid

    async def resolve_author_from_content(
        self, platform_content_id: str, platform: str,
    ) -> str | None:
        """Look up an author UUID from a content ID via Redis cache."""
        cache_key = f"content:author:{platform}:{platform_content_id}"
        cached = await self.redis.get(cache_key)
        if cached:
            return cached.decode()
        return None

    async def cache_content_author(
        self, platform_content_id: str, platform: str, author_uuid: str,
    ) -> None:
        """Cache content_id -> author_uuid mapping."""
        cache_key = f"content:author:{platform}:{platform_content_id}"
        await self.redis.set(cache_key, author_uuid, ex=604800)  # 7 days

    async def upsert_edge(
        self, source: str, target: str, edge_type: str,
        platform: str, narrative_id: str | None = None,
    ) -> None:
        """Create or strengthen a behavioral edge between two actors."""
        if source == target:
            return

        query = f"""
        MATCH (a:Actor {{internal_uuid: $source}})
        MATCH (b:Actor {{internal_uuid: $target}})
        MERGE (a)-[r:{edge_type}]->(b)
        ON CREATE SET
            r.weight = 1.0,
            r.interaction_count = 1,
            r.first_interaction = datetime(),
            r.last_interaction = datetime(),
            r.platforms = [$platform],
            r.shared_narrative_ids = CASE WHEN $narrative_id IS NOT NULL
                THEN [$narrative_id] ELSE [] END,
            r.coordination_score = 0.0
        ON MATCH SET
            r.weight = r.weight + (1.0 / (1.0 + duration.inDays(
                r.last_interaction, datetime()).days)),
            r.interaction_count = r.interaction_count + 1,
            r.last_interaction = datetime(),
            r.platforms = CASE WHEN NOT $platform IN r.platforms
                THEN r.platforms + $platform ELSE r.platforms END,
            r.shared_narrative_ids = CASE WHEN $narrative_id IS NOT NULL
                AND NOT $narrative_id IN coalesce(r.shared_narrative_ids, [])
                THEN coalesce(r.shared_narrative_ids, []) + $narrative_id
                ELSE r.shared_narrative_ids END
        """
        try:
            await self.neo4j.execute_write(
                query, source=source, target=target,
                platform=platform, narrative_id=narrative_id,
            )
        except Exception as e:
            logger.warning("edge_upsert_failed", edge_type=edge_type, error=str(e))

    async def process_content(self, record: UnifiedContentRecord) -> None:
        """Process one content record for graph updates."""
        platform = str(record.platform)
        actor_data = record.actor.model_dump()

        # 1. Upsert author node
        author_uuid = await self.upsert_actor(actor_data, platform)

        # Cache content -> author mapping for future lookups
        await self.cache_content_author(
            record.platform_content_id, platform, author_uuid,
        )

        # 2. Process reply relationships
        if record.parent_id:
            parent_author = await self.resolve_author_from_content(
                record.parent_id, platform,
            )
            if parent_author:
                await self.upsert_edge(
                    source=author_uuid, target=parent_author,
                    edge_type="REPLIED_TO", platform=platform,
                )
                await self.metrics.increment("network.edges.replied_to")

        # 3. Process conversation root relationship (for deep threads)
        if record.root_id and record.root_id != record.parent_id:
            root_author = await self.resolve_author_from_content(
                record.root_id, platform,
            )
            if root_author and root_author != author_uuid:
                await self.upsert_edge(
                    source=author_uuid, target=root_author,
                    edge_type="REPLIED_TO", platform=platform,
                )

        # 4. Process mentions from raw_payload
        raw = record.raw_payload or {}
        mentions = raw.get("mentions", [])
        for mention in mentions:
            mention_platform_key = f"{platform}:{mention}"
            mention_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, mention_platform_key))
            # Ensure mention target exists as a minimal node
            await self.neo4j.execute_write(
                """
                MERGE (a:Actor {platform_key: $pk})
                ON CREATE SET a.internal_uuid = $uuid, a.primary_platform = $plat,
                    a.username = $uname, a.first_seen = datetime(),
                    a.last_seen = datetime(), a.total_content_count = 0,
                    a.influence_score = 0.0, a.created_at = datetime()
                """,
                pk=mention_platform_key, uuid=mention_uuid,
                plat=platform, uname=mention,
            )
            await self.upsert_edge(
                source=author_uuid, target=mention_uuid,
                edge_type="MENTIONED", platform=platform,
            )
            await self.metrics.increment("network.edges.mentioned")

        # 5. Process forwards (Telegram)
        forwarded_from = raw.get("forwarded_from_channel_id")
        if forwarded_from:
            fwd_platform_key = f"telegram:{forwarded_from}"
            fwd_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, fwd_platform_key))
            await self.neo4j.execute_write(
                """
                MERGE (a:Actor {platform_key: $pk})
                ON CREATE SET a.internal_uuid = $uuid, a.primary_platform = 'telegram',
                    a.username = $uname, a.first_seen = datetime(),
                    a.last_seen = datetime(), a.total_content_count = 0,
                    a.influence_score = 0.0, a.created_at = datetime()
                """,
                pk=fwd_platform_key, uuid=fwd_uuid, uname=forwarded_from,
            )
            await self.upsert_edge(
                source=author_uuid, target=fwd_uuid,
                edge_type="FORWARDED_FROM", platform="telegram",
            )
            await self.metrics.increment("network.edges.forwarded")

        await self.metrics.increment("network.content_processed")

    async def run(self) -> None:
        """Consume from all raw topics and build the graph."""
        consumer = KafkaConsumerWrapper(
            *ALL_RAW_TOPICS, group_id="pymander-graph-builder",
        )
        await consumer.start()
        logger.info("graph_builder_started")

        try:
            async for msg in consumer.messages():
                if not self._running:
                    break
                try:
                    record = UnifiedContentRecord.model_validate(msg)
                    await self.process_content(record)
                except Exception as e:
                    logger.warning("graph_builder_error", error=str(e))
                    await self.metrics.increment("network.errors")
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
    neo4j = Neo4jClient()
    await neo4j.connect()
    await neo4j.setup_constraints()

    builder = GraphBuilder(neo4j, redis, metrics)
    try:
        await builder.run()
    except KeyboardInterrupt:
        builder.stop()
    finally:
        await neo4j.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
