"""Co-narrative edge builder — hourly batch job linking actors in shared narratives."""

from __future__ import annotations

import asyncio
import itertools
import json

import structlog
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.network.neo4j_client import Neo4jClient

logger = structlog.get_logger()


class CoNarrativeEdgeBuilder:
    """Runs hourly. For each active narrative, creates CO_NARRATIVE edges
    between the top participants.

    This reveals which accounts consistently appear in the same narratives
    — a strong behavioral coordination signal.
    """

    MAX_AUTHORS_PER_NARRATIVE = 100

    def __init__(self, neo4j: Neo4jClient, redis: Redis,
                 metrics: MetricsCollector) -> None:
        self.neo4j = neo4j
        self.redis = redis
        self.metrics = metrics
        self._running = True

    async def get_active_narratives(self) -> list[dict]:
        """Get active narratives from Redis."""
        narratives = []
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(
                cursor, match="narrative:validated:*", count=100,
            )
            for key in keys:
                raw = await self.redis.get(key)
                if raw:
                    data = json.loads(raw)
                    status = data.get("status", "")
                    if status in ("emerging", "growing", "viral", "peaking"):
                        narratives.append(data)
            if cursor == 0:
                break
        return narratives

    async def get_narrative_authors(self, narrative_id: str) -> list[str]:
        """Get top authors for a narrative from Neo4j by content count."""
        results = await self.neo4j.execute(
            """
            MATCH (a:Actor)
            WHERE $narrative_id IN coalesce(a.narratives_participated, [])
            RETURN a.internal_uuid AS uuid
            ORDER BY a.total_content_count DESC
            LIMIT $limit
            """,
            narrative_id=narrative_id,
            limit=self.MAX_AUTHORS_PER_NARRATIVE,
        )
        return [r["uuid"] for r in results]

    async def build_co_narrative_edges(self, narrative_id: str,
                                       authors: list[str]) -> int:
        """Create CO_NARRATIVE edges between all pairs of authors."""
        if len(authors) < 2:
            return 0

        pairs = list(itertools.combinations(authors, 2))
        edge_count = 0

        # Batch in groups of 100 pairs
        for i in range(0, len(pairs), 100):
            batch = pairs[i:i + 100]
            queries = []
            for author_a, author_b in batch:
                query = """
                MATCH (a:Actor {internal_uuid: $source})
                MATCH (b:Actor {internal_uuid: $target})
                MERGE (a)-[r:CO_NARRATIVE]->(b)
                ON CREATE SET
                    r.weight = 1.0,
                    r.interaction_count = 1,
                    r.first_interaction = datetime(),
                    r.last_interaction = datetime(),
                    r.platforms = ['cross_platform'],
                    r.shared_narrative_ids = [$narrative_id],
                    r.coordination_score = 0.0
                ON MATCH SET
                    r.weight = r.weight + 0.1,
                    r.interaction_count = r.interaction_count + 1,
                    r.last_interaction = datetime(),
                    r.shared_narrative_ids = CASE
                        WHEN NOT $narrative_id IN coalesce(
                            r.shared_narrative_ids, [])
                        THEN coalesce(r.shared_narrative_ids, []) +
                            $narrative_id
                        ELSE r.shared_narrative_ids END
                """
                queries.append((query, {
                    "source": author_a, "target": author_b,
                    "narrative_id": narrative_id,
                }))

            try:
                await self.neo4j.execute_batch(queries)
                edge_count += len(batch)
            except Exception as e:
                logger.warning("co_narrative_batch_error", error=str(e))

        return edge_count

    async def run_once(self) -> None:
        """Single pass: build co-narrative edges for all active narratives."""
        narratives = await self.get_active_narratives()
        logger.info("co_narrative_start", narrative_count=len(narratives))

        total_edges = 0
        for narrative in narratives:
            narrative_id = narrative.get("narrative_id", narrative.get("id", ""))
            if not narrative_id:
                continue

            authors = await self.get_narrative_authors(narrative_id)
            if len(authors) >= 2:
                edges = await self.build_co_narrative_edges(
                    narrative_id, authors,
                )
                total_edges += edges

        await self.metrics.increment(
            "network.co_narrative_edges", value=total_edges,
        )
        logger.info("co_narrative_complete", total_edges=total_edges)

    async def run(self, interval_seconds: int = 3600) -> None:
        """Run co-narrative edge builder on a schedule."""
        logger.info("co_narrative_builder_started", interval=interval_seconds)
        while self._running:
            try:
                await self.run_once()
            except Exception as e:
                logger.error("co_narrative_error", error=str(e))
                await self.metrics.increment("network.co_narrative.errors")
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

    builder = CoNarrativeEdgeBuilder(neo4j, redis, metrics)
    try:
        await builder.run()
    except KeyboardInterrupt:
        builder.stop()
    finally:
        await neo4j.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
