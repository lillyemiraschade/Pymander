"""Graph analytics pipeline — daily computation of network-level scores."""

from __future__ import annotations

import asyncio
import contextlib
import json

import structlog
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.network.neo4j_client import Neo4jClient

logger = structlog.get_logger()


class GraphAnalytics:
    """Daily computation of network-level scores using Neo4j GDS.

    Computes: PageRank, betweenness centrality, Louvain community detection,
    composite influence scoring, bridge node identification.
    """

    def __init__(self, neo4j: Neo4jClient, redis: Redis,
                 metrics: MetricsCollector) -> None:
        self.neo4j = neo4j
        self.redis = redis
        self.metrics = metrics
        self._running = True

    async def _ensure_graph_projection(self) -> None:
        """Create or replace the GDS graph projection."""
        with contextlib.suppress(Exception):
            await self.neo4j.execute("CALL gds.graph.drop('pymander_graph', false)")
        await self.neo4j.execute("""
            CALL gds.graph.project(
                'pymander_graph',
                'Actor',
                {
                    REPLIED_TO: {orientation: 'NATURAL', properties: ['weight']},
                    REPOSTED: {orientation: 'NATURAL', properties: ['weight']},
                    QUOTED: {orientation: 'NATURAL', properties: ['weight']},
                    MENTIONED: {orientation: 'NATURAL', properties: ['weight']},
                    CO_NARRATIVE: {orientation: 'UNDIRECTED', properties: ['weight']},
                    FORWARDED_FROM: {orientation: 'NATURAL', properties: ['weight']}
                },
                {nodeProperties: ['influence_score', 'total_content_count']}
            )
        """)
        logger.info("gds_graph_projected")

    async def compute_pagerank(self) -> None:
        """Run PageRank to compute global influence ranking."""
        await self.neo4j.execute("""
            CALL gds.pageRank.write('pymander_graph', {
                writeProperty: 'pagerank_score',
                maxIterations: 50,
                dampingFactor: 0.85,
                relationshipWeightProperty: 'weight'
            })
        """)
        await self.metrics.increment("network.analytics.pagerank_completed")
        logger.info("pagerank_computed")

    async def compute_betweenness(self) -> None:
        """Run betweenness centrality for bridge node detection."""
        await self.neo4j.execute("""
            CALL gds.betweenness.write('pymander_graph', {
                writeProperty: 'betweenness_score',
                samplingSize: 5000
            })
        """)
        await self.metrics.increment("network.analytics.betweenness_completed")
        logger.info("betweenness_computed")

    async def detect_communities(self) -> int:
        """Run Louvain community detection."""
        result = await self.neo4j.execute("""
            CALL gds.louvain.write('pymander_graph', {
                writeProperty: 'community_id',
                relationshipWeightProperty: 'weight',
                maxLevels: 10,
                maxIterations: 20
            })
            YIELD communityCount
            RETURN communityCount
        """)
        count = result[0]["communityCount"] if result else 0
        await self.metrics.gauge("network.communities.detected", count)
        logger.info("communities_detected", count=count)
        return count

    async def compute_influence_scores(self) -> None:
        """Compute composite influence score from multiple signals."""
        await self.neo4j.execute("""
            MATCH (a:Actor)
            WHERE a.pagerank_score IS NOT NULL
            WITH a,
                 coalesce(a.pagerank_score, 0) AS pr,
                 coalesce(a.betweenness_score, 0) AS bt,
                 coalesce(a.amplification_score, 0) AS amp,
                 SIZE(coalesce(a.narratives_participated, [])) AS narr_count,
                 a.total_content_count AS content_count
            WITH a, pr, bt, amp, narr_count,
                 CASE WHEN pr > 0.01 THEN 1.0 ELSE pr / 0.01 END AS norm_pr,
                 CASE WHEN bt > 10000 THEN 1.0 ELSE bt / 10000.0 END AS norm_bt,
                 CASE WHEN amp > 1000 THEN 1.0 ELSE amp / 1000.0 END AS norm_amp,
                 CASE WHEN narr_count > 20 THEN 1.0
                     ELSE narr_count / 20.0 END AS norm_narr
            SET a.influence_score = (
                0.30 * norm_pr +
                0.25 * norm_amp +
                0.25 * norm_bt +
                0.20 * norm_narr
            ),
            a.bridge_score = norm_bt,
            a.updated_at = datetime()
        """)
        logger.info("influence_scores_computed")

    async def cache_top_bridges(self) -> None:
        """Store top bridge nodes in Redis for fast API access."""
        results = await self.neo4j.execute("""
            MATCH (a:Actor)
            WHERE a.bridge_score > 0.3
            RETURN a.internal_uuid AS uuid, a.username AS username,
                   a.primary_platform AS platform, a.bridge_score AS bridge_score,
                   a.influence_score AS influence_score,
                   a.community_id AS community_id
            ORDER BY a.bridge_score DESC
            LIMIT 1000
        """)
        await self.redis.set(
            "network:top_bridges", json.dumps(results), ex=86400,
        )
        logger.info("top_bridges_cached", count=len(results))

    async def cache_community_summaries(self) -> None:
        """Store community summaries in Redis."""
        results = await self.neo4j.execute("""
            MATCH (a:Actor)
            WHERE a.community_id IS NOT NULL
            WITH a.community_id AS cid, collect(a) AS members
            RETURN cid AS community_id,
                   SIZE(members) AS member_count,
                   [m IN members | m.primary_platform][..5] AS sample_platforms,
                   avg([m IN members | m.influence_score]) AS avg_influence
            ORDER BY SIZE(members) DESC
            LIMIT 500
        """)
        await self.redis.set(
            "network:communities", json.dumps(results), ex=86400,
        )
        logger.info("community_summaries_cached", count=len(results))

    async def cache_network_stats(self) -> None:
        """Cache overall network statistics."""
        stats = await self.neo4j.execute("""
            MATCH (a:Actor)
            WITH count(a) AS node_count
            OPTIONAL MATCH ()-[r]->()
            WITH node_count, count(r) AS edge_count
            RETURN node_count, edge_count
        """)
        if stats:
            await self.redis.set("network:stats", json.dumps(stats[0]), ex=86400)

    async def run_daily_analytics(self) -> None:
        """Full daily analytics pass."""
        logger.info("daily_analytics_starting")

        try:
            await self._ensure_graph_projection()
            await self.compute_pagerank()
            await self.compute_betweenness()
            await self.detect_communities()
            await self.compute_influence_scores()
            await self.cache_top_bridges()
            await self.cache_community_summaries()
            await self.cache_network_stats()
        except Exception as e:
            logger.error("daily_analytics_error", error=str(e))
            await self.metrics.increment("network.analytics.errors")
            raise
        finally:
            # Clean up projection
            with contextlib.suppress(Exception):
                await self.neo4j.execute(
                    "CALL gds.graph.drop('pymander_graph', false)"
                )

        await self.metrics.increment("network.daily_analytics.completed")
        logger.info("daily_analytics_complete")

    async def run(self, interval_seconds: int = 86400) -> None:
        """Run analytics on a daily schedule."""
        logger.info("graph_analytics_started")
        while self._running:
            try:
                await self.run_daily_analytics()
            except Exception as e:
                logger.error("graph_analytics_cycle_error", error=str(e))
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

    analytics = GraphAnalytics(neo4j, redis, metrics)
    try:
        await analytics.run()
    except KeyboardInterrupt:
        analytics.stop()
    finally:
        await neo4j.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
