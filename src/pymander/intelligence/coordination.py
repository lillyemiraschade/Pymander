"""Coordination detection engine — real-time and batch analysis of inorganic amplification."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from collections import Counter
from datetime import UTC, datetime

import networkx as nx
import numpy as np
import structlog
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.consumer import KafkaConsumerWrapper
from pymander.ingestion.producer import KafkaProducerWrapper
from pymander.ingestion.topics import ALERTS_COORDINATION, RAW_REDDIT, RAW_RSS, RAW_TWITTER
from pymander.network.neo4j_client import Neo4jClient
from pymander.schemas.coordination import CoordinationCluster, CoordinationSignal
from pymander.schemas.enums import CoordinationSignalType

logger = structlog.get_logger()

TEMPORAL_BURST_THRESHOLD = 10  # unique authors in 2-min window
SEMANTIC_CLONE_THRESHOLD = 0.92
BOT_CV_THRESHOLD = 0.3
CONTENT_DIVERSITY_THRESHOLD = 0.3
CLUSTER_MIN_ACCOUNTS = 3
CLUSTER_CONFIDENCE_THRESHOLD = 0.5


class RealtimeCoordinationDetector:
    """Real-time stream detector for obvious coordination patterns.

    Runs on every piece of ingested content, maintaining sliding windows
    in Redis for fast pattern detection.
    """

    def __init__(self, redis: Redis, neo4j: Neo4jClient, metrics: MetricsCollector,
                 producer: KafkaProducerWrapper | None = None) -> None:
        self.redis = redis
        self.neo4j = neo4j
        self.metrics = metrics
        self.producer = producer
        self._running = True

    async def emit_signal(self, signal: CoordinationSignal) -> None:
        """Store signal in Redis and optionally publish to Kafka."""
        signal_data = signal.model_dump(mode="json")
        signal_key = f"coordination:signal:{signal.id}"
        await self.redis.set(signal_key, json.dumps(signal_data), ex=172800)  # 48h

        # Add to signal index
        await self.redis.zadd(
            "coordination:signals:index",
            {str(signal.id): time.time()},
        )
        await self.redis.expire("coordination:signals:index", 172800)

        if self.producer:
            await self.producer.send(
                ALERTS_COORDINATION, signal_data, key=str(signal.id),
            )
        await self.metrics.increment("coordination.signals.emitted")

    async def check_connection_density(self, accounts: list[str]) -> float:
        """Check how densely connected a set of accounts are in the graph."""
        if len(accounts) < 2:
            return 0.0
        max_edges = len(accounts) * (len(accounts) - 1) / 2
        result = await self.neo4j.execute(
            """
            UNWIND $accounts AS a_uuid
            UNWIND $accounts AS b_uuid
            WITH a_uuid, b_uuid WHERE a_uuid < b_uuid
            MATCH (a:Actor {internal_uuid: a_uuid})-[r]->(b:Actor {internal_uuid: b_uuid})
            RETURN count(r) AS edge_count
            """,
            accounts=accounts,
        )
        edge_count = result[0]["edge_count"] if result else 0
        return edge_count / max_edges if max_edges > 0 else 0.0

    async def check_temporal_burst(self, record_data: dict) -> None:
        """Check for burst of activity from unconnected accounts."""
        narrative_ids = record_data.get("narrative_ids", [])
        actor = record_data.get("actor", {})
        author_id = actor.get("platform_id", "")
        platform = record_data.get("platform", "")
        if not author_id:
            return

        for narrative_id in narrative_ids:
            key = f"coordination:temporal:{narrative_id}"
            now = time.time()
            author_key = f"{platform}:{author_id}"

            await self.redis.zadd(key, {author_key: now})
            await self.redis.expire(key, 300)

            # Count unique authors in last 2 minutes
            two_min_ago = now - 120
            recent = await self.redis.zrangebyscore(key, two_min_ago, now)
            unique_authors = set(r.decode() if isinstance(r, bytes) else r for r in recent)

            if len(unique_authors) >= TEMPORAL_BURST_THRESHOLD:
                density = await self.check_connection_density(
                    list(unique_authors)[:50],
                )
                if density < 0.1:
                    await self.emit_signal(CoordinationSignal(
                        type=CoordinationSignalType.TEMPORAL_BURST,
                        narrative_id=narrative_id,
                        accounts=list(unique_authors),
                        confidence=min(0.95, 0.5 + (len(unique_authors) - 10) * 0.05),
                        evidence={
                            "unique_authors_in_window": len(unique_authors),
                            "window_seconds": 120,
                            "connection_density": density,
                        },
                        platform=platform,
                    ))
                    await self.metrics.increment("coordination.temporal_burst.detected")

    async def check_bot_indicators(self, record_data: dict) -> None:
        """Check if the authoring account shows bot-like behavioral patterns."""
        actor = record_data.get("actor", {})
        author_id = actor.get("platform_id", "")
        platform = record_data.get("platform", "")
        if not author_id:
            return

        history_key = f"author:history:{platform}:{author_id}"
        history = await self.redis.lrange(history_key, 0, 99)

        # Update history
        created_at = record_data.get("created_at", datetime.now(UTC).isoformat())
        await self.redis.lpush(history_key, json.dumps({
            "timestamp": created_at,
            "content_type": record_data.get("content_type", ""),
            "narrative_ids": record_data.get("narrative_ids", []),
        }))
        await self.redis.ltrim(history_key, 0, 199)
        await self.redis.expire(history_key, 604800)

        if len(history) < 20:
            return

        # Parse timestamps
        timestamps = []
        narrative_counts: Counter = Counter()
        for h in history:
            data = json.loads(h)
            try:
                ts = datetime.fromisoformat(data["timestamp"])
                timestamps.append(ts)
            except (ValueError, KeyError):
                continue
            for nid in data.get("narrative_ids", []):
                narrative_counts[nid] += 1

        if len(timestamps) < 10:
            return

        timestamps.sort()
        intervals = [
            (timestamps[i + 1] - timestamps[i]).total_seconds()
            for i in range(len(timestamps) - 1)
        ]
        intervals_arr = np.array([x for x in intervals if x > 0])
        if len(intervals_arr) < 5:
            return

        mean_interval = float(np.mean(intervals_arr))
        std_interval = float(np.std(intervals_arr))
        cv = std_interval / mean_interval if mean_interval > 0 else 0

        # Posting regularity check
        if cv < BOT_CV_THRESHOLD and mean_interval < 3600:
            await self.emit_signal(CoordinationSignal(
                type=CoordinationSignalType.POSTING_REGULARITY,
                accounts=[f"{platform}:{author_id}"],
                confidence=max(0.0, min(1.0, 0.9 - cv * 2)),
                evidence={
                    "coefficient_of_variation": round(cv, 4),
                    "mean_interval_seconds": round(mean_interval, 1),
                    "sample_size": len(intervals_arr),
                },
                platform=platform,
            ))

        # Content diversity check
        if len(narrative_counts) >= 3:
            total = sum(narrative_counts.values())
            probs = [c / total for c in narrative_counts.values()]
            entropy = -sum(p * np.log2(p) for p in probs if p > 0)
            max_entropy = np.log2(len(narrative_counts))
            norm_entropy = entropy / max_entropy if max_entropy > 0 else 0

            if norm_entropy < CONTENT_DIVERSITY_THRESHOLD:
                await self.emit_signal(CoordinationSignal(
                    type=CoordinationSignalType.CONTENT_DIVERSITY_ANOMALY,
                    accounts=[f"{platform}:{author_id}"],
                    confidence=round(0.7 - norm_entropy, 3),
                    evidence={
                        "normalized_entropy": round(norm_entropy, 4),
                        "unique_narratives": len(narrative_counts),
                        "total_posts_analyzed": total,
                    },
                    platform=platform,
                ))

    async def process_record(self, record_data: dict) -> None:
        """Process one content record for coordination signals."""
        try:
            await self.check_temporal_burst(record_data)
            await self.check_bot_indicators(record_data)
        except Exception as e:
            logger.warning("coordination_check_error", error=str(e))
            await self.metrics.increment("coordination.realtime.errors")

    async def run(self) -> None:
        """Consume from raw topics and check for coordination."""
        consumer = KafkaConsumerWrapper(
            RAW_REDDIT, RAW_RSS, RAW_TWITTER,
            group_id="pymander-coordination-realtime",
        )
        await consumer.start()
        logger.info("realtime_coordination_detector_started")

        try:
            async for msg in consumer.messages():
                if not self._running:
                    break
                await self.process_record(msg)
        finally:
            await consumer.stop()

    def stop(self) -> None:
        self._running = False


class BatchCoordinationAnalyzer:
    """Batch analysis for subtle coordination patterns. Runs every 2 hours.

    Performs deep analysis: synchronized activation, amplification chains,
    star topology, fresh account swarms, cross-signal aggregation.
    """

    def __init__(self, neo4j: Neo4jClient, redis: Redis,
                 metrics: MetricsCollector,
                 producer: KafkaProducerWrapper | None = None) -> None:
        self.neo4j = neo4j
        self.redis = redis
        self.metrics = metrics
        self.producer = producer
        self._running = True

    async def get_recent_signals(self, hours: int = 48) -> list[CoordinationSignal]:
        """Fetch all coordination signals from the last N hours."""
        cutoff = time.time() - (hours * 3600)
        signal_ids = await self.redis.zrangebyscore(
            "coordination:signals:index", cutoff, "+inf",
        )
        signals = []
        for sid in signal_ids:
            sid_str = sid.decode() if isinstance(sid, bytes) else sid
            raw = await self.redis.get(f"coordination:signal:{sid_str}")
            if raw:
                data = json.loads(raw)
                signals.append(CoordinationSignal.model_validate(data))
        return signals

    async def detect_fresh_account_swarm(self) -> None:
        """Detect clusters of newly-created accounts engaging with same narrative."""
        results = await self.neo4j.execute("""
            MATCH (a:Actor)
            WHERE a.first_seen > datetime() - duration('P7D')
              AND a.total_content_count >= 3
            WITH a, a.narratives_participated AS narrs
            UNWIND narrs AS narr_id
            WITH narr_id, collect(a.internal_uuid) AS fresh_accounts
            WHERE size(fresh_accounts) >= 5
            RETURN narr_id, fresh_accounts
        """)
        for row in results:
            narr_id = row["narr_id"]
            accounts = row["fresh_accounts"]
            signal = CoordinationSignal(
                type=CoordinationSignalType.FRESH_ACCOUNT_SWARM,
                narrative_id=narr_id,
                accounts=accounts,
                confidence=min(0.90, 0.5 + (len(accounts) - 5) * 0.05),
                evidence={
                    "fresh_account_count": len(accounts),
                    "narrative_id": narr_id,
                    "account_age_days": 7,
                },
            )
            signal_data = signal.model_dump(mode="json")
            await self.redis.set(
                f"coordination:signal:{signal.id}",
                json.dumps(signal_data), ex=172800,
            )
            await self.redis.zadd(
                "coordination:signals:index",
                {str(signal.id): time.time()},
            )
            await self.metrics.increment("coordination.fresh_swarm.detected")

    async def detect_star_topology(self) -> None:
        """Detect hub-and-spoke amplification networks."""
        results = await self.neo4j.execute("""
            MATCH (hub:Actor)<-[r:REPOSTED]-(leaf:Actor)
            WITH hub, collect(DISTINCT leaf) AS leaves, count(r) AS repost_count
            WHERE size(leaves) >= 10 AND repost_count >= 20
            WITH hub, leaves, repost_count
            // Check that leaves don't interact with each other
            OPTIONAL MATCH (l1)-[lr]-(l2)
            WHERE l1 IN leaves AND l2 IN leaves AND l1 <> l2
            WITH hub, leaves, repost_count, count(lr) AS leaf_interconnections
            WHERE leaf_interconnections < size(leaves)
            RETURN hub.internal_uuid AS hub_uuid,
                   [l IN leaves | l.internal_uuid] AS leaf_uuids,
                   repost_count, leaf_interconnections
            LIMIT 50
        """)
        for row in results:
            all_accounts = [row["hub_uuid"]] + row["leaf_uuids"]
            leaf_count = len(row["leaf_uuids"])
            max_leaf_edges = leaf_count * (leaf_count - 1) / 2
            connectivity = (
                row["leaf_interconnections"] / max_leaf_edges
                if max_leaf_edges > 0 else 0
            )
            if connectivity < 0.1:
                signal = CoordinationSignal(
                    type=CoordinationSignalType.STAR_TOPOLOGY,
                    accounts=all_accounts,
                    confidence=min(0.90, 0.6 + (leaf_count - 10) * 0.02),
                    evidence={
                        "hub_uuid": row["hub_uuid"],
                        "leaf_count": leaf_count,
                        "repost_count": row["repost_count"],
                        "leaf_connectivity": round(connectivity, 4),
                    },
                )
                signal_data = signal.model_dump(mode="json")
                await self.redis.set(
                    f"coordination:signal:{signal.id}",
                    json.dumps(signal_data), ex=172800,
                )
                await self.redis.zadd(
                    "coordination:signals:index",
                    {str(signal.id): time.time()},
                )
                await self.metrics.increment("coordination.star_topology.detected")

    async def estimate_cluster_reach(self, accounts: set[str]) -> int:
        """Estimate total follower reach of cluster accounts."""
        result = await self.neo4j.execute(
            """
            UNWIND $accounts AS uuid
            MATCH (a:Actor {internal_uuid: uuid})
            RETURN sum(coalesce(a.follower_count, 0)) AS total_reach
            """,
            accounts=list(accounts),
        )
        return result[0]["total_reach"] if result else 0

    async def aggregate_into_clusters(self) -> list[CoordinationCluster]:
        """Combine individual signals into coordination clusters."""
        signals = await self.get_recent_signals(hours=48)
        if len(signals) < 2:
            return []

        # Build co-occurrence graph
        graph = nx.Graph()
        for signal in signals:
            for i, acct_a in enumerate(signal.accounts):
                for acct_b in signal.accounts[i + 1:]:
                    if graph.has_edge(acct_a, acct_b):
                        graph[acct_a][acct_b]["signals"].append(signal)
                    else:
                        graph.add_edge(acct_a, acct_b, signals=[signal])

        clusters = []
        for component in nx.connected_components(graph):
            if len(component) < CLUSTER_MIN_ACCOUNTS:
                continue

            # Collect all signals for this cluster
            cluster_signals: set[CoordinationSignal] = set()
            for acct_a, acct_b in graph.subgraph(component).edges():
                for s in graph[acct_a][acct_b]["signals"]:
                    cluster_signals.add(s)

            # Aggregate confidence: 1 - product(1 - c)
            confidences = [s.confidence for s in cluster_signals]
            aggregate_confidence = 1.0 - float(
                np.prod([1.0 - c for c in confidences])
            )

            if aggregate_confidence < CLUSTER_CONFIDENCE_THRESHOLD:
                continue

            # Check connection density — reduce confidence if organically connected
            density = 0.0
            try:
                density_result = await self.neo4j.execute(
                    """
                    UNWIND $accounts AS a_uuid
                    UNWIND $accounts AS b_uuid
                    WITH a_uuid, b_uuid WHERE a_uuid < b_uuid
                    MATCH (a:Actor {internal_uuid: a_uuid})-[r]->(b:Actor {internal_uuid: b_uuid})
                    WHERE type(r) IN ['REPLIED_TO', 'MENTIONED']
                    RETURN count(r) AS organic_edges
                    """,
                    accounts=list(component)[:50],
                )
                max_edges = len(component) * (len(component) - 1) / 2
                organic = density_result[0]["organic_edges"] if density_result else 0
                density = organic / max_edges if max_edges > 0 else 0
            except Exception:
                pass

            # Reduce confidence by 50% if accounts have organic prior history
            if density > 0.2:
                aggregate_confidence *= 0.5

            if aggregate_confidence < CLUSTER_CONFIDENCE_THRESHOLD:
                continue

            signal_types = list({s.type for s in cluster_signals})
            reach = await self.estimate_cluster_reach(component)

            from pymander.schemas.enums import AlertSeverity
            severity = AlertSeverity.CRITICAL if aggregate_confidence > 0.9 else (
                AlertSeverity.HIGH if aggregate_confidence > 0.75 else AlertSeverity.MEDIUM
            )

            cluster = CoordinationCluster(
                cluster_id=str(uuid.uuid4()),
                accounts=list(component),
                account_count=len(component),
                confidence=round(aggregate_confidence, 4),
                signal_types=signal_types,
                signal_count=len(cluster_signals),
                signals=[s.model_dump(mode="json") for s in cluster_signals],
                associated_narratives=list({
                    s.narrative_id for s in cluster_signals if s.narrative_id
                }),
                first_detected=min(s.detected_at for s in cluster_signals),
                last_signal=max(s.detected_at for s in cluster_signals),
                estimated_reach=reach,
                severity=severity,
            )
            clusters.append(cluster)

            # Store cluster
            cluster_data = cluster.model_dump(mode="json")
            await self.redis.set(
                f"coordination:cluster:{cluster.cluster_id}",
                json.dumps(cluster_data), ex=604800,
            )
            await self.redis.sadd("coordination:clusters:active", cluster.cluster_id)
            await self.redis.expire("coordination:clusters:active", 604800)

            # Tag accounts in Neo4j
            for account in component:
                with contextlib.suppress(Exception):
                    await self.neo4j.execute_write(
                        """
                        MATCH (a:Actor {internal_uuid: $uuid})
                        SET a.coordination_cluster_id = $cid,
                            a.updated_at = datetime()
                        """,
                        uuid=account, cid=cluster.cluster_id,
                    )

            # Publish alert for high-confidence clusters
            if self.producer and aggregate_confidence > 0.75:
                await self.producer.send(ALERTS_COORDINATION, {
                    "cluster_id": cluster.cluster_id,
                    "confidence": aggregate_confidence,
                    "account_count": len(component),
                    "signal_types": [str(t) for t in signal_types],
                    "narratives": cluster.associated_narratives,
                    "severity": str(severity),
                })

            await self.metrics.increment("coordination.clusters.detected")

        return clusters

    async def run_once(self) -> None:
        """Single batch analysis pass."""
        logger.info("batch_coordination_analysis_starting")
        await self.detect_fresh_account_swarm()
        await self.detect_star_topology()
        clusters = await self.aggregate_into_clusters()
        logger.info("batch_coordination_complete", clusters=len(clusters))

    async def run(self, interval_seconds: int = 7200) -> None:
        """Run batch coordination analysis on schedule."""
        logger.info("batch_coordination_analyzer_started", interval=interval_seconds)
        while self._running:
            try:
                await self.run_once()
            except Exception as e:
                logger.error("batch_coordination_error", error=str(e))
                await self.metrics.increment("coordination.batch.errors")
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
    producer = KafkaProducerWrapper()
    await producer.start()

    # Run both detectors concurrently
    realtime = RealtimeCoordinationDetector(redis, neo4j, metrics, producer)
    batch = BatchCoordinationAnalyzer(neo4j, redis, metrics, producer)

    try:
        await asyncio.gather(
            realtime.run(),
            batch.run(),
        )
    except KeyboardInterrupt:
        realtime.stop()
        batch.stop()
    finally:
        await producer.stop()
        await neo4j.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
