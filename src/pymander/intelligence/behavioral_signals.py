"""Advanced behavioral signal detection — language shifts, migrations, engagement patterns."""

from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from datetime import datetime

import numpy as np
import structlog
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.producer import KafkaProducerWrapper
from pymander.ingestion.topics import ALERTS_BEHAVIORAL, ALERTS_MIGRATION
from pymander.network.neo4j_client import Neo4jClient
from pymander.schemas.coordination import BehavioralSignalAlert, MigrationEvent

logger = structlog.get_logger()

LANGUAGE_DRIFT_THRESHOLD = 0.15
LANGUAGE_HIGH_DRIFT = 0.25


class LanguageBaseline:
    """Baseline language profile for a community."""

    def __init__(self, community_id: str, centroid: np.ndarray | None,
                 top_ngrams: dict[str, int], avg_text_length: float,
                 vocab_richness: float) -> None:
        self.community_id = community_id
        self.centroid = centroid
        self.top_ngrams = top_ngrams
        self.avg_text_length = avg_text_length
        self.vocab_richness = vocab_richness


class LanguageShiftDetector:
    """Detects when a community begins adopting new terminology or rhetoric.

    Maintains rolling baseline language profiles per community and detects
    significant drift in embedding centroids, n-gram distributions, and
    vocabulary richness.
    """

    def __init__(self, neo4j: Neo4jClient, redis: Redis,
                 metrics: MetricsCollector,
                 producer: KafkaProducerWrapper | None = None) -> None:
        self.neo4j = neo4j
        self.redis = redis
        self.metrics = metrics
        self.producer = producer
        self._running = True

    @staticmethod
    def compute_ngrams(texts: list[str], n: int = 2, top_k: int = 100) -> dict[str, int]:
        """Compute top-k n-grams from a corpus of texts."""
        counter: Counter = Counter()
        for text in texts:
            words = text.lower().split()
            for i in range(len(words) - n + 1):
                gram = " ".join(words[i:i + n])
                counter[gram] += 1
        return dict(counter.most_common(top_k))

    @staticmethod
    def compute_type_token_ratio(texts: list[str]) -> float:
        """Compute vocabulary richness as type-token ratio."""
        all_words = []
        for text in texts:
            all_words.extend(text.lower().split())
        if not all_words:
            return 0.0
        return len(set(all_words)) / len(all_words)

    async def get_communities(self) -> list[dict]:
        """Get active communities from Redis cache."""
        raw = await self.redis.get("network:communities")
        if not raw:
            return []
        return json.loads(raw)

    async def get_community_texts(
        self, community_id: str, hours: int = 24,
    ) -> list[str]:
        """Get recent text content from a community's members via Redis cache."""
        cache_key = f"community:texts:{community_id}:{hours}h"
        cached = await self.redis.get(cache_key)
        if cached:
            return json.loads(cached)

        # Fallback: get from Neo4j actor list, then content cache
        actors = await self.neo4j.execute(
            """
            MATCH (a:Actor)
            WHERE a.community_id = $cid
            RETURN a.internal_uuid AS uuid
            LIMIT 200
            """,
            cid=community_id,
        )
        texts = []
        for actor in actors:
            text_key = f"actor:recent_texts:{actor['uuid']}"
            raw = await self.redis.lrange(text_key, 0, 49)
            for item in raw:
                t = item.decode() if isinstance(item, bytes) else item
                if t:
                    texts.append(t)
            if len(texts) >= 5000:
                break

        # Cache for 1 hour
        if texts:
            await self.redis.set(cache_key, json.dumps(texts[:5000]), ex=3600)
        return texts[:5000]

    async def get_or_compute_baseline(self, community_id: str) -> LanguageBaseline | None:
        """Get cached baseline or compute from 30-day data."""
        cache_key = f"language:baseline:{community_id}"
        cached = await self.redis.get(cache_key)
        if cached:
            data = json.loads(cached)
            return LanguageBaseline(
                community_id=community_id,
                centroid=np.array(data["centroid"]) if data.get("centroid") else None,
                top_ngrams=data.get("top_ngrams", {}),
                avg_text_length=data.get("avg_text_length", 0),
                vocab_richness=data.get("vocab_richness", 0),
            )
        return None

    async def store_baseline(self, baseline: LanguageBaseline) -> None:
        """Store computed baseline in Redis."""
        data = {
            "centroid": baseline.centroid.tolist() if baseline.centroid is not None else None,
            "top_ngrams": baseline.top_ngrams,
            "avg_text_length": baseline.avg_text_length,
            "vocab_richness": baseline.vocab_richness,
        }
        await self.redis.set(
            f"language:baseline:{baseline.community_id}",
            json.dumps(data), ex=604800,  # 7 days
        )

    async def compute_current_profile(self, community_id: str) -> LanguageBaseline | None:
        """Compute current 24-hour language profile."""
        texts = await self.get_community_texts(community_id, hours=24)
        if len(texts) < 50:
            return None
        return LanguageBaseline(
            community_id=community_id,
            centroid=None,  # Would need embeddings — use n-gram comparison instead
            top_ngrams=self.compute_ngrams(texts, n=2, top_k=100),
            avg_text_length=float(np.mean([len(t.split()) for t in texts])),
            vocab_richness=self.compute_type_token_ratio(texts),
        )

    async def detect_shifts(self) -> None:
        """Run shift detection across all communities."""
        communities = await self.get_communities()
        for community in communities:
            cid = str(community.get("community_id", ""))
            if not cid:
                continue

            baseline = await self.get_or_compute_baseline(cid)
            current = await self.compute_current_profile(cid)

            if not baseline or not current:
                if current:
                    await self.store_baseline(current)
                continue

            # N-gram drift: Jaccard distance
            base_ngrams = set(baseline.top_ngrams.keys())
            curr_ngrams = set(current.top_ngrams.keys())
            union = base_ngrams | curr_ngrams
            intersection = base_ngrams & curr_ngrams
            ngram_drift = 1.0 - (len(intersection) / len(union) if union else 1.0)

            new_ngrams = curr_ngrams - base_ngrams

            if ngram_drift > LANGUAGE_DRIFT_THRESHOLD:
                severity = "high" if ngram_drift > LANGUAGE_HIGH_DRIFT else "medium"
                alert = BehavioralSignalAlert(
                    type="language_shift",
                    community_id=cid,
                    severity=severity,
                    details={
                        "ngram_drift": round(ngram_drift, 4),
                        "new_ngrams": sorted(new_ngrams)[:20],
                        "vocab_richness_change": round(
                            current.vocab_richness - baseline.vocab_richness, 4,
                        ),
                        "avg_length_change": round(
                            current.avg_text_length - baseline.avg_text_length, 1,
                        ),
                        "member_count": community.get("member_count", 0),
                    },
                )
                alert_data = alert.model_dump(mode="json")
                await self.redis.set(
                    f"behavioral:alert:{alert.id}", json.dumps(alert_data), ex=604800,
                )
                await self.redis.zadd(
                    "behavioral:alerts:index", {str(alert.id): time.time()},
                )
                if self.producer:
                    await self.producer.send(ALERTS_BEHAVIORAL, alert_data)
                await self.metrics.increment("behavioral.language_shift.detected")
                logger.info(
                    "language_shift_detected",
                    community=cid, drift=ngram_drift,
                )

    async def run(self, interval_seconds: int = 21600) -> None:
        """Run every 6 hours."""
        logger.info("language_shift_detector_started")
        while self._running:
            try:
                await self.detect_shifts()
            except Exception as e:
                logger.error("language_shift_error", error=str(e))
            await asyncio.sleep(interval_seconds)

    def stop(self) -> None:
        self._running = False


class MigrationDetector:
    """Detects when narratives cross platform boundaries.

    Migration events reveal narrative power and coordination. Tracks which
    accounts serve as bridges between platforms.
    """

    def __init__(self, neo4j: Neo4jClient, redis: Redis,
                 metrics: MetricsCollector,
                 producer: KafkaProducerWrapper | None = None) -> None:
        self.neo4j = neo4j
        self.redis = redis
        self.metrics = metrics
        self.producer = producer
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
                    if status in ("emerging", "growing", "viral"):
                        narratives.append(data)
            if cursor == 0:
                break
        return narratives

    async def get_narrative_platform_timeline(
        self, narrative_id: str,
    ) -> dict[str, dict]:
        """Get first-seen time per platform for a narrative."""
        cache_key = f"narrative:platforms:{narrative_id}"
        cached = await self.redis.get(cache_key)
        if cached:
            return json.loads(cached)
        return {}

    async def update_platform_timeline(
        self, narrative_id: str, platform: str, timestamp: str,
    ) -> None:
        """Update the platform timeline for a narrative."""
        cache_key = f"narrative:platforms:{narrative_id}"
        timeline = await self.get_narrative_platform_timeline(narrative_id)
        if platform not in timeline:
            timeline[platform] = {"first_seen": timestamp, "content_count": 1}
        else:
            timeline[platform]["content_count"] = (
                timeline[platform].get("content_count", 0) + 1
            )
        await self.redis.set(cache_key, json.dumps(timeline), ex=604800)

    async def detect_migrations(self) -> None:
        """Check for new cross-platform migrations."""
        narratives = await self.get_active_narratives()

        for narrative in narratives:
            narrative_id = narrative.get("narrative_id", narrative.get("id", ""))
            if not narrative_id:
                continue

            timeline = await self.get_narrative_platform_timeline(narrative_id)
            known_key = f"narrative:known_platforms:{narrative_id}"
            known_raw = await self.redis.smembers(known_key)
            known_platforms = {
                p.decode() if isinstance(p, bytes) else p for p in known_raw
            }

            current_platforms = set(timeline.keys())
            new_platforms = current_platforms - known_platforms

            if not new_platforms:
                continue

            # Determine origin platform (earliest)
            sorted_platforms = sorted(
                timeline.items(),
                key=lambda x: x[1].get("first_seen", ""),
            )
            origin_platform = sorted_platforms[0][0] if sorted_platforms else "unknown"
            origin_time = sorted_platforms[0][1].get("first_seen", "")

            for new_platform in new_platforms:
                new_time = timeline[new_platform].get("first_seen", "")
                try:
                    origin_dt = datetime.fromisoformat(origin_time)
                    new_dt = datetime.fromisoformat(new_time)
                    migration_seconds = int((new_dt - origin_dt).total_seconds())
                except (ValueError, TypeError):
                    migration_seconds = 0

                migration = MigrationEvent(
                    narrative_id=narrative_id,
                    from_platform=origin_platform,
                    to_platform=new_platform,
                    migration_time_seconds=max(0, migration_seconds),
                )

                # Store migration event
                event_data = migration.model_dump(mode="json")
                await self.redis.lpush(
                    f"narrative:migrations:{narrative_id}",
                    json.dumps(event_data),
                )
                await self.redis.expire(
                    f"narrative:migrations:{narrative_id}", 2592000,
                )

                # Mark platform as known
                await self.redis.sadd(known_key, new_platform)
                await self.redis.expire(known_key, 2592000)

                # Publish alert
                if self.producer:
                    await self.producer.send(ALERTS_MIGRATION, {
                        "narrative_id": narrative_id,
                        "narrative_summary": narrative.get("summary", ""),
                        "from_platform": origin_platform,
                        "to_platform": new_platform,
                        "migration_time_seconds": migration_seconds,
                        "severity": "high",
                    })

                await self.metrics.increment(
                    "migrations.detected",
                    tags={"from": origin_platform, "to": new_platform},
                )
                logger.info(
                    "migration_detected",
                    narrative=narrative_id,
                    from_platform=origin_platform,
                    to_platform=new_platform,
                    seconds=migration_seconds,
                )

    async def run(self, interval_seconds: int = 1800) -> None:
        """Run every 30 minutes."""
        logger.info("migration_detector_started")
        while self._running:
            try:
                await self.detect_migrations()
            except Exception as e:
                logger.error("migration_detection_error", error=str(e))
            await asyncio.sleep(interval_seconds)

    def stop(self) -> None:
        self._running = False


class EngagementPatternDetector:
    """Detects when a community's engagement behavior changes.

    Tracks: comment length, reply ratio, repost ratio, engagement speed,
    thread depth, unique author ratio, peak hour concentration.
    """

    METRICS = [
        "avg_comment_length", "reply_ratio", "repost_ratio",
        "original_content_ratio", "unique_author_ratio",
        "peak_hour_concentration",
    ]

    def __init__(self, redis: Redis, metrics: MetricsCollector,
                 producer: KafkaProducerWrapper | None = None) -> None:
        self.redis = redis
        self.metrics = metrics
        self.producer = producer
        self._running = True

    async def compute_engagement_metrics(
        self, community_id: str,
    ) -> dict[str, float]:
        """Compute current engagement metrics for a community."""
        key = f"community:engagement_data:{community_id}"
        raw = await self.redis.get(key)
        if not raw:
            return {}

        data = json.loads(raw)
        posts = data.get("posts", [])
        if len(posts) < 20:
            return {}

        total = len(posts)
        replies = sum(1 for p in posts if p.get("is_reply"))
        reposts = sum(1 for p in posts if p.get("is_repost"))
        originals = total - replies - reposts
        unique_authors = len({p.get("author_id") for p in posts})
        comment_lengths = [
            len(p.get("text", "").split())
            for p in posts if p.get("text")
        ]
        hours = [p.get("hour", 12) for p in posts]
        hour_counts = Counter(hours)
        peak_count = max(hour_counts.values()) if hour_counts else 0

        return {
            "avg_comment_length": float(np.mean(comment_lengths)) if comment_lengths else 0,
            "reply_ratio": replies / total if total > 0 else 0,
            "repost_ratio": reposts / total if total > 0 else 0,
            "original_content_ratio": originals / total if total > 0 else 0,
            "unique_author_ratio": unique_authors / total if total > 0 else 0,
            "peak_hour_concentration": peak_count / total if total > 0 else 0,
        }

    async def detect_shifts(self) -> None:
        """Compare current vs baseline engagement patterns."""
        communities_raw = await self.redis.get("network:communities")
        if not communities_raw:
            return

        communities = json.loads(communities_raw)
        for community in communities:
            cid = str(community.get("community_id", ""))
            if not cid:
                continue

            current = await self.compute_engagement_metrics(cid)
            if not current:
                continue

            baseline_key = f"engagement:baseline:{cid}"
            baseline_raw = await self.redis.get(baseline_key)

            if not baseline_raw:
                # Store current as baseline
                await self.redis.set(
                    baseline_key, json.dumps(current), ex=2592000,
                )
                continue

            baseline = json.loads(baseline_raw)

            # Check each metric for significant change
            shifts = {}
            for metric in self.METRICS:
                base_val = baseline.get(metric, 0)
                curr_val = current.get(metric, 0)
                if base_val > 0:
                    change = abs(curr_val - base_val) / base_val
                    if change > 0.3:  # 30% change threshold
                        shifts[metric] = {
                            "baseline": round(base_val, 4),
                            "current": round(curr_val, 4),
                            "change_pct": round(change * 100, 1),
                        }

            if shifts:
                alert = BehavioralSignalAlert(
                    type="engagement_shift",
                    community_id=cid,
                    severity="high" if len(shifts) >= 3 else "medium",
                    details={
                        "shifts": shifts,
                        "member_count": community.get("member_count", 0),
                    },
                )
                alert_data = alert.model_dump(mode="json")
                await self.redis.set(
                    f"behavioral:alert:{alert.id}",
                    json.dumps(alert_data), ex=604800,
                )
                await self.redis.zadd(
                    "behavioral:alerts:index", {str(alert.id): time.time()},
                )
                if self.producer:
                    await self.producer.send(ALERTS_BEHAVIORAL, alert_data)
                await self.metrics.increment("behavioral.engagement_shift.detected")

    async def run(self, interval_seconds: int = 21600) -> None:
        """Run every 6 hours."""
        logger.info("engagement_pattern_detector_started")
        while self._running:
            try:
                await self.detect_shifts()
            except Exception as e:
                logger.error("engagement_shift_error", error=str(e))
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

    language = LanguageShiftDetector(neo4j, redis, metrics, producer)
    migration = MigrationDetector(neo4j, redis, metrics, producer)
    engagement = EngagementPatternDetector(redis, metrics, producer)

    try:
        await asyncio.gather(
            language.run(),
            migration.run(),
            engagement.run(),
        )
    except KeyboardInterrupt:
        language.stop()
        migration.stop()
        engagement.stop()
    finally:
        await producer.stop()
        await neo4j.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
