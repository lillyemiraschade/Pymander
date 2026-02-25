"""Historical pattern matching and heuristic prediction engine."""

from __future__ import annotations

import asyncio
import json

import numpy as np
import structlog
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.schemas.briefing import PatternMatch, Prediction

logger = structlog.get_logger()

# Similarity weights for pattern matching
WEIGHTS = {
    "semantic": 0.30,
    "velocity": 0.25,
    "platform": 0.20,
    "network": 0.15,
    "origin": 0.10,
}
MIN_HISTORICAL_NARRATIVES = 20
MATCH_THRESHOLD = 0.50


class PatternMatcher:
    """Find historically similar narratives and surface their outcomes."""

    def __init__(self, redis: Redis, metrics: MetricsCollector) -> None:
        self.redis = redis
        self.metrics = metrics

    async def get_historical_narratives(self) -> list[dict]:
        """Get completed (dead/dormant) narratives for pattern matching."""
        narratives = []
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(
                cursor, match="narrative:validated:*", count=200,
            )
            for key in keys:
                raw = await self.redis.get(key)
                if raw:
                    data = json.loads(raw)
                    if data.get("status") in ("dead", "dormant", "declining"):
                        narratives.append(data)
            if cursor == 0:
                break
        return narratives

    @staticmethod
    def _velocity_similarity(velocity_a: list[float], velocity_b: list[float]) -> float:
        """Compute similarity between two velocity curves using normalized correlation."""
        if not velocity_a or not velocity_b:
            return 0.0
        # Resample to same length
        target_len = 48  # 48 data points
        a = np.interp(
            np.linspace(0, 1, target_len),
            np.linspace(0, 1, len(velocity_a)),
            velocity_a,
        )
        b = np.interp(
            np.linspace(0, 1, target_len),
            np.linspace(0, 1, len(velocity_b)),
            velocity_b,
        )
        # Normalize
        a_norm = (a - a.mean()) / (a.std() + 1e-8)
        b_norm = (b - b.mean()) / (b.std() + 1e-8)
        # Pearson correlation
        correlation = float(np.corrcoef(a_norm, b_norm)[0, 1])
        return max(0.0, (correlation + 1.0) / 2.0)  # Map [-1, 1] to [0, 1]

    @staticmethod
    def _platform_similarity(platforms_a: list[str], platforms_b: list[str]) -> float:
        """Jaccard similarity of platform distributions."""
        if not platforms_a or not platforms_b:
            return 0.0
        set_a = set(platforms_a)
        set_b = set(platforms_b)
        intersection = set_a & set_b
        union = set_a | set_b
        return len(intersection) / len(union) if union else 0.0

    @staticmethod
    def _origin_similarity(origin_a: str, origin_b: str) -> float:
        """Check if narratives originated on same platform."""
        if not origin_a or not origin_b:
            return 0.0
        return 1.0 if origin_a == origin_b else 0.0

    async def compute_similarity(
        self, current: dict, historical: dict,
    ) -> float:
        """Compute composite similarity score."""
        scores = {}

        # Semantic similarity — use category and keywords overlap
        curr_keywords = set(current.get("keywords", []))
        hist_keywords = set(historical.get("keywords", []))
        if curr_keywords and hist_keywords:
            union = curr_keywords | hist_keywords
            intersection = curr_keywords & hist_keywords
            scores["semantic"] = len(intersection) / len(union) if union else 0
        else:
            same_cat = current.get("category") == historical.get("category")
            scores["semantic"] = 0.5 if same_cat else 0

        # Velocity profile similarity
        curr_velocity = current.get("velocity_curve", [])
        hist_velocity = historical.get("velocity_curve", [])
        scores["velocity"] = self._velocity_similarity(curr_velocity, hist_velocity)

        # Platform distribution
        curr_platforms = current.get("platforms", [])
        hist_platforms = historical.get("platforms", [])
        scores["platform"] = self._platform_similarity(curr_platforms, hist_platforms)

        # Network structure (simplified — use coordination score similarity)
        curr_coord = current.get("coordination_score", 0)
        hist_coord = historical.get("coordination_score", 0)
        scores["network"] = 1.0 - abs(curr_coord - hist_coord)

        # Origin platform
        curr_origin = current.get("origin_platform", "")
        hist_origin = historical.get("origin_platform", "")
        scores["origin"] = self._origin_similarity(curr_origin, hist_origin)

        composite = sum(
            WEIGHTS[dim] * scores.get(dim, 0) for dim in WEIGHTS
        )
        return composite

    async def find_matches(
        self, narrative_id: str, top_k: int = 5,
    ) -> list[PatternMatch]:
        """Find historically similar narratives for a given narrative."""
        # Get current narrative
        raw = await self.redis.get(f"narrative:validated:{narrative_id}")
        if not raw:
            return []
        current = json.loads(raw)

        # Get historical narratives
        historical = await self.get_historical_narratives()
        if len(historical) < MIN_HISTORICAL_NARRATIVES:
            return []

        matches = []
        for hist in historical:
            hist_id = hist.get("narrative_id", hist.get("id", ""))
            if hist_id == narrative_id:
                continue
            score = await self.compute_similarity(current, hist)
            if score > MATCH_THRESHOLD:
                matches.append(PatternMatch(
                    matched_narrative_id=hist_id,
                    matched_narrative_summary=hist.get("summary", ""),
                    similarity_score=round(score, 4),
                    outcome_summary=hist.get("outcome", ""),
                    matched_lifecycle={
                        "total_duration_hours": hist.get("duration_hours", 0),
                        "peak_velocity": hist.get("peak_velocity", 0),
                        "platforms_reached": hist.get("platforms", []),
                        "coordination_detected": hist.get("coordination_score", 0) > 0.5,
                        "final_status": hist.get("status", ""),
                    },
                    similarity_dimensions={
                        dim: round(score, 3) for dim, score in {}.items()
                    },
                ))

        matches.sort(key=lambda m: m.similarity_score, reverse=True)
        return matches[:top_k]


class PredictionEngine:
    """Heuristic prediction engine based on historical pattern matching.

    v1 predictions are NOT machine learning — they are pattern-based heuristics.
    All predictions include confidence score, basis, and caveats.
    """

    def __init__(self, matcher: PatternMatcher, redis: Redis,
                 metrics: MetricsCollector) -> None:
        self.matcher = matcher
        self.redis = redis
        self.metrics = metrics

    async def predict_peak_timing(
        self, narrative_id: str, matches: list[PatternMatch],
    ) -> Prediction | None:
        """Predict when a narrative will peak based on historical patterns."""
        if not matches:
            return None

        peak_times = [
            m.matched_lifecycle.get("total_duration_hours", 0) * 0.4
            for m in matches
            if m.matched_lifecycle.get("total_duration_hours", 0) > 0
        ]
        if not peak_times:
            return None

        avg_peak = float(np.mean(peak_times))
        std_peak = float(np.std(peak_times))
        confidence = min(0.85, 0.4 + (len(matches) * 0.1))

        return Prediction(
            narrative_id=narrative_id,
            prediction_type="peak_timing",
            description=(
                f"Based on {len(matches)} similar historical narratives, "
                f"this narrative will likely peak within {avg_peak:.0f} "
                f"(+/- {std_peak:.0f}) hours from first detection."
            ),
            confidence=round(confidence, 3),
            basis=matches[:3],
            caveats=[
                "Peak timing varies significantly based on platform reach and coordination",
                "External events (news cycles, weekend/weekday) can shift timing",
                f"Estimate based on {len(matches)} historical matches"
                " (more data improves accuracy)",
            ],
            predicted_timeframe_hours=avg_peak,
        )

    async def predict_platform_migration(
        self, narrative_id: str, matches: list[PatternMatch],
    ) -> Prediction | None:
        """Predict which platforms a narrative will reach."""
        if not matches:
            return None

        # Get current narrative data
        raw = await self.redis.get(f"narrative:validated:{narrative_id}")
        if not raw:
            return None
        current = json.loads(raw)
        current_platforms = set(current.get("platforms", []))

        # Count platforms from historical matches not yet reached
        platform_counts: dict[str, int] = {}
        for m in matches:
            platforms = m.matched_lifecycle.get("platforms_reached", [])
            for p in platforms:
                if p not in current_platforms:
                    platform_counts[p] = platform_counts.get(p, 0) + 1

        if not platform_counts:
            return None

        likely_platforms = sorted(
            platform_counts.items(), key=lambda x: x[1], reverse=True,
        )
        top_platform = likely_platforms[0]
        probability = top_platform[1] / len(matches)

        if probability < 0.3:
            return None

        return Prediction(
            narrative_id=narrative_id,
            prediction_type="platform_migration",
            description=(
                f"This narrative has a {probability:.0%} probability of reaching "
                f"{top_platform[0]} based on {top_platform[1]}/{len(matches)} "
                f"similar historical narratives."
            ),
            confidence=round(min(0.85, probability), 3),
            basis=matches[:3],
            caveats=[
                "Platform migration depends on bridge accounts and content shareability",
                "Not all similar narratives follow identical migration paths",
                "Platform-specific policies and trending algorithms affect migration",
            ],
        )

    async def predict_coordination_risk(
        self, narrative_id: str, matches: list[PatternMatch],
    ) -> Prediction | None:
        """Predict probability of coordinated amplification."""
        if not matches:
            return None

        coordinated_count = sum(
            1 for m in matches
            if m.matched_lifecycle.get("coordination_detected", False)
        )
        probability = coordinated_count / len(matches) if matches else 0

        # Get current coordination score
        raw = await self.redis.get(f"narrative:validated:{narrative_id}")
        if raw:
            current = json.loads(raw)
            current_coord = current.get("coordination_score", 0)
            # Combine current signals with historical probability
            probability = probability * 0.6 + current_coord * 0.4

        return Prediction(
            narrative_id=narrative_id,
            prediction_type="coordination_risk",
            description=(
                f"This narrative has a {probability:.0%} probability of involving "
                f"coordinated amplification. "
                f"{coordinated_count}/{len(matches)} similar narratives showed "
                f"coordination patterns."
            ),
            confidence=round(min(0.90, 0.3 + len(matches) * 0.05), 3),
            basis=matches[:3],
            caveats=[
                "Coordination detection has an inherent false positive rate",
                "Some organic narratives mimic coordination patterns",
                "Low historical sample size reduces prediction reliability",
            ],
        )

    async def generate_predictions(
        self, narrative_id: str,
    ) -> list[Prediction]:
        """Generate all available predictions for a narrative."""
        matches = await self.matcher.find_matches(narrative_id)
        if not matches:
            return []

        predictions = []
        for predictor in [
            self.predict_peak_timing,
            self.predict_platform_migration,
            self.predict_coordination_risk,
        ]:
            pred = await predictor(narrative_id, matches)
            if pred:
                predictions.append(pred)
                # Store prediction
                pred_data = pred.model_dump(mode="json")
                await self.redis.set(
                    f"prediction:{pred.id}", json.dumps(pred_data), ex=604800,
                )

        # Store predictions index for this narrative
        pred_ids = [str(p.id) for p in predictions]
        if pred_ids:
            await self.redis.set(
                f"narrative:predictions:{narrative_id}",
                json.dumps(pred_ids), ex=604800,
            )

        await self.metrics.increment(
            "predictions.generated", value=len(predictions),
        )
        return predictions

    async def run(self, interval_seconds: int = 3600) -> None:
        """Generate predictions for all active narratives hourly."""
        logger.info("prediction_engine_started")
        while True:
            try:
                # Get active narratives
                cursor = 0
                narrative_ids = []
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
                                nid = data.get("narrative_id", data.get("id", ""))
                                if nid:
                                    narrative_ids.append(nid)
                    if cursor == 0:
                        break

                for nid in narrative_ids:
                    await self.generate_predictions(nid)

                logger.info(
                    "predictions_cycle_complete",
                    narratives_processed=len(narrative_ids),
                )
            except Exception as e:
                logger.error("prediction_cycle_error", error=str(e))
            await asyncio.sleep(interval_seconds)


async def main() -> None:
    settings = get_settings()
    from pymander.core.logging import setup_logging
    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)

    matcher = PatternMatcher(redis, metrics)
    engine = PredictionEngine(matcher, redis, metrics)
    try:
        await engine.run()
    except KeyboardInterrupt:
        pass
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
