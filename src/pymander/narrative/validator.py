"""Narrative validation using Claude API."""

from __future__ import annotations

import asyncio
import json

import structlog
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.producer import KafkaProducerWrapper
from pymander.ingestion.topics import NARRATIVE_EVENTS

logger = structlog.get_logger()

VALIDATION_PROMPT = """You are analyzing a cluster of online content to determine if it represents a coherent narrative.

A narrative is a specific claim, story, frame, or idea that is actively spreading. It is NOT just a topic.

Examples:
- "Company X is covering up safety data" → YES, this is a narrative
- "Technology" → NO, this is a topic
- "The president's trade policy will crash the economy" → YES, this is a narrative
- "Economics" → NO, this is a topic
- "A viral video shows police misconduct at protest" → YES, this is a narrative
- "Police" → NO, this is a topic

Here are {n} representative pieces of content from this cluster:

{content_samples}

Respond in this exact JSON format:
{{
    "is_coherent_narrative": true/false,
    "summary": "One sentence describing the specific narrative",
    "category": "one of: breaking_news, political_claim, corporate_crisis, cultural_trend, conspiracy_theory, coordinated_campaign, organic_movement, tech_discourse, financial_narrative, health_narrative",
    "confidence": 0.0-1.0,
    "reasoning": "Brief explanation of your assessment"
}}

Only JSON. No other text."""


class NarrativeValidator:
    """Validates narrative candidates using Claude API."""

    def __init__(
        self,
        redis: Redis,
        metrics: MetricsCollector,
        producer: KafkaProducerWrapper,
    ) -> None:
        self.redis = redis
        self.metrics = metrics
        self.producer = producer
        self._client = None
        self._running = True

    async def setup(self) -> None:
        import anthropic

        settings = get_settings()
        self._client = anthropic.AsyncAnthropic(
            api_key=settings.anthropic.api_key
        )
        self._model = settings.anthropic.model

    async def fetch_content_samples(
        self, content_ids: list[str]
    ) -> list[str]:
        """Fetch text previews for content IDs from Qdrant."""
        settings = get_settings()
        from qdrant_client import AsyncQdrantClient

        qdrant = AsyncQdrantClient(
            host=settings.qdrant.host, port=settings.qdrant.port
        )
        try:
            points = await qdrant.retrieve(
                collection_name="content_embeddings",
                ids=content_ids[:20],
                with_payload=True,
            )
            return [
                p.payload.get("text_preview", "") for p in points if p.payload
            ]
        finally:
            await qdrant.close()

    async def validate_candidate(self, narrative_id: str) -> dict | None:
        """Validate a single narrative candidate."""
        raw = await self.redis.get(f"narrative:candidate:{narrative_id}")
        if not raw:
            return None

        candidate = json.loads(raw)
        content_ids = candidate.get("content_ids", [])
        samples = await self.fetch_content_samples(content_ids)

        if not samples:
            logger.warning(
                "no_samples_for_validation", narrative_id=narrative_id
            )
            return None

        content_text = "\n\n---\n\n".join(
            f"[{i + 1}] {s}" for i, s in enumerate(samples) if s
        )
        prompt = VALIDATION_PROMPT.format(
            n=len(samples), content_samples=content_text
        )

        try:
            response = await self._client.messages.create(
                model=self._model,
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}],
            )
            await self.metrics.increment("claude.api.calls")

            # Estimate cost (Haiku: ~$0.25/M input, $1.25/M output)
            input_tokens = response.usage.input_tokens
            output_tokens = response.usage.output_tokens
            cost_cents = (input_tokens * 0.025 + output_tokens * 0.125) / 100
            await self.metrics.increment(
                "claude.api.cost_cents", value=int(cost_cents * 100)
            )

            text = response.content[0].text.strip()
            result = json.loads(text)

            if result.get("is_coherent_narrative") and result.get("confidence", 0) > 0.6:
                await self.producer.send(
                    NARRATIVE_EVENTS,
                    {
                        "event": "narrative_validated",
                        "narrative_id": narrative_id,
                        "summary": result.get("summary", ""),
                        "category": result.get("category", ""),
                        "confidence": result.get("confidence", 0),
                        "content_count": candidate.get("content_count", 0),
                    },
                )
                await self.metrics.increment("narratives.validated")
                logger.info(
                    "narrative_validated",
                    narrative_id=narrative_id,
                    summary=result.get("summary", ""),
                )
            else:
                await self.metrics.increment("narratives.rejected")
                logger.info(
                    "narrative_rejected",
                    narrative_id=narrative_id,
                    confidence=result.get("confidence", 0),
                )

            return result

        except json.JSONDecodeError:
            logger.warning(
                "narrative_validation_json_error",
                narrative_id=narrative_id,
            )
            await self.metrics.increment("narratives.validation_errors")
            return None
        except Exception as e:
            logger.error(
                "narrative_validation_error",
                narrative_id=narrative_id,
                error=str(e),
            )
            await self.metrics.increment("narratives.validation_errors")
            return None

    async def run(self) -> None:
        """Process narrative validation queue."""
        await self.setup()
        logger.info("narrative_validator_started")

        while self._running:
            # Scan for candidates in Redis
            cursor = 0
            found = False
            while True:
                cursor, keys = await self.redis.scan(
                    cursor, match="narrative:candidate:*", count=100
                )
                for key in keys:
                    narrative_id = key.decode().split(":")[-1]
                    found = True

                    # Check daily call budget
                    calls_today = await self.metrics.get_counter_for_day(
                        "claude.api.calls"
                    )
                    settings = get_settings()
                    if calls_today >= settings.anthropic.max_calls_per_day:
                        logger.warning(
                            "claude_daily_budget_exhausted",
                            calls=calls_today,
                        )
                        await asyncio.sleep(60)
                        continue

                    await self.validate_candidate(narrative_id)
                    # Remove candidate after processing
                    await self.redis.delete(
                        f"narrative:candidate:{narrative_id}"
                    )

                if cursor == 0:
                    break

            if not found:
                await asyncio.sleep(10)

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    producer = KafkaProducerWrapper()
    await producer.start()

    validator = NarrativeValidator(redis, metrics, producer)
    try:
        await validator.run()
    except KeyboardInterrupt:
        validator.stop()
    finally:
        await producer.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
