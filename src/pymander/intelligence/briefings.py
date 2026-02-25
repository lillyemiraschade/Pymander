"""Auto-generated intelligence briefings using Claude API."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta

import anthropic
import structlog
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.schemas.briefing import Briefing
from pymander.schemas.enums import BriefingType

logger = structlog.get_logger()

BRIEFING_STRUCTURE = """
SECTION 1: EXECUTIVE SUMMARY (3-5 sentences)
- Most significant narrative development
- Any coordination detections
- Any critical alerts

SECTION 2: TOP NARRATIVES (top 5 by behavioral impact)
For each: one-line summary, status, velocity, platform presence, key signals, historical pattern match if available

SECTION 3: NEW NARRATIVES (detected in last 24h)
For each: summary, category, origin platform, speed of emergence, organic vs coordinated assessment

SECTION 4: COORDINATION ACTIVITY
- New clusters detected
- Updated clusters
- Narratives with elevated coordination scores

SECTION 5: MIGRATION EVENTS
- Cross-platform migration events in last 24h
- Bridge nodes identified
- Migration speed analysis

SECTION 6: BEHAVIORAL SIGNALS
- Language shifts detected
- Engagement pattern changes
- Community structural changes

SECTION 7: WATCHLIST
- Narratives approaching viral threshold
- Dormant narratives showing reactivation signals
- Communities showing unusual behavioral change
"""


class BriefingGenerator:
    """Generates structured intelligence briefings from accumulated data."""

    def __init__(self, redis: Redis, metrics: MetricsCollector) -> None:
        self.redis = redis
        self.metrics = metrics
        self.settings = get_settings()
        self.client = anthropic.AsyncAnthropic(api_key=self.settings.anthropic.api_key)
        self._running = True

    async def _get_narratives(self, limit: int = 10) -> list[dict]:
        """Get top narratives from Redis."""
        narratives = []
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor, match="narrative:validated:*", count=100)
            for key in keys:
                raw = await self.redis.get(key)
                if raw:
                    narratives.append(json.loads(raw))
            if cursor == 0 or len(narratives) >= limit * 2:
                break
        narratives.sort(key=lambda n: n.get("content_count", 0), reverse=True)
        return narratives[:limit]

    async def _get_coordination_clusters(self) -> list[dict]:
        """Get active coordination clusters."""
        cluster_ids = await self.redis.smembers("coordination:clusters:active")
        clusters = []
        for cid in cluster_ids:
            cid_str = cid.decode() if isinstance(cid, bytes) else cid
            raw = await self.redis.get(f"coordination:cluster:{cid_str}")
            if raw:
                clusters.append(json.loads(raw))
        return clusters

    async def _get_migrations(self, hours: int = 24) -> list[dict]:
        """Get recent migration events."""
        migrations = []
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor, match="narrative:migrations:*", count=100)
            for key in keys:
                raw_list = await self.redis.lrange(key, 0, 9)
                for raw in raw_list:
                    data = json.loads(raw)
                    migrations.append(data)
            if cursor == 0:
                break
        return migrations[:50]

    async def _get_behavioral_alerts(self, hours: int = 24) -> list[dict]:
        """Get recent behavioral signal alerts."""
        import time
        cutoff = time.time() - (hours * 3600)
        alert_ids = await self.redis.zrangebyscore("behavioral:alerts:index", cutoff, "+inf")
        alerts = []
        for aid in alert_ids:
            aid_str = aid.decode() if isinstance(aid, bytes) else aid
            raw = await self.redis.get(f"behavioral:alert:{aid_str}")
            if raw:
                alerts.append(json.loads(raw))
        return alerts

    async def _get_platform_stats(self) -> dict:
        """Get platform ingestion stats."""
        platforms = ["reddit", "twitter", "telegram", "youtube", "4chan", "rss"]
        stats = {}
        for platform in platforms:
            count = await self.metrics.get_counter(
                f"{platform}.posts.ingested",
            )
            today = await self.metrics.get_counter_for_day(
                f"{platform}.posts.ingested",
            )
            stats[platform] = {"total": count, "today": today}
        return stats

    async def collect_data(self, hours: int = 24) -> dict:
        """Collect all data needed for briefing generation."""
        return {
            "narratives": await self._get_narratives(limit=10),
            "coordination_clusters": await self._get_coordination_clusters(),
            "migration_events": await self._get_migrations(hours),
            "behavioral_alerts": await self._get_behavioral_alerts(hours),
            "platform_stats": await self._get_platform_stats(),
            "period_hours": hours,
        }

    async def generate_briefing(
        self, briefing_type: BriefingType = BriefingType.DAILY, hours: int = 24,
    ) -> Briefing:
        """Generate an intelligence briefing."""
        data = await self.collect_data(hours)

        system_prompt = (
            "You are a narrative intelligence analyst generating a structured briefing. "
            "Be precise, factual, and actionable. No speculation. Every claim must be "
            "supported by the data provided. Use the exact section structure provided. "
            "Use specific numbers, name platforms, reference specific narratives by summary."
        )

        user_prompt = f"""Generate a {'daily' if briefing_type == BriefingType.DAILY else 'on-demand'} intelligence briefing using this structure:

{BRIEFING_STRUCTURE}

Data from the last {hours} hours:

{json.dumps(data, indent=2, default=str)}

This briefing will be read by senior communications directors and government analysts who need actionable intelligence."""

        try:
            response = await self.client.messages.create(
                model=self.settings.anthropic.briefing_model,
                max_tokens=4000,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            content = response.content[0].text
            token_cost = response.usage.input_tokens + response.usage.output_tokens
        except Exception as e:
            logger.error("briefing_generation_failed", error=str(e))
            content = f"Briefing generation failed: {e}"
            token_cost = 0

        now = datetime.now(UTC)
        briefing = Briefing(
            id=str(uuid.uuid4()),
            type=briefing_type,
            generated_at=now,
            period_start=now - timedelta(hours=hours),
            period_end=now,
            content=content,
            data_snapshot=data,
            model_used=self.settings.anthropic.briefing_model,
            token_cost=token_cost,
        )

        # Store briefing
        briefing_data = briefing.model_dump(mode="json")
        await self.redis.set(
            f"briefing:{briefing.id}", json.dumps(briefing_data), ex=2592000,
        )
        await self.redis.set(
            f"briefing:latest:{briefing_type}", json.dumps(briefing_data), ex=2592000,
        )
        await self.redis.lpush("briefing:index", briefing.id)
        await self.redis.ltrim("briefing:index", 0, 99)

        await self.metrics.increment("briefings.generated")
        await self.metrics.increment("briefings.tokens_used", value=token_cost)
        logger.info(
            "briefing_generated",
            type=briefing_type, tokens=token_cost, id=briefing.id,
        )
        return briefing

    async def run(self, schedule_hour_utc: int = 6) -> None:
        """Run daily briefing generation at specified UTC hour."""
        logger.info("briefing_generator_started", schedule_hour=schedule_hour_utc)
        while self._running:
            now = datetime.now(UTC)
            # Calculate time until next scheduled run
            next_run = now.replace(
                hour=schedule_hour_utc, minute=0, second=0, microsecond=0,
            )
            if next_run <= now:
                next_run += timedelta(days=1)
            wait_seconds = (next_run - now).total_seconds()

            logger.info("briefing_next_run", wait_seconds=int(wait_seconds))
            await asyncio.sleep(min(wait_seconds, 3600))  # Check every hour

            now = datetime.now(UTC)
            if now.hour == schedule_hour_utc:
                try:
                    await self.generate_briefing(BriefingType.DAILY)
                except Exception as e:
                    logger.error("briefing_scheduled_error", error=str(e))

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    settings = get_settings()
    from pymander.core.logging import setup_logging
    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)

    generator = BriefingGenerator(redis, metrics)
    try:
        await generator.run()
    except KeyboardInterrupt:
        generator.stop()
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
