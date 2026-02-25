"""Async Kafka consumer wrapper."""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator

import structlog
from aiokafka import AIOKafkaConsumer

from pymander.core.config import get_settings

logger = structlog.get_logger()


class KafkaConsumerWrapper:
    def __init__(self, *topics: str, group_id: str | None = None) -> None:
        self._topics = topics
        self._group_id = group_id
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        settings = get_settings()
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=settings.kafka.bootstrap_servers,
            group_id=self._group_id or settings.kafka.group_id,
            value_deserializer=lambda v: json.loads(v.decode()),
            auto_offset_reset="earliest",
        )
        await self._consumer.start()
        logger.info("kafka_consumer_started", topics=self._topics)

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
            logger.info("kafka_consumer_stopped")

    async def messages(self) -> AsyncGenerator[dict, None]:
        if not self._consumer:
            raise RuntimeError("Consumer not started")
        async for msg in self._consumer:
            yield msg.value
