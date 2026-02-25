"""Async Kafka producer wrapper."""

from __future__ import annotations

import json

import structlog
from aiokafka import AIOKafkaProducer

from pymander.core.config import get_settings

logger = structlog.get_logger()


class KafkaProducerWrapper:
    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        settings = get_settings()
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode(),
        )
        await self._producer.start()
        logger.info("kafka_producer_started")

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            logger.info("kafka_producer_stopped")

    async def send(self, topic: str, value: dict, key: str | None = None) -> None:
        if not self._producer:
            raise RuntimeError("Producer not started")
        await self._producer.send_and_wait(
            topic, value=value, key=key.encode() if key else None
        )
        logger.debug("kafka_message_sent", topic=topic, key=key)
