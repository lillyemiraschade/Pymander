"""Dependency injection for FastAPI routes."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from functools import lru_cache

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from pymander.core.config import Settings, get_settings
from pymander.db.engine import get_session


@lru_cache
def get_app_settings() -> Settings:
    return get_settings()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async for session in get_session():
        yield session


async def get_redis() -> AsyncGenerator[Redis, None]:
    settings = get_settings()
    redis = Redis.from_url(settings.redis.url)
    try:
        yield redis
    finally:
        await redis.aclose()
