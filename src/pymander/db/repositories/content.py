"""Content record query helpers."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pymander.db.models.content import ContentRecord


async def get_content_by_id(session: AsyncSession, content_id: uuid.UUID) -> ContentRecord | None:
    result = await session.execute(select(ContentRecord).where(ContentRecord.id == content_id))
    return result.scalar_one_or_none()


async def get_content_by_platform_id(
    session: AsyncSession, platform: str, platform_content_id: str
) -> ContentRecord | None:
    result = await session.execute(
        select(ContentRecord).where(
            ContentRecord.platform == platform,
            ContentRecord.platform_content_id == platform_content_id,
        )
    )
    return result.scalar_one_or_none()


async def list_content(
    session: AsyncSession, platform: str | None = None, limit: int = 50, offset: int = 0
) -> list[ContentRecord]:
    stmt = (
        select(ContentRecord).order_by(ContentRecord.created_at.desc()).limit(limit).offset(offset)
    )
    if platform:
        stmt = stmt.where(ContentRecord.platform == platform)
    result = await session.execute(stmt)
    return list(result.scalars().all())
