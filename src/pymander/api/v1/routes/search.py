"""Data search/browse API endpoints."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pymander.api.deps import get_db
from pymander.db.models.content import ContentRecord

router = APIRouter(prefix="/search", tags=["search"])


@router.get("")
async def search_content(
    q: str | None = Query(default=None),
    platform: str | None = Query(default=None),
    content_type: str | None = Query(default=None),
    since: datetime | None = Query(default=None),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Search and browse content records."""
    stmt = select(ContentRecord).order_by(
        desc(ContentRecord.content_created_at)
    )

    if q:
        stmt = stmt.where(ContentRecord.text.ilike(f"%{q}%"))
    if platform:
        stmt = stmt.where(ContentRecord.platform == platform)
    if content_type:
        stmt = stmt.where(ContentRecord.content_type == content_type)
    if since:
        stmt = stmt.where(ContentRecord.content_created_at >= since)

    stmt = stmt.limit(limit).offset(offset)
    result = await db.execute(stmt)
    records = result.scalars().all()

    # Count query
    count_stmt = select(func.count(ContentRecord.id))
    if q:
        count_stmt = count_stmt.where(ContentRecord.text.ilike(f"%{q}%"))
    if platform:
        count_stmt = count_stmt.where(ContentRecord.platform == platform)
    if content_type:
        count_stmt = count_stmt.where(ContentRecord.content_type == content_type)
    if since:
        count_stmt = count_stmt.where(ContentRecord.content_created_at >= since)

    count_result = await db.execute(count_stmt)
    total = count_result.scalar_one()

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": [
            {
                "id": str(r.id),
                "platform": r.platform,
                "content_type": r.content_type,
                "title": r.title,
                "text": (r.text[:300] + "...") if r.text and len(r.text) > 300 else r.text,
                "url": r.url,
                "created_at": (
                    r.content_created_at.isoformat()
                    if r.content_created_at
                    else None
                ),
                "actor": r.actor,
                "engagement": r.engagement,
            }
            for r in records
        ],
    }
