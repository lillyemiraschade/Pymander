"""Actor ORM model."""

from __future__ import annotations

from sqlalchemy import Boolean, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from pymander.db.models.base import Base, TimestampMixin, UUIDMixin


class Actor(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "actors"

    platform: Mapped[str] = mapped_column(String(32), nullable=False)
    platform_id: Mapped[str] = mapped_column(String(256), nullable=False)
    username: Mapped[str] = mapped_column(String(256), nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(512))
    bio: Mapped[str | None] = mapped_column(Text)
    follower_count: Mapped[int | None] = mapped_column(Integer)
    following_count: Mapped[int | None] = mapped_column(Integer)
    is_verified: Mapped[bool] = mapped_column(Boolean, default=False)
    profile_url: Mapped[str | None] = mapped_column(String(2048))
    extra: Mapped[dict | None] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_actor_platform_id", "platform", "platform_id", unique=True),
        Index("ix_actor_username", "username"),
    )
