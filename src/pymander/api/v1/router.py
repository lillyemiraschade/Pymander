"""V1 API router aggregation."""

from __future__ import annotations

from fastapi import APIRouter

from pymander.api.v1.routes.health import router as health_router

v1_router = APIRouter()
v1_router.include_router(health_router, tags=["health"])
