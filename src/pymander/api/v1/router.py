"""V1 API router aggregation."""

from __future__ import annotations

from fastapi import APIRouter

from pymander.api.v1.routes.health import router as health_router
from pymander.api.v1.routes.metrics import router as metrics_router
from pymander.api.v1.routes.narratives import router as narratives_router
from pymander.api.v1.routes.pipeline import router as pipeline_router
from pymander.api.v1.routes.search import router as search_router
from pymander.api.v1.routes.ws import router as ws_router

v1_router = APIRouter()
v1_router.include_router(health_router, tags=["health"])
v1_router.include_router(metrics_router)
v1_router.include_router(narratives_router)
v1_router.include_router(pipeline_router)
v1_router.include_router(search_router)
v1_router.include_router(ws_router)
