"""V2 API router aggregation — client-facing API."""

from __future__ import annotations

from fastapi import APIRouter

from pymander.api.v2.routes.alerts import router as alerts_router
from pymander.api.v2.routes.auth import router as auth_router
from pymander.api.v2.routes.briefings import router as briefings_router
from pymander.api.v2.routes.coordination import router as coordination_router
from pymander.api.v2.routes.narratives import router as narratives_router
from pymander.api.v2.routes.network import router as network_router
from pymander.api.v2.routes.search import router as search_router

v2_router = APIRouter()
v2_router.include_router(auth_router)
v2_router.include_router(narratives_router)
v2_router.include_router(network_router)
v2_router.include_router(coordination_router)
v2_router.include_router(briefings_router)
v2_router.include_router(alerts_router)
v2_router.include_router(search_router)
