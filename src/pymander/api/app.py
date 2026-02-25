"""FastAPI application factory."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from pymander.api.v1.router import v1_router
from pymander.core.config import get_settings
from pymander.core.constants import API_V1_PREFIX
from pymander.core.logging import setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings = get_settings()
    setup_logging(settings.log_level)
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="Pymander",
        description="16s Narrative Intelligence Platform",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(v1_router, prefix=API_V1_PREFIX)
    return app


app = create_app()
