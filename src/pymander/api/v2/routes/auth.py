"""Client API v2 — Authentication endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from redis.asyncio import Redis

from pymander.api.deps import get_redis
from pymander.api.v2.auth import create_api_token

router = APIRouter(prefix="/auth", tags=["auth"])


class TokenRequest(BaseModel):
    client_name: str
    secret: str


@router.post("/token")
async def get_token(
    request: TokenRequest,
    redis: Redis = Depends(get_redis),
) -> dict:
    """Generate API token for a client."""
    from pymander.core.config import get_settings
    settings = get_settings()
    # Simple shared secret verification for v1
    if request.secret != settings.api.secret_key:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Invalid secret")
    return await create_api_token(request.client_name, redis)


@router.post("/refresh")
async def refresh_token(
    request: TokenRequest,
    redis: Redis = Depends(get_redis),
) -> dict:
    """Refresh an API token."""
    from pymander.core.config import get_settings
    settings = get_settings()
    if request.secret != settings.api.secret_key:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Invalid secret")
    return await create_api_token(request.client_name, redis)
