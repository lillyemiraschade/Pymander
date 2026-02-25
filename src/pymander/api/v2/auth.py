"""API v2 authentication and rate limiting."""

from __future__ import annotations

import hashlib
import time
from datetime import UTC, datetime, timedelta

import jwt
import structlog
from fastapi import Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
from redis.asyncio import Redis

from pymander.api.deps import get_redis
from pymander.core.config import get_settings

logger = structlog.get_logger()

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def hash_api_key(key: str) -> str:
    """Hash an API key for storage."""
    return hashlib.sha256(key.encode()).hexdigest()


async def create_api_token(client_name: str, redis: Redis) -> dict:
    """Create a new API token for a client."""
    settings = get_settings()
    now = datetime.now(UTC)
    payload = {
        "sub": client_name,
        "iat": now.timestamp(),
        "exp": (now + timedelta(minutes=settings.api.token_expire_minutes)).timestamp(),
    }
    token = jwt.encode(payload, settings.api.secret_key, algorithm="HS256")

    # Store token metadata
    key_hash = hash_api_key(token)
    await redis.hset(f"api:token:{key_hash}", mapping={
        "client": client_name,
        "created_at": now.isoformat(),
        "requests": "0",
    })
    await redis.expire(
        f"api:token:{key_hash}",
        settings.api.token_expire_minutes * 60,
    )
    return {"token": token, "expires_at": payload["exp"], "client": client_name}


async def validate_api_key(
    api_key: str | None = Security(API_KEY_HEADER),
    redis: Redis = Depends(get_redis),
) -> dict:
    """Validate API key and return client info."""
    if not api_key:
        raise HTTPException(status_code=401, detail="API key required")

    settings = get_settings()

    # Try JWT decode
    try:
        payload = jwt.decode(api_key, settings.api.secret_key, algorithms=["HS256"])
        client_name = payload.get("sub", "unknown")
    except jwt.ExpiredSignatureError as err:
        raise HTTPException(status_code=401, detail="API key expired") from err
    except jwt.InvalidTokenError:
        # Check if it's a pre-shared key stored in Redis
        key_hash = hash_api_key(api_key)
        client_data = await redis.hgetall(f"api:token:{key_hash}")
        if not client_data:
            raise HTTPException(
                status_code=401, detail="Invalid API key",
            ) from None
        client_name = (
            client_data[b"client"].decode()
            if b"client" in client_data
            else "unknown"
        )

    # Rate limiting
    rate_key = f"api:rate:{hash_api_key(api_key)}:{int(time.time() / 60)}"
    count = await redis.incr(rate_key)
    if count == 1:
        await redis.expire(rate_key, 120)
    if count > settings.api.rate_limit_per_minute:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded ({settings.api.rate_limit_per_minute}/min)",
        )

    return {"client": client_name, "requests_this_minute": count}
