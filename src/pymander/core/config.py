"""Application settings via pydantic-settings."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DATABASE_")
    url: str = "postgresql+asyncpg://pymander:pymander@localhost:5432/pymander"
    echo: bool = False
    pool_size: int = 5


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_")
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "pymander"
    sasl_username: str = ""
    sasl_password: str = ""
    sasl_mechanism: str = "SCRAM-SHA-256"
    security_protocol: str = "PLAINTEXT"


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REDIS_")
    url: str = "redis://localhost:6379/0"


class Neo4jSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="NEO4J_")
    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: str = "pymander"


class RedditSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REDDIT_")
    client_id: str = ""
    client_secret: str = ""
    user_agent: str = "pymander/0.1.0 (narrative intelligence platform)"


class TwitterSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWITTER_")
    bearer_token: str = ""
    api_key: str = ""
    api_secret: str = ""
    stream_rules: list[str] = Field(default_factory=list)


class TelegramSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TELEGRAM_")
    api_id: int = 0
    api_hash: str = ""
    session_name: str = "pymander_telegram"
    channels: list[str] = Field(default_factory=list)


class YouTubeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="YOUTUBE_")
    api_key: str = ""
    daily_quota: int = 10000


class GoogleSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GOOGLE_")
    trends_regions: list[str] = Field(default_factory=lambda: ["US", "GB", "DE"])


class BlueskySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BLUESKY_")
    handle: str = ""
    app_password: str = ""


class QdrantSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="QDRANT_")
    host: str = "localhost"
    port: int = 6333
    grpc_port: int = 6334
    collection_name: str = "content_embeddings"
    api_key: str = ""


class AnthropicSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ANTHROPIC_")
    api_key: str = ""
    model: str = "claude-haiku-4-5-20251001"
    briefing_model: str = "claude-sonnet-4-5-20250929"
    max_calls_per_day: int = 500


class APISettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="API_")
    secret_key: str = "change-me-in-production"
    rate_limit_per_minute: int = 100
    token_expire_minutes: int = 1440


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    app_name: str = "pymander"
    log_level: str = "INFO"
    debug: bool = False

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    neo4j: Neo4jSettings = Field(default_factory=Neo4jSettings)
    reddit: RedditSettings = Field(default_factory=RedditSettings)
    twitter: TwitterSettings = Field(default_factory=TwitterSettings)
    telegram: TelegramSettings = Field(default_factory=TelegramSettings)
    youtube: YouTubeSettings = Field(default_factory=YouTubeSettings)
    google: GoogleSettings = Field(default_factory=GoogleSettings)
    bluesky: BlueskySettings = Field(default_factory=BlueskySettings)
    qdrant: QdrantSettings = Field(default_factory=QdrantSettings)
    anthropic: AnthropicSettings = Field(default_factory=AnthropicSettings)
    api: APISettings = Field(default_factory=APISettings)


def get_settings() -> Settings:
    return Settings()
