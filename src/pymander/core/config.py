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


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REDIS_")
    url: str = "redis://localhost:6379/0"


class Neo4jSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="NEO4J_")
    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: str = "pymander"


class ElasticsearchSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ELASTICSEARCH_")
    url: str = "http://localhost:9200"


class MinioSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MINIO_")
    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    secure: bool = False


class RedditSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REDDIT_")
    client_id: str = ""
    client_secret: str = ""
    user_agent: str = "pymander/0.1.0 (narrative intelligence platform)"


class QdrantSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="QDRANT_")
    host: str = "localhost"
    port: int = 6333
    grpc_port: int = 6334
    collection_name: str = "content_embeddings"


class AnthropicSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ANTHROPIC_")
    api_key: str = ""
    model: str = "claude-haiku-4-5-20251001"
    max_calls_per_day: int = 500


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "pymander"
    log_level: str = "INFO"
    debug: bool = False

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    neo4j: Neo4jSettings = Field(default_factory=Neo4jSettings)
    es: ElasticsearchSettings = Field(default_factory=ElasticsearchSettings)
    minio: MinioSettings = Field(default_factory=MinioSettings)
    reddit: RedditSettings = Field(default_factory=RedditSettings)
    qdrant: QdrantSettings = Field(default_factory=QdrantSettings)
    anthropic: AnthropicSettings = Field(default_factory=AnthropicSettings)


def get_settings() -> Settings:
    return Settings()
