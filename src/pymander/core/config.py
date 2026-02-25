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


def get_settings() -> Settings:
    return Settings()
