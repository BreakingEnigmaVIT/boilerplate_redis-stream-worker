import os

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    REDIS_URL: str = Field(..., description="Redis connection URL")
    STREAM_KEY: str = Field(..., description="Stream consumed by this worker")
    CONSUMER_GROUP: str = Field(..., description="Consumer group name")
    DLQ_KEY: str = Field(..., description="Dead-letter stream key")
    RESULTS_PREFIX: str = Field(..., description="Prefix for SETEX result keys")
    CONSUMER_ID: str = Field(default_factory=lambda: os.environ.get("HOSTNAME", "local"))
    CONCURRENCY: int = Field(default=1, ge=1)
    BLOCK_MS: int = Field(default=5000, ge=1)
    MAX_RETRIES: int = Field(default=3, ge=1)
    ORCHESTRATOR_CHANNEL: str = Field(..., description="Pub/Sub channel for completion notifications")


def load_settings() -> Settings:
    return Settings()
