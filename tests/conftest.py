import pytest
from fakeredis import aioredis

from worker.config import Settings


@pytest.fixture
async def redis_client():
    client = aioredis.FakeRedis(decode_responses=False)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def settings() -> Settings:
    return Settings(
        REDIS_URL="redis://localhost:6379/0",
        STREAM_KEY="swarm:tasks:test",
        CONSUMER_GROUP="workers:test",
        DLQ_KEY="swarm:dlq:test",
        RESULTS_PREFIX="swarm:results",
        ORCHESTRATOR_CHANNEL="swarm:orchestrator:test",
        CONSUMER_ID="test-consumer",
        CONCURRENCY=1,
        BLOCK_MS=50,
        MAX_RETRIES=3,
    )
