from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from prometheus_client import REGISTRY

from worker.consumer import StreamConsumer


@pytest.mark.asyncio
async def test_consume_ack_and_result_written(redis_client, settings):
    processed = asyncio.Event()

    async def succeed(task: dict[str, Any]) -> dict[str, Any]:
        processed.set()
        return {"echo": task.get("task_id")}

    await redis_client.xadd(settings.STREAM_KEY, {"task_id": "abc", "traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01"})

    consumer = StreamConsumer(settings, redis_client, process_fn=succeed)
    runner = asyncio.create_task(consumer.run_forever())

    await asyncio.wait_for(processed.wait(), timeout=2)
    consumer.stop_event.set()
    await asyncio.wait_for(runner, timeout=2)

    result_raw = await redis_client.get(f"{settings.RESULTS_PREFIX}:abc")
    assert result_raw is not None
    stored = json.loads(result_raw.decode() if isinstance(result_raw, bytes) else result_raw)
    assert stored["echo"] == "abc"

    pending = await redis_client.xpending_range(
        name=settings.STREAM_KEY,
        groupname=settings.CONSUMER_GROUP,
        min="-",
        max="+",
        count=10,
    )
    assert pending == []


@pytest.mark.asyncio
async def test_dlq_after_failed_attempts_exceed_max(redis_client, settings, monkeypatch):
    attempts = {"n": 0}
    delivery = {"count": 0}

    async def boom(task: dict[str, Any]) -> dict[str, Any]:
        attempts["n"] += 1
        raise RuntimeError("forced failure")

    await redis_client.xadd(settings.STREAM_KEY, {"task_id": "dlq-case"})
    await redis_client.xgroup_create(
        name=settings.STREAM_KEY,
        groupname=settings.CONSUMER_GROUP,
        id="0",
        mkstream=True,
    )
    read = await redis_client.xreadgroup(
        groupname=settings.CONSUMER_GROUP,
        consumername=settings.CONSUMER_ID,
        streams={settings.STREAM_KEY: ">"},
        count=1,
        block=0,
    )
    message_id, fields = read[0][1][0]
    mid = message_id.decode() if isinstance(message_id, bytes) else str(message_id)

    consumer = StreamConsumer(settings, redis_client, process_fn=boom)

    async def fake_delivery_count(self, message_id: str) -> int:
        delivery["count"] += 1
        return delivery["count"]

    monkeypatch.setattr(StreamConsumer, "_pending_delivery_count", fake_delivery_count)

    for _ in range(settings.MAX_RETRIES):
        await consumer._process_message(mid, {"task_id": "dlq-case"})

    await consumer._process_message(mid, {"task_id": "dlq-case"})

    assert attempts["n"] == settings.MAX_RETRIES

    dlq_len = await redis_client.xlen(settings.DLQ_KEY)
    assert dlq_len == 1

    pending = await redis_client.xpending_range(
        name=settings.STREAM_KEY,
        groupname=settings.CONSUMER_GROUP,
        min=mid,
        max=mid,
        count=10,
    )
    assert pending == []


@pytest.mark.asyncio
async def test_sigterm_drains_inflight_task(redis_client, settings, monkeypatch):
    monkeypatch.setattr(settings, "BLOCK_MS", 25)

    started = asyncio.Event()
    release = asyncio.Event()

    async def slow(task: dict[str, Any]) -> dict[str, Any]:
        started.set()
        await asyncio.wait_for(release.wait(), timeout=5)
        return {"finished": True}

    await redis_client.xadd(settings.STREAM_KEY, {"task_id": "sigterm"})

    consumer = StreamConsumer(settings, redis_client, process_fn=slow)
    runner = asyncio.create_task(consumer.run_forever())

    await asyncio.wait_for(started.wait(), timeout=2)
    consumer.stop_event.set()
    await asyncio.sleep(0.05)
    release.set()

    await asyncio.wait_for(runner, timeout=3)

    result_raw = await redis_client.get(f"{settings.RESULTS_PREFIX}:sigterm")
    assert result_raw is not None


def test_task_duration_histogram_registered():
    from worker import metrics as _metrics  # noqa: F401

    names = {sample.name for sample in REGISTRY.collect()}
    assert "task_duration_seconds" in names
