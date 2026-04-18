from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

import redis.asyncio as redis

from worker import metrics
from worker.config import Settings
from worker.handler import process_task
from worker.tracing import extract

logger = logging.getLogger(__name__)

ProcessTaskFn = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


def _decode_stream_fields(raw: dict[Any, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in raw.items():
        k = key.decode() if isinstance(key, bytes) else str(key)
        if isinstance(value, bytes):
            out[k] = value.decode()
        else:
            out[k] = value
    return out


def _delivery_count(entry: dict[str, Any]) -> int:
    for key in ("times_delivered", "delivery_count"):
        if key in entry:
            return int(entry[key])
    return 1


class StreamConsumer:
    """XREADGROUP consumer with DLQ, results persistence, and cooperative shutdown."""

    def __init__(
        self,
        settings: Settings,
        redis_client: redis.Redis,
        *,
        process_fn: ProcessTaskFn | None = None,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self._settings = settings
        self._redis = redis_client
        self._process_fn = process_fn or process_task
        self._stop_event = stop_event or asyncio.Event()
        self._semaphore = asyncio.Semaphore(settings.CONCURRENCY)
        self._inflight: set[asyncio.Task[Any]] = set()

    @property
    def stop_event(self) -> asyncio.Event:
        return self._stop_event

    def _track(self, task: asyncio.Task[Any]) -> None:
        self._inflight.add(task)

        def _done(t: asyncio.Task[Any]) -> None:
            self._inflight.discard(t)

        task.add_done_callback(_done)

    async def setup_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                name=self._settings.STREAM_KEY,
                groupname=self._settings.CONSUMER_GROUP,
                id="0",
                mkstream=True,
            )
            logger.info("created consumer group %s", self._settings.CONSUMER_GROUP)
        except redis.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug("consumer group already exists")
            else:
                raise

    async def _wait_inflight(self) -> None:
        if not self._inflight:
            return
        pending = list(self._inflight)
        await asyncio.gather(*pending, return_exceptions=True)

    async def _read_once(self, stream_id: str, block_ms: int | None) -> list[Any]:
        return await self._redis.xreadgroup(
            groupname=self._settings.CONSUMER_GROUP,
            consumername=self._settings.CONSUMER_ID,
            streams={self._settings.STREAM_KEY: stream_id},
            count=1,
            block=block_ms if block_ms is not None else self._settings.BLOCK_MS,
            noack=False,
        )

    async def _read_new_messages(self) -> list[Any]:
        """Poll ``>`` using short blocks so ``stop_event`` can preempt long waits."""
        if self._stop_event.is_set():
            return []
        remaining = max(self._settings.BLOCK_MS, 1)
        while remaining > 0 and not self._stop_event.is_set():
            chunk = min(200, remaining)
            batch = await self._read_once(">", chunk)
            if batch and batch[0][1]:
                return batch
            remaining -= chunk
        if self._stop_event.is_set():
            return []
        return await self._read_once(">", 0)

    async def _next_message(self) -> tuple[str, dict[str, Any]] | None:
        if self._stop_event.is_set():
            return None

        try:
            depth = int(await self._redis.xlen(self._settings.STREAM_KEY))
            metrics.set_queue_depth(depth)
        except Exception:
            logger.exception("failed to refresh queue depth metric")

        pending = await self._read_once("0", 0)
        if not pending:
            if self._stop_event.is_set():
                return None
            pending = await self._read_new_messages()

        if not pending:
            return None

        _stream_name, messages = pending[0]
        if not messages:
            return None

        message_id, raw_fields = messages[0]
        mid = message_id.decode() if isinstance(message_id, bytes) else str(message_id)
        fields = _decode_stream_fields(raw_fields)
        return mid, fields

    async def _pending_delivery_count(self, message_id: str) -> int:
        try:
            info = await self._redis.xpending_range(
                name=self._settings.STREAM_KEY,
                groupname=self._settings.CONSUMER_GROUP,
                min=message_id,
                max=message_id,
                count=10,
            )
        except Exception:
            logger.exception("xpending_range failed for %s", message_id)
            return 1

        for entry in info or []:
            eid = entry.get("message_id")
            eid_str = eid.decode() if isinstance(eid, bytes) else str(eid)
            if eid_str == message_id:
                return _delivery_count(entry)
        return 1

    async def _handle_dlq(self, message_id: str, fields: dict[str, Any], delivery_count: int) -> None:
        payload = {
            "original_id": message_id,
            "delivery_count": delivery_count,
            "fields": fields,
        }
        await self._redis.xadd(self._settings.DLQ_KEY, {"payload": json.dumps(payload)})
        await self._redis.xack(self._settings.STREAM_KEY, self._settings.CONSUMER_GROUP, message_id)
        metrics.record_task_outcome("dlq", 0.0)
        logger.warning("message %s moved to DLQ after %s deliveries", message_id, delivery_count)

    async def _handle_success(
        self,
        message_id: str,
        fields: dict[str, Any],
        result: dict[str, Any],
    ) -> None:
        task_id = str(fields.get("task_id") or message_id)
        result_key = f"{self._settings.RESULTS_PREFIX}:{task_id}"
        await self._redis.setex(result_key, 3600, json.dumps(result))
        notice = {
            "task_id": task_id,
            "message_id": message_id,
            "result": result,
        }
        await self._redis.publish(self._settings.ORCHESTRATOR_CHANNEL, json.dumps(notice))
        await self._redis.xack(self._settings.STREAM_KEY, self._settings.CONSUMER_GROUP, message_id)

    def _build_task(self, message_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        task = dict(fields)
        task.setdefault("message_id", message_id)
        traceparent = task.get("traceparent")
        if isinstance(traceparent, str) and traceparent.strip():
            try:
                task["tracing"] = extract(traceparent)
            except Exception:
                logger.exception("invalid traceparent on message %s", message_id)
        return task

    async def _process_message(self, message_id: str, fields: dict[str, Any]) -> None:
        delivery_count = await self._pending_delivery_count(message_id)
        # Allow up to MAX_RETRIES processing attempts; DLQ once deliveries exceed that budget.
        if delivery_count > self._settings.MAX_RETRIES:
            await self._handle_dlq(message_id, fields, delivery_count)
            return

        task = self._build_task(message_id, fields)
        start = time.perf_counter()
        try:
            async with self._semaphore:
                result = await self._process_fn(task)
        except Exception:
            duration = time.perf_counter() - start
            metrics.record_task_outcome("error", duration)
            logger.exception("process_task failed for %s", message_id)
            return

        duration = time.perf_counter() - start
        try:
            await self._handle_success(message_id, fields, result)
            metrics.record_task_outcome("success", duration)
        except Exception:
            metrics.record_task_outcome("error", duration)
            logger.exception("post-processing failed for %s", message_id)

    async def run_forever(self) -> None:
        await self.setup_group()
        logger.info("consumer ready")

        while True:
            if self._stop_event.is_set():
                await self._wait_inflight()
                return

            try:
                nxt = await self._next_message()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("read loop error")
                await asyncio.sleep(0.5)
                continue

            if nxt is None:
                if self._stop_event.is_set():
                    await self._wait_inflight()
                    return
                continue

            message_id, fields = nxt

            async def _runner(mid: str = message_id, f: dict[str, Any] = fields) -> None:
                await self._process_message(mid, f)

            t = asyncio.create_task(_runner())
            self._track(t)
