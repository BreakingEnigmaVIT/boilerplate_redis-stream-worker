from __future__ import annotations

import asyncio
import logging
import signal
import sys

import redis.asyncio as redis

from worker.config import load_settings
from worker.consumer import StreamConsumer
from worker import metrics


logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


async def _run() -> None:
    settings = load_settings()
    metrics.ensure_metrics_server(8000)

    client = redis.from_url(settings.REDIS_URL, decode_responses=False)
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    def _request_shutdown(reason: str) -> None:
        logger.info("%s; draining in-flight work", reason)
        loop.call_soon_threadsafe(stop_event.set)

    def _handle_sigterm() -> None:
        _request_shutdown("SIGTERM received")

    def _handle_sigint() -> None:
        _request_shutdown("SIGINT received")

    try:
        loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)
        loop.add_signal_handler(signal.SIGINT, _handle_sigint)
    except NotImplementedError:
        # Windows / restricted environments
        signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
        signal.signal(signal.SIGINT, lambda *_: stop_event.set())

    consumer = StreamConsumer(settings, client, stop_event=stop_event)
    try:
        await consumer.run_forever()
    finally:
        await client.aclose()


def main() -> None:
    _configure_logging()
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        logger.info("shutdown requested (keyboard interrupt)")
    sys.exit(0)


if __name__ == "__main__":
    main()
