"""Domain logic hook for the Redis Streams worker.

Agents implement **only** ``process_task`` in this module. The consumer loop in
``consumer.py`` is intentionally fixed.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


async def process_task(task: dict[str, Any]) -> dict[str, Any]:
    """Execute domain work for a single stream message.

    Contract
    --------
    * **Input** — ``task`` is the raw Redis Stream hash flattened to a Python
      ``dict`` (field names → string values). The consumer injects structured
      tracing metadata under the key ``tracing`` when a ``traceparent`` field is
      present on the message.

    * **Output** — Return a JSON-serializable ``dict``. It is serialized to JSON
      and stored under ``{RESULTS_PREFIX}:{task_id}`` with a one-hour TTL, where
      ``task_id`` is taken from ``task["task_id"]`` when present, otherwise the
      stream entry id.

    * **Errors** — Raise on failure. The consumer will **not** ``XACK`` the
      message, so it remains pending and will be redelivered until the
      ``XPENDING`` delivery counter grows beyond ``MAX_RETRIES``, at which point
      the consumer copies the payload to the DLQ stream and acknowledges the
      original entry without calling the handler again.

    * **Side effects** — Optional calls such as ``worker.metrics.increment_llm_tokens``
      are safe from here when you need observability beyond the default counters.

    Parameters
    ----------
    task:
        Message payload including any producer-defined fields.

    Returns
    -------
    dict[str, Any]
        JSON-serializable result persisted for the orchestrator / caller.
    """
    _ = task
    return {"status": "ok", "echo": "replace handler.process_task with your logic"}
