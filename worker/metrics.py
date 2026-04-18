import logging
import threading

from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger(__name__)

_tasks_processed = Counter(
    "tasks_processed_total",
    "Tasks handled by outcome",
    labelnames=("status",),
)
_task_duration = Histogram(
    "task_duration_seconds",
    "Wall time spent inside process_task",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)
_task_queue_depth = Gauge(
    "task_queue_depth",
    "Approximate depth of the primary task stream (XLEN)",
)
_llm_tokens = Counter(
    "llm_tokens_total",
    "LLM token usage attributed to handlers",
)


def start_metrics_server(port: int = 8000) -> None:
    """Start the Prometheus ``/metrics`` HTTP server (idempotent within a process)."""
    start_http_server(port)
    logger.info("metrics server listening on port %s", port)


_started = False
_lock = threading.Lock()


def ensure_metrics_server(port: int = 8000) -> None:
    global _started
    with _lock:
        if _started:
            return
        start_metrics_server(port)
        _started = True


def record_task_outcome(status: str, duration_seconds: float) -> None:
    _tasks_processed.labels(status=status).inc()
    _task_duration.observe(duration_seconds)


def set_queue_depth(depth: int) -> None:
    _task_queue_depth.set(depth)


def increment_llm_tokens(count: int = 1) -> None:
    """Increment LLM token counter; call from ``handler.py`` when usage is known."""
    if count <= 0:
        return
    _llm_tokens.inc(count)
