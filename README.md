# Redis Stream Worker Boilerplate

Async Redis Streams consumer template aligned with the swarm coordination worker plan. The consumer loop is fixed in `worker/consumer.py`; domain work lives in `worker/handler.py` via `process_task`.

## Quick start

```bash
export REDIS_URL="redis://localhost:6379/0"
export STREAM_KEY="swarm:tasks:embed"
export CONSUMER_GROUP="workers:embed"
export DLQ_KEY="swarm:dlq:embed"
export RESULTS_PREFIX="swarm:results"
export ORCHESTRATOR_CHANNEL="swarm:orchestrator:embed"
python -m worker.main
```

Prometheus metrics are exposed on port `8000` at `/metrics`.

## Retry and DLQ semantics

Each pending delivery increments the Redis `XPENDING` delivery counter. The consumer allows up to `MAX_RETRIES` failing attempts for a message. Once the delivery counter is **greater than** `MAX_RETRIES`, the payload is `XADD`ed to `DLQ_KEY` and the original message is `XACK`ed so it leaves the pending list. With the default of `3`, that means three failing `process_task` executions before the fourth delivery is dead-lettered without running the handler again.

`fakeredis` does not faithfully simulate pending redelivery counters today, so `tests/test_consumer.py` uses a narrow monkeypatch on `_pending_delivery_count` to exercise the DLQ branch while still using `fakeredis` for stream IO.

## Tests

```bash
pip install -e ".[dev]"
pytest tests/
```

## Kubernetes

`k8s/scaledjob.yaml` defines a KEDA `ScaledJob` for burst workers. `k8s/deployment.yaml` shows a `ScaledObject` wrapping a long-running `Deployment`.

`kubectl apply --dry-run=client` only succeeds when your kube-apiserver already knows the `ScaledJob` kind (install the [KEDA Helm chart](https://keda.sh/docs/latest/deploy/) or apply the upstream CRDs first). If you only want to lint the YAML locally, `python -c "import yaml; yaml.safe_load(open('k8s/scaledjob.yaml'))"` validates syntax without cluster discovery.

## Docker

```bash
docker build -t redis-stream-worker:local .
```
