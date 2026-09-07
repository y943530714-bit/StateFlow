# StateFlow MVP Runbook

## Local validation

From the repository root:

```bash
python -m compileall -q stateflow tests
python -m unittest discover -s tests -v
```

The test suite covers hard filters, success-first ordering, KV-aware cost,
latency/placement, critical override, hysteresis, state event application,
reporter isolation, protocol normalization, gateway routing, and HTTP smoke
behavior.

## Start the demo

```bash
python -m stateflow --host 127.0.0.1 --port 8080
```

The demo uses an in-memory backend. It is suitable for exercising the state
and scheduler control flow, not for production inference.

## Extension checklist

- Replace `InMemoryStateStore` with a durable append-only event log and a
  snapshot/hot-view materializer.
- Replace `HeuristicSuccessPredictor` with a calibrated predictor and keep
  uncertainty conservative; unknown state must remain safe.
- Register tokenizer/context accounting before enabling hard context-window
  enforcement for real models.
- Add authenticated gateway middleware and tenant/cache-domain isolation.
- Add streaming, retry idempotency, provider error classification, and
  production telemetry before exposing the gateway to external traffic.
