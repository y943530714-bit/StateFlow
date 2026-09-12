# StateFlow MVP Runbook

## Local validation

From the repository root:

```bash
python -m compileall -q stateflow tests
python -m unittest discover -s tests -v
```

The test suite covers canonical key normalization, source authority/CAS,
snapshot freshness and immutability, graph relation constraints, change
cursors, Candidate-Action control records, hard filters, success-first
ordering, KV-aware cost, latency/placement, critical override, hysteresis,
state event application, reporter isolation, protocol normalization, gateway
routing, Runtime/KV/Kubernetes/DCGM semantic projection, cross-source identity
resolution, source-client parsing, polling failure isolation, request-scoped
graph materialization, and HTTP smoke behavior.

## Start the demo

```bash
python -m stateflow --host 127.0.0.1 --port 8080
```

The demo uses an in-memory backend. It is suitable for exercising the state
and scheduler control flow, not for production inference.

State Plane smoke checks:

```bash
curl -s http://127.0.0.1:8080/v1/state-plane/freshness
curl -s http://127.0.0.1:8080/v1/state-plane/schema
```

In-process snapshot latency baseline:

```bash
python -m benchmarks.state_plane_snapshot --iterations 1000
```

This is a local regression baseline, not a production SLO or distributed RPC
measurement.

## Extension checklist

- Replace `InMemoryStateStore` and `InMemoryStatePlane` with a durable event
  journal, hot state/relation indexes, and distributed snapshot materializer.
- Add SGLang/Ray and native Mooncake/LMCache profiles, Kubernetes list-watch
  recovery, process packaging, and gRPC bindings for the contracts defined in
  `proto/stateflow.proto`. The current clients cover vLLM/DCGM Prometheus,
  Kubernetes list polling, and a metadata-only KV sidecar JSON contract.
- Replace `HeuristicSuccessPredictor` with a calibrated predictor and keep
  uncertainty conservative; unknown state must remain safe.
- Register tokenizer/context accounting before enabling hard context-window
  enforcement for real models.
- Add authenticated gateway middleware and tenant/cache-domain isolation.
- Add streaming, retry idempotency, provider error classification, and
  production telemetry before exposing the gateway to external traffic.
