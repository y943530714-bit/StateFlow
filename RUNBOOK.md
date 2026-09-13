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
graph materialization, independent adapter HTTP publication, and HTTP smoke
behavior.

## Run an adapter process

After installing the package, run one source per process. The process only
publishes metadata and semantic state; model requests do not pass through it.

```bash
stateflow-adapter --source vllm \
  --source-endpoint http://runtime-a:8000 \
  --state-plane-url http://stateflow:8080 \
  --runtime-id runtime-a \
  --instance-id replica-a \
  --node-id node-a
```

Supported `--source` values are `vllm`, `sglang`, `ray-serve`, `dcgm`,
`kubernetes`, `mooncake`, and `lmcache`. Ray's shared Prometheus endpoint can
be scoped with repeated `--metric-label KEY=VALUE`. Kubernetes credentials are
read from `--bearer-token-file` or `STATEFLOW_KUBERNETES_BEARER_TOKEN`, never
printed. Use `--once` for probes and fixture validation.

### Optional gRPC transport

```bash
pip install '.[grpc]'
stateflow --host 127.0.0.1 --port 8080 --grpc-port 50051

stateflow-adapter --source vllm \
  --source-endpoint http://runtime-a:8000 \
  --state-plane-transport grpc \
  --state-plane-url 127.0.0.1:50051 \
  --runtime-id runtime-a \
  --instance-id replica-a
```

Regenerate checked-in bindings after editing `proto/stateflow.proto`:

```bash
pip install '.[grpc-dev]'
python scripts/generate_grpc.py
python scripts/generate_grpc.py --check
```

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
- Validate every built-in source profile against pinned target versions, then
  add authentication/TLS and persistent watch cursors before production use.
- Replace `HeuristicSuccessPredictor` with a calibrated predictor and keep
  uncertainty conservative; unknown state must remain safe.
- Register tokenizer/context accounting before enabling hard context-window
  enforcement for real models.
- Add authenticated gateway middleware and tenant/cache-domain isolation.
- Add streaming, retry idempotency, provider error classification, and
  production telemetry before exposing the gateway to external traffic.
