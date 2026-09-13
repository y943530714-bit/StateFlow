# StateFlow

StateFlow is a dependency-free reference implementation of the StateFlow v1.1
state/prediction-driven control architecture.  It keeps the existing request
scheduler's strict lexicographic policy:

1. maximize conservative task-success probability;
2. among success-equivalent targets, minimize KV-aware effective cost;
3. among cost-equivalent targets, minimize predicted latency and choose a
   session-aware replica.

The implementation includes:

- a canonical key registry with source authority, provenance, TTL, CAS, and
  idempotent writes;
- Component and Deployment state graphs with a deliberately small set of
  allowed cross-graph relations;
- immutable state snapshot tokens with explicit completeness, missing/stale
  fields, graph queries, and resumable change cursors;
- transport-neutral Candidate-Action Prediction and
  Decision/Action/Outcome/Feedback contracts;
- a bridge that exports Success-First evaluations as replayable v1.1 control
  records;
- an Agent State v0.1 schema with event, snapshot, and hot scheduling view;
- a thread-safe in-memory state store with epoch/sequence de-duplication and
  TTL-aware freshness metadata;
- capability/security hard filtering, critical overrides, LCB success gating,
  cost/KV valuation, latency estimation, placement, and hysteresis;
- OpenAI Chat/Responses and Anthropic Messages normalization;
- a generic proxy harness adapter plus an asynchronous best-effort reporter;
- protocol-neutral Runtime, KV, Kubernetes, and DCGM semantic adapters with a
  shared correlation resolver and request-scoped graph materialization;
- dependency-free vLLM/SGLang/Ray Serve/DCGM Prometheus, Kubernetes list-watch,
  and profiled Mooncake/LMCache metadata HTTP source clients with
  failure-isolated polling runners;
- a standard-library State Plane HTTP writer and installable
  `stateflow-adapter` process entry point, keeping collection off the request path;
- an optional protobuf/gRPC State Plane server and client with unary API parity,
  resumable server-streaming `WatchState`, and Semantic Adapter writer support;
- an in-memory backend and a small `urllib` OpenAI-compatible backend;
- a standard-library HTTP server for local integration tests.

## Run

```bash
python -m unittest discover -s tests -v
python -m stateflow --host 127.0.0.1 --port 8080
```

Example request:

```bash
curl -s http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'x-stateflow-session-id: demo-session' \
  -d '{"model":"logical-agent","messages":[{"role":"user","content":"hello"}],"max_tokens":32}'
```

StateFlow adds `x-stateflow-decision-id`, `x-stateflow-selected-model`, and
`x-stateflow-selected-replica` response headers. The state view is available
at `/v1/state/view?session_id=demo-session`.

The canonical v1.1 State Plane API is available under `/v1/state-plane/*`.
For example, `/v1/state-plane/schema`, `/v1/state-plane/freshness`, and
`POST /v1/state-plane/snapshots`. See
[`docs/STATE_PLANE_API_V1_1.md`](docs/STATE_PLANE_API_V1_1.md).

An installed package can run an observability source independently:

```bash
stateflow-adapter --source vllm \
  --source-endpoint http://runtime-a:8000 \
  --state-plane-url http://stateflow:8080 \
  --runtime-id runtime-a --instance-id replica-a
```

Install the optional transport and expose it beside the HTTP gateway with:

```bash
pip install '.[grpc]'
stateflow --host 127.0.0.1 --port 8080 --grpc-port 50051
```

An adapter can select it with `--state-plane-transport grpc` and a
`host:port` value in `--state-plane-url`.

## Integration boundary

The gateway accepts `ProviderNeutralRequest`. Register one or more
`TargetCandidate` objects and a backend adapter in `BackendRegistry`; the
scheduler remains independent from provider SDKs and inference engines. A
native harness can publish `AgentStateEvent` objects directly or use the
`HarnessAdapter` interface. State reporting failures are intentionally
non-fatal to model requests.

This repository is an MVP reference implementation. The current v1.1 State
Plane backend is in-memory; durable event/metric storage, distributed logical
snapshots, target-environment source validation, learned predictors, action
execution, authentication, and production metrics remain extension points.

See [`RUNBOOK.md`](RUNBOOK.md), [`docs/implementation.md`](docs/implementation.md),
[`docs/DEVELOPMENT_PLAN_V1_1.md`](docs/DEVELOPMENT_PLAN_V1_1.md), and
[`docs/OBSERVABILITY_BRIDGE_V1_1.md`](docs/OBSERVABILITY_BRIDGE_V1_1.md) for the
validation flow, design-to-code mapping, phased roadmap, and M3 adapter contract.
