# StateFlow

## Control plane v1 (new codex branch)

This branch implements the small control-plane design in
[`docs/CONTROL_PLANE_V1.md`](docs/CONTROL_PLANE_V1.md). A harness can call the
OpenAI-compatible `/v1/chat/completions` endpoint without a harness-specific
adapter. The configured backend is reached through a provider-neutral adapter;
StateFlow selects an available model/replica, forwards the request, and returns
the backend response. OpenAI Chat SSE streaming is forwarded without buffering.

To connect an actual OpenAI-compatible server, copy
[`examples/local_gateway.json`](examples/local_gateway.json), replace the URL
and served model ID, then run:

```bash
python -m stateflow --config examples/local_gateway.json --host 127.0.0.1 --port 8080
```

The same URL can be used by any harness that supports an OpenAI-compatible
base URL. `x-stateflow-program-id` links the harness's turns; without it each
request is a separate program. Additional backends and target replicas can be
added to the JSON configuration. Target `protocols` declare which of
`openai_chat`, `openai_responses`, and `anthropic_messages` the upstream
actually supports. StateFlow forwards each protocol to a backend implementing
that endpoint; it does not translate between provider protocols. `base_success`
and `base_uncertainty` are operator-supplied priors, not measured guarantees.

The same **Unified State Interface** also exposes:

| API | Use |
| --- | --- |
| `POST /v1/control/state` | Report Agent/infra events and action feedback (`program_id`, `event_type`, `source`, `payload`). |
| `GET /v1/control/state?program_id=...` | Read the hot scheduling view. |
| `POST /v1/control/decisions` | Ask for a route action synchronously when an external scheduler sends the model request itself; this does **not** execute the action. |
| `GET /v1/control/actions` | List the declarative Action Catalog. |

Example of one harness event and one synchronous routing decision:

```bash
curl -s http://127.0.0.1:8080/v1/control/state -H 'content-type: application/json' \
  -d '{"program_id":"run-1","event_type":"TASK_STARTED","source":"harness","payload":{"identity":{"harness_type":"coding-agent"}}}'
curl -s http://127.0.0.1:8080/v1/control/decisions -H 'content-type: application/json' \
  -H 'x-stateflow-program-id: run-1' \
  -d '{"model":"logical-agent","messages":[{"role":"user","content":"hello"}]}'
```

`route_model` is the only live action in v1. Actual request forwarding is
performed by the configured backend adapter. KV pinning, Sandbox actions and
tool speculation need explicit support from those components and are not
presented as executable actions. Without enough fresh state the scheduler
prefers a capable target when one is available. An unavailable backend returns 502;
in-process state and decisions are reset on restart. Configure authentication
and network access at the deployment boundary when serving other machines.

An inference backend can post a global `TARGET_UPDATED` event to
`/v1/control/state` with `payload` containing the configured `model_id`,
`endpoint_id`, `replica_id`, and any of `healthy`, `available`,
`queue_latency_seconds`, `load_balance_score`, `local_kv_tokens` or
`remote_kv_tokens`. A report expires after `ttl_seconds` (default 30), then
the configured target values apply again; no `program_id` is required.

## Earlier reference implementation

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
- a shadow-only analytical Prediction service for deterministic transfer,
  routing latency, HBM pressure, and cost estimates, with explicit fallback
  and snapshot/model-version replay;
- an in-memory shadow Decision/Prediction/Outcome journal with MAE/MAPE,
  coverage, Brier score, and binned calibration reporting;
- side-effect-free interactive/critical/batch lexicographic policy replay with
  hard constraints and baseline-relative predicted gain;
- an Agent-aware KV dry-run controller with owner-scoped keep/offload/prefetch/
  migrate candidates, transfer/HBM guards, proposed reservations, and rollback
  preconditions;
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
The M5 dry-run control boundary is documented in
[`docs/KV_CONTROL_DRY_RUN_V1_1.md`](docs/KV_CONTROL_DRY_RUN_V1_1.md).
