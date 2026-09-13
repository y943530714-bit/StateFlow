# Design-to-code mapping

The original scheduler/state documents and Architecture Design Final v1.1 are
implemented as an incrementally compatible Python reference stack.

| Design concern | Implementation |
| --- | --- |
| Canonical State/Metric contracts and A0-A4 authority | `stateflow/state/contracts.py` |
| Canonical key schema/alias/TTL registry | `stateflow/state/registry.py` |
| Component/Deployment graphs and relation guardrails | `stateflow/state/plane.py` |
| Immutable snapshot, freshness/completeness, change cursor | `stateflow/state/plane.py` |
| Candidate-Action Prediction contract | `stateflow/prediction/` |
| Shadow analytical prediction model/service + replay journal | `stateflow/prediction/model.py`, `service.py` |
| Policy/Decision/Action/Outcome/Feedback contracts | `stateflow/control/` |
| Shadow control join and prediction evaluation | `stateflow/control/journal.py`, `evaluation.py` |
| Side-effect-free lexicographic policy replay | `stateflow/control/replay.py` |
| Existing scheduler → v1.1 replay record bridge | `stateflow/scheduler/contract_bridge.py` |
| Southbound/Northbound HTTP facade | `stateflow/state/api.py`, `stateflow/gateway/server/http.py` |
| Gateway Request/Runtime/Instance projection | `stateflow/adapters/gateway.py` |
| Stable ID and cross-source correlation | `stateflow/adapters/identity.py` |
| Runtime/KV/Kubernetes/DCGM semantic adapters | `stateflow/adapters/runtime.py`, `kv.py`, `kubernetes.py`, `dcgm.py` |
| Request-scoped two-graph materialization | `stateflow/adapters/bridge.py` |
| HTTP/Prometheus source ingestion | `stateflow/adapters/source.py`, `clients.py` |
| Failure-isolated polling process loop | `stateflow/adapters/runner.py` |
| Independent adapter process + HTTP/gRPC writer | `stateflow/adapters/process.py`, `stateflow/state/http_client.py`, `stateflow/rpc/client.py` |
| Optional State Plane gRPC server/client | `proto/stateflow.proto`, `stateflow/rpc/` |
| Agent State header and full schema | `stateflow/state/schema.py` |
| Event + snapshot + hot view | `stateflow/state/event.py`, `stateflow/state/store/in_memory.py`, `stateflow/state/scheduling_view/` |
| Success LCB and success gate | `success_predictor.py`, `success_gate.py` |
| Hard capability/security filters | `capability_filter.py` |
| Critical override / Unknown → Safe | `critical_override.py`, scheduler policy path |
| KV-aware effective cost and future value | `cost_model.py`, `kv_value.py`, `cost_gate.py` |
| Queue/KV/prefill/decode latency | `latency_model.py` |
| Session locality, migration penalty, hysteresis | `placement.py`, `scheduler.py` |
| OpenAI/Anthropic protocol normalization | `gateway/normalizer/request.py` |
| Generic proxy and thin asynchronous state plane | `harness/` |
| Backend/data-plane boundary | `backend/`, `gateway/service.py` |
| Local HTTP harness | `gateway/server/http.py`, `stateflow/demo.py` |

The existing serving hot path remains intentionally explicit:

```text
normalize -> state event -> hard filter -> critical override
          -> success LCB gate -> cost/KV gate -> latency/placement
          -> backend adapter -> response/state event
```

The scheduler never uses a weighted blend of success, cost, and latency. Each
later objective only operates on the candidates that survive the preceding
gate. The current MVP stores prompt token counts and KV metadata, not prompt
content or KV tensors.

The v1.1 State Plane is additive at this stage.  It provides the generic
read-only substrate while `InMemoryStateStore` continues to materialize the
legacy `HarnessSchedulingView`.  The migration seam is
`routing_control_bundle()`: it converts the current routing result into common
Prediction and Decision/Action records without changing request behavior.

The canonical HTTP surface is documented in
[`STATE_PLANE_API_V1_1.md`](STATE_PLANE_API_V1_1.md). The existing
`/v1/state/events` endpoint remains available for legacy AgentState events;
new canonical operations use `/v1/state-plane/*`.

The M3 observability bridge is transport-neutral. Source-specific collectors
convert owner APIs, watches, or metric windows into the observation records in
`stateflow.adapters`; the adapters then normalize IDs, keys, authority,
freshness, provenance, and relations before writing to the State Plane. See
[`OBSERVABILITY_BRIDGE_V1_1.md`](OBSERVABILITY_BRIDGE_V1_1.md).
