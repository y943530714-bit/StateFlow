# Prediction MVP v1.1

`stateflow.prediction.PredictionService` evaluates a `CandidateAction` against
one immutable State Plane snapshot. It is currently a shadow service: it does
not change the Success-First request path or execute actions.

## Analytical baseline

The first model is deterministic and reports its version as
`analytical-0.1`:

- transfer time = `setup + transfer_bytes / effective_bw`;
- prefill time = uncached prompt tokens / prefill throughput;
- decode time = expected output tokens / decode throughput;
- routing E2E = queue + transfer + prefill + decode;
- predicted HBM bytes = used + reserved + predicted KV growth;
- GPU seconds = routing E2E × allocated GPU;
- monetary cost = fixed + GPU + network + storage terms.

Dynamic features come from canonical snapshot keys. Candidate parameters are
used for action-specific quantities and static target capabilities:

| Candidate parameter | Snapshot fallback |
| --- | --- |
| `queue_seconds` | `request.queue_time` |
| `transfer_bytes` | `kv.size` |
| `effective_bw_bytes_per_second` | `link.effective_bw` |
| `prompt_tokens` | `request.context_tokens` |
| `expected_output_tokens` | `request.expected_output_tokens` |
| `prefill_tokens_per_second` | `runtime.prefill_tps` |
| `decode_tokens_per_second` | `runtime.decode_tps` |
| `hbm_used_bytes` / `hbm_reserved_bytes` | `resource.hbm.*` |
| `allocated_gpu` | `instance.allocated_gpu` |
| `queue_depth` | `runtime.queue_depth` |
| `gpu_utilization` | `resource.gpu.util` |
| `target_kv_location` | `kv.location` |

Other explicit parameters include `cached_tokens`, `transfer_setup_seconds`,
`predicted_kv_growth_bytes`, `hbm_capacity_bytes`, `deadline_seconds`, and the
`fixed_cost`, `gpu_second_cost`, `network_byte_cost`, and
`storage_io_byte_cost` accounting rates.

## Unknown and fallback semantics

A critical rate is required only when the corresponding work amount is
positive. Missing bandwidth, prefill throughput, decode throughput, or HBM
capacity produces an unknown output rather than an optimistic default. The
prediction records missing or ambiguous features in `notes`, reports observed
feature age in `feature_freshness_ms`, and derives confidence from required
feature coverage and snapshot completeness.

`PredictionService` marks low-confidence or failed model evaluations for the
existing `success-first-heuristic` fallback. This keeps the analytical model
off the request hot path until journal, replay, and calibration acceptance
criteria are met.

## Journal and replay

Every service prediction is appended to a thread-safe shadow journal together
with a deep copy of its request and immutable snapshot. Records can be filtered
by `snapshot_id` and `model_version`. `replay(prediction_id)` uses the recorded
snapshot rather than current State Plane values, so eviction or later writes do
not change the original replay. Additional model versions can be registered to
compare the same recorded input with a newer model.

```python
from stateflow.prediction import CandidateAction, PredictionRequest, PredictionService

service = PredictionService(state_plane)
prediction = service.predict(
    PredictionRequest(
        snapshot_id=snapshot.token,
        candidate_action=CandidateAction(
            action_type="route",
            target_component="component/component/runtime-a",
            parameters={
                "cached_tokens": 4096,
                "hbm_capacity_bytes": 80_000_000_000,
                "gpu_second_cost": 0.0008,
            },
        ),
    )
)
```

The next increment joins selected predictions and decisions to outcomes and
materializes aggregate MAE/MAPE, coverage, and calibration reports.
