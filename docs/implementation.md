# Design-to-code mapping

The two supplied design documents are implemented as a small Python reference
stack.

| Design concern | Implementation |
| --- | --- |
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

The hot path is intentionally explicit:

```text
normalize -> state event -> hard filter -> critical override
          -> success LCB gate -> cost/KV gate -> latency/placement
          -> backend adapter -> response/state event
```

The scheduler never uses a weighted blend of success, cost, and latency. Each
later objective only operates on the candidates that survive the preceding
gate. The current MVP stores prompt token counts and KV metadata, not prompt
content or KV tensors.
