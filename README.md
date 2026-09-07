# StateFlow

StateFlow is a dependency-free MVP for scheduling stateful-agent requests with
the design document's strict lexicographic policy:

1. maximize conservative task-success probability;
2. among success-equivalent targets, minimize KV-aware effective cost;
3. among cost-equivalent targets, minimize predicted latency and choose a
   session-aware replica.

The implementation includes:

- an Agent State v0.1 schema with event, snapshot, and hot scheduling view;
- a thread-safe in-memory state store with epoch/sequence de-duplication and
  TTL-aware freshness metadata;
- capability/security hard filtering, critical overrides, LCB success gating,
  cost/KV valuation, latency estimation, placement, and hysteresis;
- OpenAI Chat/Responses and Anthropic Messages normalization;
- a generic proxy harness adapter plus an asynchronous best-effort reporter;
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

## Integration boundary

The gateway accepts `ProviderNeutralRequest`. Register one or more
`TargetCandidate` objects and a backend adapter in `BackendRegistry`; the
scheduler remains independent from provider SDKs and inference engines. A
native harness can publish `AgentStateEvent` objects directly or use the
`HarnessAdapter` interface. State reporting failures are intentionally
non-fatal to model requests.

This repository is an MVP reference implementation. The durable event log,
distributed snapshot store, tokenizer service, learned predictor, streaming
passthrough, authentication, and production metrics are extension points.

See [`RUNBOOK.md`](RUNBOOK.md) and [`docs/implementation.md`](docs/implementation.md)
for the validation flow and design-to-code mapping.
