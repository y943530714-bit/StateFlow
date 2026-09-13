# Agent-aware KV dry-run control v1.1

`AgentAwareKVController` implements the first M5 control slice without invoking
a KV data-plane API. It reads one immutable snapshot, generates owner-authorized
`keep`, `offload`, `prefetch`, and `migrate` candidates, predicts every
candidate, selects one action, and journals a complete `DecisionRecord` bundle.
The returned action always has `dry_run=True`.

## Inputs and ownership

`KVControlRequest` identifies the snapshot, KV object, optional agent, and the
available storage targets. `KVActionOwner` defines the component that may
eventually execute the action, its managed KV prefixes, and allowed action
types.

Planning rejects the request when:

- the KV ref is outside the action owner's managed prefixes;
- the KV entity or action-owner component is absent;
- the KV state has no authoritative owner;
- `expected_state_owner` no longer matches the entity owner;
- the current location is missing from the target set; or
- no owner-authorized `keep` baseline can be generated.

State ownership and action ownership are intentionally distinct. The former
identifies who published authoritative KV state; the latter identifies the
domain component allowed to execute a proposed move.

## Selection policy

The initial deterministic policy is conservative:

| Condition | Proposed action |
| --- | --- |
| HBM pressure is high, Agent is waiting/blocked/paused, reuse is low | `offload` |
| KV is outside HBM, expected resume is near, reuse is high | `prefetch` |
| No trigger or any safety gate fails | `keep` |

Candidates are evaluated by the analytical Prediction service. A move requires
a complete critical prediction, sufficient confidence, and a transfer ETA.
Prefetch additionally requires projected HBM pressure not to exceed the
configured guard. An unknown or active `kv.transfer_state` also forces `keep`.

The Prediction model now accepts `hbm_release_bytes`, so offload candidates
report post-release HBM pressure rather than the unchanged current pressure.
Zero bandwidth and zero HBM capacity are treated as unavailable critical
features, not valid complete predictions.

## Action safety envelope

Every proposed action carries:

- immutable snapshot token and logical time;
- expected KV location, transfer state, and state owner;
- explicit action owner and deterministic idempotency key;
- a proposed HBM reservation for prefetch/HBM migration;
- timeout derived from transfer ETA;
- rollback metadata pointing to the original location; and
- `dry_run=True`.

No reserve, commit, transfer, or rollback call is made in this milestone. The
next controlled-loop step must add a domain-owner Action Adapter, atomic
reservation lifecycle, stale-decision checks, cooldown/hysteresis, outcome
collection, and kill-switch metrics before execution can be enabled.

## Validation

`tests/test_kv_control.py` covers offload, prefetch, proposed reservations,
snapshot/prediction fallback, transfer-state and HBM guards, owner scope, and
state-owner preconditions. `tests/test_prediction.py` covers HBM release and
invalid zero-rate behavior.
