# Shadow Control Evaluation v1.1

`InMemoryControlJournal` records a validated Decision/Prediction bundle and
joins the selected prediction to an `Outcome` by `action_id`. It never invokes
an Action Adapter, so recording a decision has no serving or infrastructure
side effects.

For multi-candidate decisions, `selected_action.parameters.candidate_id` is
required. The Success-First conversion bridge now writes this identity
explicitly. The journal also verifies that candidate IDs, prediction IDs,
snapshot ID, and model versions match the Decision record before accepting it.

## Feedback

When an outcome arrives, the journal materializes a `Feedback` record with:

- absolute latency and cost errors;
- absolute percentage errors when the actual value is greater than zero;
- success absolute error and Brier score;
- predicted probability and observed success calibration sample;
- actual policy KPI values and the prediction replay pointer.

Repeated identical Decision or Outcome writes are idempotent. Conflicting
writes are rejected instead of replacing the original shadow evidence.

## Aggregate report

`journal.report()` provides:

- latency and cost MAE;
- latency and cost MAPE as a ratio, excluding zero-valued actuals only from the
  MAPE denominator;
- prediction coverage over observed latency, cost, and success fields;
- success Brier score and binned expected calibration error (ECE);
- fallback rate and non-empty calibration-bin details.

Reports can be filtered by `model_version`.

```python
from stateflow.control import InMemoryControlJournal

journal = InMemoryControlJournal()
journal.record_decision(decision, predictions)
feedback = journal.record_outcome(outcome)
report = journal.report(model_version="analytical-0.1")
```

## Controlled policy replay

`controlled_replay()` applies `PolicyIntent.interactive()`, `critical()`, or
`batch()` objective order to the predictions from one historical decision.
Hard confidence, success, latency, cost, model-version, and fallback
constraints run before the lexicographic objective gates. Unknown primary
objectives fail closed.

The result is always `dry_run=True`. It identifies the historical baseline and
replayed selection, records every rejection and objective gate, and reports
predicted success delta, latency reduction, cost reduction, and throughput
delta where both candidates contain the required prediction. It does not
construct or dispatch an executable action.

```python
from stateflow.control import PolicyIntent, ReplayConstraints, controlled_replay

record = journal.get(decision_id)
replay = controlled_replay(
    record,
    PolicyIntent.critical(),
    constraints=ReplayConstraints(min_success_probability=0.9),
)
```

Durable journal storage, retention, tenant isolation, and production traffic
capture remain production work.
