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

Reports can be filtered by `model_version`. This implementation is an
in-memory MVP; durable journal storage, retention, tenant isolation, and
offline controlled-policy replay remain production work.

```python
from stateflow.control import InMemoryControlJournal

journal = InMemoryControlJournal()
journal.record_decision(decision, predictions)
feedback = journal.record_outcome(outcome)
report = journal.report(model_version="analytical-0.1")
```
