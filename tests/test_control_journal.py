from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from stateflow.control import (
    Action,
    ActionStatus,
    DecisionRecord,
    InMemoryControlJournal,
    Outcome,
)
from stateflow.prediction import (
    CostPrediction,
    PerformancePrediction,
    Prediction,
    ReliabilityPrediction,
)


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _prediction(
    candidate_id: str,
    prediction_id: str,
    *,
    latency: float | None,
    cost: float | None,
    success: float | None,
    fallback: str = "none",
) -> Prediction:
    return Prediction(
        snapshot_id="snapshot-control",
        candidate_id=candidate_id,
        prediction_id=prediction_id,
        performance=PerformancePrediction(e2e_seconds=latency),
        reliability=ReliabilityPrediction(success_probability=success),
        cost=CostPrediction(monetary_cost=cost),
        confidence=0.9,
        model_version="analytical-0.1",
        applicability="routing",
        fallback=fallback,
    )


def _decision(
    decision_id: str,
    action_id: str,
    predictions: tuple[Prediction, ...],
    *,
    selected_candidate: str = "",
) -> DecisionRecord:
    parameters = (
        {"candidate_id": selected_candidate} if selected_candidate else {}
    )
    return DecisionRecord(
        decision_id=decision_id,
        snapshot_id="snapshot-control",
        policy_version="shadow-v1",
        candidate_ids=tuple(item.candidate_id for item in predictions),
        prediction_ids=tuple(item.prediction_id for item in predictions),
        model_versions=("analytical-0.1",),
        selected_action=Action(
            action_id=action_id,
            action_type="route",
            target_component="runtime-a",
            parameters=parameters,
            dry_run=True,
        ),
        reason="shadow",
    )


class ControlJournalTests(unittest.TestCase):
    def test_joins_selected_prediction_and_materializes_report(self) -> None:
        journal = InMemoryControlJournal()
        rejected = _prediction(
            "candidate-a",
            "prediction-a",
            latency=1.0,
            cost=0.5,
            success=0.9,
        )
        selected = _prediction(
            "candidate-b",
            "prediction-b",
            latency=1.5,
            cost=1.0,
            success=0.8,
        )
        first = _decision(
            "decision-a",
            "action-a",
            (rejected, selected),
            selected_candidate="candidate-b",
        )
        journal.record_decision(first, (rejected, selected))
        feedback = journal.record_outcome(
            Outcome(
                action_id="action-a",
                status=ActionStatus.COMMITTED,
                started_at=NOW,
                completed_at=NOW + timedelta(seconds=2),
                actual_latency_seconds=2.0,
                actual_cost=0.8,
                actual_success=True,
            )
        )

        fallback = _prediction(
            "candidate-c",
            "prediction-c",
            latency=None,
            cost=0.1,
            success=0.2,
            fallback="success-first-heuristic",
        )
        second = _decision("decision-b", "action-b", (fallback,))
        journal.record_decision(second, (fallback,))
        journal.record_outcome(
            Outcome(
                action_id="action-b",
                status=ActionStatus.FAILED,
                started_at=NOW,
                completed_at=NOW + timedelta(seconds=1),
                actual_latency_seconds=1.0,
                actual_cost=0.0,
                actual_success=False,
            )
        )

        report = journal.report(calibration_bin_count=10)

        self.assertEqual(feedback.prediction_id, "prediction-b")
        self.assertAlmostEqual(
            feedback.prediction_error["latency_absolute_error"], 0.5
        )
        self.assertEqual(feedback.replay_pointer, "prediction:prediction-b")
        self.assertEqual(report.samples, 2)
        self.assertEqual(report.observed_fields, 6)
        self.assertEqual(report.predicted_fields, 5)
        self.assertAlmostEqual(report.coverage, 5 / 6)
        self.assertEqual(report.latency_samples, 1)
        self.assertAlmostEqual(report.latency_mae or 0, 0.5)
        self.assertEqual(report.latency_mape_samples, 1)
        self.assertAlmostEqual(report.latency_mape or 0, 0.25)
        self.assertEqual(report.cost_samples, 2)
        self.assertAlmostEqual(report.cost_mae or 0, 0.15)
        self.assertEqual(report.cost_mape_samples, 1)
        self.assertAlmostEqual(report.cost_mape or 0, 0.25)
        self.assertAlmostEqual(report.success_brier_score or 0, 0.04)
        self.assertAlmostEqual(report.success_ece or 0, 0.2)
        self.assertAlmostEqual(report.fallback_rate, 0.5)
        self.assertEqual(sum(item.samples for item in report.calibration_bins), 2)

    def test_bundle_validation_and_outcome_idempotency(self) -> None:
        prediction = _prediction(
            "candidate-a",
            "prediction-a",
            latency=1.0,
            cost=0.5,
            success=0.9,
        )
        decision = _decision("decision-a", "action-a", (prediction,))
        journal = InMemoryControlJournal()

        first = journal.record_decision(decision, (prediction,))
        duplicate = journal.record_decision(decision, (prediction,))
        outcome = Outcome(
            action_id="action-a",
            status=ActionStatus.COMMITTED,
            started_at=NOW,
            completed_at=NOW + timedelta(seconds=1),
            actual_success=True,
        )
        feedback = journal.record_outcome(outcome)

        self.assertEqual(first, duplicate)
        self.assertEqual(journal.record_outcome(outcome), feedback)
        self.assertEqual(journal.get("decision-a").feedback, feedback)
        with self.assertRaisesRegex(ValueError, "conflicting outcome"):
            journal.record_outcome(
                Outcome(
                    action_id="action-a",
                    status=ActionStatus.FAILED,
                    started_at=NOW,
                    completed_at=NOW + timedelta(seconds=1),
                    actual_success=False,
                )
            )
        with self.assertRaisesRegex(KeyError, "unknown action"):
            journal.record_outcome(
                Outcome(
                    action_id="missing",
                    status=ActionStatus.FAILED,
                    started_at=NOW,
                    completed_at=NOW,
                )
            )

    def test_multiple_candidates_require_explicit_selected_candidate(self) -> None:
        first = _prediction(
            "candidate-a",
            "prediction-a",
            latency=1.0,
            cost=0.5,
            success=0.9,
        )
        second = _prediction(
            "candidate-b",
            "prediction-b",
            latency=2.0,
            cost=0.4,
            success=0.8,
        )
        decision = _decision("decision-a", "action-a", (first, second))

        with self.assertRaisesRegex(ValueError, "identify candidate_id"):
            InMemoryControlJournal().record_decision(decision, (first, second))


if __name__ == "__main__":
    unittest.main()
