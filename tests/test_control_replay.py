from __future__ import annotations

import unittest

from stateflow.control import (
    Action,
    DecisionRecord,
    JoinedControlRecord,
    NoReplayCandidate,
    Objective,
    PolicyIntent,
    ReplayConstraints,
    controlled_replay,
)
from stateflow.prediction import (
    CostPrediction,
    PerformancePrediction,
    Prediction,
    ReliabilityPrediction,
)


def _prediction(
    candidate_id: str,
    *,
    success: float,
    latency: float,
    cost: float,
    throughput: float,
) -> Prediction:
    return Prediction(
        snapshot_id="snapshot-replay",
        candidate_id=candidate_id,
        prediction_id=f"prediction-{candidate_id}",
        performance=PerformancePrediction(
            e2e_seconds=latency,
            throughput=throughput,
        ),
        reliability=ReliabilityPrediction(success_probability=success),
        cost=CostPrediction(monetary_cost=cost),
        confidence=0.9,
        model_version="analytical-0.1",
        applicability="routing",
        fallback="none",
    )


def _record() -> JoinedControlRecord:
    predictions = (
        _prediction("a", success=0.95, latency=2.0, cost=2.0, throughput=50),
        _prediction("b", success=0.93, latency=1.0, cost=1.0, throughput=80),
        _prediction("c", success=0.80, latency=0.5, cost=0.2, throughput=100),
    )
    decision = DecisionRecord(
        decision_id="decision-baseline",
        snapshot_id="snapshot-replay",
        policy_version="success-first-0.1",
        candidate_ids=tuple(item.candidate_id for item in predictions),
        prediction_ids=tuple(item.prediction_id for item in predictions),
        model_versions=("analytical-0.1",),
        selected_action=Action(
            action_id="action-baseline",
            action_type="route",
            target_component="runtime-a",
            parameters={"candidate_id": "a"},
            dry_run=True,
        ),
        reason="baseline",
    )
    return JoinedControlRecord(decision, predictions)


class ControlledReplayTests(unittest.TestCase):
    def test_policy_objective_order_changes_shadow_selection(self) -> None:
        record = _record()

        critical = controlled_replay(record, PolicyIntent.critical())
        interactive = controlled_replay(record, PolicyIntent.interactive())
        batch = controlled_replay(record, PolicyIntent.batch())

        self.assertEqual(critical.selected_candidate_id, "b")
        self.assertEqual(interactive.selected_candidate_id, "c")
        self.assertEqual(batch.selected_candidate_id, "c")
        self.assertTrue(critical.changed)
        self.assertTrue(critical.dry_run)
        self.assertAlmostEqual(critical.potential_gain["success_delta"], -0.02)
        self.assertAlmostEqual(
            critical.potential_gain["latency_reduction_seconds"], 1.0
        )
        self.assertAlmostEqual(
            critical.potential_gain["monetary_cost_reduction"], 1.0
        )
        self.assertIn("objective_gate:success", critical.rejections["c"])

    def test_hard_constraints_apply_before_lexicographic_policy(self) -> None:
        replay = controlled_replay(
            _record(),
            PolicyIntent.batch(),
            constraints=ReplayConstraints(min_success_probability=0.9),
        )

        self.assertEqual(replay.eligible_candidate_ids, ("a", "b"))
        self.assertEqual(replay.selected_candidate_id, "b")
        self.assertIn("success_below_minimum", replay.rejections["c"])

    def test_unknown_primary_objective_fails_closed(self) -> None:
        with self.assertRaisesRegex(NoReplayCandidate, "energy is unavailable"):
            controlled_replay(
                _record(),
                PolicyIntent("energy", (Objective.ENERGY,)),
            )


if __name__ == "__main__":
    unittest.main()
