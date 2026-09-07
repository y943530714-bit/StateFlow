from __future__ import annotations

import unittest

from stateflow.scheduler.harness.success_first import SuccessFirstScheduler
from stateflow.scheduler.harness.success_first.success_gate import success_gate
from stateflow.scheduler.types import CandidateEvaluation, NoFeasibleTarget, SchedulerConfig
from stateflow.state.schema import AgentPhase, HarnessSchedulingView, TargetCandidate


def make_view(**overrides) -> HarnessSchedulingView:
    values = dict(
        session_id="session-1",
        task_id="task-1",
        state_id="state-1",
        state_version=7,
        session_turn=0,
        phase=AgentPhase.RUNNABLE,
        runnable=True,
        prompt_tokens=100,
        predicted_output_tokens=20,
        state_completeness=1.0,
        state_freshness=1.0,
    )
    values.update(overrides)
    return HarnessSchedulingView(**values)


def target(model: str, replica: str, **overrides) -> TargetCandidate:
    values = dict(
        model_id=model,
        endpoint_id=f"endpoint-{replica}",
        replica_id=replica,
        base_success=0.95,
        base_uncertainty=0.01,
        inference_cost=0.1,
        fallback_path_cost=0.2,
        prefill_tokens_per_second=1000.0,
        decode_tokens_per_second=100.0,
    )
    values.update(overrides)
    return TargetCandidate(**values)


class SuccessFirstSchedulerTests(unittest.TestCase):
    def test_success_gate_is_lexicographically_first(self) -> None:
        view = make_view()
        cheap_but_risky = target(
            "cheap",
            "r0",
            tier="efficient",
            base_success=0.78,
            inference_cost=0.001,
        )
        reliable = target(
            "reliable",
            "r0",
            tier="capable",
            base_success=0.96,
            inference_cost=1.0,
        )

        decision = SuccessFirstScheduler().schedule(view, [cheap_but_risky, reliable])

        self.assertEqual(decision.selected_model, "reliable")
        self.assertEqual(decision.decision_reason.value, "SUCCESS_DOMINANT")
        by_model = {item.model_id: item for item in decision.candidates}
        self.assertFalse(by_model["cheap"].success_gate)
        self.assertTrue(by_model["reliable"].success_gate)

    def test_cost_is_used_only_among_success_equivalent_models(self) -> None:
        view = make_view()
        cheap = target("cheap", "r0", inference_cost=0.01, base_success=0.94)
        expensive = target("expensive", "r0", inference_cost=0.4, base_success=0.95)

        decision = SuccessFirstScheduler().schedule(view, [cheap, expensive])

        self.assertEqual(decision.selected_model, "cheap")
        self.assertEqual(decision.decision_reason.value, "COST_DOMINANT")
        selected = next(item for item in decision.candidates if item.selected)
        self.assertLess(selected.effective_cost, 0.1)

    def test_latency_breaks_an_exact_cost_tie(self) -> None:
        view = make_view()
        slow = target("model-a", "slow", inference_cost=0.1, queue_latency_seconds=0.5)
        fast = target("model-b", "fast", inference_cost=0.1, queue_latency_seconds=0.1)

        decision = SuccessFirstScheduler().schedule(view, [slow, fast])

        self.assertEqual(decision.selected_replica, "fast")
        self.assertEqual(decision.decision_reason.value, "LATENCY_TIEBREAK")

    def test_future_kv_value_can_make_local_cache_the_cheapest_target(self) -> None:
        view = make_view(
            continuation_probability=0.9,
            kv_reuse_probability=0.9,
            predicted_remaining_turns=2,
        )
        local = target(
            "local",
            "r0",
            inference_cost=0.11,
            local_kv_tokens=100,
            kv_recompute_cost_per_token=0.001,
        )
        cold = target(
            "cold",
            "r0",
            inference_cost=0.01,
            kv_recompute_cost_per_token=0.001,
        )

        decision = SuccessFirstScheduler().schedule(view, [local, cold])

        self.assertEqual(decision.selected_model, "local")
        selected = next(item for item in decision.candidates if item.selected)
        self.assertGreater(selected.future_kv_value, 0.0)
        self.assertEqual(selected.local_kv_tokens, 100)

    def test_critical_override_excludes_efficient_targets(self) -> None:
        view = make_view(failure_streak=2, consecutive_failures=2)
        efficient = target(
            "efficient",
            "r0",
            tier="efficient",
            base_success=0.99,
            inference_cost=0.001,
        )
        capable = target(
            "capable",
            "r0",
            tier="capable",
            base_success=0.91,
            inference_cost=0.2,
        )

        decision = SuccessFirstScheduler().schedule(view, [efficient, capable])

        self.assertEqual(decision.selected_model, "capable")
        self.assertEqual(decision.decision_reason.value, "CRITICAL_OVERRIDE")
        self.assertIn("failure_streak", decision.override_reasons)
        efficient_eval = next(item for item in decision.candidates if item.model_id == "efficient")
        self.assertFalse(efficient_eval.eligible)
        self.assertIn("critical_override_capable_only", efficient_eval.rejection_reasons)

    def test_predictor_uncertainty_switches_to_safe_capable_policy(self) -> None:
        view = make_view()
        uncertain_efficient = target(
            "uncertain-efficient",
            "r0",
            tier="efficient",
            base_success=0.99,
            base_uncertainty=0.30,
            inference_cost=0.001,
        )
        capable = target(
            "safe-capable",
            "r0",
            tier="capable",
            base_success=0.92,
            base_uncertainty=0.01,
            inference_cost=0.2,
        )

        decision = SuccessFirstScheduler().schedule(view, [uncertain_efficient, capable])

        self.assertEqual(decision.selected_model, "safe-capable")
        self.assertIn("predictor_uncertainty_or_ood", decision.override_reasons)

    def test_hysteresis_holds_current_capable_tier_during_deescalation(self) -> None:
        view = make_view(
            session_turn=1,
            current_model="capable",
            current_tier="capable",
            stable_steps=0,
        )
        capable = target(
            "capable",
            "capable-r0",
            tier="capable",
            inference_cost=0.10,
            queue_latency_seconds=0.1,
        )
        efficient = target(
            "efficient",
            "efficient-r0",
            tier="efficient",
            inference_cost=0.096,
            queue_latency_seconds=0.0,
        )

        decision = SuccessFirstScheduler().schedule(view, [capable, efficient])

        self.assertEqual(decision.selected_model, "capable")
        self.assertEqual(decision.decision_reason.value, "HYSTERESIS_HOLD")

    def test_hard_filters_are_not_overruled_by_a_good_score(self) -> None:
        view = make_view(
            required_capabilities={"reasoning"},
            security_domain="restricted",
            allowed_regions={"eu"},
        )
        invalid = target(
            "invalid",
            "r0",
            capabilities=set(),
            region="us",
            security_domain="public",
            base_success=1.0,
        )

        with self.assertRaises(NoFeasibleTarget) as context:
            SuccessFirstScheduler().schedule(view, [invalid])
        reasons = context.exception.rejections[0].reasons
        self.assertIn("missing_capabilities:reasoning", reasons)
        self.assertIn("region_not_allowed", reasons)
        self.assertIn("security_domain_mismatch", reasons)

    def test_hard_cost_budget_rejects_targets_before_routing(self) -> None:
        view = make_view(cost_budget=0.05)
        over_budget = target("expensive", "r0", inference_cost=0.10)

        with self.assertRaises(NoFeasibleTarget) as context:
            SuccessFirstScheduler().schedule(view, [over_budget])
        self.assertIn("hard_cost_budget_exceeded", context.exception.rejections[0].reasons)

    def test_success_gate_falls_back_to_highest_lcb_when_no_model_meets_minimum(self) -> None:
        evaluations = [
            CandidateEvaluation(model_id="a", endpoint_id="e", replica_id="r", tier="efficient", success_lcb=0.7),
            CandidateEvaluation(model_id="b", endpoint_id="e", replica_id="r", tier="capable", success_lcb=0.8),
        ]

        result = success_gate(evaluations, s_min=0.9, epsilon_success=0.03)

        self.assertTrue(result.fallback)
        self.assertEqual(result.model_ids, frozenset({"b"}))


if __name__ == "__main__":
    unittest.main()
