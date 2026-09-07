"""Success → Cost → Latency/Placement scheduler.

The implementation follows the design document's strict lexicographic
pipeline.  It never combines success, cost, and latency into a weighted score.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

from ....state.schema import DecisionReason, HarnessSchedulingView, TargetCandidate
from ...types import (
    CandidateEvaluation,
    FilterRejection,
    NoFeasibleTarget,
    RoutingDecision,
    SchedulerConfig,
    SuccessEstimate,
)
from .capability_filter import hard_filter
from .cost_gate import cost_gate
from .cost_model import effective_cost
from .critical_override import CriticalOverride, capable_only, critical_override
from .decision import build_routing_decision
from .latency_model import predicted_latency
from .placement import choose_placement
from .success_gate import success_gate
from .success_predictor import HeuristicSuccessPredictor, SuccessPredictor, _tier_rank


DecisionSink = Callable[[HarnessSchedulingView, RoutingDecision], None]


@dataclass
class _EvaluatedTarget:
    target: TargetCandidate
    evaluation: CandidateEvaluation
    estimate: SuccessEstimate


class SuccessFirstScheduler:
    """Reference scheduler for StateFlow's request data plane."""

    def __init__(
        self,
        config: SchedulerConfig | None = None,
        *,
        predictor: SuccessPredictor | None = None,
        decision_sink: DecisionSink | None = None,
    ) -> None:
        self.config = config or SchedulerConfig()
        self.predictor = predictor or HeuristicSuccessPredictor()
        self.decision_sink = decision_sink

    def schedule(
        self,
        view: HarnessSchedulingView,
        targets: Iterable[TargetCandidate] | None = None,
    ) -> RoutingDecision:
        """Return a routing decision for one normalized model request."""

        target_list = list(targets if targets is not None else view.candidates)
        if not target_list:
            raise NoFeasibleTarget("no scheduling targets were registered")

        filtered = hard_filter(view, target_list)
        if not filtered.eligible:
            raise NoFeasibleTarget(
                "all scheduling targets failed hard capability/security filters",
                rejections=filtered.rejected,
            )

        override = critical_override(view, self.config)
        policy_targets = capable_only(filtered.eligible) if override.active else filtered.eligible
        estimates = self._estimate_by_model(view, policy_targets)
        if not override.active and self._predictor_requires_safe_mode(estimates.values()):
            override = CriticalOverride(
                active=True,
                reasons=("predictor_uncertainty_or_ood",),
            )
            policy_targets = capable_only(filtered.eligible)
            estimates = self._estimate_by_model(view, policy_targets)
        evaluated: list[_EvaluatedTarget] = []

        for target in policy_targets:
            estimate = estimates[target.model_id]
            estimate.with_lcb(self.config.beta)
            evaluation = CandidateEvaluation(
                model_id=target.model_id,
                endpoint_id=target.endpoint_id,
                replica_id=target.replica_id,
                tier=target.tier,
                eligible=True,
                predicted_success=estimate.predicted_success,
                success_uncertainty=estimate.uncertainty,
                success_confidence=estimate.confidence,
                success_lcb=estimate.lower_confidence_bound,
                metadata={
                    "predictor_version": estimate.predictor_version,
                    "predictor_notes": list(estimate.notes),
                    "load_balance_score": target.load_balance_score,
                },
            )
            item = _EvaluatedTarget(target, evaluation, estimate)
            evaluated.append(item)

        # The success gate is model-level.  Replicas of a model inherit the
        # model's conservative LCB and only enter cost comparison afterwards.
        gate = success_gate(
            [item.evaluation for item in evaluated],
            s_min=self.config.s_min,
            epsilon_success=self.config.epsilon_success,
        )
        success_models = set(gate.model_ids)
        fallback = gate.fallback

        success_items = [item for item in evaluated if item.target.model_id in success_models]
        for item in evaluated:
            item.evaluation.success_gate = item.target.model_id in success_models

        if not success_items:
            raise NoFeasibleTarget("success predictor produced no usable candidate")

        # Cost is evaluated only within the success-equivalent model set.
        for item in success_items:
            breakdown = effective_cost(view, item.target, item.estimate, self.config)
            item.evaluation.inference_cost = breakdown.inference_cost
            item.evaluation.kv_fetch_cost = breakdown.kv_fetch_cost
            item.evaluation.kv_recompute_cost = breakdown.kv_recompute_cost
            item.evaluation.expected_retry_cost = breakdown.expected_retry_cost
            item.evaluation.routing_overhead_cost = breakdown.routing_overhead_cost
            item.evaluation.future_kv_value = breakdown.future_kv_value
            item.evaluation.effective_cost = breakdown.effective_cost
            item.evaluation.local_kv_tokens = breakdown.local_kv_tokens
            item.evaluation.remote_kv_tokens = breakdown.remote_kv_tokens
            item.evaluation.miss_tokens = breakdown.miss_tokens

        if view.cost_budget is not None:
            budget = float(view.cost_budget)
            within_budget = [
                item
                for item in success_items
                if item.evaluation.effective_cost <= budget + 1e-12
            ]
            if not within_budget:
                raise NoFeasibleTarget(
                    "all success-eligible targets exceeded the hard cost budget",
                    rejections=[
                        FilterRejection(
                            item.target.model_id,
                            item.target.endpoint_id,
                            item.target.replica_id,
                            ("effective_cost_budget_exceeded",),
                        )
                        for item in success_items
                    ],
                )
            success_items = within_budget

        cost_items = cost_gate(
            [item.evaluation for item in success_items],
            self.config.epsilon_cost,
        )
        cost_keys = {
            (item.model_id, item.endpoint_id, item.replica_id)
            for item in cost_items
        }
        for item in success_items:
            item.evaluation.cost_gate = (
                item.target.model_id,
                item.target.endpoint_id,
                item.target.replica_id,
            ) in cost_keys

        # Latency is the final objective after both gates.
        cost_pairs: list[tuple[TargetCandidate, CandidateEvaluation]] = []
        item_by_key = {
            (item.target.model_id, item.target.endpoint_id, item.target.replica_id): item
            for item in success_items
        }
        for evaluation in cost_items:
            item = item_by_key[(evaluation.model_id, evaluation.endpoint_id, evaluation.replica_id)]
            latency = predicted_latency(view, item.target)
            evaluation.queue_latency_seconds = latency.queue_latency_seconds
            evaluation.kv_latency_seconds = latency.kv_latency_seconds
            evaluation.prefill_latency_seconds = latency.prefill_latency_seconds
            evaluation.decode_latency_seconds = latency.decode_latency_seconds
            evaluation.predicted_latency_seconds = latency.predicted_latency_seconds
            evaluation.critical_path_latency_seconds = latency.critical_path_latency_seconds
            cost_pairs.append((item.target, evaluation))

        placement = choose_placement(view, cost_pairs)
        selected = placement.evaluation
        reason = self._decision_reason(
            view,
            selected,
            success_items,
            cost_items,
            placement.reason,
            override.active,
            fallback,
        )

        # Asymmetric hysteresis: upgrade immediately, downgrade only after a
        # stable window, and only if the current tier survived both gates.
        if not override.active and self._should_hold_tier(view, selected, cost_pairs):
            hold_pairs = [
                pair for pair in cost_pairs
                if pair[0].model_id == view.current_model
            ]
            if hold_pairs:
                held = choose_placement(view, hold_pairs)
                selected = held.evaluation
                reason = DecisionReason.HYSTERESIS_HOLD

        # Include hard-filter rejections in the persisted candidate trace.
        all_evaluations: list[CandidateEvaluation] = []
        rejection_by_key = {
            (item.model_id, item.endpoint_id, item.replica_id): item
            for item in filtered.rejected
        }
        evaluated_by_key = {
            (item.target.model_id, item.target.endpoint_id, item.target.replica_id): item.evaluation
            for item in evaluated
        }
        for target in target_list:
            key = (target.model_id, target.endpoint_id, target.replica_id)
            if key in evaluated_by_key:
                all_evaluations.append(evaluated_by_key[key])
            elif key in rejection_by_key:
                rejected = rejection_by_key[key]
                all_evaluations.append(
                    CandidateEvaluation(
                        model_id=target.model_id,
                        endpoint_id=target.endpoint_id,
                        replica_id=target.replica_id,
                        tier=target.tier,
                        eligible=False,
                        rejection_reasons=list(rejected.reasons),
                    )
                )
            else:
                # An efficient target removed by critical override is retained
                # in the decision trace as policy-ineligible.
                all_evaluations.append(
                    CandidateEvaluation(
                        model_id=target.model_id,
                        endpoint_id=target.endpoint_id,
                        replica_id=target.replica_id,
                        tier=target.tier,
                        eligible=False,
                        rejection_reasons=["critical_override_capable_only"],
                    )
                )

        decision = build_routing_decision(
            view,
            selected,
            all_evaluations,
            reason=reason,
            config=self.config,
            override_reasons=list(override.reasons),
            fallback=fallback,
        )
        if self.decision_sink is not None:
            try:
                self.decision_sink(view, decision)
            except Exception:
                # State/observability writes must not break the model request.
                pass
        return decision

    def _predictor_requires_safe_mode(self, estimates) -> bool:
        threshold = max(0.0, self.config.predictor_uncertainty_override)
        for estimate in estimates:
            if estimate.uncertainty >= threshold:
                return True
            if any(
                note.startswith("predictor_error:") or "out_of_distribution" in note
                for note in estimate.notes
            ):
                return True
        return False

    def _estimate_by_model(
        self,
        view: HarnessSchedulingView,
        targets: list[TargetCandidate],
    ) -> dict[str, SuccessEstimate]:
        estimates: dict[str, SuccessEstimate] = {}
        for target in targets:
            if target.model_id in estimates:
                continue
            try:
                estimate = self.predictor.estimate(view, target)
            except Exception as exc:
                # Predictor unavailable/OOD is Unknown → Safe.  A conservative
                # estimate lets the capable fallback remain available.
                estimate = SuccessEstimate(
                    predicted_success=0.0,
                    uncertainty=1.0,
                    confidence=0.0,
                    predictor_version="fallback-unavailable",
                    notes=[f"predictor_error:{type(exc).__name__}"],
                )
            estimates[target.model_id] = estimate
        return estimates

    def _should_hold_tier(
        self,
        view: HarnessSchedulingView,
        selected: CandidateEvaluation,
        cost_pairs: list[tuple[TargetCandidate, CandidateEvaluation]],
    ) -> bool:
        if not view.current_tier or not view.current_model:
            return False
        current_rank = _tier_rank(view.current_tier)
        selected_rank = _tier_rank(selected.tier)
        return (
            current_rank > selected_rank
            and view.stable_steps < self.config.deescalation_stable_steps
            and any(item[0].model_id == view.current_model for item in cost_pairs)
        )

    def _decision_reason(
        self,
        view: HarnessSchedulingView,
        selected: CandidateEvaluation,
        success_items: list[_EvaluatedTarget],
        cost_items: list[CandidateEvaluation],
        placement_reason: str,
        override_active: bool,
        fallback: bool,
    ) -> DecisionReason:
        if override_active:
            return DecisionReason.CRITICAL_OVERRIDE
        if fallback:
            return DecisionReason.FALLBACK
        if len({item.target.model_id for item in success_items}) == 1:
            return DecisionReason.SUCCESS_DOMINANT
        if placement_reason == "KV_AFFINITY":
            return DecisionReason.KV_AFFINITY
        if placement_reason == "LOAD_BALANCE":
            return DecisionReason.LOAD_BALANCE
        if len(cost_items) < len(success_items):
            return DecisionReason.COST_DOMINANT
        return DecisionReason.LATENCY_TIEBREAK


__all__ = ["SuccessFirstScheduler"]
