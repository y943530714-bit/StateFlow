"""Success predictors used by the scheduler."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import math
from typing import Protocol

from ....state.schema import HarnessSchedulingView, TargetCandidate, clamp
from ...types import SuccessEstimate


class SuccessPredictor(Protocol):
    def estimate(self, view: HarnessSchedulingView, target: TargetCandidate) -> SuccessEstimate:
        ...


def _tier_rank(tier: str) -> int:
    return {
        "efficient": 0,
        "cheap": 0,
        "capable": 1,
        "strong": 1,
        "frontier": 2,
        "premium": 2,
    }.get(tier.lower(), 0)


@dataclass
class HeuristicSuccessPredictor:
    """MVP predictor: capability profile + state risk adjustments.

    It estimates whole-task completion probability, not HTTP success.  The
    predictor is intentionally conservative when state visibility is missing.
    """

    version: str = "heuristic-0.1"

    def estimate(self, view: HarnessSchedulingView, target: TargetCandidate) -> SuccessEstimate:
        metadata = target.metadata or {}
        predicted = float(metadata.get("base_success", target.base_success))
        phase_success = metadata.get("success_by_phase", {})
        if isinstance(phase_success, dict) and view.phase.value in phase_success:
            predicted = float(phase_success[view.phase.value])

        risk = max(view.error_severity, view.spinning_score, view.recovery_score)
        rank = _tier_rank(target.tier)
        if risk > 0:
            if rank >= 2:
                predicted += 0.04 * risk
            elif rank == 1:
                predicted += 0.015 * risk
            else:
                predicted -= 0.10 * risk

        if view.exploring_score > 0.65 or view.phase.value in {"PLANNING", "REPLANNING"}:
            predicted += 0.02 * min(1.0, rank / 2.0)
        if view.production_score > 0.65 and view.plan_stability > 0.55 and rank == 0:
            predicted += 0.02
        if view.verification_score > 0.65 and rank == 0:
            predicted -= 0.03
        if view.truncation_risk > 0.7 and rank == 0:
            predicted -= 0.05 * view.truncation_risk

        uncertainty = float(metadata.get("uncertainty", target.base_uncertainty))
        uncertainty += 0.18 * (1.0 - clamp(view.state_completeness))
        uncertainty += 0.05 * clamp(view.truncation_risk)
        if metadata.get("out_of_distribution"):
            uncertainty += 0.10
        uncertainty = clamp(uncertainty, 0.0, 0.5)
        predicted = clamp(predicted)
        return SuccessEstimate(
            predicted_success=predicted,
            uncertainty=uncertainty,
            confidence=clamp(1.0 - uncertainty),
            predictor_version=self.version,
            notes=["heuristic_capability_profile"],
        )


@dataclass
class BetaPosteriorSuccessPredictor:
    """Trace-backed statistical predictor from task/phase/model buckets."""

    alpha: float = 1.0
    beta: float = 1.0
    version: str = "beta-posterior-0.1"

    def __post_init__(self) -> None:
        self._counts: dict[tuple[str, str, str], list[int]] = defaultdict(lambda: [0, 0])

    def record_outcome(
        self,
        *,
        task_type: str,
        phase: str,
        model_id: str,
        success: bool,
    ) -> None:
        bucket = self._counts[(task_type, phase, model_id)]
        bucket[0 if success else 1] += 1

    def estimate(self, view: HarnessSchedulingView, target: TargetCandidate) -> SuccessEstimate:
        success_count, failure_count = self._counts[(view.task_type, view.phase.value, target.model_id)]
        posterior_alpha = self.alpha + success_count
        posterior_beta = self.beta + failure_count
        total = posterior_alpha + posterior_beta
        mean = posterior_alpha / total
        variance = (posterior_alpha * posterior_beta) / (total * total * (total + 1.0))
        uncertainty = min(0.5, math.sqrt(max(0.0, variance)) * 2.0)
        # With no observations, the capability profile is a better prior than
        # an uninformed 0.5 mean.  The posterior takes over as evidence grows.
        if success_count + failure_count == 0:
            mean = target.base_success
            uncertainty = max(uncertainty, target.base_uncertainty)
        return SuccessEstimate(
            predicted_success=clamp(mean),
            uncertainty=clamp(uncertainty),
            confidence=clamp(1.0 - uncertainty),
            predictor_version=self.version,
            notes=[f"beta_bucket_observations={success_count + failure_count}"],
        )


__all__ = [
    "BetaPosteriorSuccessPredictor",
    "HeuristicSuccessPredictor",
    "SuccessPredictor",
]
