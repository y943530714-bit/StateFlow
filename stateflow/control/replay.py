"""Side-effect-free lexicographic policy replay over recorded predictions."""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Mapping

from ..prediction import Prediction
from ..state.schema import to_jsonable
from .contracts import Objective, PolicyIntent
from .journal import JoinedControlRecord


@dataclass(frozen=True)
class ReplayConstraints:
    min_confidence: float = 0.0
    min_success_probability: float | None = None
    max_latency_seconds: float | None = None
    max_monetary_cost: float | None = None
    model_version: str = ""
    allow_fallback: bool = True

    def __post_init__(self) -> None:
        if not 0.0 <= self.min_confidence <= 1.0:
            raise ValueError("min_confidence must be between zero and one")
        if self.min_success_probability is not None and not (
            0.0 <= self.min_success_probability <= 1.0
        ):
            raise ValueError(
                "min_success_probability must be between zero and one"
            )
        for name, value in (
            ("max_latency_seconds", self.max_latency_seconds),
            ("max_monetary_cost", self.max_monetary_cost),
        ):
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative")


@dataclass(frozen=True)
class ReplayPolicyConfig:
    success_absolute_tolerance: float = 0.03
    latency_relative_tolerance: float = 0.0
    cost_relative_tolerance: float = 0.0
    throughput_relative_tolerance: float = 0.0

    def __post_init__(self) -> None:
        for name, value in (
            ("success_absolute_tolerance", self.success_absolute_tolerance),
            ("latency_relative_tolerance", self.latency_relative_tolerance),
            ("cost_relative_tolerance", self.cost_relative_tolerance),
            ("throughput_relative_tolerance", self.throughput_relative_tolerance),
        ):
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between zero and one")


@dataclass(frozen=True)
class ReplayDecision:
    source_decision_id: str
    snapshot_id: str
    policy_name: str
    policy_version: str
    baseline_candidate_id: str
    selected_candidate_id: str
    selected_prediction_id: str
    candidate_ids: tuple[str, ...]
    eligible_candidate_ids: tuple[str, ...]
    objective_values: Mapping[str, float]
    potential_gain: Mapping[str, float]
    rejections: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    selection_trace: tuple[str, ...] = ()
    changed: bool = False
    dry_run: bool = True

    def to_dict(self):
        return to_jsonable(self)


class NoReplayCandidate(RuntimeError):
    pass


def controlled_replay(
    record: JoinedControlRecord,
    policy: PolicyIntent,
    *,
    constraints: ReplayConstraints | None = None,
    config: ReplayPolicyConfig | None = None,
) -> ReplayDecision:
    """Re-evaluate recorded candidates without producing an executable action."""

    constraints = constraints or ReplayConstraints()
    config = config or ReplayPolicyConfig()
    if not policy.objectives:
        raise ValueError("replay policy must contain at least one objective")
    predictions = record.predictions
    if not predictions:
        raise NoReplayCandidate("record contains no predictions")
    if len({item.candidate_id for item in predictions}) != len(predictions):
        raise ValueError("replay requires one prediction per candidate_id")

    rejections: dict[str, list[str]] = {
        item.candidate_id: [] for item in predictions
    }
    eligible = [
        item
        for item in predictions
        if _passes_constraints(item, constraints, rejections[item.candidate_id])
    ]
    if not eligible:
        raise NoReplayCandidate("hard constraints rejected every prediction")

    initially_eligible = tuple(item.candidate_id for item in eligible)
    trace: list[str] = []
    for objective in policy.objectives:
        known = [
            (item, _objective_value(item, objective)) for item in eligible
        ]
        available = [(item, value) for item, value in known if value is not None]
        if not available:
            raise NoReplayCandidate(
                f"objective {objective.value} is unavailable for every candidate"
            )
        for item, value in known:
            if value is None:
                rejections[item.candidate_id].append(
                    f"missing_objective:{objective.value}"
                )
        survivors = _objective_survivors(available, objective, config)
        survivor_ids = {item.candidate_id for item in survivors}
        for item, _ in available:
            if item.candidate_id not in survivor_ids:
                rejections[item.candidate_id].append(
                    f"objective_gate:{objective.value}"
                )
        eligible = survivors
        values = [value for _, value in available]
        best_value = (
            max(values)
            if objective in {Objective.SUCCESS, Objective.THROUGHPUT}
            else min(values)
        )
        trace.append(
            f"{objective.value}:{best_value}:{','.join(item.candidate_id for item in eligible)}"
        )
        if len(eligible) == 1:
            break

    selected = eligible[0]
    baseline = _baseline_prediction(record)
    objective_values = {
        objective.value: value
        for objective in policy.objectives
        if (value := _objective_value(selected, objective)) is not None
    }
    return ReplayDecision(
        source_decision_id=record.decision.decision_id,
        snapshot_id=record.decision.snapshot_id,
        policy_name=policy.name,
        policy_version=policy.version,
        baseline_candidate_id=baseline.candidate_id,
        selected_candidate_id=selected.candidate_id,
        selected_prediction_id=selected.prediction_id,
        candidate_ids=tuple(item.candidate_id for item in predictions),
        eligible_candidate_ids=initially_eligible,
        objective_values=objective_values,
        potential_gain=_potential_gain(selected, baseline),
        rejections={
            key: tuple(value) for key, value in rejections.items() if value
        },
        selection_trace=tuple(trace),
        changed=selected.candidate_id != baseline.candidate_id,
    )


def _passes_constraints(
    prediction: Prediction,
    constraints: ReplayConstraints,
    reasons: list[str],
) -> bool:
    if prediction.confidence < constraints.min_confidence:
        reasons.append("confidence_below_minimum")
    if constraints.model_version and (
        prediction.model_version != constraints.model_version
    ):
        reasons.append("model_version_mismatch")
    if not constraints.allow_fallback and prediction.fallback != "none":
        reasons.append("fallback_prediction")
    _maximum_constraint(
        prediction.performance.e2e_seconds,
        constraints.max_latency_seconds,
        "latency",
        reasons,
    )
    _maximum_constraint(
        prediction.cost.monetary_cost,
        constraints.max_monetary_cost,
        "cost",
        reasons,
    )
    if constraints.min_success_probability is not None:
        value = prediction.reliability.success_probability
        if value is None:
            reasons.append("missing_constraint:success")
        elif not math.isfinite(value) or not 0.0 <= value <= 1.0:
            raise ValueError("success probability must be between zero and one")
        elif value < constraints.min_success_probability:
            reasons.append("success_below_minimum")
    return not reasons


def _maximum_constraint(
    value: float | None,
    maximum: float | None,
    name: str,
    reasons: list[str],
) -> None:
    if maximum is None:
        return
    if value is None:
        reasons.append(f"missing_constraint:{name}")
    elif not math.isfinite(value) or value < 0:
        raise ValueError(f"predicted {name} must be finite and non-negative")
    elif value > maximum:
        reasons.append(f"{name}_above_maximum")


def _objective_survivors(
    available: list[tuple[Prediction, float]],
    objective: Objective,
    config: ReplayPolicyConfig,
) -> list[Prediction]:
    maximize = objective in {Objective.SUCCESS, Objective.THROUGHPUT}
    values = [value for _, value in available]
    best = max(values) if maximize else min(values)
    if objective == Objective.SUCCESS:
        tolerance = config.success_absolute_tolerance
    elif objective == Objective.LATENCY:
        tolerance = abs(best) * config.latency_relative_tolerance
    elif objective == Objective.COST:
        tolerance = abs(best) * config.cost_relative_tolerance
    elif objective == Objective.THROUGHPUT:
        tolerance = abs(best) * config.throughput_relative_tolerance
    else:
        tolerance = 0.0
    if maximize:
        threshold = best - tolerance
        return [item for item, value in available if value >= threshold]
    threshold = best + tolerance
    return [item for item, value in available if value <= threshold]


def _objective_value(prediction: Prediction, objective: Objective) -> float | None:
    value: float | None
    if objective == Objective.SUCCESS:
        value = prediction.reliability.success_probability
    elif objective == Objective.LATENCY:
        value = prediction.performance.e2e_seconds
    elif objective == Objective.COST:
        value = prediction.cost.monetary_cost
    elif objective == Objective.THROUGHPUT:
        value = prediction.performance.throughput
    else:
        value = None
    if value is None:
        return None
    if not math.isfinite(value):
        raise ValueError(f"objective {objective.value} must be finite")
    if objective == Objective.SUCCESS:
        if not 0.0 <= value <= 1.0:
            raise ValueError("success probability must be between zero and one")
    elif value < 0:
        raise ValueError(f"objective {objective.value} must be non-negative")
    return value


def _baseline_prediction(record: JoinedControlRecord) -> Prediction:
    candidate_id = record.decision.selected_action.parameters.get("candidate_id")
    if not candidate_id and len(record.predictions) == 1:
        return record.predictions[0]
    matches = [
        item for item in record.predictions if item.candidate_id == candidate_id
    ]
    if len(matches) != 1:
        raise ValueError("record has no unique baseline candidate prediction")
    return matches[0]


def _potential_gain(
    selected: Prediction,
    baseline: Prediction,
) -> dict[str, float]:
    result: dict[str, float] = {}
    _delta(
        result,
        "success_delta",
        selected.reliability.success_probability,
        baseline.reliability.success_probability,
    )
    _reduction(
        result,
        "latency_reduction_seconds",
        selected.performance.e2e_seconds,
        baseline.performance.e2e_seconds,
    )
    _reduction(
        result,
        "monetary_cost_reduction",
        selected.cost.monetary_cost,
        baseline.cost.monetary_cost,
    )
    _delta(
        result,
        "throughput_delta",
        selected.performance.throughput,
        baseline.performance.throughput,
    )
    return result


def _delta(
    output: dict[str, float],
    key: str,
    selected: float | None,
    baseline: float | None,
) -> None:
    if selected is not None and baseline is not None:
        output[key] = selected - baseline


def _reduction(
    output: dict[str, float],
    key: str,
    selected: float | None,
    baseline: float | None,
) -> None:
    if selected is not None and baseline is not None:
        output[key] = baseline - selected


__all__ = [
    "NoReplayCandidate",
    "ReplayConstraints",
    "ReplayDecision",
    "ReplayPolicyConfig",
    "controlled_replay",
]
