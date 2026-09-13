"""Shadow prediction error and calibration materialization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..prediction import Prediction
from ..state.schema import to_jsonable
from .contracts import DecisionRecord, Feedback, Outcome


@dataclass(frozen=True)
class CalibrationBin:
    lower_bound: float
    upper_bound: float
    samples: int
    mean_confidence: float
    observed_success_rate: float
    absolute_gap: float


@dataclass(frozen=True)
class PredictionEvaluationReport:
    samples: int
    observed_fields: int
    predicted_fields: int
    coverage: float
    latency_samples: int
    latency_mae: float | None
    latency_mape_samples: int
    latency_mape: float | None
    cost_samples: int
    cost_mae: float | None
    cost_mape_samples: int
    cost_mape: float | None
    success_samples: int
    success_brier_score: float | None
    success_ece: float | None
    fallback_rate: float
    calibration_bins: tuple[CalibrationBin, ...] = ()

    def to_dict(self):
        return to_jsonable(self)


def build_feedback(
    decision: DecisionRecord,
    prediction: Prediction,
    outcome: Outcome,
    *,
    replay_pointer: str = "",
) -> Feedback:
    """Join one selected prediction to its observed outcome."""

    if outcome.action_id != decision.selected_action.action_id:
        raise ValueError("outcome action_id does not match the selected action")
    if prediction.prediction_id not in decision.prediction_ids:
        raise ValueError("prediction does not belong to the decision")

    errors: dict[str, float] = {}
    kpis: dict[str, float] = {}
    calibration: dict[str, object] = {}

    _continuous_error(
        "latency",
        prediction.performance.e2e_seconds,
        outcome.actual_latency_seconds,
        errors,
        kpis,
    )
    _continuous_error(
        "cost",
        prediction.cost.monetary_cost,
        outcome.actual_cost,
        errors,
        kpis,
    )
    probability = prediction.reliability.success_probability
    if outcome.actual_success is not None:
        if not isinstance(outcome.actual_success, bool):
            raise ValueError("actual success must be a boolean")
        observed = 1.0 if outcome.actual_success else 0.0
        kpis["actual_success"] = observed
        if probability is not None:
            if not 0.0 <= probability <= 1.0:
                raise ValueError("success probability must be between zero and one")
            errors["success_absolute_error"] = abs(probability - observed)
            errors["success_brier"] = (probability - observed) ** 2
            calibration = {
                "predicted_probability": probability,
                "observed_success": observed,
                "model_version": prediction.model_version,
                "applicability": prediction.applicability,
            }

    return Feedback(
        decision_id=decision.decision_id,
        action_id=outcome.action_id,
        prediction_id=prediction.prediction_id,
        outcome=outcome,
        prediction_error=errors,
        policy_kpis=kpis,
        calibration_sample=calibration,
        replay_pointer=replay_pointer,
    )


def materialize_evaluation_report(
    samples: Iterable[tuple[Prediction, Feedback]],
    *,
    calibration_bin_count: int = 10,
) -> PredictionEvaluationReport:
    if calibration_bin_count <= 0:
        raise ValueError("calibration_bin_count must be positive")

    entries = tuple(samples)
    latency_errors: list[float] = []
    latency_percentage_errors: list[float] = []
    cost_errors: list[float] = []
    cost_percentage_errors: list[float] = []
    success_samples: list[tuple[float, float]] = []
    observed_fields = 0
    predicted_fields = 0
    fallback_count = 0

    for prediction, feedback in entries:
        outcome = feedback.outcome
        if prediction.fallback != "none":
            fallback_count += 1
        observed_fields += sum(
            item is not None
            for item in (
                outcome.actual_latency_seconds,
                outcome.actual_cost,
                outcome.actual_success,
            )
        )
        for actual, predicted in (
            (outcome.actual_latency_seconds, prediction.performance.e2e_seconds),
            (outcome.actual_cost, prediction.cost.monetary_cost),
            (
                outcome.actual_success,
                prediction.reliability.success_probability,
            ),
        ):
            if actual is not None and predicted is not None:
                predicted_fields += 1

        _append_error(
            feedback,
            "latency_absolute_error",
            "latency_absolute_percentage_error",
            latency_errors,
            latency_percentage_errors,
        )
        _append_error(
            feedback,
            "cost_absolute_error",
            "cost_absolute_percentage_error",
            cost_errors,
            cost_percentage_errors,
        )
        calibration = feedback.calibration_sample
        probability = calibration.get("predicted_probability")
        observed = calibration.get("observed_success")
        if isinstance(probability, (int, float)) and isinstance(
            observed, (int, float)
        ):
            success_samples.append((float(probability), float(observed)))

    bins = _calibration_bins(success_samples, calibration_bin_count)
    success_ece = (
        sum(item.samples * item.absolute_gap for item in bins)
        / len(success_samples)
        if success_samples
        else None
    )
    return PredictionEvaluationReport(
        samples=len(entries),
        observed_fields=observed_fields,
        predicted_fields=predicted_fields,
        coverage=(predicted_fields / observed_fields if observed_fields else 0.0),
        latency_samples=len(latency_errors),
        latency_mae=_mean(latency_errors),
        latency_mape_samples=len(latency_percentage_errors),
        latency_mape=_mean(latency_percentage_errors),
        cost_samples=len(cost_errors),
        cost_mae=_mean(cost_errors),
        cost_mape_samples=len(cost_percentage_errors),
        cost_mape=_mean(cost_percentage_errors),
        success_samples=len(success_samples),
        success_brier_score=_mean(
            [(probability - observed) ** 2 for probability, observed in success_samples]
        ),
        success_ece=success_ece,
        fallback_rate=(fallback_count / len(entries) if entries else 0.0),
        calibration_bins=bins,
    )


def _continuous_error(
    name: str,
    predicted: float | None,
    actual: float | None,
    errors: dict[str, float],
    kpis: dict[str, float],
) -> None:
    if actual is None:
        return
    if actual < 0:
        raise ValueError(f"actual {name} must be non-negative")
    kpis[f"actual_{name}"] = actual
    if predicted is None:
        return
    if predicted < 0:
        raise ValueError(f"predicted {name} must be non-negative")
    absolute = abs(predicted - actual)
    errors[f"{name}_absolute_error"] = absolute
    if actual > 0:
        errors[f"{name}_absolute_percentage_error"] = absolute / actual


def _append_error(
    feedback: Feedback,
    absolute_key: str,
    percentage_key: str,
    absolute: list[float],
    percentage: list[float],
) -> None:
    absolute_value = feedback.prediction_error.get(absolute_key)
    if absolute_value is not None:
        absolute.append(float(absolute_value))
    percentage_value = feedback.prediction_error.get(percentage_key)
    if percentage_value is not None:
        percentage.append(float(percentage_value))


def _calibration_bins(
    samples: tuple[tuple[float, float], ...] | list[tuple[float, float]],
    count: int,
) -> tuple[CalibrationBin, ...]:
    grouped: list[list[tuple[float, float]]] = [[] for _ in range(count)]
    for probability, observed in samples:
        if not 0.0 <= probability <= 1.0:
            raise ValueError("success probability must be between zero and one")
        index = min(count - 1, int(probability * count))
        grouped[index].append((probability, observed))
    width = 1.0 / count
    result: list[CalibrationBin] = []
    for index, values in enumerate(grouped):
        if not values:
            continue
        confidence = sum(item[0] for item in values) / len(values)
        observed = sum(item[1] for item in values) / len(values)
        result.append(
            CalibrationBin(
                lower_bound=index * width,
                upper_bound=(index + 1) * width,
                samples=len(values),
                mean_confidence=confidence,
                observed_success_rate=observed,
                absolute_gap=abs(confidence - observed),
            )
        )
    return tuple(result)


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


__all__ = [
    "CalibrationBin",
    "PredictionEvaluationReport",
    "build_feedback",
    "materialize_evaluation_report",
]
