"""In-memory shadow journal for Decision, Prediction, Outcome, and Feedback."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import threading
from typing import Iterable

from ..prediction import Prediction
from .contracts import DecisionRecord, Feedback, Outcome
from .evaluation import (
    PredictionEvaluationReport,
    build_feedback,
    materialize_evaluation_report,
)


@dataclass(frozen=True)
class JoinedControlRecord:
    decision: DecisionRecord
    predictions: tuple[Prediction, ...]
    outcome: Outcome | None = None
    feedback: Feedback | None = None


class InMemoryControlJournal:
    """Validate and join shadow records without executing the selected action."""

    def __init__(self) -> None:
        self._decisions: dict[str, DecisionRecord] = {}
        self._predictions: dict[str, Prediction] = {}
        self._decision_predictions: dict[str, tuple[str, ...]] = {}
        self._action_decisions: dict[str, str] = {}
        self._selected_predictions: dict[str, str] = {}
        self._outcomes: dict[str, Outcome] = {}
        self._feedback: dict[str, Feedback] = {}
        self._order: list[str] = []
        self._lock = threading.RLock()

    def record_decision(
        self,
        decision: DecisionRecord,
        predictions: Iterable[Prediction],
    ) -> JoinedControlRecord:
        values = tuple(predictions)
        self._validate_bundle(decision, values)
        selected_prediction = self._selected_prediction(decision, values)
        with self._lock:
            existing = self._decisions.get(decision.decision_id)
            if existing is not None:
                joined = self.get(decision.decision_id)
                if existing == decision and joined.predictions == values:
                    return joined
                raise ValueError(f"conflicting decision: {decision.decision_id}")
            action_id = decision.selected_action.action_id
            if action_id in self._action_decisions:
                raise ValueError(f"action already belongs to a decision: {action_id}")
            for prediction in values:
                owner = self._predictions.get(prediction.prediction_id)
                if owner is not None and owner != prediction:
                    raise ValueError(
                        f"conflicting prediction: {prediction.prediction_id}"
                    )
            self._decisions[decision.decision_id] = deepcopy(decision)
            self._decision_predictions[decision.decision_id] = decision.prediction_ids
            self._action_decisions[action_id] = decision.decision_id
            self._selected_predictions[action_id] = selected_prediction.prediction_id
            for prediction in values:
                self._predictions[prediction.prediction_id] = deepcopy(prediction)
            self._order.append(decision.decision_id)
            return self.get(decision.decision_id)

    def record_outcome(self, outcome: Outcome) -> Feedback:
        if outcome.completed_at < outcome.started_at:
            raise ValueError("outcome completed_at must not precede started_at")
        with self._lock:
            try:
                decision_id = self._action_decisions[outcome.action_id]
            except KeyError as exc:
                raise KeyError(f"unknown action: {outcome.action_id}") from exc
            existing = self._outcomes.get(outcome.action_id)
            if existing is not None:
                if existing == outcome:
                    return deepcopy(self._feedback[outcome.action_id])
                raise ValueError(f"conflicting outcome: {outcome.action_id}")
            decision = self._decisions[decision_id]
            prediction_id = self._selected_predictions[outcome.action_id]
            prediction = self._predictions[prediction_id]
            feedback = build_feedback(
                decision,
                prediction,
                outcome,
                replay_pointer=f"prediction:{prediction_id}",
            )
            self._outcomes[outcome.action_id] = deepcopy(outcome)
            self._feedback[outcome.action_id] = deepcopy(feedback)
            return deepcopy(feedback)

    def get(self, decision_id: str) -> JoinedControlRecord:
        with self._lock:
            try:
                decision = self._decisions[decision_id]
            except KeyError as exc:
                raise KeyError(f"unknown decision: {decision_id}") from exc
            prediction_ids = self._decision_predictions[decision_id]
            action_id = decision.selected_action.action_id
            return JoinedControlRecord(
                decision=deepcopy(decision),
                predictions=tuple(
                    deepcopy(self._predictions[item]) for item in prediction_ids
                ),
                outcome=deepcopy(self._outcomes.get(action_id)),
                feedback=deepcopy(self._feedback.get(action_id)),
            )

    def list(self) -> tuple[JoinedControlRecord, ...]:
        with self._lock:
            return tuple(self.get(item) for item in self._order)

    def report(
        self,
        *,
        model_version: str = "",
        calibration_bin_count: int = 10,
    ) -> PredictionEvaluationReport:
        with self._lock:
            samples: list[tuple[Prediction, Feedback]] = []
            for action_id, feedback in self._feedback.items():
                prediction = self._predictions[self._selected_predictions[action_id]]
                if model_version and prediction.model_version != model_version:
                    continue
                samples.append((deepcopy(prediction), deepcopy(feedback)))
        return materialize_evaluation_report(
            samples,
            calibration_bin_count=calibration_bin_count,
        )

    @staticmethod
    def _validate_bundle(
        decision: DecisionRecord,
        predictions: tuple[Prediction, ...],
    ) -> None:
        if not predictions:
            raise ValueError("a decision must contain at least one prediction")
        if decision.prediction_ids != tuple(item.prediction_id for item in predictions):
            raise ValueError("decision prediction_ids do not match predictions")
        if decision.candidate_ids != tuple(item.candidate_id for item in predictions):
            raise ValueError("decision candidate_ids do not match predictions")
        if any(item.snapshot_id != decision.snapshot_id for item in predictions):
            raise ValueError("all predictions must use the decision snapshot")
        versions = tuple(sorted({item.model_version for item in predictions}))
        if decision.model_versions != versions:
            raise ValueError("decision model_versions do not match predictions")

    @staticmethod
    def _selected_prediction(
        decision: DecisionRecord,
        predictions: tuple[Prediction, ...],
    ) -> Prediction:
        candidate_id = decision.selected_action.parameters.get("candidate_id")
        if not candidate_id:
            if len(predictions) == 1:
                return predictions[0]
            raise ValueError("selected action must identify candidate_id")
        matches = [item for item in predictions if item.candidate_id == candidate_id]
        if len(matches) != 1:
            raise ValueError("selected action candidate_id has no unique prediction")
        return matches[0]


__all__ = ["InMemoryControlJournal", "JoinedControlRecord"]
