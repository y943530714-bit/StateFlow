"""Prediction orchestration over immutable State Plane snapshots."""

from __future__ import annotations

from dataclasses import replace
from typing import Iterable

from ..state import InMemoryStatePlane
from .contracts import Prediction, PredictionRequest
from .model import AnalyticalPredictionModel, PredictionModel


class PredictionService:
    """Run a prediction model without coupling it to a controller hot path."""

    def __init__(
        self,
        plane: InMemoryStatePlane,
        model: PredictionModel | None = None,
        *,
        min_confidence: float = 0.5,
        fallback: str = "success-first-heuristic",
    ) -> None:
        if not 0.0 <= min_confidence <= 1.0:
            raise ValueError("min_confidence must be between zero and one")
        self.plane = plane
        self.model = model or AnalyticalPredictionModel()
        self.min_confidence = min_confidence
        self.fallback = fallback

    def predict(self, request: PredictionRequest) -> Prediction:
        snapshot = self.plane.read_snapshot(request.snapshot_id)
        try:
            prediction = self.model.predict(snapshot, request.candidate_action)
        except Exception as exc:
            return Prediction(
                snapshot_id=snapshot.token,
                candidate_id=request.candidate_action.candidate_id,
                confidence=0.0,
                model_version=self.model.version,
                applicability="unavailable",
                fallback=self.fallback,
                notes=(f"predictor_error:{type(exc).__name__}",),
            )
        if prediction.confidence >= self.min_confidence:
            return prediction
        return replace(
            prediction,
            fallback=self.fallback,
            notes=prediction.notes + ("low_confidence_fallback",),
        )

    def predict_many(
        self, requests: Iterable[PredictionRequest]
    ) -> tuple[Prediction, ...]:
        return tuple(self.predict(request) for request in requests)


__all__ = ["PredictionService"]
