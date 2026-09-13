"""Prediction orchestration over immutable State Plane snapshots."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime
import threading
from typing import Iterable

from ..state import InMemoryStatePlane, Snapshot
from ..state.schema import utcnow
from .contracts import Prediction, PredictionRequest
from .model import AnalyticalPredictionModel, PredictionModel


@dataclass(frozen=True)
class PredictionRecord:
    request: PredictionRequest
    snapshot: Snapshot
    prediction: Prediction
    created_at: datetime

    @property
    def replay_pointer(self) -> str:
        return f"prediction:{self.prediction.prediction_id}"


class InMemoryPredictionJournal:
    """Thread-safe shadow journal retaining replay inputs and outputs."""

    def __init__(self) -> None:
        self._records: dict[str, PredictionRecord] = {}
        self._order: list[str] = []
        self._lock = threading.RLock()

    def append(self, record: PredictionRecord) -> None:
        prediction_id = record.prediction.prediction_id
        with self._lock:
            if prediction_id in self._records:
                raise ValueError(f"prediction already journaled: {prediction_id}")
            self._records[prediction_id] = deepcopy(record)
            self._order.append(prediction_id)

    def get(self, prediction_id: str) -> PredictionRecord:
        with self._lock:
            try:
                return deepcopy(self._records[prediction_id])
            except KeyError as exc:
                raise KeyError(f"unknown prediction: {prediction_id}") from exc

    def list(
        self,
        *,
        snapshot_id: str = "",
        model_version: str = "",
    ) -> tuple[PredictionRecord, ...]:
        with self._lock:
            records = (self._records[item] for item in self._order)
            return tuple(
                deepcopy(record)
                for record in records
                if (not snapshot_id or record.prediction.snapshot_id == snapshot_id)
                and (
                    not model_version
                    or record.prediction.model_version == model_version
                )
            )


class PredictionService:
    """Run a prediction model without coupling it to a controller hot path."""

    def __init__(
        self,
        plane: InMemoryStatePlane,
        model: PredictionModel | None = None,
        *,
        min_confidence: float = 0.5,
        fallback: str = "success-first-heuristic",
        journal: InMemoryPredictionJournal | None = None,
        additional_models: Iterable[PredictionModel] = (),
    ) -> None:
        if not 0.0 <= min_confidence <= 1.0:
            raise ValueError("min_confidence must be between zero and one")
        self.plane = plane
        self.model = model or AnalyticalPredictionModel()
        self.min_confidence = min_confidence
        self.fallback = fallback
        self.journal = (
            journal if journal is not None else InMemoryPredictionJournal()
        )
        self._models: dict[str, PredictionModel] = {self.model.version: self.model}
        for item in additional_models:
            self.register_model(item)

    def predict(self, request: PredictionRequest) -> Prediction:
        snapshot = self.plane.read_snapshot(request.snapshot_id)
        prediction = self._evaluate(snapshot, request, self.model)
        self.journal.append(
            PredictionRecord(
                request=deepcopy(request),
                snapshot=deepcopy(snapshot),
                prediction=deepcopy(prediction),
                created_at=utcnow(),
            )
        )
        return prediction

    def register_model(self, model: PredictionModel, *, replace: bool = False) -> None:
        if model.version in self._models and not replace:
            raise ValueError(f"prediction model already registered: {model.version}")
        self._models[model.version] = model

    def replay(
        self,
        prediction_id: str,
        *,
        model_version: str = "",
    ) -> Prediction:
        record = self.journal.get(prediction_id)
        version = model_version or record.prediction.model_version
        try:
            model = self._models[version]
        except KeyError as exc:
            raise KeyError(
                f"prediction model version is unavailable: {version}"
            ) from exc
        replayed = self._evaluate(record.snapshot, record.request, model)
        if version == record.prediction.model_version:
            replayed = replace(replayed, prediction_id=prediction_id)
        return replayed

    def records(
        self,
        *,
        snapshot_id: str = "",
        model_version: str = "",
    ) -> tuple[PredictionRecord, ...]:
        return self.journal.list(
            snapshot_id=snapshot_id,
            model_version=model_version,
        )

    def predict_many(
        self, requests: Iterable[PredictionRequest]
    ) -> tuple[Prediction, ...]:
        return tuple(self.predict(request) for request in requests)

    def _evaluate(
        self,
        snapshot: Snapshot,
        request: PredictionRequest,
        model: PredictionModel,
    ) -> Prediction:
        try:
            prediction = model.predict(snapshot, request.candidate_action)
        except Exception as exc:
            return Prediction(
                snapshot_id=snapshot.token,
                candidate_id=request.candidate_action.candidate_id,
                confidence=0.0,
                model_version=model.version,
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


__all__ = [
    "InMemoryPredictionJournal",
    "PredictionRecord",
    "PredictionService",
]
