"""Prediction = f(State Snapshot, Candidate Action) transport contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping
import uuid

from ..state.schema import clamp, to_jsonable


@dataclass(frozen=True)
class CandidateAction:
    action_type: str
    target_component: str
    parameters: Mapping[str, Any] = field(default_factory=dict)
    candidate_id: str = field(default_factory=lambda: f"candidate-{uuid.uuid4().hex[:16]}")


@dataclass(frozen=True)
class PerformancePrediction:
    ttft_seconds: float | None = None
    tpot_seconds: float | None = None
    e2e_seconds: float | None = None
    throughput: float | None = None
    transfer_eta_seconds: float | None = None


@dataclass(frozen=True)
class ReliabilityPrediction:
    success_probability: float | None = None
    oom_probability: float | None = None
    timeout_probability: float | None = None
    slo_violation_probability: float | None = None


@dataclass(frozen=True)
class CostPrediction:
    gpu_seconds: float | None = None
    memory_time: float | None = None
    network_bytes: int | None = None
    storage_io_bytes: int | None = None
    monetary_cost: float | None = None


@dataclass(frozen=True)
class FutureStatePrediction:
    queue_depth: float | None = None
    hbm_pressure: float | None = None
    kv_location: str | None = None
    demand: float | None = None
    contention: float | None = None
    values: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PredictionRequest:
    snapshot_id: str
    candidate_action: CandidateAction


@dataclass(frozen=True)
class Prediction:
    snapshot_id: str
    candidate_id: str
    performance: PerformancePrediction = field(default_factory=PerformancePrediction)
    reliability: ReliabilityPrediction = field(default_factory=ReliabilityPrediction)
    cost: CostPrediction = field(default_factory=CostPrediction)
    future_state: FutureStatePrediction = field(default_factory=FutureStatePrediction)
    confidence: float = 0.0
    model_version: str = "analytical-0.1"
    applicability: str = "unknown"
    feature_freshness_ms: Mapping[str, int] = field(default_factory=dict)
    calibration_error: float | None = None
    fallback: str = "baseline"
    notes: tuple[str, ...] = ()
    prediction_id: str = field(default_factory=lambda: f"prediction-{uuid.uuid4().hex[:16]}")

    def __post_init__(self) -> None:
        object.__setattr__(self, "confidence", clamp(self.confidence))

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)

    @classmethod
    def from_routing_evaluation(
        cls,
        snapshot_id: str,
        candidate: CandidateAction,
        evaluation: Any,
        *,
        model_version: str,
    ) -> "Prediction":
        """Bridge the existing Success-First evaluation into the v1.1 contract."""

        return cls(
            snapshot_id=snapshot_id,
            candidate_id=candidate.candidate_id,
            performance=PerformancePrediction(
                e2e_seconds=getattr(evaluation, "predicted_latency_seconds", None),
            ),
            reliability=ReliabilityPrediction(
                success_probability=getattr(evaluation, "predicted_success", None),
            ),
            cost=CostPrediction(
                network_bytes=getattr(evaluation, "metadata", {}).get("kv_transfer_bytes"),
                monetary_cost=getattr(evaluation, "effective_cost", None),
            ),
            future_state=FutureStatePrediction(
                values={"future_kv_value": getattr(evaluation, "future_kv_value", 0.0)},
            ),
            confidence=getattr(evaluation, "success_confidence", 0.0),
            model_version=model_version,
            applicability="routing",
            notes=tuple(getattr(evaluation, "metadata", {}).get("predictor_notes", ())),
        )


__all__ = [
    "CandidateAction",
    "CostPrediction",
    "FutureStatePrediction",
    "PerformancePrediction",
    "Prediction",
    "PredictionRequest",
    "ReliabilityPrediction",
]
