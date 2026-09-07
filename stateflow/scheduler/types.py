"""Public scheduler contracts and decision trace objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
import uuid

from ..state.schema import DecisionReason, HarnessSchedulingView, TargetCandidate, to_jsonable, utcnow


@dataclass
class SchedulerConfig:
    # Success is a hard lexicographic gate, not a weighted score.
    s_min: float = 0.90
    epsilon_success: float = 0.03
    beta: float = 1.0
    epsilon_cost: float = 0.05
    lambda_future: float = 1.0

    # Safety and anti-ping-pong policy.
    failure_streak_override: int = 2
    deescalation_stable_steps: int = 2
    error_severity_override: float = 0.75
    spinning_score_override: float = 0.70
    min_state_completeness_for_downgrade: float = 0.50
    predictor_uncertainty_override: float = 0.25

    # Future KV value decay.
    future_value_decay_seconds: float = 5.0
    remote_future_reuse_factor: float = 0.5

    scheduler_version: str = "success-first-0.1"


@dataclass(frozen=True)
class FilterRejection:
    model_id: str
    endpoint_id: str
    replica_id: str
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)


@dataclass
class SuccessEstimate:
    predicted_success: float
    uncertainty: float
    confidence: float
    lower_confidence_bound: float = 0.0
    predictor_version: str = "heuristic-0.1"
    notes: list[str] = field(default_factory=list)

    def with_lcb(self, beta: float) -> "SuccessEstimate":
        self.lower_confidence_bound = max(
            0.0,
            min(1.0, self.predicted_success - beta * self.uncertainty),
        )
        return self


@dataclass
class CostBreakdown:
    inference_cost: float = 0.0
    kv_fetch_cost: float = 0.0
    kv_recompute_cost: float = 0.0
    expected_retry_cost: float = 0.0
    routing_overhead_cost: float = 0.0
    future_kv_value: float = 0.0
    effective_cost: float = 0.0
    local_kv_tokens: int = 0
    remote_kv_tokens: int = 0
    miss_tokens: int = 0


@dataclass
class LatencyBreakdown:
    queue_latency_seconds: float = 0.0
    kv_latency_seconds: float = 0.0
    prefill_latency_seconds: float = 0.0
    decode_latency_seconds: float = 0.0
    predicted_latency_seconds: float = 0.0
    critical_path_latency_seconds: float = 0.0


@dataclass
class CandidateEvaluation:
    model_id: str
    endpoint_id: str
    replica_id: str
    tier: str
    eligible: bool = False
    rejection_reasons: list[str] = field(default_factory=list)

    predicted_success: float = 0.0
    success_uncertainty: float = 1.0
    success_confidence: float = 0.0
    success_lcb: float = 0.0
    success_gate: bool = False
    cost_gate: bool = False
    selected: bool = False

    inference_cost: float = 0.0
    kv_fetch_cost: float = 0.0
    kv_recompute_cost: float = 0.0
    expected_retry_cost: float = 0.0
    routing_overhead_cost: float = 0.0
    future_kv_value: float = 0.0
    effective_cost: float = 0.0

    queue_latency_seconds: float = 0.0
    kv_latency_seconds: float = 0.0
    prefill_latency_seconds: float = 0.0
    decode_latency_seconds: float = 0.0
    predicted_latency_seconds: float = 0.0
    critical_path_latency_seconds: float = 0.0

    local_kv_tokens: int = 0
    remote_kv_tokens: int = 0
    miss_tokens: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)


@dataclass
class RoutingDecision:
    selected_model: str
    selected_endpoint: str
    selected_replica: str
    predicted_success: float
    predicted_cost: float
    predicted_latency_seconds: float
    decision_reason: DecisionReason
    candidates: list[CandidateEvaluation]
    scheduler_version: str
    state_id: str = ""
    state_version: int = 0
    decision_id: str = field(default_factory=lambda: f"decision-{uuid.uuid4().hex[:16]}")
    timestamp: datetime = field(default_factory=utcnow)
    override_reasons: list[str] = field(default_factory=list)
    fallback: bool = False

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)


class NoFeasibleTarget(RuntimeError):
    """Raised when hard constraints leave no target to serve a request."""

    def __init__(self, message: str, *, rejections: list[FilterRejection] | None = None):
        super().__init__(message)
        self.rejections = rejections or []


@dataclass
class FilterResult:
    eligible: list[TargetCandidate]
    rejected: list[FilterRejection]


__all__ = [
    "CandidateEvaluation",
    "CostBreakdown",
    "FilterRejection",
    "FilterResult",
    "LatencyBreakdown",
    "NoFeasibleTarget",
    "RoutingDecision",
    "SchedulerConfig",
    "SuccessEstimate",
]
