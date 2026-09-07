"""Decision construction helpers."""

from __future__ import annotations

from datetime import datetime

from ....state.schema import DecisionReason, HarnessSchedulingView, utcnow
from ...types import CandidateEvaluation, RoutingDecision, SchedulerConfig


def build_routing_decision(
    view: HarnessSchedulingView,
    selected: CandidateEvaluation,
    evaluations: list[CandidateEvaluation],
    *,
    reason: DecisionReason,
    config: SchedulerConfig,
    override_reasons: list[str] | None = None,
    fallback: bool = False,
    timestamp: datetime | None = None,
) -> RoutingDecision:
    selected.selected = True
    return RoutingDecision(
        selected_model=selected.model_id,
        selected_endpoint=selected.endpoint_id,
        selected_replica=selected.replica_id,
        predicted_success=selected.predicted_success,
        predicted_cost=selected.effective_cost,
        predicted_latency_seconds=selected.predicted_latency_seconds,
        decision_reason=reason,
        candidates=evaluations,
        scheduler_version=config.scheduler_version,
        state_id=view.state_id,
        state_version=view.state_version,
        override_reasons=list(override_reasons or []),
        fallback=fallback,
        timestamp=timestamp or utcnow(),
    )


__all__ = ["build_routing_decision"]
