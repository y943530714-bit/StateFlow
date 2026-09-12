"""Bridge the existing routing scheduler to v1.1 prediction/control records."""

from __future__ import annotations

from dataclasses import dataclass

from ..control import Action, DecisionRecord
from ..prediction import CandidateAction, Prediction
from .types import RoutingDecision


@dataclass(frozen=True)
class RoutingControlBundle:
    decision: DecisionRecord
    predictions: tuple[Prediction, ...]


def routing_control_bundle(
    routing: RoutingDecision,
    *,
    snapshot_id: str = "",
    policy_version: str = "success-first-0.1",
) -> RoutingControlBundle:
    """Materialize replayable candidate predictions and a selected action."""

    snapshot_id = snapshot_id or f"agent-state:{routing.state_id}:{routing.state_version}"
    candidates: list[CandidateAction] = []
    predictions: list[Prediction] = []
    selected_action: Action | None = None
    constraints: list[str] = []
    for evaluation in routing.candidates:
        candidate = CandidateAction(
            action_type="route",
            target_component=evaluation.endpoint_id,
            parameters={
                "model_id": evaluation.model_id,
                "endpoint_id": evaluation.endpoint_id,
                "replica_id": evaluation.replica_id,
                "tier": evaluation.tier,
            },
        )
        candidates.append(candidate)
        predictions.append(
            Prediction.from_routing_evaluation(
                snapshot_id,
                candidate,
                evaluation,
                model_version=routing.scheduler_version,
            )
        )
        constraints.extend(evaluation.rejection_reasons)
        if evaluation.selected:
            selected_action = Action(
                target_component=evaluation.endpoint_id,
                action_type="route",
                parameters=dict(candidate.parameters),
                preconditions={"candidate_eligible": evaluation.eligible},
                idempotency_key=f"route:{routing.decision_id}",
                action_id=f"action:{routing.decision_id}",
            )
    if selected_action is None:
        raise ValueError("routing decision has no selected candidate")
    decision = DecisionRecord(
        decision_id=routing.decision_id,
        snapshot_id=snapshot_id,
        policy_version=policy_version,
        candidate_ids=tuple(item.candidate_id for item in candidates),
        prediction_ids=tuple(item.prediction_id for item in predictions),
        model_versions=tuple(sorted({item.model_version for item in predictions})),
        selected_action=selected_action,
        reason=routing.decision_reason.value,
        constraints=tuple(sorted(set(constraints))),
        timestamp=routing.timestamp,
    )
    return RoutingControlBundle(decision, tuple(predictions))


__all__ = ["RoutingControlBundle", "routing_control_bundle"]
