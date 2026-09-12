"""Controller-plane contracts for safe, replayable closed loops."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Mapping
import uuid

from ..state.schema import to_jsonable, utcnow


class Objective(str, Enum):
    SUCCESS = "success"
    LATENCY = "latency"
    COST = "cost"
    THROUGHPUT = "throughput"
    ENERGY = "energy"


@dataclass(frozen=True)
class PolicyIntent:
    name: str
    objectives: tuple[Objective, ...]
    version: str = "1"
    workload: str = "default"
    tenant: str = "default"
    sla: str = ""

    @classmethod
    def interactive(cls) -> "PolicyIntent":
        return cls("interactive", (Objective.LATENCY, Objective.SUCCESS, Objective.COST))

    @classmethod
    def critical(cls) -> "PolicyIntent":
        return cls("critical", (Objective.SUCCESS, Objective.LATENCY, Objective.COST))

    @classmethod
    def batch(cls) -> "PolicyIntent":
        return cls("batch", (Objective.COST, Objective.SUCCESS, Objective.LATENCY))


@dataclass(frozen=True)
class Reservation:
    resource_ref: str
    owner: str
    amount: Mapping[str, float]
    expires_at: datetime
    status: str = "reserved"


@dataclass(frozen=True)
class Action:
    target_component: str
    action_type: str
    parameters: Mapping[str, Any] = field(default_factory=dict)
    preconditions: Mapping[str, Any] = field(default_factory=dict)
    idempotency_key: str = field(default_factory=lambda: f"action-key-{uuid.uuid4().hex[:16]}")
    reservations: tuple[Reservation, ...] = ()
    timeout_seconds: float = 30.0
    rollback_action: Mapping[str, Any] = field(default_factory=dict)
    dry_run: bool = False
    action_id: str = field(default_factory=lambda: f"action-{uuid.uuid4().hex[:16]}")


@dataclass(frozen=True)
class DecisionRecord:
    snapshot_id: str
    policy_version: str
    candidate_ids: tuple[str, ...]
    prediction_ids: tuple[str, ...]
    model_versions: tuple[str, ...]
    selected_action: Action
    reason: str
    constraints: tuple[str, ...] = ()
    decision_id: str = field(default_factory=lambda: f"decision-{uuid.uuid4().hex[:16]}")
    timestamp: datetime = field(default_factory=utcnow)

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)


class ActionStatus(str, Enum):
    STARTED = "started"
    COMMITTED = "committed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ROLLED_BACK = "rolled_back"


@dataclass(frozen=True)
class Outcome:
    action_id: str
    status: ActionStatus
    started_at: datetime
    completed_at: datetime
    actual_latency_seconds: float | None = None
    actual_cost: float | None = None
    actual_success: bool | None = None
    resource_delta: Mapping[str, float] = field(default_factory=dict)
    error: str = ""


@dataclass(frozen=True)
class Feedback:
    decision_id: str
    action_id: str
    prediction_id: str
    outcome: Outcome
    prediction_error: Mapping[str, float] = field(default_factory=dict)
    policy_kpis: Mapping[str, float] = field(default_factory=dict)
    calibration_sample: Mapping[str, Any] = field(default_factory=dict)
    replay_pointer: str = ""
    timestamp: datetime = field(default_factory=utcnow)
    feedback_id: str = field(default_factory=lambda: f"feedback-{uuid.uuid4().hex[:16]}")

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)


__all__ = [
    "Action",
    "ActionStatus",
    "DecisionRecord",
    "Feedback",
    "Objective",
    "Outcome",
    "PolicyIntent",
    "Reservation",
]
