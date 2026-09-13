"""Agent-aware KV candidate generation and side-effect-free planning."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import math
from typing import Any, Mapping

from ..prediction import CandidateAction, Prediction, PredictionRequest, PredictionService
from ..state import GraphEntity, Snapshot, StateValue
from .contracts import Action, DecisionRecord, Reservation
from .journal import InMemoryControlJournal


class KVActionType(str, Enum):
    KEEP = "keep"
    OFFLOAD = "offload"
    PREFETCH = "prefetch"
    MIGRATE = "migrate"


@dataclass(frozen=True)
class KVTarget:
    location_ref: str
    tier: str
    effective_bw_bytes_per_second: float = 0.0
    transfer_setup_seconds: float = 0.0
    hbm_capacity_bytes: int = 0
    network_byte_cost: float = 0.0
    storage_io_byte_cost: float = 0.0
    available: bool = True
    healthy: bool = True

    def __post_init__(self) -> None:
        if not self.location_ref:
            raise ValueError("KV target location_ref is required")
        if not self.tier:
            raise ValueError("KV target tier is required")
        for name, value in (
            ("effective_bw_bytes_per_second", self.effective_bw_bytes_per_second),
            ("transfer_setup_seconds", self.transfer_setup_seconds),
            ("hbm_capacity_bytes", self.hbm_capacity_bytes),
            ("network_byte_cost", self.network_byte_cost),
            ("storage_io_byte_cost", self.storage_io_byte_cost),
        ):
            if isinstance(value, bool) or not math.isfinite(float(value)) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")

    @property
    def is_hbm(self) -> bool:
        return self.tier.lower() in {"hbm", "gpu", "gpu_hbm"}


@dataclass(frozen=True)
class KVActionOwner:
    component_ref: str
    managed_kv_prefixes: tuple[str, ...]
    allowed_actions: tuple[KVActionType, ...] = tuple(KVActionType)

    def __post_init__(self) -> None:
        if not self.component_ref:
            raise ValueError("KV action owner component_ref is required")
        if not self.managed_kv_prefixes or any(
            not item for item in self.managed_kv_prefixes
        ):
            raise ValueError("KV action owner requires managed_kv_prefixes")
        normalized = tuple(KVActionType(item) for item in self.allowed_actions)
        if KVActionType.KEEP not in normalized:
            raise ValueError("KV action owner must allow the keep baseline")
        object.__setattr__(self, "allowed_actions", normalized)

    def manages(self, kv_ref: str) -> bool:
        return any(kv_ref.startswith(prefix) for prefix in self.managed_kv_prefixes)


@dataclass(frozen=True)
class KVControlRequest:
    snapshot_id: str
    kv_ref: str
    targets: tuple[KVTarget, ...]
    agent_ref: str = ""
    expected_state_owner: str = ""
    policy_version: str = "agent-aware-kv-0.1"

    def __post_init__(self) -> None:
        if not self.snapshot_id or not self.kv_ref:
            raise ValueError("KV control snapshot_id and kv_ref are required")
        if not self.policy_version:
            raise ValueError("KV control policy_version is required")


@dataclass(frozen=True)
class KVControllerConfig:
    min_snapshot_completeness: float = 0.5
    min_prediction_confidence: float = 0.5
    offload_hbm_pressure: float = 0.9
    offload_max_reuse_probability: float = 0.3
    prefetch_min_reuse_probability: float = 0.6
    prefetch_horizon_seconds: float = 5.0
    max_prefetch_hbm_pressure: float = 0.98
    reservation_ttl_seconds: float = 5.0
    offload_phases: tuple[str, ...] = ("TOOL_WAITING", "BLOCKED", "PAUSED")
    stable_transfer_states: tuple[str, ...] = ("idle", "resident", "ready")

    def __post_init__(self) -> None:
        for name, value in (
            ("min_snapshot_completeness", self.min_snapshot_completeness),
            ("min_prediction_confidence", self.min_prediction_confidence),
            ("offload_hbm_pressure", self.offload_hbm_pressure),
            ("offload_max_reuse_probability", self.offload_max_reuse_probability),
            ("prefetch_min_reuse_probability", self.prefetch_min_reuse_probability),
            ("max_prefetch_hbm_pressure", self.max_prefetch_hbm_pressure),
        ):
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between zero and one")
        if (
            not math.isfinite(self.prefetch_horizon_seconds)
            or not math.isfinite(self.reservation_ttl_seconds)
            or self.prefetch_horizon_seconds < 0
            or self.reservation_ttl_seconds <= 0
        ):
            raise ValueError("KV timing configuration is invalid")
        if not self.stable_transfer_states or any(
            not item for item in self.stable_transfer_states
        ):
            raise ValueError("KV control requires stable transfer states")


@dataclass(frozen=True)
class KVControlPlan:
    decision: DecisionRecord
    predictions: tuple[Prediction, ...]
    current_location: str
    selected_target: str
    observed: Mapping[str, Any] = field(default_factory=dict)


class NoKVControlCandidate(RuntimeError):
    pass


class AgentAwareKVController:
    """Generate and journal a KV decision without dispatching it."""

    def __init__(
        self,
        plane,
        owner: KVActionOwner,
        *,
        prediction_service: PredictionService | None = None,
        journal: InMemoryControlJournal | None = None,
        config: KVControllerConfig | None = None,
    ) -> None:
        self.plane = plane
        self.owner = owner
        self.prediction_service = (
            prediction_service
            if prediction_service is not None
            else PredictionService(plane)
        )
        self.journal = journal if journal is not None else InMemoryControlJournal()
        self.config = config if config is not None else KVControllerConfig()

    def plan(self, request: KVControlRequest) -> KVControlPlan:
        snapshot = self.plane.read_snapshot(request.snapshot_id)
        kv_entity, owner_entity = self._validate_ownership(request)
        if not request.targets:
            raise NoKVControlCandidate("at least one KV target is required")
        targets = {item.location_ref: item for item in request.targets}
        if len(targets) != len(request.targets):
            raise ValueError("KV target location_ref values must be unique")

        size = int(_required_number(snapshot, request.kv_ref, "kv.size"))
        current_location = _required_string(
            snapshot, request.kv_ref, "kv.location"
        )
        try:
            current_target = targets[current_location]
        except KeyError as exc:
            raise NoKVControlCandidate(
                "current KV location must be present in targets"
            ) from exc
        reuse = _optional_number(snapshot, request.kv_ref, "kv.reuse_probability")
        transfer_state = _optional_string(
            snapshot, request.kv_ref, "kv.transfer_state"
        )
        phase = (
            _optional_string(snapshot, request.agent_ref, "agent.phase")
            if request.agent_ref
            else None
        )
        expected_resume = (
            _optional_datetime(
                snapshot,
                request.agent_ref,
                "agent.expected_resume_time",
            )
            if request.agent_ref
            else None
        )
        resume_seconds = (
            max(0.0, (expected_resume - snapshot.created_at).total_seconds())
            if expected_resume is not None
            else None
        )
        hbm_pressure = _hbm_pressure(snapshot, current_target)

        candidates = self._candidates(
            snapshot,
            request,
            current_target,
            size,
        )
        predictions = self.prediction_service.predict_many(
            PredictionRequest(snapshot.token, candidate) for candidate in candidates
        )
        selected, reason = self._select(
            snapshot,
            candidates,
            predictions,
            current_target,
            phase=phase,
            reuse=reuse,
            resume_seconds=resume_seconds,
            hbm_pressure=hbm_pressure,
            transfer_state=transfer_state,
        )
        selected_candidate, selected_prediction = selected
        selected_target = targets[
            str(selected_candidate.parameters["target_location"])
        ]
        action = self._action(
            snapshot,
            request,
            kv_entity,
            owner_entity,
            current_target,
            selected_target,
            selected_candidate,
            selected_prediction,
            size,
            transfer_state,
        )
        decision = DecisionRecord(
            snapshot_id=snapshot.token,
            policy_version=request.policy_version,
            candidate_ids=tuple(item.candidate_id for item in candidates),
            prediction_ids=tuple(item.prediction_id for item in predictions),
            model_versions=tuple(
                sorted({item.model_version for item in predictions})
            ),
            selected_action=action,
            reason=reason,
            constraints=(
                f"snapshot_completeness={snapshot.completeness:.6f}",
                f"prediction_confidence={selected_prediction.confidence:.6f}",
                f"owner={self.owner.component_ref}",
            ),
        )
        self.journal.record_decision(decision, predictions)
        return KVControlPlan(
            decision=decision,
            predictions=predictions,
            current_location=current_location,
            selected_target=selected_target.location_ref,
            observed={
                "phase": phase,
                "reuse_probability": reuse,
                "resume_seconds": resume_seconds,
                "hbm_pressure": hbm_pressure,
            },
        )

    def _validate_ownership(
        self, request: KVControlRequest
    ) -> tuple[GraphEntity, GraphEntity]:
        if not self.owner.manages(request.kv_ref):
            raise PermissionError(
                f"KV action owner does not manage {request.kv_ref}"
            )
        kv_entity = self.plane.get_entity(request.kv_ref)
        if kv_entity is None or kv_entity.entity_type != "stateful_object":
            raise KeyError(f"unknown KV entity: {request.kv_ref}")
        if not kv_entity.owner_component_ref:
            raise PermissionError("KV state has no authoritative owner")
        if request.expected_state_owner and (
            kv_entity.owner_component_ref != request.expected_state_owner
        ):
            raise PermissionError("KV state owner changed")
        owner_entity = self.plane.get_entity(self.owner.component_ref)
        if owner_entity is None or owner_entity.entity_type != "component":
            raise KeyError(f"unknown KV action owner: {self.owner.component_ref}")
        return kv_entity, owner_entity

    def _candidates(
        self,
        snapshot: Snapshot,
        request: KVControlRequest,
        current: KVTarget,
        size: int,
    ) -> tuple[CandidateAction, ...]:
        result: list[CandidateAction] = []
        for target in request.targets:
            action_type = _action_type(current, target)
            if action_type not in self.owner.allowed_actions:
                continue
            if target.location_ref != current.location_ref and (
                not target.available or not target.healthy
            ):
                continue
            transfer_bytes = 0 if target.location_ref == current.location_ref else size
            hbm_resource = target if target.is_hbm else current if current.is_hbm else None
            hbm_used = (
                _optional_number(
                    snapshot,
                    hbm_resource.location_ref,
                    "resource.hbm.used",
                )
                if hbm_resource is not None
                else None
            )
            hbm_reserved = (
                _optional_number(
                    snapshot,
                    hbm_resource.location_ref,
                    "resource.hbm.reserved",
                )
                if hbm_resource is not None
                else None
            )
            parameters: dict[str, Any] = {
                "kv_ref": request.kv_ref,
                "source_location": current.location_ref,
                "target_location": target.location_ref,
                "target_kv_location": target.location_ref,
                "transfer_bytes": transfer_bytes,
                "transfer_setup_seconds": target.transfer_setup_seconds,
                "network_byte_cost": target.network_byte_cost,
                "storage_io_bytes": transfer_bytes if not target.is_hbm else 0,
                "storage_io_byte_cost": target.storage_io_byte_cost,
                "predicted_kv_growth_bytes": (
                    size if target.is_hbm and current.location_ref != target.location_ref else 0
                ),
                "hbm_release_bytes": (
                    size if current.is_hbm and not target.is_hbm else 0
                ),
                "base_success_probability": 1.0,
            }
            if transfer_bytes == 0 or target.effective_bw_bytes_per_second > 0:
                parameters["effective_bw_bytes_per_second"] = (
                    target.effective_bw_bytes_per_second
                )
            if hbm_resource is not None and hbm_resource.hbm_capacity_bytes > 0:
                parameters["hbm_capacity_bytes"] = hbm_resource.hbm_capacity_bytes
            if hbm_used is not None:
                parameters["hbm_used_bytes"] = hbm_used
            if hbm_reserved is not None:
                parameters["hbm_reserved_bytes"] = hbm_reserved
            result.append(
                CandidateAction(
                    candidate_id=(
                        f"candidate:kv:{snapshot.logical_time}:"
                        f"{request.kv_ref}:{action_type.value}:{target.location_ref}"
                    ),
                    action_type=action_type.value,
                    target_component=self.owner.component_ref,
                    parameters=parameters,
                )
            )
        if not result or not any(
            item.action_type == KVActionType.KEEP.value for item in result
        ):
            raise NoKVControlCandidate("no owner-authorized keep baseline")
        return tuple(result)

    def _select(
        self,
        snapshot: Snapshot,
        candidates: tuple[CandidateAction, ...],
        predictions: tuple[Prediction, ...],
        current: KVTarget,
        *,
        phase: str | None,
        reuse: float | None,
        resume_seconds: float | None,
        hbm_pressure: float | None,
        transfer_state: str | None,
    ) -> tuple[tuple[CandidateAction, Prediction], str]:
        pairs = tuple(zip(candidates, predictions))
        keep = next(
            item for item in pairs if item[0].action_type == KVActionType.KEEP.value
        )
        if snapshot.completeness < self.config.min_snapshot_completeness:
            return keep, "snapshot_incomplete_keep"

        desired: KVActionType | None = None
        reason = "baseline_keep"
        if (
            current.is_hbm
            and phase in self.config.offload_phases
            and hbm_pressure is not None
            and hbm_pressure >= self.config.offload_hbm_pressure
            and reuse is not None
            and reuse <= self.config.offload_max_reuse_probability
        ):
            desired = KVActionType.OFFLOAD
            reason = "hbm_pressure_offload"
        elif (
            not current.is_hbm
            and resume_seconds is not None
            and resume_seconds <= self.config.prefetch_horizon_seconds
            and reuse is not None
            and reuse >= self.config.prefetch_min_reuse_probability
        ):
            desired = KVActionType.PREFETCH
            reason = "expected_resume_prefetch"
        if desired is None:
            return keep, reason

        if transfer_state not in self.config.stable_transfer_states:
            return keep, "transfer_state_unsafe_keep"

        eligible = [
            item
            for item in pairs
            if item[0].action_type == desired.value
            and item[1].confidence >= self.config.min_prediction_confidence
            and item[1].fallback == "none"
            and item[1].performance.transfer_eta_seconds is not None
        ]
        if not eligible:
            return keep, "prediction_fallback_keep"
        if desired == KVActionType.PREFETCH:
            resource_safe = [
                item
                for item in eligible
                if item[1].future_state.hbm_pressure is not None
                and item[1].future_state.hbm_pressure
                <= self.config.max_prefetch_hbm_pressure
            ]
            if not resource_safe:
                return keep, "resource_guard_keep"
            eligible = resource_safe
        return min(
            eligible,
            key=lambda item: (
                item[1].performance.transfer_eta_seconds,
                item[1].cost.monetary_cost
                if item[1].cost.monetary_cost is not None
                else float("inf"),
                str(item[0].parameters["target_location"]),
            ),
        ), reason

    def _action(
        self,
        snapshot: Snapshot,
        request: KVControlRequest,
        kv_entity: GraphEntity,
        owner_entity: GraphEntity,
        current: KVTarget,
        target: KVTarget,
        candidate: CandidateAction,
        prediction: Prediction,
        size: int,
        transfer_state: str | None,
    ) -> Action:
        action_type = KVActionType(candidate.action_type)
        reservations: tuple[Reservation, ...] = ()
        if target.is_hbm and target.location_ref != current.location_ref:
            reservations = (
                Reservation(
                    resource_ref=target.location_ref,
                    owner=self.owner.component_ref,
                    amount={"hbm_bytes": float(size)},
                    expires_at=snapshot.created_at
                    + timedelta(seconds=self.config.reservation_ttl_seconds),
                    status="proposed",
                ),
            )
        rollback: Mapping[str, Any] = {}
        if action_type != KVActionType.KEEP:
            rollback = {
                "action_type": KVActionType.MIGRATE.value,
                "kv_ref": request.kv_ref,
                "target_location": current.location_ref,
            }
        transfer_eta = prediction.performance.transfer_eta_seconds or 0.0
        return Action(
            target_component=owner_entity.ref,
            action_type=action_type.value,
            parameters={
                **candidate.parameters,
                "candidate_id": candidate.candidate_id,
                "size_bytes": size,
            },
            preconditions={
                "snapshot_id": snapshot.token,
                "snapshot_logical_time": snapshot.logical_time,
                "kv_ref": request.kv_ref,
                "expected_location": current.location_ref,
                "expected_state_owner": kv_entity.owner_component_ref,
                "action_owner": self.owner.component_ref,
                "expected_transfer_state": transfer_state,
            },
            idempotency_key=f"kv:{snapshot.token}:{candidate.candidate_id}",
            reservations=reservations,
            timeout_seconds=max(1.0, transfer_eta * 2.0),
            rollback_action=rollback,
            dry_run=True,
        )


def _action_type(current: KVTarget, target: KVTarget) -> KVActionType:
    if current.location_ref == target.location_ref:
        return KVActionType.KEEP
    if current.is_hbm and not target.is_hbm:
        return KVActionType.OFFLOAD
    if not current.is_hbm and target.is_hbm:
        return KVActionType.PREFETCH
    return KVActionType.MIGRATE


def _state_value(snapshot: Snapshot, entity_ref: str, key: str) -> StateValue | None:
    matches = [
        item
        for item in snapshot.values
        if item.entity_ref == entity_ref and item.key == key
    ]
    if len(matches) > 1:
        raise ValueError(f"snapshot contains duplicate state value: {entity_ref}:{key}")
    return matches[0] if matches else None


def _required_number(snapshot: Snapshot, entity_ref: str, key: str) -> float:
    value = _optional_number(snapshot, entity_ref, key)
    if value is None:
        raise NoKVControlCandidate(f"missing required KV state: {entity_ref}:{key}")
    return value


def _optional_number(
    snapshot: Snapshot, entity_ref: str, key: str
) -> float | None:
    state = _state_value(snapshot, entity_ref, key)
    if state is None:
        return None
    if isinstance(state.value, bool) or not isinstance(state.value, (int, float)):
        raise ValueError(f"KV control state must be numeric: {entity_ref}:{key}")
    value = float(state.value)
    if not math.isfinite(value) or value < 0:
        raise ValueError(
            f"KV control state must be finite and non-negative: {entity_ref}:{key}"
        )
    return value


def _required_string(snapshot: Snapshot, entity_ref: str, key: str) -> str:
    value = _optional_string(snapshot, entity_ref, key)
    if value is None or not value:
        raise NoKVControlCandidate(f"missing required KV state: {entity_ref}:{key}")
    return value


def _optional_string(
    snapshot: Snapshot, entity_ref: str, key: str
) -> str | None:
    state = _state_value(snapshot, entity_ref, key)
    if state is None:
        return None
    if not isinstance(state.value, str):
        raise ValueError(f"KV control state must be a string: {entity_ref}:{key}")
    return state.value


def _optional_datetime(
    snapshot: Snapshot, entity_ref: str, key: str
) -> datetime | None:
    state = _state_value(snapshot, entity_ref, key)
    if state is None:
        return None
    value = state.value
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError(f"KV control state must be a timestamp: {entity_ref}:{key}")
    return result.replace(tzinfo=result.tzinfo or timezone.utc)


def _hbm_pressure(snapshot: Snapshot, target: KVTarget) -> float | None:
    if not target.is_hbm or target.hbm_capacity_bytes <= 0:
        return None
    used = _optional_number(snapshot, target.location_ref, "resource.hbm.used")
    reserved = _optional_number(
        snapshot, target.location_ref, "resource.hbm.reserved"
    )
    if used is None or reserved is None:
        return None
    return (used + reserved) / target.hbm_capacity_bytes


__all__ = [
    "AgentAwareKVController",
    "KVActionOwner",
    "KVActionType",
    "KVControlPlan",
    "KVControlRequest",
    "KVControllerConfig",
    "KVTarget",
    "NoKVControlCandidate",
]
