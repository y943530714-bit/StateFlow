"""State ingress, action planning, and dispatch boundaries.

An action describes what a component should do. The component (or a backend
adapter in proxy mode) remains responsible for doing it.
"""

from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Lock
from typing import Callable, ContextManager, Iterable, Iterator, TypeVar

from ..gateway.normalizer.request import ProviderNeutralRequest
from ..scheduler.harness.success_first.scheduler import SuccessFirstScheduler
from ..scheduler.types import NoFeasibleTarget, RoutingDecision
from ..state.event import AgentStateEvent, AppendResult
from ..state.schema import TargetCandidate, to_jsonable
from ..state.store.in_memory import InMemoryStateStore


@dataclass(frozen=True)
class Action:
    decision_id: str
    program_id: str
    action_id: str
    target: str
    params: dict[str, str]
    state_version: int

    def to_dict(self) -> dict:
        return to_jsonable(self)


@dataclass(frozen=True)
class ActionDefinition:
    action_id: str
    target_type: str
    required_params: tuple[str, ...]

    def to_dict(self) -> dict:
        return to_jsonable(self)


class ActionDispatchError(RuntimeError):
    """An action is invalid or has already been dispatched."""


class ActionCatalog:
    """Declarative capabilities; no component execution lives here."""

    def __init__(self, definitions: Iterable[ActionDefinition] | None = None) -> None:
        self._definitions = {
            item.action_id: item
            for item in (definitions if definitions is not None else (
                ActionDefinition("route_model", "request_gateway", ("model_id", "endpoint_id", "replica_id")),
            ))
        }

    def all(self) -> list[dict]:
        return [item.to_dict() for item in self._definitions.values()]

    def supports(self, action_id: str) -> bool:
        return action_id in self._definitions

    def validate(self, action: Action) -> None:
        definition = self._definitions.get(action.action_id)
        if definition is None:
            raise ActionDispatchError(f"unsupported action: {action.action_id}")
        if not action.decision_id or not action.program_id or not action.target:
            raise ActionDispatchError("decision_id, program_id and target are required")
        missing = [key for key in definition.required_params if not action.params.get(key)]
        if missing:
            raise ActionDispatchError(f"missing action parameters: {', '.join(missing)}")


T = TypeVar("T")


class ControlPlane:
    """One ingress and one dispatch interface, with a pluggable planner policy."""

    def __init__(
        self,
        state_manager: InMemoryStateStore,
        planner: SuccessFirstScheduler,
        catalog: ActionCatalog | None = None,
    ) -> None:
        self.state_manager = state_manager
        self.planner = planner
        self.catalog = catalog or ActionCatalog()
        self._dispatched: OrderedDict[str, None] = OrderedDict()
        self._dispatch_lock = Lock()

    def ingest(self, event: AgentStateEvent) -> AppendResult:
        """Receive a component state event, including action feedback."""

        return self.state_manager.append_event(event)

    def plan_route(
        self, request: ProviderNeutralRequest, targets: Iterable[TargetCandidate]
    ) -> tuple[RoutingDecision, Action]:
        """Select one model and replica without executing the route."""

        if not self.catalog.supports("route_model"):
            raise NoFeasibleTarget("route_model is not supported by the Action Catalog")
        view = self.state_manager.get_scheduling_view(request.session_id, request.task_id)
        view.prompt_tokens = request.prompt_tokens
        view.predicted_output_tokens = request.predicted_output_tokens
        view.logical_model = request.model or view.logical_model
        view.required_capabilities.update(request.required_capabilities)
        # Current request restrictions are authoritative, even if no harness
        # event or scheduling snapshot has arrived yet.
        if request.tenant_id != "default" and view.tenant_id not in {"default", request.tenant_id}:
            raise NoFeasibleTarget("tenant identity conflicts with the recorded program")
        if request.security_domain != "default" and view.security_domain not in {"default", request.security_domain}:
            raise NoFeasibleTarget("security domain conflicts with the recorded program")
        if request.tenant_id != "default":
            view.tenant_id = request.tenant_id
        if request.security_domain != "default":
            view.security_domain = request.security_domain
        for key in ("allowed_models", "allowed_regions", "allowed_cache_domains"):
            current = getattr(view, key)
            requested = getattr(request, key)
            if current and requested and not current.intersection(requested):
                raise NoFeasibleTarget(f"{key} conflicts with the recorded program")
            setattr(view, key, current.intersection(requested) if current and requested else current or requested)
        view.remote_kv_allowed = view.remote_kv_allowed and request.remote_kv_allowed
        if request.cost_budget is not None:
            view.cost_budget = min(view.cost_budget, request.cost_budget) if view.cost_budget is not None else request.cost_budget
        decision = self.planner.schedule(view, targets)
        action = Action(
            decision_id=decision.decision_id,
            program_id=request.session_id,
            action_id="route_model",
            target=decision.selected_endpoint,
            params={
                "model_id": decision.selected_model,
                "endpoint_id": decision.selected_endpoint,
                "replica_id": decision.selected_replica,
            },
            state_version=decision.state_version,
        )
        self.catalog.validate(action)
        return decision, action

    def dispatch(
        self,
        action: Action,
        handler: Callable[[], T],
        *,
        request: ProviderNeutralRequest,
    ) -> T:
        """Deliver a selected action to the component adapter and collect feedback."""

        self._claim(action)
        self._feedback(request, action, "ACTION_DISPATCHED")
        try:
            result = handler()
        except Exception as exc:
            self._feedback(request, action, "ACTION_FAILED", type(exc).__name__)
            raise
        self._feedback(request, action, "ACTION_SUCCEEDED")
        return result

    @contextmanager
    def dispatch_stream(
        self,
        action: Action,
        stream_factory: Callable[[], ContextManager[Iterator[bytes]]],
        *,
        request: ProviderNeutralRequest,
    ) -> Iterator[Iterator[bytes]]:
        """Keep action feedback open until the backend stream has finished."""

        self._claim(action)
        self._feedback(request, action, "ACTION_DISPATCHED")
        try:
            with stream_factory() as chunks:
                yield chunks
        except Exception as exc:
            self._feedback(request, action, "ACTION_FAILED", type(exc).__name__)
            raise
        self._feedback(request, action, "ACTION_SUCCEEDED")

    def _claim(self, action: Action) -> None:
        self.catalog.validate(action)
        with self._dispatch_lock:
            if action.decision_id in self._dispatched:
                raise ActionDispatchError(f"decision already dispatched: {action.decision_id}")
            self._dispatched[action.decision_id] = None
            if len(self._dispatched) > 4096:
                self._dispatched.popitem(last=False)

    def _feedback(
        self, request: ProviderNeutralRequest, action: Action, event_type: str,
        error: str = "",
    ) -> None:
        self.ingest(AgentStateEvent(
            event_type=event_type,
            session_id=request.session_id,
            task_id=request.task_id,
            turn_id=request.turn_id,
            request_id=request.request_id,
            source="stateflow-dispatch",
            payload={"action": {**action.to_dict(), "error": error}},
            authoritative=True,
        ))
