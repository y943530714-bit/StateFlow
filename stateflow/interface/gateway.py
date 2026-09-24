"""Protocol-compatible StateFlow request gateway."""

from __future__ import annotations

from dataclasses import dataclass, field
from contextlib import contextmanager
from typing import Any, Iterable, Iterator

from stateflow.interface.backend import BackendError, BackendRegistry
from stateflow.action_catalog import Action, ActionCatalog
from stateflow.interface.control_plane import ControlPlane
from stateflow.state_manager import StateManager, TargetRegistry
from stateflow.planner.scheduler import SuccessFirstScheduler
from stateflow.planner.types import NoFeasibleTarget, RoutingDecision
from stateflow.state_manager.event import AgentStateEvent
from stateflow.state_manager.schema import TargetCandidate, to_jsonable
from stateflow.state_manager.store import InMemoryStateStore
from stateflow.interface.request import ProviderNeutralRequest


@dataclass
class GatewayResponse:
    status_code: int
    payload: dict[str, Any]
    decision: RoutingDecision | None = None
    headers: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        result = {
            "status_code": self.status_code,
            "payload": self.payload,
            "headers": self.headers,
        }
        if self.decision is not None:
            result["decision"] = self.decision.to_dict()
        return to_jsonable(result)


class RequestGateway:
    """Routes normalized requests and keeps adapter/state failures off hot path."""

    def __init__(
        self,
        scheduler: SuccessFirstScheduler,
        state_store: InMemoryStateStore,
        targets: TargetRegistry,
        backends: BackendRegistry,
        action_catalog: ActionCatalog | None = None,
    ) -> None:
        self.scheduler = scheduler
        self.state_store = state_store
        self.targets = targets
        self.backends = backends
        if self.scheduler.decision_sink is None:
            self.scheduler.decision_sink = self.state_store.record_routing_decision
        self.state_manager = StateManager(self.state_store, self.targets)
        self.control_plane = ControlPlane(self.state_manager, self.scheduler, action_catalog)

    def plan(self, request: ProviderNeutralRequest) -> tuple[RoutingDecision, Action]:
        """Synchronous decision hook for schedulers that execute requests themselves."""

        self._record_model_request(request)
        return self.control_plane.plan_route(request, self._supported_targets(request))

    def _supported_targets(self, request: ProviderNeutralRequest) -> tuple[TargetCandidate, ...]:
        return tuple(
            target for target in self.targets.all()
            if request.protocol in target.metadata.get(
                "protocols", ("openai_chat", "openai_responses", "anthropic_messages")
            )
        )

    def handle(self, request: ProviderNeutralRequest) -> GatewayResponse:
        target_list = self._supported_targets(request)
        self._record_model_request(request)
        try:
            decision, action = self.control_plane.plan_route(request, target_list)
        except NoFeasibleTarget as exc:
            self._record_failure(request, "NO_FEASIBLE_TARGET", str(exc))
            return GatewayResponse(
                status_code=503,
                payload={
                    "error": {
                        "type": "no_feasible_target",
                        "message": str(exc),
                        "rejections": [item.to_dict() for item in exc.rejections],
                    }
                },
            )

        target = next(
            item for item in target_list
            if item.model_id == decision.selected_model
            and item.endpoint_id == decision.selected_endpoint
            and item.replica_id == decision.selected_replica
        )
        try:
            adapter = self.backends.require(target.backend_key)
            backend_response = self.control_plane.dispatch(
                action, lambda: adapter.send(request, target), request=request
            )
        except BackendError as exc:
            self._record_failure(request, "BACKEND", str(exc))
            return GatewayResponse(
                status_code=502,
                payload={"error": {"type": "backend_error", "message": str(exc)}},
                decision=decision,
                headers={"x-stateflow-decision-id": decision.decision_id},
            )

        self._record_model_response(request, backend_response.output_tokens)
        return GatewayResponse(
            status_code=backend_response.status_code,
            payload=backend_response.payload,
            decision=decision,
            headers={
                "x-stateflow-decision-id": decision.decision_id,
                "x-stateflow-selected-model": decision.selected_model,
                "x-stateflow-selected-replica": decision.selected_replica,
            },
        )

    @contextmanager
    def stream(self, request: ProviderNeutralRequest) -> Iterator[tuple[GatewayResponse, Iterator[bytes]]]:
        """Plan once, then forward the upstream event stream unchanged."""

        targets = self._supported_targets(request)
        self._record_model_request(request)
        decision, action = self.control_plane.plan_route(request, targets)
        target = next(item for item in targets if (
            item.model_id, item.endpoint_id, item.replica_id
        ) == (
            decision.selected_model, decision.selected_endpoint, decision.selected_replica
        ))
        try:
            adapter = self.backends.require(target.backend_key)
            if not hasattr(adapter, "open_stream"):
                raise BackendError(f"backend {target.backend_key!r} does not support streaming")
            with self.control_plane.dispatch_stream(
                action, lambda: adapter.open_stream(request, target), request=request
            ) as chunks:
                yield GatewayResponse(
                    status_code=200,
                    payload={},
                    decision=decision,
                    headers={
                        "x-stateflow-decision-id": decision.decision_id,
                        "x-stateflow-selected-model": decision.selected_model,
                        "x-stateflow-selected-replica": decision.selected_replica,
                    },
                ), chunks
        except BackendError as exc:
            self._record_failure(request, "BACKEND", str(exc))
            raise
        self._record_model_response(request, 0)

    def _record_model_request(self, request: ProviderNeutralRequest) -> None:
        payload = request.to_state_payload()
        # Request-local identity, security and budget constraints are applied
        # by the Planner. Persisting their defaults as a state patch could
        # silently erase stricter program-wide constraints supplied by a harness.
        for key in ("identity", "security", "qos"):
            payload.pop(key, None)
        self.control_plane.ingest(
            AgentStateEvent(
                event_type="MODEL_REQUESTED",
                session_id=request.session_id,
                task_id=request.task_id,
                turn_id=request.turn_id,
                step_id=request.step_id,
                request_id=request.request_id,
                attempt_id=request.attempt_id,
                trace_id=request.trace_id,
                source="stateflow-gateway",
                payload=payload,
                authoritative=True,
            )
        )

    def _record_model_response(self, request: ProviderNeutralRequest, output_tokens: int) -> None:
        self.control_plane.ingest(
            AgentStateEvent(
                event_type="MODEL_RESPONSE_RECEIVED",
                session_id=request.session_id,
                task_id=request.task_id,
                turn_id=request.turn_id,
                step_id=request.step_id,
                request_id=request.request_id,
                attempt_id=request.attempt_id,
                trace_id=request.trace_id,
                source="stateflow-gateway",
                payload={"success": True, "output_tokens": output_tokens},
                authoritative=True,
            )
        )

    def _record_failure(self, request: ProviderNeutralRequest, component: str, error: str) -> None:
        self.control_plane.ingest(
            AgentStateEvent(
                event_type="MODEL_FAILED",
                session_id=request.session_id,
                task_id=request.task_id,
                turn_id=request.turn_id,
                step_id=request.step_id,
                request_id=request.request_id,
                attempt_id=request.attempt_id,
                trace_id=request.trace_id,
                source="stateflow-gateway",
                payload={
                    "failed_component": component,
                    "error": error,
                    "error_severity": 0.8,
                },
                authoritative=True,
            )
        )
