"""Protocol-compatible StateFlow request gateway."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from ..backend.base import BackendError, BackendRegistry
from ..scheduler.harness.success_first.scheduler import SuccessFirstScheduler
from ..scheduler.types import NoFeasibleTarget, RoutingDecision
from ..state.event import AgentStateEvent
from ..state.schema import TargetCandidate, to_jsonable, utcnow
from ..state.store.in_memory import InMemoryStateStore
from .normalizer.request import ProviderNeutralRequest


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


class TargetRegistry:
    def __init__(self, targets: Iterable[TargetCandidate] = ()) -> None:
        self._targets = list(targets)

    def register(self, target: TargetCandidate) -> None:
        self._targets.append(target)

    def all(self) -> list[TargetCandidate]:
        return list(self._targets)


class RequestGateway:
    """Routes normalized requests and keeps adapter/state failures off hot path."""

    def __init__(
        self,
        scheduler: SuccessFirstScheduler,
        state_store: InMemoryStateStore,
        targets: TargetRegistry,
        backends: BackendRegistry,
    ) -> None:
        self.scheduler = scheduler
        self.state_store = state_store
        self.targets = targets
        self.backends = backends
        if self.scheduler.decision_sink is None:
            self.scheduler.decision_sink = self.state_store.record_routing_decision

    def handle(self, request: ProviderNeutralRequest) -> GatewayResponse:
        self._record_model_request(request)
        view = self.state_store.get_scheduling_view(request.session_id, request.task_id)
        # The request itself is authoritative for current prompt/decoder size;
        # state events from a native adapter may enrich the other fields.
        view.prompt_tokens = request.prompt_tokens
        view.predicted_output_tokens = request.predicted_output_tokens
        view.logical_model = request.model or view.logical_model
        view.required_capabilities.update(request.required_capabilities)

        try:
            decision = self.scheduler.schedule(view, self.targets.all())
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
            item for item in self.targets.all()
            if item.model_id == decision.selected_model
            and item.endpoint_id == decision.selected_endpoint
            and item.replica_id == decision.selected_replica
        )
        try:
            adapter = self.backends.require(target.backend_key)
            backend_response = adapter.send(request, target)
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

    def _record_model_request(self, request: ProviderNeutralRequest) -> None:
        self.state_store.append_event(
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
                payload=request.to_state_payload(),
                authoritative=True,
            )
        )

    def _record_model_response(self, request: ProviderNeutralRequest, output_tokens: int) -> None:
        self.state_store.append_event(
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
        self.state_store.append_event(
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
