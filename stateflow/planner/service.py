"""Request-time route selection; returns a declared action without executing it."""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

from stateflow.action_catalog import Action, ActionCatalog
from stateflow.planner.scheduler import SuccessFirstScheduler
from stateflow.planner.types import NoFeasibleTarget, RoutingDecision
from stateflow.state_manager.schema import TargetCandidate
from stateflow.state_manager import StateManager


if TYPE_CHECKING:
    from stateflow.interface.request import ProviderNeutralRequest


class Planner:
    """Apply the current route policy to program state and catalog capabilities."""

    def __init__(self, state_manager: StateManager, policy: SuccessFirstScheduler, catalog: ActionCatalog) -> None:
        self.state_manager = state_manager
        self.policy = policy
        self.catalog = catalog

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
        decision = self.policy.schedule(view, targets)
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
