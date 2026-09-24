"""Composition root for the four v1 control-plane modules."""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, ContextManager, Iterable, Iterator, TypeVar

from ..action_catalog import Action, ActionCatalog
from ..interface import UnifiedStateInterface
from ..planner import Planner
from ..scheduler.harness.success_first.scheduler import SuccessFirstScheduler
from ..scheduler.types import RoutingDecision
from ..state.event import AgentStateEvent, AppendResult
from ..state.schema import TargetCandidate
from ..state_manager import StateManager

T = TypeVar("T")


if TYPE_CHECKING:
    from ..gateway.normalizer.request import ProviderNeutralRequest


class ControlPlane:
    """Wire state, planner, action definitions and the unified interface."""

    def __init__(
        self,
        state_manager: StateManager,
        planner: SuccessFirstScheduler,
        catalog: ActionCatalog | None = None,
    ) -> None:
        self.state_manager = state_manager
        self.catalog = catalog or ActionCatalog()
        self.planner = Planner(state_manager, planner, self.catalog)
        self.interface = UnifiedStateInterface(state_manager, self.catalog)

    def ingest(self, event: AgentStateEvent) -> AppendResult:
        return self.interface.ingest(event)

    def plan_route(
        self, request: ProviderNeutralRequest, targets: Iterable[TargetCandidate]
    ) -> tuple[RoutingDecision, Action]:
        return self.planner.plan_route(request, targets)

    def dispatch(
        self, action: Action, handler: Callable[[], T], *, request: ProviderNeutralRequest
    ) -> T:
        return self.interface.dispatch(action, handler, request=request)

    def dispatch_stream(
        self,
        action: Action,
        stream_factory: Callable[[], ContextManager[Iterator[bytes]]],
        *,
        request: ProviderNeutralRequest,
    ) -> ContextManager[Iterator[bytes]]:
        return self.interface.dispatch_stream(action, stream_factory, request=request)
