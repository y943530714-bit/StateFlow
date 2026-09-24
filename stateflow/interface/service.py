"""One external boundary for state reports and delivery to component adapters."""

from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from threading import Lock
from typing import TYPE_CHECKING, Callable, ContextManager, Iterator, TypeVar

from ..action_catalog import Action, ActionCatalog, ActionDispatchError
from ..state.event import AgentStateEvent, AppendResult
from ..state_manager import StateManager

T = TypeVar("T")


if TYPE_CHECKING:
    from ..gateway.normalizer.request import ProviderNeutralRequest


class UnifiedStateInterface:
    """Accept events, dispatch chosen actions, and ingest execution feedback."""

    def __init__(self, state_manager: StateManager, catalog: ActionCatalog) -> None:
        self.state_manager = state_manager
        self.catalog = catalog
        self._dispatched: OrderedDict[str, None] = OrderedDict()
        self._dispatch_lock = Lock()

    def ingest(self, event: AgentStateEvent) -> AppendResult:
        return self.state_manager.ingest(event)

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
