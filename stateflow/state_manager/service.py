"""Small state API for the first control-plane loop."""

from __future__ import annotations

from ..state.event import AgentStateEvent, AppendResult
from ..state.store.in_memory import InMemoryStateStore
from .targets import TargetRegistry


class StateManager:
    """Own program events and target status; expose a read-only planning view."""

    def __init__(self, store: InMemoryStateStore, targets: TargetRegistry) -> None:
        self.store = store
        self.targets = targets

    def ingest(self, event: AgentStateEvent) -> AppendResult:
        return self.store.append_event(event)

    def get_scheduling_view(self, program_id: str, task_id: str):
        return self.store.get_scheduling_view(program_id, task_id)

    def update_target(self, report: dict) -> None:
        self.targets.update(report)
