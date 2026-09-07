"""Harness adapter contracts.

Adapters translate harness-native state into AgentStateEvent.  They do not
choose models, endpoints, replicas, or KV policies.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol

from ...state.event import AgentStateEvent
from ...state.schema import to_jsonable, utcnow


@dataclass(frozen=True)
class SessionIdentity:
    session_id: str
    task_id: str = "default-task"
    tenant_id: str = "default"
    harness_type: str = "generic-proxy"
    harness_version: str = "0"
    agent_type: str = "unknown"
    agent_version: str = "0"

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)


class EventSink(Protocol):
    def append_event(self, event: AgentStateEvent) -> Any:
        ...


class HarnessAdapter:
    """Base interface shared by Generic Proxy and native thin adapters."""

    source = "harness-adapter"

    def identify_session(self, native_state: Mapping[str, Any] | None = None) -> SessionIdentity:
        raise NotImplementedError

    def snapshot(self, native_state: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return dict(native_state or {})

    def to_event(
        self,
        event_type: str,
        *,
        identity: SessionIdentity,
        payload: Mapping[str, Any] | None = None,
        turn_id: int = 0,
        step_id: str = "",
        request_id: str = "",
        attempt_id: str = "",
        trace_id: str = "",
        sequence: int = 0,
        epoch: int = 0,
        observed_at: datetime | None = None,
        confidence: float = 1.0,
        ttl: float | None = None,
        authoritative: bool = False,
    ) -> AgentStateEvent:
        from datetime import timedelta

        return AgentStateEvent(
            event_type=event_type,
            session_id=identity.session_id,
            task_id=identity.task_id,
            turn_id=turn_id,
            step_id=step_id,
            request_id=request_id,
            attempt_id=attempt_id,
            trace_id=trace_id,
            source=self.source,
            observed_at=observed_at or utcnow(),
            epoch=epoch,
            sequence=sequence,
            payload=dict(payload or {}),
            confidence=confidence,
            ttl=timedelta(seconds=ttl) if ttl is not None else None,
            authoritative=authoritative,
        )

    # Optional event hooks.  Concrete adapters may override only what their
    # harness exposes; absent hooks never block a request.
    def on_task_start(self, event: Mapping[str, Any]) -> AgentStateEvent | None:
        return None

    def on_turn_start(self, event: Mapping[str, Any]) -> AgentStateEvent | None:
        return None

    def on_model_request(self, event: Mapping[str, Any]) -> AgentStateEvent | None:
        return None

    def on_model_response(self, event: Mapping[str, Any]) -> AgentStateEvent | None:
        return None

    def on_tool_start(self, event: Mapping[str, Any]) -> AgentStateEvent | None:
        return None

    def on_tool_progress(self, event: Mapping[str, Any]) -> AgentStateEvent | None:
        return None

    def on_tool_result(self, event: Mapping[str, Any]) -> AgentStateEvent | None:
        return None

    def on_context_compaction(self, event: Mapping[str, Any]) -> AgentStateEvent | None:
        return None

    def on_subagent_start(self, event: Mapping[str, Any]) -> AgentStateEvent | None:
        return None

    def on_task_complete(self, event: Mapping[str, Any]) -> AgentStateEvent | None:
        return None
