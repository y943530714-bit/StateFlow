"""L0 GenericProxyHarnessAdapter.

It derives the minimum useful Agent State from API traffic and can optionally
publish events through AsyncStateReporter.  It is intentionally provider and
harness neutral.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ...state.event import AgentStateEvent
from ...state.schema import to_jsonable
from ..base.adapter import HarnessAdapter, SessionIdentity
from ..base.reporter import AsyncStateReporter


def _value(event: Mapping[str, Any] | Any, name: str, default: Any = None) -> Any:
    if isinstance(event, Mapping):
        return event.get(name, default)
    return getattr(event, name, default)


@dataclass
class GenericProxyHarnessAdapter(HarnessAdapter):
    source: str = "generic-proxy-adapter"
    harness_type: str = "generic-proxy"
    harness_version: str = "0"
    reporter: AsyncStateReporter | None = None

    def identify_session(self, native_state: Mapping[str, Any] | None = None) -> SessionIdentity:
        native_state = native_state or {}
        return SessionIdentity(
            session_id=str(native_state.get("session_id", "")),
            task_id=str(native_state.get("task_id", "default-task")),
            tenant_id=str(native_state.get("tenant_id", "default")),
            harness_type=str(native_state.get("harness_type", self.harness_type)),
            harness_version=str(native_state.get("harness_version", self.harness_version)),
            agent_type=str(native_state.get("agent_type", "unknown")),
            agent_version=str(native_state.get("agent_version", "0")),
        )

    def snapshot(self, native_state: Mapping[str, Any] | None = None) -> dict[str, Any]:
        # L0 intentionally keeps metadata only; no prompt/tool result body is
        # stored in the state plane.
        state = dict(native_state or {})
        for key in ("prompt", "messages", "tool_result", "result", "kv_tensor"):
            state.pop(key, None)
        return state

    def _event(
        self,
        event_type: str,
        native: Mapping[str, Any] | Any,
        *,
        payload: Mapping[str, Any] | None = None,
    ) -> AgentStateEvent:
        identity = self.identify_session(native if isinstance(native, Mapping) else None)
        raw_payload = dict(payload or {})
        if not raw_payload and isinstance(native, Mapping):
            raw_payload = self.snapshot(native)
            for key in ("session_id", "task_id", "tenant_id", "harness_type", "harness_version"):
                raw_payload.pop(key, None)
        event = self.to_event(
            event_type,
            identity=identity,
            payload=raw_payload,
            turn_id=int(_value(native, "turn_id", 0) or 0),
            step_id=str(_value(native, "step_id", "") or ""),
            request_id=str(_value(native, "request_id", "") or ""),
            attempt_id=str(_value(native, "attempt_id", "") or ""),
            trace_id=str(_value(native, "trace_id", "") or ""),
            sequence=int(_value(native, "sequence", 0) or 0),
            epoch=int(_value(native, "epoch", 0) or 0),
            confidence=float(_value(native, "confidence", 1.0) or 0.0),
            ttl=_value(native, "ttl_seconds", None),
            authoritative=bool(_value(native, "authoritative", False)),
        )
        if self.reporter is not None:
            self.reporter.publish(event)
        return event

    def on_task_start(self, event: Mapping[str, Any]) -> AgentStateEvent:
        return self._event("TASK_STARTED", event)

    def on_turn_start(self, event: Mapping[str, Any]) -> AgentStateEvent:
        return self._event("TURN_STARTED", event)

    def on_model_request(self, event: Mapping[str, Any]) -> AgentStateEvent:
        payload = {
            key: event[key]
            for key in (
                "model",
                "logical_model",
                "prompt_tokens",
                "predicted_output_tokens",
                "required_capabilities",
                "context",
                "security",
                "inference",
            )
            if key in event
        }
        return self._event("MODEL_REQUESTED", event, payload=payload)

    def on_model_response(self, event: Mapping[str, Any]) -> AgentStateEvent:
        payload = {
            key: event[key]
            for key in (
                "success",
                "output_tokens",
                "progress",
                "next_action",
                "task_complete",
                "progress_signals",
                "prediction",
            )
            if key in event
        }
        return self._event("MODEL_RESPONSE_RECEIVED", event, payload=payload)

    def on_tool_start(self, event: Mapping[str, Any]) -> AgentStateEvent:
        return self._event("TOOL_STARTED", event, payload=dict(event))

    def on_tool_progress(self, event: Mapping[str, Any]) -> AgentStateEvent:
        return self._event("TOOL_PROGRESS", event, payload=dict(event))

    def on_tool_result(self, event: Mapping[str, Any]) -> AgentStateEvent:
        return self._event("TOOL_RESULT_READY", event, payload=dict(event))

    def on_context_compaction(self, event: Mapping[str, Any]) -> AgentStateEvent:
        return self._event("CONTEXT_COMPACTED", event, payload=dict(event))

    def on_subagent_start(self, event: Mapping[str, Any]) -> AgentStateEvent:
        return self._event("SUBAGENT_STARTED", event, payload=dict(event))

    def on_task_complete(self, event: Mapping[str, Any]) -> AgentStateEvent:
        return self._event("TASK_COMPLETED", event, payload=dict(event))


__all__ = ["GenericProxyHarnessAdapter"]
