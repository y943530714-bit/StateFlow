"""Thread-safe in-memory Event + Snapshot + Hot View state plane.

This is the reference implementation for the MVP.  A production deployment
can replace it with a durable event log and a distributed snapshot store while
keeping the scheduler-facing API unchanged.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import fields, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import threading
from typing import Any, Callable, Iterable

from stateflow.state_manager.event import AgentStateEvent, AppendResult
from stateflow.state_manager.schema import (
    AgentPhase,
    AgentState,
    AgentProgressSignals,
    ContextSegment,
    ExecutionNode,
    InferenceEndpointState,
    KVCacheTier,
    KVSegment,
    StateField,
    ToolExecution,
    ToolStatus,
    build_scheduling_view,
    new_agent_state,
    utcnow,
)


StateCallback = Callable[[AgentStateEvent, AgentState], None]


class InMemoryStateStore:
    """State store with epoch/sequence de-duplication and TTL-aware views."""

    def __init__(self) -> None:
        self._states: dict[tuple[str, str], AgentState] = {}
        self._last_order: dict[tuple[str, str], tuple[int, int]] = {}
        self._events: list[AgentStateEvent] = []
        self._subscribers: list[StateCallback] = []
        self._lock = threading.RLock()

    def subscribe(self, callback: StateCallback) -> None:
        with self._lock:
            self._subscribers.append(callback)

    def get_or_create(
        self,
        session_id: str,
        task_id: str = "default-task",
        *,
        tenant_id: str = "default",
        harness_type: str = "generic-proxy",
    ) -> AgentState:
        key = (session_id, task_id)
        with self._lock:
            state = self._states.get(key)
            if state is None:
                state = new_agent_state(
                    session_id,
                    task_id,
                    tenant_id=tenant_id,
                    harness_type=harness_type,
                )
                self._states[key] = state
            return deepcopy(state)

    def append_event(self, event: AgentStateEvent) -> AppendResult:
        key = (event.session_id, event.task_id)
        callbacks: list[StateCallback] = []
        state_copy: AgentState | None = None
        with self._lock:
            last_epoch, last_sequence = self._last_order.get(key, (-1, -1))
            if event.sequence <= 0:
                event.sequence = last_sequence + 1 if event.epoch == last_epoch else 1

            current_order = (event.epoch, event.sequence)
            previous_order = (last_epoch, last_sequence)
            if current_order == previous_order:
                state = self._states.get(key)
                return AppendResult(
                    accepted=False,
                    duplicate=True,
                    state_id=state.header.state_id if state else "",
                    state_version=state.header.sequence_number if state else 0,
                    reason="duplicate epoch/sequence",
                )
            if current_order < previous_order:
                state = self._states.get(key)
                return AppendResult(
                    accepted=False,
                    stale=True,
                    state_id=state.header.state_id if state else "",
                    state_version=state.header.sequence_number if state else 0,
                    reason="stale epoch/sequence",
                )

            state = self._states.get(key)
            if state is None:
                state = new_agent_state(event.session_id, event.task_id)
                self._states[key] = state
            self._apply_event(state, event)
            state.header.session_id = event.session_id
            state.header.task_id = event.task_id
            state.header.turn_id = max(state.header.turn_id, event.turn_id)
            if event.step_id:
                state.header.step_id = event.step_id
            if event.request_id:
                state.header.request_id = event.request_id
            if event.attempt_id:
                state.header.attempt_id = event.attempt_id
            if event.parent_step_id:
                state.header.parent_step_id = event.parent_step_id
            if event.trace_id:
                state.header.trace_id = event.trace_id
            state.header.epoch = event.epoch
            state.header.sequence_number = event.sequence
            state.header.updated_at = event.observed_at
            state.header.update_source = event.source
            state.header.dirty_fields.update(event.payload.keys())
            self._last_order[key] = current_order
            self._events.append(deepcopy(event))
            callbacks = list(self._subscribers)
            state_copy = deepcopy(state)

            result = AppendResult(
                accepted=True,
                state_id=state.header.state_id,
                state_version=state.header.sequence_number,
                reason="accepted",
            )

        for callback in callbacks:
            try:
                callback(event, state_copy or AgentState())
            except Exception:
                # State observers are intentionally out of the request hot path.
                # A broken observer must not turn a successful event append into
                # a model request failure.
                continue
        return result

    def append_events(self, events: Iterable[AgentStateEvent]) -> list[AppendResult]:
        return [self.append_event(event) for event in events]

    def get_state(self, session_id: str, task_id: str = "default-task") -> AgentState | None:
        with self._lock:
            state = self._states.get((session_id, task_id))
            return deepcopy(state) if state is not None else None

    def get_scheduling_view(
        self,
        session_id: str,
        task_id: str = "default-task",
    ):
        state = self.get_state(session_id, task_id)
        if state is None:
            state = new_agent_state(session_id, task_id)
        return build_scheduling_view(state)

    def record_routing_decision(self, view, decision) -> None:
        """Write a complete decision summary back to the state snapshot."""

        key = (view.session_id, view.task_id)
        with self._lock:
            state = self._states.get(key)
            if state is None:
                state = new_agent_state(view.session_id, view.task_id, tenant_id=view.tenant_id)
                self._states[key] = state
            state.model.selected_model = decision.selected_model
            state.model.selected_endpoint = decision.selected_endpoint
            state.model.selected_replica = decision.selected_replica
            state.scheduling.preferred_endpoint = decision.selected_endpoint
            state.scheduling.preferred_replica = decision.selected_replica
            state.scheduling.routing_history.current_model = decision.selected_model
            state.scheduling.routing_history.current_endpoint = decision.selected_endpoint
            state.scheduling.routing_history.current_replica = decision.selected_replica
            selected = next(
                (item for item in decision.candidates if item.model_id == decision.selected_model
                 and item.endpoint_id == decision.selected_endpoint
                 and item.replica_id == decision.selected_replica),
                None,
            )
            selected_tier = selected.tier if selected else state.scheduling.routing_history.current_tier
            previous_tier = state.scheduling.routing_history.current_tier
            if previous_tier == selected_tier:
                state.scheduling.routing_history.stable_steps += 1
            else:
                state.scheduling.routing_history.stable_steps = 1
                state.scheduling.routing_history.last_tier_change = decision.timestamp
                state.scheduling.routing_history.last_change_reason = decision.decision_reason.value
                if previous_tier and selected_tier and previous_tier != selected_tier:
                    state.scheduling.routing_history.escalation_count += 1
            state.scheduling.routing_history.current_tier = selected_tier
            state.scheduling.routing_history.failure_streak = view.failure_streak
            state.scheduling.decision_history.append(decision.to_dict())
            # Bound the in-memory history.  Durable implementations should put
            # the full decision log in the event/trace plane.
            del state.scheduling.decision_history[:-1000]
            state.header.sequence_number += 1
            # A routing decision is a snapshot mutation rather than a new
            # AgentStateEvent, so reserve its version in the order tracker.
            # The next auto-sequenced event must not reuse this version.
            self._last_order[key] = (state.header.epoch, state.header.sequence_number)
            state.header.updated_at = decision.timestamp
            state.header.update_source = "success-first-scheduler"
            state.header.dirty_fields.update({"model", "scheduling.routing_history", "decision_history"})

    def events(self) -> list[AgentStateEvent]:
        with self._lock:
            return deepcopy(self._events)

    def events_for(
        self,
        session_id: str,
        task_id: str = "default-task",
    ) -> list[AgentStateEvent]:
        with self._lock:
            return deepcopy(
                [event for event in self._events if (event.session_id, event.task_id) == (session_id, task_id)]
            )

    def _apply_event(self, state: AgentState, event: AgentStateEvent) -> None:
        event_type = event.event_type.upper()
        payload = dict(event.payload or {})
        self._apply_identity(state, payload.get("identity", payload), event)

        if event_type in {"SESSION_STARTED", "TASK_STARTED"}:
            self._set_section(state, "task", payload.get("task", payload), event)
            self._set_phase(state, AgentPhase.RUNNABLE, event)
            state.lifecycle.runnable = True
            self._set_section(state, "prediction", payload.get("prediction", {}), event)
            self._set_section(state, "qos", payload.get("qos", {}), event)
            self._mark(state, "lifecycle", event)
            return

        if event_type == "TURN_STARTED":
            state.lifecycle.current_turn = event.turn_id
            state.header.turn_id = event.turn_id
            if event.step_id:
                state.lifecycle.current_step = event.step_id
            self._set_section(state, "context", payload.get("context", payload), event)
            self._set_section(state, "prediction", payload.get("prediction", {}), event)
            self._set_phase(state, AgentPhase.RUNNABLE, event)
            state.lifecycle.runnable = True
            self._mark(state, "lifecycle", event)
            return

        if event_type in {"MODEL_REQUEST", "MODEL_REQUESTED"}:
            self._apply_model_request(state, payload, event)
            return

        if event_type in {"MODEL_STARTED", "MODEL_PREFILL"}:
            self._set_phase(state, AgentPhase.MODEL_PREFILL, event)
            state.lifecycle.runnable = True
            self._mark(state, "lifecycle", event)
            return

        if event_type in {"MODEL_DECODE", "MODEL_STREAM_STARTED"}:
            self._set_phase(state, AgentPhase.MODEL_DECODE, event)
            state.lifecycle.runnable = True
            self._mark(state, "lifecycle", event)
            return

        if event_type in {"MODEL_RESPONSE", "MODEL_RESPONSE_RECEIVED", "MODEL_COMPLETED"}:
            self._apply_model_response(state, payload, event)
            return

        if event_type in {"TOOL_STARTED", "TOOL_START"}:
            self._apply_tool_started(state, payload, event)
            return

        if event_type in {"TOOL_PROGRESS", "TOOL_WAITING"}:
            self._apply_tool_progress(state, payload, event)
            return

        if event_type in {"TOOL_RESULT_READY", "TOOL_COMPLETED", "TOOL_RESULT"}:
            self._apply_tool_result(state, payload, event)
            return

        if event_type in {"CONTEXT_COMPACTION", "CONTEXT_COMPACTED"}:
            self._set_section(state, "context", payload.get("context", payload), event)
            state.context.compaction_needed = False
            self._mark(state, "context", event)
            return

        if event_type in {"TASK_FAILED", "MODEL_FAILED", "TOOL_FAILED"}:
            self._apply_failure(state, payload, event, event_type)
            return

        if event_type in {"TASK_COMPLETED", "TASK_COMPLETE"}:
            state.lifecycle.progress = 1.0
            state.lifecycle.runnable = False
            self._set_phase(state, AgentPhase.COMPLETED, event)
            self._set_section(state, "task", payload.get("task", {}), event)
            self._mark(state, "lifecycle", event)
            return

        if event_type in {"INFERENCE_UPDATED", "KV_UPDATED", "TOOL_STATE_UPDATED"}:
            section = {
                "INFERENCE_UPDATED": "inference",
                "KV_UPDATED": "kv_cache",
                "TOOL_STATE_UPDATED": "tools",
            }[event_type]
            self._set_section(state, section, payload.get(section, payload), event)
            return

        if event_type in {"STATE_PATCH", "PATCH"}:
            self._apply_patch(state, payload, event)
            return

        # Unknown events remain in the event log and may still update obvious
        # state sections.  This makes the core schema extensible without making
        # the hot path depend on every future harness event type.
        self._apply_patch(state, payload, event)

    def _apply_model_request(self, state: AgentState, payload: dict[str, Any], event: AgentStateEvent) -> None:
        self._set_phase(state, AgentPhase.MODEL_QUEUED, event)
        state.lifecycle.runnable = True
        if event.step_id:
            state.lifecycle.current_step = event.step_id
        model = payload.get("logical_model") or payload.get("model") or state.model.logical_model
        if model:
            state.model.logical_model = str(model)
            self._mark(state, "model.logical_model", event)
        if "prompt_tokens" in payload:
            tokens = max(0, int(payload["prompt_tokens"]))
            state.model.prompt_tokens = tokens
            state.context.prompt_tokens = tokens
            state.context.total_tokens = max(state.context.total_tokens, tokens)
        if "predicted_output_tokens" in payload:
            state.model.predicted_output_tokens = max(0, int(payload["predicted_output_tokens"]))
        if "context" in payload:
            self._set_section(state, "context", payload["context"], event)
        if "required_capabilities" in payload:
            state.identity.required_capabilities = set(payload["required_capabilities"] or [])
        if "security" in payload:
            self._set_section(state, "security", payload["security"], event)
        if "inference" in payload:
            self._set_section(state, "inference", payload["inference"], event)
        if "qos" in payload:
            self._set_section(state, "qos", payload["qos"], event)
        self._mark(state, "model", event)
        self._mark(state, "context.prompt_tokens", event)
        self._mark(state, "lifecycle", event)

    def _apply_model_response(self, state: AgentState, payload: dict[str, Any], event: AgentStateEvent) -> None:
        success = payload.get("success", True)
        if "output_tokens" in payload:
            output_tokens = max(0, int(payload["output_tokens"]))
            state.context.assistant_tokens += output_tokens
            state.context.total_tokens += output_tokens
        if "progress" in payload:
            self._set_progress(state, float(payload["progress"]), event)
        if "progress_signals" in payload:
            self._set_section(state, "progress_signals", payload["progress_signals"], event)
        if "prediction" in payload:
            self._set_section(state, "prediction", payload["prediction"], event)
        next_action = str(payload.get("next_action", "")).upper()
        if payload.get("task_complete") or next_action == "TERMINATE":
            state.lifecycle.progress = 1.0
            state.lifecycle.runnable = False
            self._set_phase(state, AgentPhase.COMPLETED, event)
        elif next_action == "TOOL":
            state.lifecycle.runnable = False
            self._set_phase(state, AgentPhase.TOOL_PREPARING, event)
        else:
            state.lifecycle.runnable = True
            self._set_phase(state, AgentPhase.PLANNING, event)
        if success:
            state.failure.failure_streak = 0
            state.progress_signals.consecutive_failures = 0
        else:
            self._apply_failure(state, payload, event, "MODEL_RESPONSE")
        self._mark(state, "lifecycle", event)
        self._mark(state, "failure", event)

    def _apply_tool_started(self, state: AgentState, payload: dict[str, Any], event: AgentStateEvent) -> None:
        tool_id = str(payload.get("tool_id") or event.step_id or f"tool-{len(state.tools.active_tools)+1}")
        tool = self._find_tool(state, tool_id, str(payload.get("tool_name", "unknown")))
        self._set_section(tool, "", payload, event, state_override=state, field_prefix="tools.active_tools")
        tool.tool_id = tool_id
        tool.tool_name = str(payload.get("tool_name", tool.tool_name))
        tool.status = _enum_value(ToolStatus, payload.get("status", ToolStatus.RUNNING), ToolStatus.RUNNING)
        tool.remaining_time_seconds = max(0.0, float(payload.get("remaining_time_seconds", tool.remaining_time_seconds)))
        state.tools.last_tool_name = tool.tool_name
        state.tools.predicted_time_until_next_model_call_seconds = tool.remaining_time_seconds
        state.lifecycle.runnable = False
        self._set_phase(state, AgentPhase.TOOL_RUNNING, event)
        self._mark(state, "tools", event)
        self._mark(state, "lifecycle", event)

    def _apply_tool_progress(self, state: AgentState, payload: dict[str, Any], event: AgentStateEvent) -> None:
        tool = self._find_tool(
            state,
            str(payload.get("tool_id", "")),
            str(payload.get("tool_name", state.tools.last_tool_name)),
        )
        for key in (
            "elapsed_seconds",
            "remaining_time_seconds",
            "predicted_output_bytes",
            "predicted_output_tokens",
        ):
            if key in payload and hasattr(tool, key):
                setattr(tool, key, max(0, type(getattr(tool, key))(payload[key])))
        if "status" in payload:
            tool.status = _enum_value(ToolStatus, payload["status"], tool.status)
        if "progress" in payload:
            self._set_progress(state, float(payload["progress"]), event)
        state.tools.predicted_time_until_next_model_call_seconds = max(0.0, tool.remaining_time_seconds)
        state.lifecycle.runnable = False
        state.lifecycle.phase = AgentPhase.TOOL_WAITING if tool.status == ToolStatus.WAITING_IO else AgentPhase.TOOL_RUNNING
        self._mark(state, "tools", event)

    def _apply_tool_result(self, state: AgentState, payload: dict[str, Any], event: AgentStateEvent) -> None:
        tool = self._find_tool(
            state,
            str(payload.get("tool_id", "")),
            str(payload.get("tool_name", state.tools.last_tool_name)),
        )
        tool.status = _enum_value(ToolStatus, payload.get("status", ToolStatus.RESULT_READY), ToolStatus.RESULT_READY)
        tool.remaining_time_seconds = 0.0
        result_tokens = max(0, int(payload.get("result_tokens", 0)))
        state.context.tool_tokens += result_tokens
        state.context.total_tokens += result_tokens
        state.context.prompt_tokens = max(state.context.prompt_tokens, state.context.total_tokens)
        state.tools.predicted_time_until_next_model_call_seconds = 0.0
        state.lifecycle.runnable = True
        self._set_phase(state, AgentPhase.TOOL_RESULT_READY, event)
        state.prediction.next_action_probabilities = {"MODEL": 1.0}
        state.prediction.predicted_inter_request_gap_seconds = 0.0
        self._mark(state, "tools", event)
        self._mark(state, "context", event)
        self._mark(state, "prediction", event)

    def _apply_failure(
        self,
        state: AgentState,
        payload: dict[str, Any],
        event: AgentStateEvent,
        component: str,
    ) -> None:
        state.failure.retry_count += 1
        state.failure.failure_streak += 1
        state.failure.failed_component = str(payload.get("failed_component", component))
        state.failure.error_type = str(payload.get("error_type", payload.get("error", "unknown")))
        state.failure.error_severity = max(
            state.failure.error_severity,
            min(1.0, float(payload.get("error_severity", 0.0))),
        )
        state.failure.critical_error = bool(payload.get("critical_error", False))
        state.progress_signals.consecutive_failures = state.failure.failure_streak
        state.lifecycle.runnable = True
        self._set_phase(state, AgentPhase.REPLANNING, event)
        self._mark(state, "failure", event)
        self._mark(state, "progress_signals", event)
        self._mark(state, "lifecycle", event)

    def _apply_patch(self, state: AgentState, payload: dict[str, Any], event: AgentStateEvent) -> None:
        for section in (
            "header",
            "identity",
            "lifecycle",
            "task",
            "execution",
            "context",
            "model",
            "tools",
            "inference",
            "kv_cache",
            "scheduling",
            "prediction",
            "qos",
            "affinity",
            "failure",
            "observability",
            "security",
            "progress_signals",
        ):
            if section in payload and hasattr(state, section):
                self._set_section(state, section, payload[section], event)
        if "extensions" in payload:
            state.extensions.update(dict(payload["extensions"] or {}))
            self._mark(state, "extensions", event)
        if "state_fields" in payload:
            for name, field_payload in dict(payload["state_fields"] or {}).items():
                if isinstance(field_payload, dict) and "value" in field_payload:
                    self._set_meta(state, name, field_payload, event)

    def _apply_identity(self, state: AgentState, payload: dict[str, Any], event: AgentStateEvent) -> None:
        if not isinstance(payload, dict):
            return
        identity = payload.get("identity") if isinstance(payload.get("identity"), dict) else payload
        keys = {
            "tenant_id",
            "namespace",
            "harness_type",
            "harness_version",
            "agent_type",
            "agent_version",
            "workflow_type",
            "workload_class",
            "sandbox",
            "workspace_id",
            "required_capabilities",
        }
        patch = {key: identity[key] for key in keys if key in identity}
        if patch:
            self._set_section(state, "identity", patch, event)
            if "tenant_id" in patch:
                state.security.tenant_id = str(patch["tenant_id"])

    def _set_section(
        self,
        state: AgentState | ToolExecution,
        section: str,
        payload: Any,
        event: AgentStateEvent,
        *,
        state_override: AgentState | None = None,
        field_prefix: str | None = None,
    ) -> None:
        if not isinstance(payload, dict):
            return
        target = state if not section else getattr(state, section, None)
        if target is None:
            return
        root_state = state_override or (state if isinstance(state, AgentState) else None)
        prefix = field_prefix or section
        for key, value in payload.items():
            if key.startswith("_") or not hasattr(target, key):
                continue
            old = getattr(target, key)
            field_name = f"{prefix}.{key}" if prefix else key
            converted = _convert_for_field(old, value, field_name)
            setattr(target, key, converted)
            if root_state is not None:
                self._mark(root_state, field_name, event)
        if isinstance(state, AgentState) and section == "context":
            # Prompt tokens are a lower bound for the live context size even
            # when a harness only reports a partial context patch.
            state.context.total_tokens = max(
                state.context.total_tokens,
                state.context.prompt_tokens,
            )

    def _set_progress(self, state: AgentState, progress: float, event: AgentStateEvent) -> None:
        previous = state.lifecycle.progress
        state.lifecycle.progress = max(0.0, min(1.0, progress))
        state.progress_signals.progress_velocity = state.lifecycle.progress - previous
        if state.lifecycle.progress > previous:
            state.progress_signals.consecutive_no_progress_steps = 0
        else:
            state.progress_signals.consecutive_no_progress_steps += 1
        self._mark(state, "lifecycle.progress", event)
        self._mark(state, "progress_signals", event)

    def _set_phase(self, state: AgentState, phase: AgentPhase, event: AgentStateEvent) -> None:
        if state.lifecycle.phase != phase:
            state.lifecycle.previous_phase = state.lifecycle.phase
            state.lifecycle.phase_enter_time = event.observed_at
            state.lifecycle.phase = phase
        self._mark(state, "lifecycle.phase", event)

    def _find_tool(self, state: AgentState, tool_id: str, tool_name: str) -> ToolExecution:
        for tool in state.tools.active_tools:
            if (tool_id and tool.tool_id == tool_id) or (tool_name and tool.tool_name == tool_name):
                return tool
        tool = ToolExecution(tool_id=tool_id or f"tool-{len(state.tools.active_tools)+1}", tool_name=tool_name or "unknown")
        state.tools.active_tools.append(tool)
        return tool

    def _mark(self, state: AgentState, name: str, event: AgentStateEvent) -> None:
        value: Any = state
        for part in name.split("."):
            if part == "":
                continue
            if hasattr(value, part):
                value = getattr(value, part)
            else:
                value = None
                break
        state.field_meta[name] = StateField(
            value=deepcopy(value),
            observed_at=event.observed_at,
            source=event.source,
            confidence=event.confidence,
            ttl=event.ttl,
            authoritative=event.authoritative,
            version=event.sequence,
        )

    def _set_meta(self, state: AgentState, name: str, payload: dict[str, Any], event: AgentStateEvent) -> None:
        observed_at = payload.get("observed_at", event.observed_at)
        if isinstance(observed_at, str):
            observed_at = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
        if isinstance(observed_at, datetime) and observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        ttl = payload.get("ttl", event.ttl)
        if isinstance(ttl, (int, float)):
            ttl = timedelta(seconds=float(ttl))
        state.field_meta[name] = StateField(
            value=deepcopy(payload.get("value")),
            observed_at=observed_at,
            source=str(payload.get("source", event.source)),
            confidence=float(payload.get("confidence", event.confidence)),
            ttl=ttl,
            authoritative=bool(payload.get("authoritative", event.authoritative)),
            version=int(payload.get("version", event.sequence)),
        )


def _enum_value(enum_type, value: Any, default: Any):
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value).upper())
    except (TypeError, ValueError):
        return default


def _convert_for_field(old: Any, value: Any, field_name: str = "") -> Any:
    if isinstance(old, Enum):
        return _enum_value(type(old), value, old)
    if isinstance(old, set):
        return set(value or [])
    if isinstance(old, tuple):
        return tuple(value or [])
    if isinstance(old, datetime) and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    if isinstance(old, timedelta) and isinstance(value, (int, float)):
        return timedelta(seconds=float(value))
    if isinstance(old, list):
        if not value:
            return []
        item_type = type(old[0]) if old else None
        item_type = item_type or {
            "tools.active_tools": ToolExecution,
            "inference.endpoints": InferenceEndpointState,
            "kv_cache.segments": KVSegment,
            "context.segments": ContextSegment,
            "execution.nodes": ExecutionNode,
        }.get(field_name)
        if item_type in {
            ToolExecution,
            InferenceEndpointState,
            KVSegment,
            ContextSegment,
            ExecutionNode,
        }:
            return [
                _dataclass_from_mapping(item_type, item) if isinstance(item, dict) else item
                for item in value
            ]
    return value


def _dataclass_from_mapping(cls, value: dict[str, Any]):
    kwargs = {}
    for item in fields(cls):
        if item.name not in value:
            continue
        current = item.default if item.default is not None else None
        raw = value[item.name]
        if cls is ToolExecution and item.name == "status":
            raw = _enum_value(ToolStatus, raw, ToolStatus.PREPARING)
        if cls is KVSegment and item.name == "tier":
            raw = _enum_value(KVCacheTier, raw, KVCacheTier.GPU_HBM)
        kwargs[item.name] = raw
    return cls(**kwargs)
