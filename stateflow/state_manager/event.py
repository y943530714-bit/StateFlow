"""State-plane event contract.

Events are deliberately small and metadata-first.  Payloads may contain
incremental state patches, but never need to contain prompts, tool results, or
KV tensors themselves.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from stateflow.state_manager.schema import to_jsonable, utcnow


@dataclass
class AgentStateEvent:
    event_type: str
    session_id: str
    task_id: str = "default-task"
    turn_id: int = 0
    step_id: str = ""
    request_id: str = ""
    attempt_id: str = ""
    parent_step_id: str = ""
    trace_id: str = ""
    source: str = "unknown"
    observed_at: datetime = field(default_factory=utcnow)
    epoch: int = 0
    sequence: int = 0
    payload: dict[str, Any] = field(default_factory=dict)
    confidence: float = 1.0
    ttl: timedelta | None = None
    authoritative: bool = False

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AgentStateEvent":
        raw = dict(value)
        if "event_type" not in raw:
            raw["event_type"] = raw.pop("event", "STATE_PATCH")
        if "sequence" not in raw:
            raw["sequence"] = raw.pop("sequence_number", 0)
        raw["payload"] = dict(raw.get("payload") or {})
        if "timestamp" in raw and "observed_at" not in raw:
            raw["observed_at"] = raw.pop("timestamp")
        if isinstance(raw.get("observed_at"), str):
            raw["observed_at"] = datetime.fromisoformat(
                raw["observed_at"].replace("Z", "+00:00")
            )
        if isinstance(raw.get("observed_at"), datetime) and raw["observed_at"].tzinfo is None:
            raw["observed_at"] = raw["observed_at"].replace(tzinfo=timezone.utc)
        if isinstance(raw.get("ttl"), (int, float)):
            raw["ttl"] = timedelta(seconds=float(raw["ttl"]))
        return cls(**{key: raw[key] for key in cls.__dataclass_fields__ if key in raw})


@dataclass(frozen=True)
class AppendResult:
    accepted: bool
    duplicate: bool = False
    stale: bool = False
    state_id: str = ""
    state_version: int = 0
    reason: str = ""
