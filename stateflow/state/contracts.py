"""Canonical State Plane contracts from Architecture Design Final v1.1.

These records are intentionally transport-neutral.  They can be mapped to
protobuf, HTTP, an embedded store, or a distributed implementation without
coupling controllers to a storage engine.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum, IntEnum
from typing import Any, Mapping
import uuid

from .schema import clamp, to_jsonable, utcnow


class SourceAuthority(IntEnum):
    """Source precedence.  Larger values are more authoritative."""

    LOG_HINT = 0
    DERIVED = 1
    DIRECT_TELEMETRY = 2
    STRUCTURED_LIFECYCLE = 3
    AUTHORITATIVE_DOMAIN = 4

    @classmethod
    def parse(cls, value: "SourceAuthority | str | int") -> "SourceAuthority":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            if value.lstrip("+-").isdigit():
                return cls(int(value))
            normalized = value.upper().removeprefix("A0_").removeprefix("A1_")
            normalized = normalized.removeprefix("A2_").removeprefix("A3_").removeprefix("A4_")
            aliases = {
                "DOMAIN_OWNER": cls.AUTHORITATIVE_DOMAIN,
                "OWNER": cls.AUTHORITATIVE_DOMAIN,
                "LIFECYCLE": cls.STRUCTURED_LIFECYCLE,
                "TELEMETRY": cls.DIRECT_TELEMETRY,
                "LOG": cls.LOG_HINT,
            }
            if normalized in aliases:
                return aliases[normalized]
            return cls[normalized]
        return cls(int(value))


class StateSemantic(str, Enum):
    OBSERVED = "observed"
    DERIVED = "derived"
    RESERVED = "reserved"
    COMMITTED = "committed"


class GraphKind(str, Enum):
    COMPONENT = "component"
    DEPLOYMENT = "deployment"


COMPONENT_ENTITY_TYPES = frozenset(
    {
        "component",
        "agent",
        "session",
        "task",
        "request",
        "tool_call",
        "stateful_object",
        "runtime_object",
        "state_key_catalog",
    }
)
DEPLOYMENT_ENTITY_TYPES = frozenset({"cluster", "node", "instance", "resource", "link"})


@dataclass(frozen=True)
class SourceRef:
    backend: str
    reference: str
    trace_id: str = ""
    span_id: str = ""


@dataclass(frozen=True)
class CorrelationIdentity:
    trace_id: str = ""
    span_id: str = ""
    agent_id: str = ""
    session_id: str = ""
    request_id: str = ""
    action_id: str = ""
    component_id: str = ""
    instance_id: str = ""


@dataclass(frozen=True)
class CanonicalKey:
    key: str
    entity_types: tuple[str, ...]
    value_type: str
    unit: str = ""
    semantic: StateSemantic = StateSemantic.OBSERVED
    source_priority: tuple[SourceAuthority, ...] = ()
    update_mode: str = "event"
    ttl: timedelta | None = None
    aggregation: str = "latest"
    cardinality_policy: str = "bounded"
    schema_version: str = "1.1"
    deprecated: bool = False
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class ComponentDescriptor:
    component_id: str
    kind: str
    implementation: str = ""
    version: str = ""
    schema_versions: tuple[str, ...] = ("1.1",)
    produces_state: tuple[str, ...] = ()
    consumes_state: tuple[str, ...] = ()
    produces_metrics: tuple[str, ...] = ()
    consumes_metrics: tuple[str, ...] = ()
    manages_types: tuple[str, ...] = ()
    relation_capabilities: tuple[str, ...] = ()
    update_mode: str = "event"
    endpoint: str = ""


@dataclass(frozen=True)
class GraphEntity:
    ref: str
    graph: GraphKind
    entity_type: str
    lifecycle: str = ""
    owner_component_ref: str = ""
    parent_ref: str = ""
    labels: Mapping[str, str] = field(default_factory=dict)
    schema_version: str = "1.1"

    def __post_init__(self) -> None:
        graph = GraphKind(self.graph)
        object.__setattr__(self, "graph", graph)
        allowed = COMPONENT_ENTITY_TYPES if graph == GraphKind.COMPONENT else DEPLOYMENT_ENTITY_TYPES
        if self.entity_type not in allowed:
            raise ValueError(f"entity type {self.entity_type!r} is invalid for {graph.value} graph")
        prefix = f"{graph.value}/{self.entity_type}/"
        if not self.ref.startswith(prefix) or len(self.ref) == len(prefix):
            raise ValueError("entity ref must be '<graph>/<entity_type>/<id>'")


@dataclass(frozen=True)
class RelationUpdate:
    source_ref: str
    relation_type: str
    target_ref: str
    producer: str
    timestamp: datetime = field(default_factory=utcnow)
    authority: SourceAuthority = SourceAuthority.STRUCTURED_LIFECYCLE
    attributes: Mapping[str, Any] = field(default_factory=dict)
    ttl: timedelta | None = None
    version: int = 0
    expected_version: int | None = None
    idempotency_key: str = ""


@dataclass(frozen=True)
class GraphRelation:
    source_ref: str
    relation_type: str
    target_ref: str
    producer: str
    timestamp: datetime
    authority: SourceAuthority
    attributes: Mapping[str, Any]
    ttl: timedelta | None
    version: int

    def is_fresh(self, now: datetime | None = None) -> bool:
        return self.ttl is None or _aware(now or utcnow()) <= _aware(self.timestamp) + self.ttl


@dataclass(frozen=True)
class StateUpdate:
    entity_ref: str
    key: str
    value: Any
    producer: str
    timestamp: datetime = field(default_factory=utcnow)
    ttl: timedelta | None = None
    authority: SourceAuthority = SourceAuthority.DIRECT_TELEMETRY
    version: int = 0
    semantic: StateSemantic = StateSemantic.OBSERVED
    confidence: float = 1.0
    source_ref: SourceRef | None = None
    correlation: CorrelationIdentity = field(default_factory=CorrelationIdentity)
    expected_version: int | None = None
    idempotency_key: str = ""


@dataclass(frozen=True)
class MetricSample:
    entity_ref: str
    metric_key: str
    value: float
    unit: str
    producer: str
    timestamp: datetime = field(default_factory=utcnow)
    labels: Mapping[str, str] = field(default_factory=dict)
    correlation: CorrelationIdentity = field(default_factory=CorrelationIdentity)


@dataclass(frozen=True)
class StateEvent:
    event_id: str
    event_type: str
    subject_ref: str
    producer: str
    timestamp: datetime = field(default_factory=utcnow)
    correlation: CorrelationIdentity = field(default_factory=CorrelationIdentity)
    payload: Mapping[str, Any] = field(default_factory=dict)
    source_ref: SourceRef | None = None


@dataclass(frozen=True)
class Heartbeat:
    subject_ref: str
    producer: str
    source_watermark: str = ""
    schema_version: str = "1.1"
    timestamp: datetime = field(default_factory=utcnow)
    ttl: timedelta = timedelta(seconds=10)


@dataclass(frozen=True)
class StateValue:
    entity_ref: str
    key: str
    value: Any
    producer: str
    timestamp: datetime
    ttl: timedelta | None
    authority: SourceAuthority
    version: int
    semantic: StateSemantic
    confidence: float
    source_ref: SourceRef | None = None
    correlation: CorrelationIdentity = field(default_factory=CorrelationIdentity)

    def age(self, now: datetime | None = None) -> timedelta:
        return max(timedelta(0), _aware(now or utcnow()) - _aware(self.timestamp))

    def is_fresh(self, now: datetime | None = None, max_age: timedelta | None = None) -> bool:
        limit = self.ttl
        if max_age is not None:
            limit = max_age if limit is None else min(limit, max_age)
        return limit is None or self.age(now) <= limit

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)


@dataclass(frozen=True)
class WriteResult:
    accepted: bool
    duplicate: bool = False
    conflict: bool = False
    entity_ref: str = ""
    key: str = ""
    version: int = 0
    reason: str = ""


@dataclass(frozen=True)
class SnapshotRequest:
    entities: tuple[str, ...]
    keys: tuple[str, ...]
    max_age_ms: Mapping[str, int] = field(default_factory=dict)
    min_authority: SourceAuthority = SourceAuthority.LOG_HINT
    include_relations: bool = True


@dataclass(frozen=True)
class Snapshot:
    token: str
    logical_time: int
    created_at: datetime
    completeness: float
    values: tuple[StateValue, ...]
    relations: tuple[GraphRelation, ...]
    missing: tuple[str, ...] = ()
    stale: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)


@dataclass(frozen=True)
class GraphQueryResult:
    snapshot_token: str
    entities: tuple[GraphEntity, ...]
    relations: tuple[GraphRelation, ...]


@dataclass(frozen=True)
class StateChange:
    cursor: int
    kind: str
    subject_ref: str
    key_or_relation: str
    version: int
    timestamp: datetime = field(default_factory=utcnow)


def snapshot_token(revision: int) -> str:
    return f"snapshot-{revision}-{uuid.uuid4().hex[:12]}"


def normalize_confidence(value: float) -> float:
    return clamp(value)


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


__all__ = [
    "COMPONENT_ENTITY_TYPES",
    "DEPLOYMENT_ENTITY_TYPES",
    "CanonicalKey",
    "ComponentDescriptor",
    "CorrelationIdentity",
    "GraphEntity",
    "GraphKind",
    "GraphQueryResult",
    "GraphRelation",
    "Heartbeat",
    "MetricSample",
    "RelationUpdate",
    "Snapshot",
    "SnapshotRequest",
    "SourceAuthority",
    "SourceRef",
    "StateChange",
    "StateEvent",
    "StateSemantic",
    "StateUpdate",
    "StateValue",
    "WriteResult",
]
