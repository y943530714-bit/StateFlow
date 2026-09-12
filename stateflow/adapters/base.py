"""Shared primitives for protocol-neutral State Plane adapters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Protocol
from urllib.parse import quote

from ..state import (
    ComponentDescriptor,
    GraphEntity,
    GraphKind,
    Heartbeat,
    RelationUpdate,
    SourceRef,
    StateUpdate,
    WriteResult,
)
from ..state.schema import utcnow


def entity_ref(graph: GraphKind | str, entity_type: str, identifier: str) -> str:
    """Build a stable graph reference without leaking path delimiters."""

    graph_kind = GraphKind(graph)
    value = str(identifier).strip()
    if not value:
        raise ValueError("entity identifier must not be empty")
    return f"{graph_kind.value}/{entity_type}/{quote(value, safe='')}"


def component_ref(component_id: str) -> str:
    return entity_ref(GraphKind.COMPONENT, "component", component_id)


def source_attributes(source: SourceRef) -> dict[str, str]:
    """Preserve relation provenance until SourceRef is added to the wire type."""

    result = {"source_backend": source.backend, "source_reference": source.reference}
    if source.trace_id:
        result["trace_id"] = source.trace_id
    if source.span_id:
        result["span_id"] = source.span_id
    return result


@dataclass(frozen=True)
class AdapterPublishReport:
    state_results: tuple[WriteResult, ...] = ()
    relation_results: tuple[WriteResult, ...] = ()

    @property
    def accepted(self) -> int:
        return sum(result.accepted for result in self.results)

    @property
    def rejected(self) -> int:
        return len(self.results) - self.accepted

    @property
    def results(self) -> tuple[WriteResult, ...]:
        return self.state_results + self.relation_results


class AdapterStatePlane(Protocol):
    """Writer/query subset required by semantic adapters."""

    def list_components(self) -> tuple[ComponentDescriptor, ...]: ...

    def register_component(self, descriptor: ComponentDescriptor) -> None: ...

    def upsert_entity(self, entity: GraphEntity) -> None: ...

    def get_entity(self, entity_ref: str) -> GraphEntity | None: ...

    def publish_state(self, updates: Iterable[StateUpdate]) -> list[WriteResult]: ...

    def upsert_relations(self, updates: Iterable[RelationUpdate]) -> list[WriteResult]: ...

    def heartbeat(self, heartbeat: Heartbeat) -> None: ...


class StatePlaneAdapter:
    """Common registration and heartbeat behavior for semantic adapters."""

    def __init__(
        self,
        plane: AdapterStatePlane,
        *,
        component_id: str,
        kind: str,
        produces_state: Iterable[str],
        manages_types: Iterable[str],
        relation_capabilities: Iterable[str] = (),
        implementation: str = "stateflow",
    ) -> None:
        self.plane = plane
        self.component_id = component_id
        self.producer = component_id
        descriptor = ComponentDescriptor(
            component_id=component_id,
            kind=kind,
            implementation=implementation,
            version="0.3",
            schema_versions=("1.1",),
            produces_state=tuple(produces_state),
            manages_types=tuple(manages_types),
            relation_capabilities=tuple(relation_capabilities),
            update_mode="event",
        )
        existing = {item.component_id: item for item in plane.list_components()}.get(component_id)
        if existing is None:
            plane.register_component(descriptor)
        elif existing != descriptor:
            raise ValueError(f"adapter component already registered differently: {component_id}")
        plane.upsert_entity(
            GraphEntity(
                component_ref(component_id),
                GraphKind.COMPONENT,
                "component",
                lifecycle="ready",
                labels={"kind": kind, "adapter": "true"},
            )
        )

    def heartbeat(
        self,
        watermark: str = "",
        *,
        timestamp: datetime | None = None,
        ttl: timedelta = timedelta(seconds=10),
    ) -> None:
        self.plane.heartbeat(
            Heartbeat(
                subject_ref=self.component_id,
                producer=self.producer,
                source_watermark=watermark,
                timestamp=timestamp or utcnow(),
                ttl=ttl,
            )
        )

    def ensure_entity(self, entity: GraphEntity) -> None:
        """Create referenced entities without replacing another owner's projection."""

        if self.plane.get_entity(entity.ref) is None:
            self.plane.upsert_entity(entity)


__all__ = [
    "AdapterPublishReport",
    "AdapterStatePlane",
    "StatePlaneAdapter",
    "component_ref",
    "entity_ref",
    "source_attributes",
]
