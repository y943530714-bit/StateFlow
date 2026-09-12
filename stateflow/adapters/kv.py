"""KV manager adapter for location, lifecycle, and request correlation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from ..state import (
    CorrelationIdentity,
    GraphEntity,
    GraphKind,
    RelationUpdate,
    SourceAuthority,
    SourceRef,
    StateUpdate,
)
from ..state.schema import utcnow
from .base import AdapterPublishReport, StatePlaneAdapter, component_ref, source_attributes
from .identity import CorrelationResolver


@dataclass(frozen=True)
class KVLocation:
    identifier: str
    entity_type: str = "resource"
    tier: str = ""
    primary: bool = False

    def __post_init__(self) -> None:
        if self.entity_type not in {"resource", "node"}:
            raise ValueError("KV location entity_type must be resource or node")


@dataclass(frozen=True)
class KVObservation:
    kv_id: str
    observed_at: datetime = field(default_factory=utcnow)
    request_ids: tuple[str, ...] = ()
    locations: tuple[KVLocation, ...] = ()
    size_bytes: int | None = None
    replica_count: int | None = None
    cache_hit: bool | None = None
    transfer_state: str | None = None
    soft_pin: bool | None = None
    lifecycle: str = "active"
    trace_id: str = ""
    watermark: str = ""
    observation_id: str = ""
    source_ref: SourceRef | None = None


class KVStateAdapter(StatePlaneAdapter):
    """Project KV metadata while leaving tensors in the KV data plane."""

    def __init__(
        self,
        plane,
        resolver: CorrelationResolver | None = None,
        *,
        component_id: str = "stateflow-kv-adapter",
    ) -> None:
        super().__init__(
            plane,
            component_id=component_id,
            kind="kv_adapter",
            implementation="kv-manager",
            produces_state=(
                "kv.location",
                "kv.size",
                "kv.replica_count",
                "kv.cache_hit",
                "kv.transfer_state",
                "kv.soft_pin",
            ),
            manages_types=("stateful_object",),
            relation_capabilities=("located_on", "uses"),
        )
        self.resolver = resolver or CorrelationResolver()

    def publish(self, observation: KVObservation) -> AdapterPublishReport:
        kv_ref = self.resolver.kv_ref(observation.kv_id)
        source = observation.source_ref or SourceRef("kv", observation.kv_id)
        self.plane.upsert_entity(
            GraphEntity(
                kv_ref,
                GraphKind.COMPONENT,
                "stateful_object",
                lifecycle=observation.lifecycle,
                owner_component_ref=component_ref(self.component_id),
                labels={"kind": "kv_cache"},
            )
        )
        self.resolver.bind(kv_ref, kv_id=observation.kv_id, trace_id=observation.trace_id)

        location_refs: list[str] = []
        relations: list[RelationUpdate] = []
        for location in observation.locations:
            if location.entity_type == "resource":
                location_ref = self.resolver.resource_ref(location.identifier)
            else:
                location_ref = self.resolver.node_ref(location.identifier)
                self.resolver.bind(location_ref, node_id=location.identifier)
            location_refs.append(location_ref)
            self.ensure_entity(
                GraphEntity(
                    location_ref,
                    GraphKind.DEPLOYMENT,
                    location.entity_type,
                    labels={"tier": location.tier} if location.tier else {},
                )
            )
            relation_attributes = source_attributes(source)
            if location.tier:
                relation_attributes["tier"] = location.tier
            relation_attributes["primary"] = str(location.primary).lower()
            relations.append(
                RelationUpdate(
                    kv_ref,
                    "located_on",
                    location_ref,
                    self.producer,
                    timestamp=observation.observed_at,
                    authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                    attributes=relation_attributes,
                    idempotency_key=self._key(observation, f"located_on:{location_ref}"),
                )
            )

        for request_id in observation.request_ids:
            request_ref = self.resolver.request_ref(request_id)
            self.ensure_entity(
                GraphEntity(request_ref, GraphKind.COMPONENT, "request", lifecycle="active")
            )
            self.resolver.bind(request_ref, request_id=request_id, trace_id=observation.trace_id)
            relations.append(
                RelationUpdate(
                    request_ref,
                    "uses",
                    kv_ref,
                    self.producer,
                    timestamp=observation.observed_at,
                    authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                    attributes=source_attributes(source),
                    idempotency_key=self._key(observation, f"request_uses:{request_id}"),
                )
            )

        primary_location = next(
            (ref for ref, location in zip(location_refs, observation.locations) if location.primary),
            location_refs[0] if location_refs else None,
        )
        state_values = {
            "kv.location": primary_location,
            "kv.size": observation.size_bytes,
            "kv.replica_count": observation.replica_count
            if observation.replica_count is not None
            else (len(location_refs) if location_refs else None),
            "kv.transfer_state": observation.transfer_state,
            "kv.soft_pin": observation.soft_pin,
        }
        correlation = CorrelationIdentity(
            trace_id=observation.trace_id,
            request_id=observation.request_ids[0] if len(observation.request_ids) == 1 else "",
            component_id=self.component_id,
        )
        updates = [
            StateUpdate(
                kv_ref,
                key,
                value,
                self.producer,
                timestamp=observation.observed_at,
                authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                source_ref=source,
                correlation=correlation,
                idempotency_key=self._key(observation, key),
            )
            for key, value in state_values.items()
            if value is not None
        ]
        if observation.cache_hit is not None:
            for request_id in observation.request_ids:
                updates.append(
                    StateUpdate(
                        self.resolver.request_ref(request_id),
                        "kv.cache_hit",
                        observation.cache_hit,
                        self.producer,
                        timestamp=observation.observed_at,
                        authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                        source_ref=source,
                        correlation=CorrelationIdentity(
                            trace_id=observation.trace_id,
                            request_id=request_id,
                            component_id=self.component_id,
                        ),
                        idempotency_key=self._key(observation, f"cache_hit:{request_id}"),
                    )
                )
        state_results = tuple(self.plane.publish_state(updates))
        relation_results = tuple(self.plane.upsert_relations(relations))
        self.heartbeat(observation.watermark, timestamp=observation.observed_at)
        return AdapterPublishReport(state_results, relation_results)

    @staticmethod
    def _key(observation: KVObservation, suffix: str) -> str:
        return f"kv:{observation.observation_id}:{suffix}" if observation.observation_id else ""


__all__ = ["KVLocation", "KVObservation", "KVStateAdapter"]
