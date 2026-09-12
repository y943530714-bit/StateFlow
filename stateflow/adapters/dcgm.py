"""DCGM/NVML/NIC window adapter for decision-oriented hardware state."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from ..state import (
    GraphEntity,
    GraphKind,
    RelationUpdate,
    SourceAuthority,
    SourceRef,
    StateSemantic,
    StateUpdate,
)
from ..state.schema import utcnow
from .base import AdapterPublishReport, StatePlaneAdapter, source_attributes
from .identity import CorrelationResolver


@dataclass(frozen=True)
class HardwareObservation:
    node_id: str
    gpu_id: str
    observed_at: datetime = field(default_factory=utcnow)
    hbm_used_bytes: int | None = None
    hbm_reserved_bytes: int | None = None
    gpu_util_ratio: float | None = None
    node_health: str | None = None
    peer_node_id: str = ""
    effective_bandwidth_bytes_per_second: float | None = None
    watermark: str = ""
    observation_id: str = ""
    source_ref: SourceRef | None = None


class DCGMStateAdapter(StatePlaneAdapter):
    """Publish aggregated hardware windows instead of raw high-rate counters."""

    def __init__(
        self,
        plane,
        resolver: CorrelationResolver | None = None,
        *,
        component_id: str = "stateflow-dcgm-adapter",
    ) -> None:
        super().__init__(
            plane,
            component_id=component_id,
            kind="hardware_adapter",
            implementation="dcgm-nvml-nic",
            produces_state=(
                "resource.hbm.used",
                "resource.hbm.reserved",
                "resource.gpu.util",
                "node.health",
                "link.effective_bw",
            ),
            manages_types=("resource", "link"),
            relation_capabilities=("contains", "connected_to"),
        )
        self.resolver = resolver or CorrelationResolver()

    def publish(self, observation: HardwareObservation) -> AdapterPublishReport:
        node_ref = self.resolver.node_ref(observation.node_id)
        resource_id = f"{observation.node_id}/gpu/{observation.gpu_id}"
        resource_ref = self.resolver.resource_ref(resource_id)
        source = observation.source_ref or SourceRef("dcgm", resource_id)
        self.ensure_entity(
            GraphEntity(node_ref, GraphKind.DEPLOYMENT, "node", labels={"node_id": observation.node_id})
        )
        self.plane.upsert_entity(
            GraphEntity(
                resource_ref,
                GraphKind.DEPLOYMENT,
                "resource",
                lifecycle="healthy" if observation.node_health != "unhealthy" else "degraded",
                parent_ref=node_ref,
                labels={"kind": "gpu", "gpu_id": observation.gpu_id},
            )
        )
        self.resolver.bind(node_ref, node_id=observation.node_id)
        self.resolver.bind(resource_ref, resource_id=resource_id, gpu_id=observation.gpu_id)
        relations = [
            RelationUpdate(
                node_ref,
                "contains",
                resource_ref,
                self.producer,
                timestamp=observation.observed_at,
                authority=SourceAuthority.DIRECT_TELEMETRY,
                attributes=source_attributes(source),
                idempotency_key=self._key(observation, "node_contains_gpu"),
            )
        ]

        updates = []
        state_values = {
            "resource.hbm.used": observation.hbm_used_bytes,
            "resource.hbm.reserved": observation.hbm_reserved_bytes,
            "resource.gpu.util": observation.gpu_util_ratio,
        }
        for key, value in state_values.items():
            if value is None:
                continue
            updates.append(
                StateUpdate(
                    resource_ref,
                    key,
                    value,
                    self.producer,
                    timestamp=observation.observed_at,
                    authority=SourceAuthority.DIRECT_TELEMETRY,
                    source_ref=source,
                    idempotency_key=self._key(observation, key),
                )
            )
        if observation.node_health is not None:
            updates.append(
                StateUpdate(
                    node_ref,
                    "node.health",
                    observation.node_health,
                    self.producer,
                    timestamp=observation.observed_at,
                    authority=SourceAuthority.DIRECT_TELEMETRY,
                    source_ref=source,
                    idempotency_key=self._key(observation, "node.health"),
                )
            )

        if observation.peer_node_id and observation.effective_bandwidth_bytes_per_second is not None:
            peer_ref = self.resolver.node_ref(observation.peer_node_id)
            link_id = f"{observation.node_id}->{observation.peer_node_id}"
            link_ref = self.resolver.link_ref(link_id)
            self.ensure_entity(
                GraphEntity(peer_ref, GraphKind.DEPLOYMENT, "node", labels={"node_id": observation.peer_node_id})
            )
            self.plane.upsert_entity(
                GraphEntity(
                    link_ref,
                    GraphKind.DEPLOYMENT,
                    "link",
                    lifecycle="active",
                    labels={"source": node_ref, "target": peer_ref},
                )
            )
            relations.append(
                RelationUpdate(
                    node_ref,
                    "connected_to",
                    peer_ref,
                    self.producer,
                    timestamp=observation.observed_at,
                    authority=SourceAuthority.DIRECT_TELEMETRY,
                    attributes={**source_attributes(source), "link_ref": link_ref},
                    idempotency_key=self._key(observation, "node_connected_to_peer"),
                )
            )
            updates.append(
                StateUpdate(
                    link_ref,
                    "link.effective_bw",
                    observation.effective_bandwidth_bytes_per_second,
                    self.producer,
                    timestamp=observation.observed_at,
                    authority=SourceAuthority.DERIVED,
                    semantic=StateSemantic.DERIVED,
                    source_ref=source,
                    idempotency_key=self._key(observation, "link.effective_bw"),
                )
            )
            self.resolver.bind(peer_ref, node_id=observation.peer_node_id)
            self.resolver.bind(link_ref, link_id=link_id)

        state_results = tuple(self.plane.publish_state(updates))
        relation_results = tuple(self.plane.upsert_relations(relations))
        self.heartbeat(observation.watermark, timestamp=observation.observed_at)
        return AdapterPublishReport(state_results, relation_results)

    @staticmethod
    def _key(observation: HardwareObservation, suffix: str) -> str:
        return f"dcgm:{observation.observation_id}:{suffix}" if observation.observation_id else ""


__all__ = ["DCGMStateAdapter", "HardwareObservation"]
