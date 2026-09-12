"""Kubernetes/Ray deployment adapter for authoritative placement state."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping

from ..state import (
    GraphEntity,
    GraphKind,
    RelationUpdate,
    SourceAuthority,
    SourceRef,
    StateUpdate,
)
from ..state.schema import utcnow
from .base import AdapterPublishReport, StatePlaneAdapter, source_attributes
from .identity import CorrelationResolver


@dataclass(frozen=True)
class DeploymentObservation:
    cluster_id: str
    node_id: str
    observed_at: datetime = field(default_factory=utcnow)
    instance_id: str = ""
    runtime_id: str = ""
    namespace: str = ""
    pod_uid: str = ""
    node_health: str | None = None
    node_allocatable: Mapping[str, object] | None = None
    instance_ready: bool | None = None
    allocated_gpu: float | None = None
    labels: Mapping[str, str] = field(default_factory=dict)
    watermark: str = ""
    observation_id: str = ""
    source_ref: SourceRef | None = None


class KubernetesStateAdapter(StatePlaneAdapter):
    """Materialize cluster topology and owner-sourced instance binding."""

    def __init__(
        self,
        plane,
        resolver: CorrelationResolver | None = None,
        *,
        component_id: str = "stateflow-kubernetes-adapter",
    ) -> None:
        super().__init__(
            plane,
            component_id=component_id,
            kind="deployment_adapter",
            implementation="kubernetes",
            produces_state=(
                "node.allocatable",
                "node.health",
                "instance.ready",
                "instance.allocated_gpu",
            ),
            manages_types=("cluster", "node", "instance"),
            relation_capabilities=("contains", "deployed_on"),
        )
        self.resolver = resolver or CorrelationResolver()

    def publish(self, observation: DeploymentObservation) -> AdapterPublishReport:
        cluster_ref = self.resolver.cluster_ref(observation.cluster_id)
        node_ref = self.resolver.node_ref(observation.node_id)
        source = observation.source_ref or SourceRef(
            "kubernetes",
            observation.pod_uid or f"{observation.cluster_id}/{observation.node_id}",
        )
        self.plane.upsert_entity(
            GraphEntity(
                cluster_ref,
                GraphKind.DEPLOYMENT,
                "cluster",
                lifecycle="active",
                labels={"cluster_id": observation.cluster_id},
            )
        )
        self.plane.upsert_entity(
            GraphEntity(
                node_ref,
                GraphKind.DEPLOYMENT,
                "node",
                lifecycle="ready" if observation.node_health in {None, "healthy", "ready"} else "degraded",
                parent_ref=cluster_ref,
                labels={"node_id": observation.node_id, **dict(observation.labels)},
            )
        )
        self.resolver.bind(cluster_ref, cluster_id=observation.cluster_id)
        self.resolver.bind(node_ref, node_id=observation.node_id)
        relations = [
            RelationUpdate(
                cluster_ref,
                "contains",
                node_ref,
                self.producer,
                timestamp=observation.observed_at,
                authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                attributes=source_attributes(source),
                idempotency_key=self._key(observation, "cluster_contains_node"),
            )
        ]

        instance_ref = ""
        if observation.instance_id:
            instance_ref = self.resolver.instance_ref(observation.instance_id)
            runtime_ref = (
                self.resolver.runtime_ref(observation.runtime_id)
                if observation.runtime_id
                else ""
            )
            self.plane.upsert_entity(
                GraphEntity(
                    instance_ref,
                    GraphKind.DEPLOYMENT,
                    "instance",
                    lifecycle="ready" if observation.instance_ready is not False else "unavailable",
                    owner_component_ref=runtime_ref,
                    parent_ref=node_ref,
                    labels={
                        "namespace": observation.namespace,
                        "pod_uid": observation.pod_uid,
                        **dict(observation.labels),
                    },
                )
            )
            self.resolver.bind(
                instance_ref,
                instance_id=observation.instance_id,
                pod_uid=observation.pod_uid,
            )
            relations.append(
                RelationUpdate(
                    node_ref,
                    "contains",
                    instance_ref,
                    self.producer,
                    timestamp=observation.observed_at,
                    authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                    attributes=source_attributes(source),
                    idempotency_key=self._key(observation, "node_contains_instance"),
                )
            )
            if runtime_ref:
                self.ensure_entity(
                    GraphEntity(
                        runtime_ref,
                        GraphKind.COMPONENT,
                        "component",
                        labels={"kind": "inference_runtime"},
                    )
                )
                self.resolver.bind(runtime_ref, component_id=observation.runtime_id)
                relations.append(
                    RelationUpdate(
                        runtime_ref,
                        "deployed_on",
                        instance_ref,
                        self.producer,
                        timestamp=observation.observed_at,
                        authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                        attributes=source_attributes(source),
                        idempotency_key=self._key(observation, "runtime_deployed_on_instance"),
                    )
                )

        updates = []
        if observation.node_allocatable is not None:
            updates.append(
                StateUpdate(
                    node_ref,
                    "node.allocatable",
                    dict(observation.node_allocatable),
                    self.producer,
                    timestamp=observation.observed_at,
                    authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                    source_ref=source,
                    idempotency_key=self._key(observation, "node.allocatable"),
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
                    authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                    source_ref=source,
                    idempotency_key=self._key(observation, "node.health"),
                )
            )
        if instance_ref and observation.instance_ready is not None:
            updates.append(
                StateUpdate(
                    instance_ref,
                    "instance.ready",
                    observation.instance_ready,
                    self.producer,
                    timestamp=observation.observed_at,
                    authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                    source_ref=source,
                    idempotency_key=self._key(observation, "instance.ready"),
                )
            )
        if instance_ref and observation.allocated_gpu is not None:
            updates.append(
                StateUpdate(
                    instance_ref,
                    "instance.allocated_gpu",
                    observation.allocated_gpu,
                    self.producer,
                    timestamp=observation.observed_at,
                    authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                    source_ref=source,
                    idempotency_key=self._key(observation, "instance.allocated_gpu"),
                )
            )
        state_results = tuple(self.plane.publish_state(updates))
        relation_results = tuple(self.plane.upsert_relations(relations))
        self.heartbeat(observation.watermark, timestamp=observation.observed_at)
        return AdapterPublishReport(state_results, relation_results)

    @staticmethod
    def _key(observation: DeploymentObservation, suffix: str) -> str:
        return f"k8s:{observation.observation_id}:{suffix}" if observation.observation_id else ""


__all__ = ["DeploymentObservation", "KubernetesStateAdapter"]
