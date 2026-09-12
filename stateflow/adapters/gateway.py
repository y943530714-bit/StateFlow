"""Project gateway requests and registered targets into canonical state."""

from __future__ import annotations

from ..gateway.normalizer.request import ProviderNeutralRequest
from ..scheduler.types import RoutingDecision
from ..state import (
    ComponentDescriptor,
    CorrelationIdentity,
    GraphEntity,
    GraphKind,
    InMemoryStatePlane,
    RelationUpdate,
    SourceAuthority,
    StateSemantic,
    StateUpdate,
)
from ..state.schema import TargetCandidate
from .identity import CorrelationResolver


class GatewayStateProjector:
    """Best-effort Adapter + Semantic Normalization for the request gateway."""

    producer = "stateflow-gateway-adapter"

    def __init__(
        self,
        plane: InMemoryStatePlane,
        resolver: CorrelationResolver | None = None,
    ) -> None:
        self.plane = plane
        self.resolver = resolver or CorrelationResolver()
        self.plane.register_component(
            ComponentDescriptor(
                component_id="stateflow-gateway",
                kind="request_scheduler",
                implementation="stateflow",
                version="0.3",
                schema_versions=("1.1",),
                produces_state=(
                    "request.context_tokens",
                    "request.expected_output_tokens",
                    "request.target_instance",
                ),
                manages_types=("request",),
                relation_capabilities=("executing_on",),
                update_mode="event",
            )
        )
        self.plane.upsert_entity(
            GraphEntity(
                "component/component/stateflow-gateway",
                GraphKind.COMPONENT,
                "component",
                lifecycle="ready",
                labels={"kind": "request_scheduler"},
            )
        )

    def record_request(
        self,
        request: ProviderNeutralRequest,
        targets: tuple[TargetCandidate, ...],
    ) -> None:
        request_ref = self.request_ref(request.request_id)
        correlation = self._correlation(request)
        self.resolver.bind(
            request_ref,
            request_id=request.request_id,
            session_id=request.session_id,
            trace_id=request.trace_id,
        )
        self.plane.upsert_entity(
            GraphEntity(
                request_ref,
                GraphKind.COMPONENT,
                "request",
                lifecycle="queued",
                owner_component_ref="component/component/stateflow-gateway",
                labels={"tenant_id": request.tenant_id, "model": request.model},
            )
        )
        self.plane.publish_state(
            (
                StateUpdate(
                    request_ref,
                    "request.context_tokens",
                    request.prompt_tokens,
                    self.producer,
                    authority=SourceAuthority.STRUCTURED_LIFECYCLE,
                    correlation=correlation,
                    idempotency_key=f"{request.request_id}:context_tokens",
                ),
                StateUpdate(
                    request_ref,
                    "request.expected_output_tokens",
                    request.predicted_output_tokens,
                    self.producer,
                    authority=SourceAuthority.STRUCTURED_LIFECYCLE,
                    correlation=correlation,
                    idempotency_key=f"{request.request_id}:expected_output_tokens",
                ),
            )
        )
        for target in targets:
            self.record_target(target)

    def record_target(self, target: TargetCandidate) -> None:
        component_ref = self.runtime_ref(target.endpoint_id)
        instance_ref = self.instance_ref(target.replica_id)
        self.resolver.bind(component_ref, component_id=target.endpoint_id)
        self.resolver.bind(instance_ref, instance_id=target.replica_id)
        self.plane.register_component(
            ComponentDescriptor(
                component_id=target.endpoint_id,
                kind="inference_runtime",
                implementation=str(target.metadata.get("engine", "unknown")),
                version=str(target.metadata.get("version", "")),
                produces_state=("runtime.queue_depth", "runtime.health"),
                manages_types=("runtime_object",),
                relation_capabilities=("deployed_on", "executes"),
                update_mode="direct_query",
                endpoint=target.endpoint_id,
            )
        )
        self.plane.upsert_entity(
            GraphEntity(
                component_ref,
                GraphKind.COMPONENT,
                "component",
                lifecycle="ready" if target.available and target.healthy else "degraded",
                labels={"model_id": target.model_id},
            )
        )
        self.plane.upsert_entity(
            GraphEntity(
                instance_ref,
                GraphKind.DEPLOYMENT,
                "instance",
                lifecycle="ready" if target.available else "unavailable",
                owner_component_ref=component_ref,
                labels={"replica_id": target.replica_id, "model_id": target.model_id},
            )
        )
        relation_updates = [
            RelationUpdate(
                component_ref,
                "deployed_on",
                instance_ref,
                self.producer,
                authority=SourceAuthority.STRUCTURED_LIFECYCLE,
                idempotency_key=f"deployed_on:{component_ref}:{instance_ref}",
            )
        ]
        node_id = str(target.metadata.get("node_id", ""))
        if node_id:
            node_ref = self.node_ref(node_id)
            self.plane.upsert_entity(
                GraphEntity(node_ref, GraphKind.DEPLOYMENT, "node", lifecycle="ready")
            )
            relation_updates.append(
                RelationUpdate(
                    node_ref,
                    "contains",
                    instance_ref,
                    self.producer,
                    authority=SourceAuthority.STRUCTURED_LIFECYCLE,
                    idempotency_key=f"contains:{node_ref}:{instance_ref}",
                )
            )
        self.plane.upsert_relations(relation_updates)

        updates = [
            StateUpdate(
                component_ref,
                "runtime.health",
                "healthy" if target.healthy else "unhealthy",
                self.producer,
                authority=SourceAuthority.DERIVED,
                semantic=StateSemantic.DERIVED,
                confidence=0.5,
            ),
            StateUpdate(
                instance_ref,
                "instance.ready",
                target.available,
                self.producer,
                authority=SourceAuthority.DERIVED,
                semantic=StateSemantic.DERIVED,
                confidence=0.5,
            ),
        ]
        if "queue_depth" in target.metadata:
            updates.append(
                StateUpdate(
                    component_ref,
                    "runtime.queue_depth",
                    int(target.metadata["queue_depth"]),
                    self.producer,
                    authority=SourceAuthority.DERIVED,
                    semantic=StateSemantic.DERIVED,
                    confidence=0.5,
                )
            )
        if "allocated_gpu" in target.metadata:
            updates.append(
                StateUpdate(
                    instance_ref,
                    "instance.allocated_gpu",
                    float(target.metadata["allocated_gpu"]),
                    self.producer,
                    authority=SourceAuthority.DERIVED,
                    semantic=StateSemantic.DERIVED,
                    confidence=0.5,
                )
            )
        self.plane.publish_state(updates)

    def record_decision(self, request: ProviderNeutralRequest, decision: RoutingDecision) -> None:
        selected = next(
            item
            for item in decision.candidates
            if item.model_id == decision.selected_model
            and item.endpoint_id == decision.selected_endpoint
            and item.replica_id == decision.selected_replica
        )
        request_ref = self.request_ref(request.request_id)
        runtime_ref = self.runtime_ref(selected.endpoint_id)
        instance_ref = self.instance_ref(selected.replica_id)
        correlation = self._correlation(request, decision.decision_id)
        self.resolver.bind(request_ref, action_id=decision.decision_id)
        self.plane.publish_state(
            (
                StateUpdate(
                    request_ref,
                    "request.target_instance",
                    instance_ref,
                    self.producer,
                    authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                    correlation=correlation,
                    idempotency_key=f"{request.request_id}:target:{decision.decision_id}",
                ),
            )
        )
        self.plane.upsert_relations(
            (
                RelationUpdate(
                    request_ref,
                    "executing_on",
                    runtime_ref,
                    self.producer,
                    authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                    idempotency_key=f"exec-runtime:{request.request_id}:{decision.decision_id}",
                ),
                RelationUpdate(
                    request_ref,
                    "executing_on",
                    instance_ref,
                    self.producer,
                    authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                    idempotency_key=f"exec-instance:{request.request_id}:{decision.decision_id}",
                ),
            )
        )

    @staticmethod
    def request_ref(request_id: str) -> str:
        return CorrelationResolver.request_ref(request_id)

    @staticmethod
    def runtime_ref(endpoint_id: str) -> str:
        return CorrelationResolver.runtime_ref(endpoint_id)

    @staticmethod
    def instance_ref(replica_id: str) -> str:
        return CorrelationResolver.instance_ref(replica_id)

    @staticmethod
    def node_ref(node_id: str) -> str:
        return CorrelationResolver.node_ref(node_id)

    @staticmethod
    def _correlation(request: ProviderNeutralRequest, action_id: str = "") -> CorrelationIdentity:
        return CorrelationIdentity(
            trace_id=request.trace_id,
            session_id=request.session_id,
            request_id=request.request_id,
            action_id=action_id,
            component_id="stateflow-gateway",
        )

__all__ = ["GatewayStateProjector"]
