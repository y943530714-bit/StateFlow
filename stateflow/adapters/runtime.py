"""Runtime telemetry adapter for vLLM/SGLang-style observations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from ..state import (
    ComponentDescriptor,
    CorrelationIdentity,
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
class RuntimeObservation:
    runtime_id: str
    instance_id: str
    observed_at: datetime = field(default_factory=utcnow)
    node_id: str = ""
    request_id: str = ""
    trace_id: str = ""
    queue_depth: int | None = None
    running: int | None = None
    ttft_p95_seconds: float | None = None
    tpot_p95_seconds: float | None = None
    prefill_tokens_per_second: float | None = None
    decode_tokens_per_second: float | None = None
    kv_usage_ratio: float | None = None
    health: str | None = None
    ready: bool | None = None
    implementation: str = "unknown"
    version: str = ""
    watermark: str = ""
    observation_id: str = ""
    source_ref: SourceRef | None = None


class RuntimeStateAdapter(StatePlaneAdapter):
    """Materialize decision-oriented runtime state, not raw counters."""

    def __init__(
        self,
        plane,
        resolver: CorrelationResolver | None = None,
        *,
        component_id: str = "stateflow-runtime-adapter",
    ) -> None:
        super().__init__(
            plane,
            component_id=component_id,
            kind="runtime_adapter",
            implementation="runtime-observability",
            produces_state=(
                "runtime.queue_depth",
                "runtime.running",
                "runtime.ttft_p95",
                "runtime.tpot_p95",
                "runtime.prefill_tps",
                "runtime.decode_tps",
                "runtime.kv_usage",
                "runtime.health",
                "instance.ready",
            ),
            manages_types=("component",),
            relation_capabilities=("deployed_on", "executing_on"),
        )
        self.resolver = resolver or CorrelationResolver()

    def publish(self, observation: RuntimeObservation) -> AdapterPublishReport:
        runtime_ref = self.resolver.runtime_ref(observation.runtime_id)
        instance_ref = self.resolver.instance_ref(observation.instance_id)
        source = observation.source_ref or SourceRef(
            "runtime", f"{observation.runtime_id}/{observation.instance_id}"
        )
        correlation = CorrelationIdentity(
            trace_id=observation.trace_id,
            request_id=observation.request_id,
            component_id=observation.runtime_id,
            instance_id=observation.instance_id,
        )
        self._register_runtime(observation)
        self.plane.upsert_entity(
            GraphEntity(
                runtime_ref,
                GraphKind.COMPONENT,
                "component",
                lifecycle="ready" if observation.health in {None, "healthy"} else "degraded",
                labels={"kind": "inference_runtime"},
            )
        )
        self.plane.upsert_entity(
            GraphEntity(
                instance_ref,
                GraphKind.DEPLOYMENT,
                "instance",
                lifecycle="ready" if observation.ready is not False else "unavailable",
                owner_component_ref=runtime_ref,
                labels={"runtime_id": observation.runtime_id},
            )
        )
        self.resolver.bind(runtime_ref, component_id=observation.runtime_id)
        self.resolver.bind(instance_ref, instance_id=observation.instance_id)

        relations = [
            RelationUpdate(
                runtime_ref,
                "deployed_on",
                instance_ref,
                self.producer,
                timestamp=observation.observed_at,
                authority=SourceAuthority.STRUCTURED_LIFECYCLE,
                attributes=source_attributes(source),
                idempotency_key=self._key(observation, "deployed_on"),
            )
        ]
        if observation.node_id:
            node_ref = self.resolver.node_ref(observation.node_id)
            self.ensure_entity(
                GraphEntity(node_ref, GraphKind.DEPLOYMENT, "node", labels={"node_id": observation.node_id})
            )
            self.resolver.bind(node_ref, node_id=observation.node_id)
            relations.append(
                RelationUpdate(
                    node_ref,
                    "contains",
                    instance_ref,
                    self.producer,
                    timestamp=observation.observed_at,
                    authority=SourceAuthority.STRUCTURED_LIFECYCLE,
                    attributes=source_attributes(source),
                    idempotency_key=self._key(observation, "node_contains_instance"),
                )
            )
        if observation.request_id:
            request_ref = self.resolver.request_ref(observation.request_id)
            self.ensure_entity(
                GraphEntity(request_ref, GraphKind.COMPONENT, "request", lifecycle="running")
            )
            self.resolver.bind(
                request_ref,
                request_id=observation.request_id,
                trace_id=observation.trace_id,
            )
            relations.extend(
                (
                    RelationUpdate(
                        request_ref,
                        "executing_on",
                        runtime_ref,
                        self.producer,
                        timestamp=observation.observed_at,
                        authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                        attributes=source_attributes(source),
                        idempotency_key=self._key(observation, "request_exec_runtime"),
                    ),
                    RelationUpdate(
                        request_ref,
                        "executing_on",
                        instance_ref,
                        self.producer,
                        timestamp=observation.observed_at,
                        authority=SourceAuthority.AUTHORITATIVE_DOMAIN,
                        attributes=source_attributes(source),
                        idempotency_key=self._key(observation, "request_exec_instance"),
                    ),
                )
            )

        state_values = {
            "runtime.queue_depth": observation.queue_depth,
            "runtime.running": observation.running,
            "runtime.ttft_p95": observation.ttft_p95_seconds,
            "runtime.tpot_p95": observation.tpot_p95_seconds,
            "runtime.prefill_tps": observation.prefill_tokens_per_second,
            "runtime.decode_tps": observation.decode_tokens_per_second,
            "runtime.kv_usage": observation.kv_usage_ratio,
            "runtime.health": observation.health,
        }
        updates = [
            StateUpdate(
                runtime_ref,
                key,
                value,
                self.producer,
                timestamp=observation.observed_at,
                authority=SourceAuthority.DIRECT_TELEMETRY,
                source_ref=source,
                correlation=correlation,
                idempotency_key=self._key(observation, key),
            )
            for key, value in state_values.items()
            if value is not None
        ]
        if observation.ready is not None:
            updates.append(
                StateUpdate(
                    instance_ref,
                    "instance.ready",
                    observation.ready,
                    self.producer,
                    timestamp=observation.observed_at,
                    authority=SourceAuthority.STRUCTURED_LIFECYCLE,
                    source_ref=source,
                    correlation=correlation,
                    idempotency_key=self._key(observation, "instance.ready"),
                )
            )
        state_results = tuple(self.plane.publish_state(updates))
        relation_results = tuple(self.plane.upsert_relations(relations))
        self.heartbeat(observation.watermark, timestamp=observation.observed_at)
        return AdapterPublishReport(state_results, relation_results)

    def _register_runtime(self, observation: RuntimeObservation) -> None:
        if any(item.component_id == observation.runtime_id for item in self.plane.list_components()):
            return
        self.plane.register_component(
            ComponentDescriptor(
                component_id=observation.runtime_id,
                kind="inference_runtime",
                implementation=observation.implementation,
                version=observation.version,
                schema_versions=("1.1",),
                produces_state=(
                    "runtime.queue_depth",
                    "runtime.running",
                    "runtime.ttft_p95",
                    "runtime.tpot_p95",
                    "runtime.prefill_tps",
                    "runtime.decode_tps",
                    "runtime.kv_usage",
                    "runtime.health",
                ),
                manages_types=("runtime_object",),
                relation_capabilities=("deployed_on", "executes"),
                update_mode="metric_and_event",
            )
        )

    @staticmethod
    def _key(observation: RuntimeObservation, suffix: str) -> str:
        return f"runtime:{observation.observation_id}:{suffix}" if observation.observation_id else ""


__all__ = ["RuntimeObservation", "RuntimeStateAdapter"]
