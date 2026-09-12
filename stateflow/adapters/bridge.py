"""Composition root for the v1.1 observability bridge."""

from __future__ import annotations

from dataclasses import dataclass

from ..state import GraphQueryResult, InMemoryStatePlane, Snapshot, SnapshotRequest, SourceAuthority
from .dcgm import DCGMStateAdapter
from .identity import CorrelationResolver
from .kubernetes import KubernetesStateAdapter
from .kv import KVStateAdapter
from .runtime import RuntimeStateAdapter


@dataclass(frozen=True)
class MaterializedRequestView:
    request_ref: str
    graph: GraphQueryResult
    snapshot: Snapshot


class ObservabilityBridge:
    """Share identity resolution across Runtime/KV/Deployment/HW adapters."""

    default_keys = (
        "request.*",
        "runtime.*",
        "kv.*",
        "node.*",
        "instance.*",
        "resource.*",
        "link.*",
    )

    def __init__(
        self,
        plane: InMemoryStatePlane,
        resolver: CorrelationResolver | None = None,
    ) -> None:
        self.plane = plane
        self.resolver = resolver or CorrelationResolver()
        self.runtime = RuntimeStateAdapter(plane, self.resolver)
        self.kv = KVStateAdapter(plane, self.resolver)
        self.kubernetes = KubernetesStateAdapter(plane, self.resolver)
        self.dcgm = DCGMStateAdapter(plane, self.resolver)

    def materialize_request(
        self,
        request_id: str,
        *,
        keys: tuple[str, ...] = default_keys,
        depth: int = 3,
        min_authority: SourceAuthority = SourceAuthority.LOG_HINT,
    ) -> MaterializedRequestView:
        request_ref = self.resolver.resolve("request_id", request_id)
        if request_ref is None:
            request_ref = self.resolver.request_ref(request_id)
        graph = self.plane.query_graph((request_ref,), depth=depth)
        entities = tuple(entity.ref for entity in graph.entities)
        if not entities:
            raise KeyError(f"request is not materialized: {request_id}")
        snapshot = self.plane.get_snapshot(
            SnapshotRequest(
                entities=entities,
                keys=keys,
                min_authority=min_authority,
                include_relations=True,
            )
        )
        frozen_graph = self.plane.query_graph(
            (request_ref,),
            depth=depth,
            snapshot=snapshot.token,
        )
        return MaterializedRequestView(request_ref, frozen_graph, snapshot)


__all__ = ["MaterializedRequestView", "ObservabilityBridge"]
