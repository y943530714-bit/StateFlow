"""Reproducible in-process State Plane snapshot latency baseline."""

from __future__ import annotations

import argparse
from datetime import timedelta
import json
from statistics import mean
from time import perf_counter_ns

from stateflow.state import (
    GraphEntity,
    GraphKind,
    InMemoryStatePlane,
    RelationUpdate,
    SnapshotRequest,
    StateUpdate,
)


def build_plane() -> tuple[InMemoryStatePlane, SnapshotRequest]:
    plane = InMemoryStatePlane()
    entities = (
        GraphEntity("component/request/bench-r", GraphKind.COMPONENT, "request"),
        GraphEntity("component/component/runtime-a", GraphKind.COMPONENT, "component"),
        GraphEntity("component/component/runtime-b", GraphKind.COMPONENT, "component"),
        GraphEntity("deployment/instance/instance-a", GraphKind.DEPLOYMENT, "instance"),
        GraphEntity("deployment/instance/instance-b", GraphKind.DEPLOYMENT, "instance"),
        GraphEntity("component/stateful_object/kv-a", GraphKind.COMPONENT, "stateful_object"),
        GraphEntity("deployment/resource/gpu-a", GraphKind.DEPLOYMENT, "resource"),
    )
    for entity in entities:
        plane.upsert_entity(entity)
    ttl = timedelta(minutes=5)
    plane.publish_state(
        (
            StateUpdate(entities[0].ref, "request.context_tokens", 4096, "benchmark", ttl=ttl),
            StateUpdate(entities[0].ref, "request.expected_output_tokens", 256, "benchmark", ttl=ttl),
            StateUpdate(entities[1].ref, "runtime.queue_depth", 4, "benchmark", ttl=ttl),
            StateUpdate(entities[1].ref, "runtime.ttft_p95", 0.08, "benchmark", ttl=ttl),
            StateUpdate(entities[2].ref, "runtime.queue_depth", 6, "benchmark", ttl=ttl),
            StateUpdate(entities[2].ref, "runtime.ttft_p95", 0.11, "benchmark", ttl=ttl),
            StateUpdate(entities[3].ref, "instance.ready", True, "benchmark", ttl=ttl),
            StateUpdate(entities[4].ref, "instance.ready", True, "benchmark", ttl=ttl),
            StateUpdate(entities[5].ref, "kv.location", entities[3].ref, "benchmark", ttl=ttl),
            StateUpdate(entities[5].ref, "kv.size", 64 * 1024 * 1024, "benchmark", ttl=ttl),
        )
    )
    plane.upsert_relations(
        (
            RelationUpdate(entities[0].ref, "executing_on", entities[1].ref, "benchmark"),
            RelationUpdate(entities[1].ref, "deployed_on", entities[3].ref, "benchmark"),
            RelationUpdate(entities[2].ref, "deployed_on", entities[4].ref, "benchmark"),
            RelationUpdate(entities[5].ref, "located_on", entities[6].ref, "benchmark"),
        )
    )
    request = SnapshotRequest(
        entities=tuple(entity.ref for entity in entities),
        keys=(
            "request.context_tokens",
            "request.expected_output_tokens",
            "runtime.queue_depth",
            "runtime.ttft_p95",
            "instance.ready",
            "kv.location",
            "kv.size",
        ),
    )
    return plane, request


def percentile(values: list[float], percentile_value: int) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, (percentile_value * len(ordered) + 99) // 100 - 1))
    return ordered[index]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=100)
    args = parser.parse_args()
    if args.iterations <= 0 or args.warmup < 0:
        parser.error("iterations must be positive and warmup must be non-negative")

    plane, request = build_plane()
    for _ in range(args.warmup):
        plane.get_snapshot(request)
    samples_ms: list[float] = []
    for _ in range(args.iterations):
        started = perf_counter_ns()
        snapshot = plane.get_snapshot(request)
        samples_ms.append((perf_counter_ns() - started) / 1_000_000)
    if snapshot.completeness != 1.0:
        raise RuntimeError(f"benchmark snapshot is incomplete: {snapshot.completeness}")
    print(
        json.dumps(
            {
                "iterations": args.iterations,
                "entities": len(request.entities),
                "values": len(snapshot.values),
                "relations": len(snapshot.relations),
                "mean_ms": round(mean(samples_ms), 4),
                "p50_ms": round(percentile(samples_ms, 50), 4),
                "p95_ms": round(percentile(samples_ms, 95), 4),
                "p99_ms": round(percentile(samples_ms, 99), 4),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
