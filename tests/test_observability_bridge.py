from __future__ import annotations

from datetime import timedelta
import unittest

from stateflow.adapters import (
    CorrelationResolver,
    DeploymentObservation,
    HardwareObservation,
    IdentityConflict,
    KVLocation,
    KVObservation,
    ObservabilityBridge,
    RuntimeObservation,
)
from stateflow.state import InMemoryStatePlane, SourceAuthority, SourceRef
from stateflow.state.schema import utcnow


class ObservabilityBridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plane = InMemoryStatePlane()
        self.bridge = ObservabilityBridge(self.plane)

    def test_materializes_request_instance_node_and_kv_chain(self) -> None:
        self.bridge.kubernetes.publish(
            DeploymentObservation(
                cluster_id="prod-a",
                node_id="node-a",
                instance_id="replica-a",
                runtime_id="runtime-a",
                namespace="serving",
                pod_uid="pod-123",
                node_health="healthy",
                node_allocatable={"gpu": 8, "memory_bytes": 512_000_000_000},
                instance_ready=True,
                allocated_gpu=1.0,
                watermark="rv-42",
                observation_id="k8s-42",
            )
        )
        self.bridge.runtime.publish(
            RuntimeObservation(
                runtime_id="runtime-a",
                instance_id="replica-a",
                node_id="node-a",
                request_id="request/a",
                trace_id="trace-a",
                queue_depth=3,
                running=7,
                ttft_p95_seconds=0.24,
                tpot_p95_seconds=0.018,
                prefill_tokens_per_second=1800.0,
                decode_tokens_per_second=115.0,
                kv_usage_ratio=0.61,
                health="healthy",
                ready=True,
                watermark="runtime-9",
                observation_id="runtime-9",
                source_ref=SourceRef("vllm", "metrics/runtime-a", "trace-a"),
            )
        )
        self.bridge.dcgm.publish(
            HardwareObservation(
                node_id="node-a",
                gpu_id="0",
                hbm_used_bytes=32_000_000_000,
                hbm_reserved_bytes=2_000_000_000,
                gpu_util_ratio=0.72,
                node_health="healthy",
                watermark="dcgm-7",
                observation_id="dcgm-7",
            )
        )
        self.bridge.kv.publish(
            KVObservation(
                kv_id="kv/request-a",
                request_ids=("request/a",),
                locations=(
                    KVLocation("node-a/gpu/0", entity_type="resource", tier="hbm", primary=True),
                ),
                size_bytes=4_096_000,
                replica_count=1,
                cache_hit=True,
                transfer_state="resident",
                soft_pin=True,
                trace_id="trace-a",
                watermark="kv-11",
                observation_id="kv-11",
                source_ref=SourceRef("mooncake", "object/kv-request-a", "trace-a"),
            )
        )

        view = self.bridge.materialize_request(
            "request/a",
            depth=3,
            min_authority=SourceAuthority.DIRECT_TELEMETRY,
        )

        entity_types = {entity.entity_type for entity in view.graph.entities}
        self.assertTrue(
            {"request", "component", "stateful_object", "cluster", "node", "instance", "resource"}
            <= entity_types
        )
        relation_types = {relation.relation_type for relation in view.graph.relations}
        self.assertTrue(
            {"executing_on", "deployed_on", "uses", "located_on", "contains"}
            <= relation_types
        )
        values = {(value.entity_ref, value.key): value for value in view.snapshot.values}
        runtime_ref = self.bridge.resolver.require("component_id", "runtime-a")
        kv_ref = self.bridge.resolver.require("kv_id", "kv/request-a")
        self.assertEqual(values[(runtime_ref, "runtime.queue_depth")].value, 3)
        self.assertEqual(values[(runtime_ref, "runtime.queue_depth")].source_ref.backend, "vllm")
        self.assertEqual(values[(kv_ref, "kv.size")].value, 4_096_000)
        self.assertEqual(values[(kv_ref, "kv.size")].source_ref.backend, "mooncake")
        self.assertEqual(
            self.bridge.resolver.require("request_id", "request/a"),
            view.request_ref,
        )
        self.assertEqual(self.plane.freshness()["adapter_count"], 4)

    def test_stale_runtime_state_and_adapter_are_explicit(self) -> None:
        observed_at = utcnow() - timedelta(seconds=12)
        report = self.bridge.runtime.publish(
            RuntimeObservation(
                runtime_id="runtime-stale",
                instance_id="replica-stale",
                request_id="request-stale",
                observed_at=observed_at,
                queue_depth=9,
                observation_id="runtime-stale-1",
            )
        )
        self.assertEqual(report.rejected, 0)

        view = self.bridge.materialize_request("request-stale", depth=2)
        runtime_ref = self.bridge.resolver.require("component_id", "runtime-stale")
        self.assertIn(f"{runtime_ref}:runtime.queue_depth", view.snapshot.stale)
        self.assertEqual(self.plane.freshness()["stale_adapter_count"], 1)

    def test_observation_id_makes_adapter_write_idempotent(self) -> None:
        observation = RuntimeObservation(
            runtime_id="runtime-idempotent",
            instance_id="replica-idempotent",
            queue_depth=1,
            observation_id="sample-1",
        )
        first = self.bridge.runtime.publish(observation)
        second = self.bridge.runtime.publish(observation)

        self.assertGreater(first.accepted, 0)
        self.assertEqual(second.accepted, 0)
        self.assertTrue(all(result.duplicate for result in second.results))

    def test_trace_can_fan_out_but_unique_identity_cannot_conflict(self) -> None:
        resolver = CorrelationResolver()
        resolver.bind("component/request/a", request_id="a", trace_id="shared")
        resolver.bind("component/request/b", request_id="b", trace_id="shared")

        self.assertEqual(
            resolver.resolve_all("trace_id", "shared"),
            ("component/request/a", "component/request/b"),
        )
        with self.assertRaises(IdentityConflict):
            resolver.bind("component/request/c", request_id="a")


if __name__ == "__main__":
    unittest.main()
