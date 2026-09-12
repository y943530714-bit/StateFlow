from __future__ import annotations

from datetime import timedelta
import unittest

from stateflow.control import Objective, PolicyIntent
from stateflow.prediction import Prediction
from stateflow.scheduler import routing_control_bundle
from stateflow.scheduler.harness.success_first import SuccessFirstScheduler
from stateflow.state import (
    GraphEntity,
    GraphKind,
    InMemoryStatePlane,
    RelationUpdate,
    SnapshotRequest,
    SourceAuthority,
    StateUpdate,
    default_key_registry,
)
from stateflow.state.schema import AgentPhase, HarnessSchedulingView, TargetCandidate, utcnow


class ArchitectureV11StatePlaneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plane = InMemoryStatePlane()
        self.request = GraphEntity("component/request/r1", GraphKind.COMPONENT, "request")
        self.runtime = GraphEntity("component/component/runtime-a", GraphKind.COMPONENT, "component")
        self.instance = GraphEntity("deployment/instance/i1", GraphKind.DEPLOYMENT, "instance")
        self.node = GraphEntity("deployment/node/n1", GraphKind.DEPLOYMENT, "node")
        for entity in (self.request, self.runtime, self.instance, self.node):
            self.plane.upsert_entity(entity)

    def test_registry_normalizes_adapter_aliases(self) -> None:
        registry = default_key_registry()
        self.assertEqual(registry.canonicalize("num_waiting"), "runtime.queue_depth")
        self.assertEqual(registry.resolve("runtime.queue_depth").unit, "")
        self.assertEqual(
            SourceAuthority.parse("domain_owner"),
            SourceAuthority.AUTHORITATIVE_DOMAIN,
        )

    def test_authority_cas_idempotency_and_immutable_snapshot(self) -> None:
        accepted = self.plane.publish_state(
            [
                StateUpdate(
                    self.runtime.ref,
                    "num_waiting",
                    4,
                    "runtime-adapter",
                    authority=SourceAuthority.DIRECT_TELEMETRY,
                    idempotency_key="runtime-a:queue:1",
                )
            ]
        )[0]
        self.assertTrue(accepted.accepted)
        duplicate = self.plane.publish_state(
            [
                StateUpdate(
                    self.runtime.ref,
                    "runtime.queue_depth",
                    99,
                    "runtime-adapter",
                    idempotency_key="runtime-a:queue:1",
                )
            ]
        )[0]
        self.assertTrue(duplicate.duplicate)

        snapshot = self.plane.get_snapshot(
            SnapshotRequest((self.runtime.ref,), ("runtime.*",))
        )
        self.assertEqual(snapshot.values[0].key, "runtime.queue_depth")
        self.assertEqual(snapshot.values[0].value, 4)

        lower_authority = self.plane.publish_state(
            [
                StateUpdate(
                    self.runtime.ref,
                    "runtime.queue_depth",
                    8,
                    "derived-estimator",
                    authority=SourceAuthority.DERIVED,
                    expected_version=1,
                )
            ]
        )[0]
        self.assertTrue(lower_authority.conflict)

        updated = self.plane.publish_state(
            [
                StateUpdate(
                    self.runtime.ref,
                    "runtime.queue_depth",
                    5,
                    "runtime-adapter",
                    authority=SourceAuthority.DIRECT_TELEMETRY,
                    expected_version=1,
                )
            ]
        )[0]
        self.assertTrue(updated.accepted)
        frozen = self.plane.get_state(
            self.runtime.ref,
            ("runtime.queue_depth",),
            snapshot=snapshot.token,
        )
        current = self.plane.get_state(self.runtime.ref, ("runtime.queue_depth",))
        alias_read = self.plane.get_state(self.runtime.ref, ("queue_length",))
        self.assertEqual(frozen[0].value, 4)
        self.assertEqual(current[0].value, 5)
        self.assertEqual(alias_read[0].value, 5)

    def test_snapshot_reports_stale_and_missing_without_defaults(self) -> None:
        self.plane.publish_state(
            [
                StateUpdate(
                    self.request.ref,
                    "request.context_tokens",
                    128,
                    "gateway",
                    timestamp=utcnow() - timedelta(seconds=10),
                    ttl=timedelta(milliseconds=100),
                    authority=SourceAuthority.STRUCTURED_LIFECYCLE,
                )
            ]
        )
        snapshot = self.plane.get_snapshot(
            SnapshotRequest(
                (self.request.ref,),
                ("request.context_tokens", "request.retry_count"),
            )
        )
        self.assertEqual(snapshot.completeness, 0.0)
        self.assertIn(f"{self.request.ref}:request.context_tokens", snapshot.stale)
        self.assertIn(f"{self.request.ref}:request.retry_count", snapshot.missing)

    def test_snapshot_patterns_apply_only_to_compatible_entity_types(self) -> None:
        self.plane.publish_state(
            [
                StateUpdate(self.request.ref, "request.retry_count", 0, "gateway"),
                StateUpdate(self.instance.ref, "instance.ready", True, "sidecar"),
            ]
        )
        snapshot = self.plane.get_snapshot(
            SnapshotRequest(
                (self.request.ref, self.instance.ref),
                ("request.retry_count", "instance.ready"),
            )
        )
        self.assertEqual(snapshot.completeness, 1.0)
        self.assertFalse(snapshot.missing)

    def test_canonical_value_type_is_enforced(self) -> None:
        result = self.plane.publish_state(
            [StateUpdate(self.instance.ref, "instance.ready", "yes", "broken-sidecar")]
        )[0]
        self.assertFalse(result.accepted)
        self.assertIn("declared type bool", result.reason)

    def test_only_declared_cross_graph_relations_are_allowed(self) -> None:
        deployed = self.plane.upsert_relations(
            [RelationUpdate(self.runtime.ref, "deployed_on", self.instance.ref, "k8s-watch")]
        )[0]
        invalid = self.plane.upsert_relations(
            [RelationUpdate(self.runtime.ref, "contains", self.node.ref, "bad-adapter")]
        )[0]
        self.assertTrue(deployed.accepted)
        self.assertFalse(invalid.accepted)

        result = self.plane.query_graph((self.runtime.ref,), depth=2)
        self.assertEqual({item.ref for item in result.entities}, {self.runtime.ref, self.instance.ref})
        self.assertEqual(result.relations[0].relation_type, "deployed_on")

    def test_change_cursor_is_resumable(self) -> None:
        cursor = self.plane.revision
        self.plane.publish_state(
            [StateUpdate(self.instance.ref, "instance.ready", True, "sidecar")]
        )
        changes = self.plane.changes(after_cursor=cursor, keys=("instance.*",))
        self.assertEqual(len(changes), 1)
        self.assertGreater(changes[0].cursor, cursor)


class ArchitectureV11PredictionTests(unittest.TestCase):
    def test_routing_decision_exports_candidate_action_predictions(self) -> None:
        view = HarnessSchedulingView(
            session_id="s1",
            state_id="state-1",
            state_version=3,
            phase=AgentPhase.RUNNABLE,
            runnable=True,
            prompt_tokens=100,
            predicted_output_tokens=20,
            state_completeness=1.0,
            state_freshness=1.0,
        )
        target = TargetCandidate(
            "model-a",
            "runtime-a",
            "replica-a",
            base_success=0.97,
            inference_cost=0.1,
            prefill_tokens_per_second=1000.0,
            decode_tokens_per_second=100.0,
        )
        routing = SuccessFirstScheduler().schedule(view, (target,))
        bundle = routing_control_bundle(routing, policy_version="critical-v2")

        self.assertEqual(bundle.decision.snapshot_id, "agent-state:state-1:3")
        self.assertEqual(bundle.decision.policy_version, "critical-v2")
        self.assertEqual(bundle.decision.selected_action.action_type, "route")
        self.assertIsInstance(bundle.predictions[0], Prediction)
        self.assertAlmostEqual(
            bundle.predictions[0].reliability.success_probability or 0.0,
            routing.predicted_success,
        )

    def test_policy_objective_order_is_configurable(self) -> None:
        self.assertEqual(PolicyIntent.interactive().objectives[0], Objective.LATENCY)
        self.assertEqual(PolicyIntent.critical().objectives[0], Objective.SUCCESS)
        self.assertEqual(PolicyIntent.batch().objectives[0], Objective.COST)


if __name__ == "__main__":
    unittest.main()
