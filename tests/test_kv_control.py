from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from stateflow.control import (
    AgentAwareKVController,
    InMemoryControlJournal,
    KVActionOwner,
    KVControlRequest,
    KVTarget,
)
from stateflow.state import (
    GraphEntity,
    GraphKind,
    Snapshot,
    SourceAuthority,
    StateSemantic,
    StateValue,
)


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
KV_REF = "component/stateful_object/kv-a"
AGENT_REF = "component/agent/agent-a"
ACTION_OWNER = "component/component/mooncake-a"
STATE_OWNER = "component/component/stateflow-kv-adapter"
HBM_REF = "deployment/resource/gpu-a"
DRAM_REF = "deployment/resource/dram-a"


def _value(entity_ref: str, key: str, value) -> StateValue:
    return StateValue(
        entity_ref=entity_ref,
        key=key,
        value=value,
        producer="kv-control-test",
        timestamp=NOW - timedelta(milliseconds=100),
        ttl=timedelta(seconds=10),
        authority=SourceAuthority.DIRECT_TELEMETRY,
        version=1,
        semantic=StateSemantic.OBSERVED,
        confidence=1.0,
    )


class _Plane:
    def __init__(
        self,
        snapshot: Snapshot,
        *,
        state_owner: str = STATE_OWNER,
    ) -> None:
        self.snapshot = snapshot
        self.entities = {
            KV_REF: GraphEntity(
                KV_REF,
                GraphKind.COMPONENT,
                "stateful_object",
                owner_component_ref=state_owner,
            ),
            ACTION_OWNER: GraphEntity(
                ACTION_OWNER,
                GraphKind.COMPONENT,
                "component",
            ),
        }

    def read_snapshot(self, token: str) -> Snapshot:
        if token != self.snapshot.token:
            raise KeyError(token)
        return self.snapshot

    def get_entity(self, ref: str) -> GraphEntity | None:
        return self.entities.get(ref)


def _snapshot(
    current_location: str,
    *,
    reuse: float,
    phase: str = "TOOL_WAITING",
    resume_seconds: float = 2.0,
    completeness: float = 1.0,
    hbm_used: int = 9_500_000,
    hbm_reserved: int = 0,
    transfer_state: str = "idle",
) -> Snapshot:
    return Snapshot(
        token="snapshot-kv-a",
        logical_time=42,
        created_at=NOW,
        completeness=completeness,
        values=(
            _value(KV_REF, "kv.size", 1_000_000),
            _value(KV_REF, "kv.location", current_location),
            _value(KV_REF, "kv.reuse_probability", reuse),
            _value(KV_REF, "kv.transfer_state", transfer_state),
            _value(AGENT_REF, "agent.phase", phase),
            _value(
                AGENT_REF,
                "agent.expected_resume_time",
                (NOW + timedelta(seconds=resume_seconds)).isoformat(),
            ),
            _value(HBM_REF, "resource.hbm.used", hbm_used),
            _value(HBM_REF, "resource.hbm.reserved", hbm_reserved),
        ),
        relations=(),
    )


def _targets(*, dram_bandwidth: float = 2_000_000) -> tuple[KVTarget, ...]:
    return (
        KVTarget(
            HBM_REF,
            "hbm",
            effective_bw_bytes_per_second=2_000_000,
            hbm_capacity_bytes=10_000_000,
        ),
        KVTarget(
            DRAM_REF,
            "dram",
            effective_bw_bytes_per_second=dram_bandwidth,
            storage_io_byte_cost=1e-9,
        ),
    )


def _request(targets: tuple[KVTarget, ...] | None = None) -> KVControlRequest:
    return KVControlRequest(
        snapshot_id="snapshot-kv-a",
        kv_ref=KV_REF,
        targets=targets if targets is not None else _targets(),
        agent_ref=AGENT_REF,
        expected_state_owner=STATE_OWNER,
    )


def _controller(
    snapshot: Snapshot,
    *,
    journal: InMemoryControlJournal | None = None,
    state_owner: str = STATE_OWNER,
) -> AgentAwareKVController:
    return AgentAwareKVController(
        _Plane(snapshot, state_owner=state_owner),
        KVActionOwner(ACTION_OWNER, ("component/stateful_object/kv-",)),
        journal=journal,
    )


class AgentAwareKVControlTests(unittest.TestCase):
    def test_high_pressure_low_reuse_plans_dry_run_offload(self) -> None:
        journal = InMemoryControlJournal()
        controller = _controller(
            _snapshot(HBM_REF, reuse=0.1),
            journal=journal,
        )

        plan = controller.plan(_request())
        action = plan.decision.selected_action
        prediction = next(
            item
            for item in plan.predictions
            if item.candidate_id == action.parameters["candidate_id"]
        )

        self.assertEqual(action.action_type, "offload")
        self.assertTrue(action.dry_run)
        self.assertEqual(action.target_component, ACTION_OWNER)
        self.assertEqual(action.preconditions["snapshot_id"], "snapshot-kv-a")
        self.assertEqual(action.preconditions["expected_location"], HBM_REF)
        self.assertEqual(action.preconditions["expected_state_owner"], STATE_OWNER)
        self.assertEqual(action.preconditions["expected_transfer_state"], "idle")
        self.assertEqual(action.reservations, ())
        self.assertEqual(action.rollback_action["target_location"], HBM_REF)
        self.assertEqual(plan.decision.reason, "hbm_pressure_offload")
        self.assertEqual(plan.selected_target, DRAM_REF)
        self.assertAlmostEqual(plan.observed["hbm_pressure"], 0.95)
        self.assertAlmostEqual(prediction.future_state.hbm_pressure or 0, 0.85)
        self.assertIn(KV_REF, action.parameters["candidate_id"])
        self.assertEqual(len(journal.list()), 1)

    def test_imminent_reuse_plans_prefetch_with_proposed_reservation(self) -> None:
        controller = _controller(
            _snapshot(
                DRAM_REF,
                reuse=0.9,
                hbm_used=8_000_000,
                hbm_reserved=500_000,
            )
        )

        plan = controller.plan(_request())
        action = plan.decision.selected_action

        self.assertEqual(action.action_type, "prefetch")
        self.assertEqual(plan.decision.reason, "expected_resume_prefetch")
        self.assertEqual(plan.selected_target, HBM_REF)
        self.assertEqual(len(action.reservations), 1)
        reservation = action.reservations[0]
        self.assertEqual(reservation.resource_ref, HBM_REF)
        self.assertEqual(reservation.owner, ACTION_OWNER)
        self.assertEqual(reservation.amount, {"hbm_bytes": 1_000_000.0})
        self.assertEqual(reservation.status, "proposed")
        self.assertEqual(reservation.expires_at, NOW + timedelta(seconds=5))
        self.assertEqual(action.rollback_action["target_location"], DRAM_REF)

    def test_incomplete_snapshot_and_missing_bandwidth_keep_baseline(self) -> None:
        incomplete = _controller(
            _snapshot(HBM_REF, reuse=0.1, completeness=0.4)
        ).plan(_request())
        no_bandwidth_targets = _targets(dram_bandwidth=0)
        no_bandwidth = _controller(
            _snapshot(HBM_REF, reuse=0.1)
        ).plan(_request(no_bandwidth_targets))

        self.assertEqual(incomplete.decision.selected_action.action_type, "keep")
        self.assertEqual(incomplete.decision.reason, "snapshot_incomplete_keep")
        self.assertEqual(no_bandwidth.decision.selected_action.action_type, "keep")
        self.assertEqual(no_bandwidth.decision.reason, "prediction_fallback_keep")
        offload_prediction = next(
            item for item in no_bandwidth.predictions if item.candidate_id.endswith(DRAM_REF)
        )
        self.assertIsNone(offload_prediction.performance.transfer_eta_seconds)
        self.assertNotEqual(offload_prediction.fallback, "none")

    def test_owner_scope_and_state_owner_preconditions_are_enforced(self) -> None:
        snapshot = _snapshot(HBM_REF, reuse=0.1)
        plane = _Plane(snapshot)
        unauthorized = AgentAwareKVController(
            plane,
            KVActionOwner(ACTION_OWNER, ("component/stateful_object/other-",)),
        )

        with self.assertRaisesRegex(PermissionError, "does not manage"):
            unauthorized.plan(_request())
        with self.assertRaisesRegex(PermissionError, "state owner changed"):
            _controller(snapshot, state_owner="component/component/other").plan(
                _request()
            )
        with self.assertRaisesRegex(PermissionError, "no authoritative owner"):
            _controller(snapshot, state_owner="").plan(
                KVControlRequest(
                    snapshot_id="snapshot-kv-a",
                    kv_ref=KV_REF,
                    targets=_targets(),
                    agent_ref=AGENT_REF,
                )
            )

    def test_active_transfer_and_hbm_capacity_guard_keep_baseline(self) -> None:
        active_transfer = _controller(
            _snapshot(HBM_REF, reuse=0.1, transfer_state="transferring")
        ).plan(_request())
        capacity_guard = _controller(
            _snapshot(
                DRAM_REF,
                reuse=0.9,
                hbm_used=9_500_000,
                hbm_reserved=0,
            )
        ).plan(_request())

        self.assertEqual(active_transfer.decision.selected_action.action_type, "keep")
        self.assertEqual(
            active_transfer.decision.reason,
            "transfer_state_unsafe_keep",
        )
        self.assertEqual(capacity_guard.decision.selected_action.action_type, "keep")
        self.assertEqual(capacity_guard.decision.reason, "resource_guard_keep")


if __name__ == "__main__":
    unittest.main()
