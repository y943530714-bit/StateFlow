from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from stateflow.prediction import (
    AnalyticalPredictionModel,
    CandidateAction,
    PredictionRequest,
    PredictionService,
)
from stateflow.state import (
    GraphEntity,
    GraphKind,
    InMemoryStatePlane,
    Snapshot,
    SnapshotRequest,
    SourceAuthority,
    StateSemantic,
    StateUpdate,
    StateValue,
)


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _value(entity_ref: str, key: str, value) -> StateValue:
    return StateValue(
        entity_ref=entity_ref,
        key=key,
        value=value,
        producer="prediction-test",
        timestamp=NOW - timedelta(milliseconds=100),
        ttl=timedelta(seconds=10),
        authority=SourceAuthority.DIRECT_TELEMETRY,
        version=1,
        semantic=StateSemantic.OBSERVED,
        confidence=1.0,
    )


def _snapshot() -> Snapshot:
    return Snapshot(
        token="snapshot-test",
        logical_time=10,
        created_at=NOW,
        completeness=1.0,
        values=(
            _value("component/request/r1", "request.queue_time", 0.2),
            _value("component/request/r1", "request.context_tokens", 1000),
            _value(
                "component/request/r1",
                "request.expected_output_tokens",
                100,
            ),
            _value("component/component/runtime-a", "runtime.queue_depth", 3),
            _value("component/component/runtime-a", "runtime.prefill_tps", 1000.0),
            _value("component/component/runtime-a", "runtime.decode_tps", 100.0),
            _value("component/stateful_object/kv-r1", "kv.size", 1_000_000),
            _value("component/stateful_object/kv-r1", "kv.location", "remote-a"),
            _value("deployment/link/fabric-a", "link.effective_bw", 1_000_000.0),
            _value("deployment/resource/gpu-a", "resource.hbm.used", 8_000_000_000),
            _value(
                "deployment/resource/gpu-a",
                "resource.hbm.reserved",
                1_000_000_000,
            ),
            _value("deployment/resource/gpu-a", "resource.gpu.util", 0.75),
            _value("deployment/instance/i-a", "instance.allocated_gpu", 2.0),
        ),
        relations=(),
    )


class _SnapshotPlane:
    def __init__(self, snapshot: Snapshot) -> None:
        self.snapshot = snapshot

    def read_snapshot(self, token: str) -> Snapshot:
        if token != self.snapshot.token:
            raise KeyError(token)
        return self.snapshot


class AnalyticalPredictionTests(unittest.TestCase):
    def test_service_reads_an_immutable_state_plane_snapshot(self) -> None:
        plane = InMemoryStatePlane()
        request_ref = "component/request/prediction-r1"
        plane.upsert_entity(
            GraphEntity(request_ref, GraphKind.COMPONENT, "request")
        )
        plane.publish_state(
            [
                StateUpdate(
                    request_ref,
                    "request.context_tokens",
                    200,
                    "prediction-test",
                )
            ]
        )
        snapshot = plane.get_snapshot(
            SnapshotRequest(
                (request_ref,),
                ("request.context_tokens",),
                include_relations=False,
            )
        )
        prediction = PredictionService(plane).predict(
            PredictionRequest(
                snapshot.token,
                CandidateAction(
                    candidate_id="candidate-state-plane",
                    action_type="route",
                    target_component="component/component/runtime-a",
                    parameters={"prefill_tokens_per_second": 100.0},
                ),
            )
        )

        self.assertEqual(prediction.snapshot_id, snapshot.token)
        self.assertEqual(prediction.performance.e2e_seconds, 2.0)
        self.assertEqual(prediction.confidence, 1.0)
        self.assertEqual(prediction.fallback, "none")

    def test_deterministic_latency_pressure_and_cost_formulas(self) -> None:
        candidate = CandidateAction(
            candidate_id="candidate-route-a",
            action_type="route",
            target_component="component/component/runtime-a",
            parameters={
                "cached_tokens": 500,
                "transfer_setup_seconds": 0.1,
                "predicted_kv_growth_bytes": 1_000_000_000,
                "hbm_capacity_bytes": 10_000_000_000,
                "queue_delta": 1,
                "base_success_probability": 0.95,
                "deadline_seconds": 5.0,
                "gpu_second_cost": 0.01,
                "network_byte_cost": 1e-9,
            },
        )

        prediction = AnalyticalPredictionModel().predict(_snapshot(), candidate)

        self.assertAlmostEqual(prediction.performance.transfer_eta_seconds or 0, 1.1)
        self.assertAlmostEqual(prediction.performance.ttft_seconds or 0, 1.8)
        self.assertAlmostEqual(prediction.performance.e2e_seconds or 0, 2.8)
        self.assertAlmostEqual(prediction.performance.tpot_seconds or 0, 0.01)
        self.assertAlmostEqual(prediction.future_state.hbm_pressure or 0, 1.0)
        self.assertAlmostEqual(prediction.future_state.queue_depth or 0, 4.0)
        self.assertAlmostEqual(prediction.reliability.oom_probability or 0, 0.2)
        self.assertAlmostEqual(
            prediction.reliability.success_probability or 0,
            0.76,
        )
        self.assertAlmostEqual(prediction.cost.gpu_seconds or 0, 5.6)
        self.assertAlmostEqual(prediction.cost.monetary_cost or 0, 0.057)
        self.assertEqual(prediction.cost.network_bytes, 1_000_000)
        self.assertEqual(prediction.feature_freshness_ms["runtime.decode_tps"], 100)
        self.assertEqual(prediction.applicability, "routing")
        self.assertEqual(prediction.confidence, 1.0)
        self.assertEqual(prediction.fallback, "none")

    def test_missing_critical_rate_propagates_unknown_and_fallback(self) -> None:
        snapshot = Snapshot(
            token="snapshot-missing-bandwidth",
            logical_time=1,
            created_at=NOW,
            completeness=1.0,
            values=(
                _value("component/stateful_object/kv-r1", "kv.size", 1000),
            ),
            relations=(),
        )
        candidate = CandidateAction(
            candidate_id="candidate-prefetch",
            action_type="prefetch",
            target_component="component/component/runtime-a",
        )

        prediction = AnalyticalPredictionModel().predict(snapshot, candidate)

        self.assertIsNone(prediction.performance.transfer_eta_seconds)
        self.assertIsNone(prediction.performance.e2e_seconds)
        self.assertLess(prediction.confidence, 0.5)
        self.assertEqual(prediction.fallback, "baseline")
        self.assertIn("missing_feature:link.effective_bw", prediction.notes)

    def test_service_marks_low_confidence_and_model_errors_for_fallback(self) -> None:
        snapshot = Snapshot(
            token="snapshot-low-confidence",
            logical_time=1,
            created_at=NOW,
            completeness=0.4,
            values=(),
            relations=(),
        )
        service = PredictionService(_SnapshotPlane(snapshot))
        low = service.predict(
            PredictionRequest(
                snapshot.token,
                CandidateAction(
                    candidate_id="candidate-low",
                    action_type="route",
                    target_component="runtime-a",
                ),
            )
        )
        failed = service.predict(
            PredictionRequest(
                snapshot.token,
                CandidateAction(
                    candidate_id="candidate-invalid",
                    action_type="route",
                    target_component="runtime-a",
                    parameters={"transfer_bytes": -1},
                ),
            )
        )

        self.assertEqual(low.fallback, "success-first-heuristic")
        self.assertIn("low_confidence_fallback", low.notes)
        self.assertEqual(failed.applicability, "unavailable")
        self.assertEqual(failed.fallback, "success-first-heuristic")
        self.assertIn("predictor_error:ValueError", failed.notes)


if __name__ == "__main__":
    unittest.main()
