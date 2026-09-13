from __future__ import annotations

from pathlib import Path
import unittest

from stateflow.state import InMemoryStatePlane
from stateflow.adapters import RuntimeObservation, RuntimeStateAdapter


try:
    from stateflow.rpc import (
        StatePlaneGRPCClient,
        StatePlaneGRPCError,
        create_grpc_server,
    )

    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False


class GRPCContractTests(unittest.TestCase):
    def test_proto_declares_state_plane_service_and_streaming_watch(self) -> None:
        contract = Path("proto/stateflow.proto").read_text(encoding="utf-8")
        self.assertIn("service StatePlaneService", contract)
        self.assertIn("rpc PublishState", contract)
        self.assertIn("rpc GetSnapshot", contract)
        self.assertIn(
            "rpc WatchState(WatchStateRequest) returns (stream StateChange)",
            contract,
        )


@unittest.skipUnless(GRPC_AVAILABLE, "StateFlow grpc extra is not installed")
class GRPCInteroperabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plane = InMemoryStatePlane()
        self.server, port = create_grpc_server(self.plane, "127.0.0.1:0")
        self.server.start()
        self.client = StatePlaneGRPCClient(f"127.0.0.1:{port}", timeout=2.0)

    def tearDown(self) -> None:
        self.client.close()
        self.server.stop(grace=0).wait()

    def test_state_snapshot_graph_schema_and_watch_round_trip(self) -> None:
        status, entities = self.client.call(
            "UpsertEntities",
            payload={
                "entities": [
                    {
                        "ref": "deployment/instance/grpc-i1",
                        "graph": "deployment",
                        "entity_type": "instance",
                        "lifecycle": "ready",
                    }
                ]
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(entities["accepted"], 1)

        update = {
            "updates": [
                {
                    "entity_ref": "deployment/instance/grpc-i1",
                    "key": "instance.ready",
                    "value": True,
                    "producer": "grpc-test",
                    "authority": "STRUCTURED_LIFECYCLE",
                    "idempotency_key": "grpc-ready-1",
                }
            ]
        }
        status, published = self.client.call("PublishState", payload=update)
        self.assertEqual(status, 200)
        self.assertEqual(published["accepted"], 1)
        status, duplicate = self.client.call("PublishState", payload=update)
        self.assertEqual(status, 409)
        self.assertTrue(duplicate["results"][0]["duplicate"])

        status, snapshot = self.client.call(
            "GetSnapshot",
            payload={
                "entities": ["deployment/instance/grpc-i1"],
                "keys": ["instance.ready"],
                "min_authority": "DIRECT_TELEMETRY",
            },
        )
        self.assertEqual(status, 201)
        self.assertEqual(snapshot["completeness"], 1.0)
        self.assertTrue(snapshot["values"][0]["value"])

        status, frozen = self.client.call(
            "ReadSnapshot", payload={"token": snapshot["token"]}
        )
        self.assertEqual(status, 200)
        self.assertEqual(frozen, snapshot)

        status, graph = self.client.call(
            "QueryGraph",
            payload={"roots": ["deployment/instance/grpc-i1"], "depth": 0},
        )
        self.assertEqual(status, 200)
        self.assertEqual(graph["entities"][0]["ref"], "deployment/instance/grpc-i1")

        status, schema = self.client.call(
            "ListSchema", query={"key": "instance.ready"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(schema["key"]["key"], "instance.ready")

        changes = tuple(
            self.client.watch_state(
                keys=("instance.ready",), batch_size=10, follow=False
            )
        )
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0].key_or_relation, "instance.ready")
        self.assertGreater(changes[0].cursor, 0)

    def test_invalid_request_uses_grpc_status(self) -> None:
        with self.assertRaises(StatePlaneGRPCError) as raised:
            self.client.call("GetState")
        self.assertEqual(raised.exception.code, "INVALID_ARGUMENT")

    def test_semantic_adapter_uses_grpc_writer_interface(self) -> None:
        adapter = RuntimeStateAdapter(
            self.client, component_id="stateflow-grpc-runtime-adapter"
        )
        report = adapter.publish(
            RuntimeObservation(
                runtime_id="grpc-runtime",
                instance_id="grpc-replica",
                node_id="grpc-node",
                queue_depth=5,
                running=2,
                observation_id="grpc-runtime-1",
                watermark="grpc-runtime-1",
            )
        )

        self.assertEqual(report.rejected, 0)
        values = self.plane.get_state(
            "component/component/grpc-runtime",
            ("runtime.queue_depth", "runtime.running"),
        )
        self.assertEqual([value.value for value in values], [5, 2])


if __name__ == "__main__":
    unittest.main()
