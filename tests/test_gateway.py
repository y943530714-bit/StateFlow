from __future__ import annotations

import json
import threading
import unittest
from urllib.request import Request, urlopen
from urllib.parse import quote

from stateflow.backend import BackendRegistry, InMemoryBackend
from stateflow.gateway import RequestGateway, TargetRegistry, normalize_request
from stateflow.gateway.server import StateFlowHTTPServer
from stateflow.scheduler.harness.success_first import SuccessFirstScheduler
from stateflow.state.schema import TargetCandidate
from stateflow.state.store import InMemoryStateStore


def build_gateway() -> RequestGateway:
    store = InMemoryStateStore()
    targets = TargetRegistry(
        [
            TargetCandidate(
                model_id="logical-small",
                endpoint_id="ep-1",
                replica_id="rep-1",
                tier="efficient",
                base_success=0.84,
                base_uncertainty=0.01,
                inference_cost=0.01,
                backend_key="memory",
            ),
            TargetCandidate(
                model_id="logical-capable",
                endpoint_id="ep-1",
                replica_id="rep-2",
                tier="capable",
                base_success=0.96,
                base_uncertainty=0.01,
                inference_cost=0.05,
                backend_key="memory",
            ),
        ]
    )
    backends = BackendRegistry()
    backends.register("memory", InMemoryBackend())
    return RequestGateway(SuccessFirstScheduler(), store, targets, backends)


class GatewayTests(unittest.TestCase):
    def test_normalizer_supports_all_design_protocols(self) -> None:
        chat = normalize_request(
            "/v1/chat/completions",
            {"model": "agent", "messages": [{"role": "user", "content": "hello"}]},
            {"X-StateFlow-Session-Id": "s-chat"},
        )
        responses = normalize_request(
            "/v1/responses",
            {"model": "agent", "input": "hello", "max_output_tokens": 12},
            {"x-session-id": "s-responses"},
        )
        messages = normalize_request(
            "/v1/messages",
            {"model": "agent", "system": "be concise", "messages": [{"role": "user", "content": "hi"}]},
            {"x-session-id": "s-messages"},
        )
        restricted = normalize_request(
            "/v1/chat/completions",
            {"model": "agent", "messages": [], "cost_budget": 0.25},
            {
                "x-session-id": "s-restricted",
                "x-stateflow-security-domain": "restricted",
                "x-stateflow-required-capabilities": "reasoning,tool_calling",
                "x-stateflow-remote-kv-allowed": "false",
            },
        )

        self.assertEqual(chat.protocol, "openai_chat")
        self.assertEqual(responses.protocol, "openai_responses")
        self.assertEqual(messages.protocol, "anthropic_messages")
        self.assertEqual(chat.session_id, "s-chat")
        self.assertEqual(responses.predicted_output_tokens, 12)
        self.assertGreater(messages.prompt_tokens, 0)
        self.assertEqual(restricted.security_domain, "restricted")
        self.assertEqual(restricted.required_capabilities, {"reasoning", "tool_calling"})
        self.assertFalse(restricted.remote_kv_allowed)
        self.assertEqual(restricted.cost_budget, 0.25)

    def test_gateway_routes_and_records_state(self) -> None:
        gateway = build_gateway()
        request = normalize_request(
            "/v1/chat/completions",
            {
                "model": "logical-agent",
                "messages": [{"role": "user", "content": "hello"}],
                "max_tokens": 20,
            },
            {"x-stateflow-session-id": "gateway-session"},
        )

        response = gateway.handle(request)

        self.assertEqual(response.status_code, 200)
        self.assertIn("x-stateflow-decision-id", response.headers)
        self.assertEqual(response.decision.selected_model, "logical-capable")
        state = gateway.state_store.get_state("gateway-session")
        assert state is not None
        self.assertTrue(state.scheduling.decision_history)
        self.assertEqual(state.model.selected_model, "logical-capable")

        request_ref = gateway.state_projector.request_ref(request.request_id)
        target_state = gateway.state_plane.get_state(request_ref, ("request.target_instance",))
        self.assertEqual(
            target_state[0].value,
            gateway.state_projector.instance_ref(response.decision.selected_replica),
        )
        graph = gateway.state_plane.query_graph((request_ref,), depth=1)
        self.assertIn("executing_on", {relation.relation_type for relation in graph.relations})

    def test_http_server_health_and_request_smoke(self) -> None:
        server = StateFlowHTTPServer(build_gateway(), port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.address
        try:
            with urlopen(f"http://{host}:{port}/healthz", timeout=2) as response:
                self.assertEqual(response.status, 200)

            payload = json.dumps(
                {"model": "agent", "messages": [{"role": "user", "content": "hello"}]}
            ).encode("utf-8")
            request = Request(
                f"http://{host}:{port}/v1/messages",
                data=payload,
                headers={"content-type": "application/json", "x-session-id": "http-session"},
                method="POST",
            )
            with urlopen(request, timeout=2) as response:
                body = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(body["type"], "message")
        finally:
            server.shutdown()
            thread.join(timeout=2)

    def test_state_projection_failure_is_observable_and_nonfatal(self) -> None:
        gateway = build_gateway()

        def fail_projection(*args, **kwargs):
            raise RuntimeError("projection unavailable")

        gateway.state_projector.record_request = fail_projection
        request = normalize_request(
            "/v1/chat/completions",
            {"model": "agent", "messages": [{"role": "user", "content": "hello"}]},
            {"x-session-id": "projection-failure"},
        )
        response = gateway.handle(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(gateway.state_projection_errors, 1)
        self.assertEqual(gateway.last_state_projection_error, "RuntimeError")

    def test_state_plane_http_southbound_and_snapshot_api(self) -> None:
        server = StateFlowHTTPServer(build_gateway(), port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.address

        def post(path: str, value: dict) -> tuple[int, dict]:
            request = Request(
                f"http://{host}:{port}{path}",
                data=json.dumps(value).encode("utf-8"),
                headers={"content-type": "application/json"},
                method="POST",
            )
            with urlopen(request, timeout=2) as response:
                return response.status, json.loads(response.read().decode("utf-8"))

        try:
            status, _ = post(
                "/v1/state-plane/entities/upsert",
                {
                    "entities": [
                        {
                            "ref": "deployment/instance/http-i1",
                            "graph": "deployment",
                            "entity_type": "instance",
                            "lifecycle": "ready",
                        }
                    ]
                },
            )
            self.assertEqual(status, 200)
            status, published = post(
                "/v1/state-plane/state/publish",
                {
                    "updates": [
                        {
                            "entity_ref": "deployment/instance/http-i1",
                            "key": "instance.ready",
                            "value": True,
                            "producer": "http-test",
                            "authority": "STRUCTURED_LIFECYCLE",
                        }
                    ]
                },
            )
            self.assertEqual(status, 200)
            self.assertEqual(published["accepted"], 1)

            status, snapshot = post(
                "/v1/state-plane/snapshots",
                {
                    "entities": ["deployment/instance/http-i1"],
                    "keys": ["instance.ready"],
                    "max_age_ms": {"instance": 5000},
                    "min_authority": "DIRECT_TELEMETRY",
                },
            )
            self.assertEqual(status, 201)
            self.assertEqual(snapshot["completeness"], 1.0)
            self.assertTrue(snapshot["values"][0]["value"])

            for value in (1.0, 3.0):
                status, metrics = post(
                    "/v1/state-plane/metrics/publish",
                    {
                        "samples": [
                            {
                                "entity_ref": "deployment/instance/http-i1",
                                "metric_key": "instance.allocated_gpu",
                                "value": value,
                                "unit": "gpu",
                                "producer": "http-test",
                            }
                        ]
                    },
                )
                self.assertEqual(status, 200)
                self.assertEqual(metrics["accepted"], 1)
            metric_entity = quote("deployment/instance/http-i1", safe="")
            with urlopen(
                f"http://{host}:{port}/v1/state-plane/metrics?entity_ref={metric_entity}&keys=instance.allocated_gpu&agg=avg",
                timeout=2,
            ) as response:
                metrics = json.loads(response.read().decode("utf-8"))
                self.assertEqual(metrics["values"][0]["value"], 2.0)

            status, event = post(
                "/v1/state-plane/events/publish",
                {
                    "event_id": "event-http-1",
                    "event_type": "instance_ready",
                    "subject_ref": "deployment/instance/http-i1",
                    "producer": "http-test",
                },
            )
            self.assertEqual(status, 200)
            self.assertEqual(event["accepted"], 1)
            status, heartbeat = post(
                "/v1/state-plane/heartbeat",
                {
                    "subject_ref": "deployment/instance/http-i1",
                    "producer": "http-test",
                    "source_watermark": "42",
                },
            )
            self.assertEqual(status, 200)
            self.assertEqual(heartbeat["heartbeat"]["source_watermark"], "42")

            token = quote(snapshot["token"], safe="")
            with urlopen(
                f"http://{host}:{port}/v1/state-plane/snapshots/{token}", timeout=2
            ) as response:
                frozen = json.loads(response.read().decode("utf-8"))
                self.assertEqual(frozen["token"], snapshot["token"])

            entity = quote("deployment/instance/http-i1", safe="")
            with urlopen(
                f"http://{host}:{port}/v1/state-plane/state?entity_ref={entity}&keys=instance.ready",
                timeout=2,
            ) as response:
                current = json.loads(response.read().decode("utf-8"))
                self.assertTrue(current["values"][0]["value"])
        finally:
            server.shutdown()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
