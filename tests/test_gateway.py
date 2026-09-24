from __future__ import annotations

import json
import threading
import unittest
from urllib.request import Request, urlopen
from urllib.parse import quote

from stateflow.interface.backend import BackendRegistry
from stateflow.interface.memory_backend import InMemoryBackend
from stateflow.interface.gateway import RequestGateway
from stateflow.interface.request import normalize_request
from stateflow.state_manager import TargetRegistry
from stateflow.interface.http import StateFlowHTTPServer
from stateflow.planner.scheduler import SuccessFirstScheduler
from stateflow.state_manager.schema import TargetCandidate
from stateflow.state_manager.store import InMemoryStateStore


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
