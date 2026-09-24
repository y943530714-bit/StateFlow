from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from stateflow.interface.config import build_configured_gateway
from stateflow.interface.http import StateFlowHTTPServer


class _BackendHandler(BaseHTTPRequestHandler):
    calls: list[dict] = []

    def do_POST(self) -> None:  # noqa: N802
        data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        type(self).calls.append({"path": self.path, "body": data})
        if data.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self.wfile.write(b'data: {"choices":[{"delta":{"content":"ok"}}]}\n\n')
            self.wfile.write(b"data: [DONE]\n\n")
            return
        output = json.dumps({
            "id": "upstream-1", "object": "chat.completion", "model": data["model"],
            "choices": [{"message": {"role": "assistant", "content": "ok"}}],
            "usage": {"completion_tokens": 2},
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(output)))
        self.end_headers()
        self.wfile.write(output)

    def log_message(self, format: str, *args: object) -> None:
        return


class ControlPlaneIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        _BackendHandler.calls = []
        self.backend = ThreadingHTTPServer(("127.0.0.1", 0), _BackendHandler)
        self.backend_thread = threading.Thread(target=self.backend.serve_forever, daemon=True)
        self.backend_thread.start()
        self.temp = tempfile.TemporaryDirectory()
        path = Path(self.temp.name) / "gateway.json"
        path.write_text(json.dumps({
            "backends": {"local": {
                "base_url": f"http://127.0.0.1:{self.backend.server_port}/v1",
                "timeout_seconds": 2,
            }},
            "targets": [
                {"model_id": "fast", "endpoint_id": "local", "replica_id": "a",
                 "tier": "efficient", "backend_key": "local", "base_success": 0.95,
                 "base_uncertainty": 0.01, "inference_cost": 0.01},
                {"model_id": "capable", "endpoint_id": "local", "replica_id": "b",
                 "tier": "capable", "backend_key": "local", "base_success": 0.98,
                 "base_uncertainty": 0.01, "inference_cost": 0.05},
            ],
            "policy": {
                "s_min": 0.75, "epsilon_success": 0.06,
                "min_state_completeness_for_downgrade": 0.25,
            },
        }), encoding="utf-8")
        self.gateway = build_configured_gateway(path)
        self.server = StateFlowHTTPServer(self.gateway, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.address[1]}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.backend.shutdown()
        self.backend.server_close()
        self.backend_thread.join(timeout=2)
        self.temp.cleanup()

    def post(self, path: str, value: dict, *, session: str = "run-1") -> tuple[int, dict, dict]:
        req = Request(self.url + path, data=json.dumps(value).encode(), headers={
            "Content-Type": "application/json", "x-stateflow-session-id": session,
        })
        try:
            with urlopen(req, timeout=3) as response:
                return response.status, json.loads(response.read()), dict(response.headers)
        except HTTPError as exc:
            return exc.code, json.loads(exc.read()), dict(exc.headers)

    def test_configured_proxy_control_plane_and_failure_escalation(self) -> None:
        with urlopen(self.url + "/v1/control/actions", timeout=2) as response:
            actions = json.loads(response.read())["actions"]
        self.assertEqual([item["action_id"] for item in actions], ["route_model"])

        status, _, _ = self.post("/v1/control/state", {
            "program_id": "run-1", "event_type": "TASK_STARTED", "source": "harness",
            "payload": {"identity": {"harness_type": "coding-agent"}},
        })
        self.assertEqual(status, 200)

        request = {"model": "logical-agent", "messages": [{"role": "user", "content": "hi"}],
                   "cost_budget": 0.2, "required_capabilities": []}
        status, body, headers = self.post("/v1/chat/completions", request)
        self.assertEqual(status, 200)
        self.assertEqual(body["model"], "fast")
        self.assertEqual(headers["x-stateflow-selected-model"], "fast")
        self.assertEqual(_BackendHandler.calls[0]["path"], "/v1/chat/completions")
        self.assertNotIn("cost_budget", _BackendHandler.calls[0]["body"])
        self.assertNotIn("required_capabilities", _BackendHandler.calls[0]["body"])
        events = self.gateway.state_store.events_for("run-1")
        self.assertIn("ACTION_SUCCEEDED", [event.event_type for event in events])

        for sequence in (1, 2):
            status, result, _ = self.post("/v1/control/state", {
                "program_id": "run-1", "event_type": "MODEL_FAILED",
                "source": "harness", "payload": {"error": "test failure", "error_severity": 0.8},
            })
            self.assertEqual(status, 200)
            self.assertTrue(result["accepted"])
        status, decision, _ = self.post("/v1/control/decisions", request)
        self.assertEqual(status, 200)
        self.assertEqual(decision["action"]["action_id"], "route_model")
        self.assertEqual(decision["action"]["params"]["model_id"], "capable")
        self.assertEqual(len(_BackendHandler.calls), 1)  # planning does not execute
        status, body, _ = self.post("/v1/chat/completions", request)
        self.assertEqual(status, 200)
        self.assertEqual(body["model"], "capable")

    def test_stream_forwards_sse_and_rejects_unsupported_protocol(self) -> None:
        req = Request(self.url + "/v1/chat/completions", data=json.dumps({
            "model": "logical-agent", "messages": [], "stream": True,
        }).encode(), headers={"Content-Type": "application/json", "x-stateflow-session-id": "stream-1"})
        with urlopen(req, timeout=3) as response:
            self.assertEqual(response.headers["Content-Type"], "text/event-stream")
            self.assertIn("data: [DONE]", response.read().decode())
        self.assertIn("ACTION_SUCCEEDED", [
            event.event_type for event in self.gateway.state_store.events_for("stream-1")
        ])
        status, body, _ = self.post("/v1/messages", {"model": "agent", "messages": [], "stream": True})
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["type"], "unsupported_stream")

    def test_program_identity_and_request_constraints_cannot_widen_state(self) -> None:
        self.assertEqual(self.post("/v1/control/state", {
            "program_id": "restricted", "event_type": "STATE_PATCH", "source": "harness",
            "payload": {"security": {"allowed_models": ["capable"]}},
        }, session="restricted")[0], 200)
        req = Request(self.url + "/v1/chat/completions", data=json.dumps({
            "model": "agent", "messages": [], "metadata": {"allowed_models": ["fast"]},
        }).encode(), headers={
            "Content-Type": "application/json", "x-stateflow-program-id": "restricted",
        })
        with self.assertRaises(HTTPError) as failure:
            urlopen(req, timeout=3)
        self.assertEqual(failure.exception.code, 503)
        self.assertIn("conflicts", failure.exception.read().decode())
        self.assertEqual(_BackendHandler.calls, [])

        status, body, _ = self.post("/v1/responses", {"model": "agent", "input": "hello"})
        self.assertEqual(status, 503)
        self.assertEqual(body["error"]["type"], "no_feasible_target")

    def test_engine_status_report_changes_eligible_target(self) -> None:
        self.post("/v1/control/state", {
            "program_id": "status-1", "event_type": "TASK_STARTED", "source": "harness",
            "payload": {"identity": {"harness_type": "coding-agent"}},
        }, session="status-1")
        status, body, _ = self.post("/v1/control/state", {
            "event_type": "TARGET_UPDATED", "source": "engine", "payload": {
                "model_id": "fast", "endpoint_id": "local", "replica_id": "a",
                "healthy": False, "ttl_seconds": 30,
            },
        })
        self.assertEqual(status, 200)
        self.assertTrue(body["accepted"])
        status, body, _ = self.post("/v1/chat/completions", {
            "model": "logical-agent", "messages": [],
        }, session="status-1")
        self.assertEqual(status, 200)
        self.assertEqual(body["model"], "capable")

    def test_invalid_config_and_unavailable_backend(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown backend_key"):
            path = Path(self.temp.name) / "bad.json"
            path.write_text(json.dumps({
                "backends": {"a": {"base_url": "http://127.0.0.1"}},
                "targets": [{"model_id": "m", "endpoint_id": "e", "replica_id": "r",
                             "backend_key": "missing"}],
            }))
            build_configured_gateway(path)
        self.backend.shutdown()
        status, body, _ = self.post("/v1/chat/completions", {"model": "agent", "messages": []})
        self.assertEqual(status, 502)
        self.assertEqual(body["error"]["type"], "backend_error")
