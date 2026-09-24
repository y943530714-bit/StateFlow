"""Small standard-library HTTP gateway for local MVP deployments."""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from urllib.parse import parse_qs, urlparse
from typing import Any

from stateflow.state_manager.event import AgentStateEvent
from stateflow.state_manager.schema import to_jsonable
from stateflow.planner.types import NoFeasibleTarget
from stateflow.interface.backend import BackendError
from stateflow.interface.request import normalize_request
from stateflow.interface.gateway import RequestGateway


class StateFlowHTTPServer:
    def __init__(self, gateway: RequestGateway, host: str = "127.0.0.1", port: int = 8080) -> None:
        self.gateway = gateway
        self.server = ThreadingHTTPServer((host, port), self._handler_class())

    @property
    def address(self) -> tuple[str, int]:
        return self.server.server_address[0], self.server.server_address[1]

    def serve_forever(self) -> None:
        self.server.serve_forever()

    def shutdown(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def _handler_class(self):
        gateway = self.gateway

        class Handler(BaseHTTPRequestHandler):
            server_version = "StateFlow/0.5"

            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path == "/healthz":
                    self._write(200, {"status": "ok"})
                    return
                if parsed.path == "/v1/control/actions":
                    self._write(200, {"actions": gateway.control_plane.catalog.all()})
                    return
                if parsed.path in {"/v1/state/view", "/v1/control/state"}:
                    query = parse_qs(parsed.query)
                    session_id = query.get("program_id", query.get("session_id", [""]))[0]
                    task_id = query.get("task_id", ["default-task"])[0]
                    if not session_id:
                        self._write(400, {"error": {"message": "session_id is required"}})
                        return
                    self._write(
                        200,
                        gateway.state_manager.get_scheduling_view(session_id, task_id).to_dict(),
                    )
                    return
                self._write(404, {"error": {"message": "not found"}})

            def do_POST(self) -> None:  # noqa: N802
                try:
                    body = self._read_json()
                    parsed = urlparse(self.path)
                    if parsed.path in {"/v1/state/events", "/v1/control/state"}:
                        if body.get("event_type") == "TARGET_UPDATED":
                            gateway.state_manager.update_target(dict(body.get("payload") or {}))
                            self._write(200, {"accepted": True})
                            return
                        if "program_id" in body and "session_id" not in body:
                            body["session_id"] = body.pop("program_id")
                        if not body.get("session_id") or not (body.get("event_type") or body.get("event")):
                            raise ValueError("program_id/session_id and event_type are required")
                        event = AgentStateEvent.from_mapping(body)
                        result = gateway.control_plane.ingest(event)
                        self._write(200, to_jsonable(result))
                        return
                    if parsed.path == "/v1/control/decisions":
                        protocol = body.get("protocol", "openai_chat")
                        path_by_protocol = {
                            "openai_chat": "/v1/chat/completions",
                            "openai_responses": "/v1/responses",
                            "anthropic_messages": "/v1/messages",
                        }
                        if protocol not in path_by_protocol:
                            raise ValueError(f"unsupported protocol: {protocol}")
                        request = normalize_request(path_by_protocol[protocol], body, self.headers)
                        try:
                            decision, action = gateway.plan(request)
                        except NoFeasibleTarget as exc:
                            self._write(503, {"error": {
                                "type": "no_feasible_target", "message": str(exc),
                                "rejections": [item.to_dict() for item in exc.rejections],
                            }})
                            return
                        self._write(200, {"decision": decision.to_dict(), "action": action.to_dict()})
                        return
                    if parsed.path in {"/v1/chat/completions", "/v1/responses", "/v1/messages"}:
                        request = normalize_request(parsed.path, body, self.headers)
                        if request.stream:
                            if request.protocol != "openai_chat":
                                self._write(400, {"error": {
                                    "type": "unsupported_stream",
                                    "message": "v1 streaming supports OpenAI chat completions only",
                                }})
                                return
                            started = False
                            try:
                                with gateway.stream(request) as (response, chunks):
                                    self.send_response(response.status_code)
                                    self.send_header("Content-Type", "text/event-stream")
                                    self.send_header("Cache-Control", "no-cache")
                                    self.send_header("Connection", "close")
                                    for key, value in response.headers.items():
                                        self.send_header(key, value)
                                    self.end_headers()
                                    started = True
                                    for chunk in chunks:
                                        self.wfile.write(chunk)
                                        self.wfile.flush()
                            except NoFeasibleTarget as exc:
                                if not started:
                                    self._write(503, {"error": {"type": "no_feasible_target", "message": str(exc)}})
                            except BackendError as exc:
                                if not started:
                                    self._write(502, {"error": {"type": "backend_error", "message": str(exc)}})
                            except (BrokenPipeError, ConnectionResetError):
                                pass
                            finally:
                                self.close_connection = True
                            return
                        response = gateway.handle(request)
                        self._write(response.status_code, response.payload, response.headers)
                        return
                    self._write(404, {"error": {"message": "not found"}})
                except ValueError as exc:
                    self._write(400, {"error": {"type": "invalid_request", "message": str(exc)}})
                except KeyError as exc:
                    self._write(404, {"error": {"type": "not_found", "message": str(exc)}})
                except Exception as exc:  # pragma: no cover - defensive HTTP boundary
                    self._write(500, {"error": {"type": "internal_error", "message": str(exc)}})

            def log_message(self, format: str, *args: Any) -> None:
                return

            def _read_json(self) -> dict[str, Any]:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0:
                    raise ValueError("JSON request body is required")
                raw = self.rfile.read(length)
                value = json.loads(raw.decode("utf-8"))
                if not isinstance(value, dict):
                    raise ValueError("JSON request body must be an object")
                return value

            def _write(
                self,
                status: int,
                payload: dict[str, Any],
                headers: dict[str, str] | None = None,
            ) -> None:
                raw = json.dumps(to_jsonable(payload), ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                for key, value in (headers or {}).items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(raw)

        return Handler


__all__ = ["StateFlowHTTPServer"]
