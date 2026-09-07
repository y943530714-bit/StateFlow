"""Small standard-library HTTP gateway for local MVP deployments."""

from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from urllib.parse import parse_qs, urlparse
from typing import Any

from ...state.event import AgentStateEvent
from ...state.schema import to_jsonable
from ..normalizer.request import normalize_request
from ..service import RequestGateway


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
            server_version = "StateFlow/0.1"

            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path == "/healthz":
                    self._write(200, {"status": "ok"})
                    return
                if parsed.path == "/v1/state/view":
                    query = parse_qs(parsed.query)
                    session_id = query.get("session_id", [""])[0]
                    task_id = query.get("task_id", ["default-task"])[0]
                    if not session_id:
                        self._write(400, {"error": {"message": "session_id is required"}})
                        return
                    self._write(
                        200,
                        gateway.state_store.get_scheduling_view(session_id, task_id).to_dict(),
                    )
                    return
                self._write(404, {"error": {"message": "not found"}})

            def do_POST(self) -> None:  # noqa: N802
                try:
                    body = self._read_json()
                    parsed = urlparse(self.path)
                    if parsed.path == "/v1/state/events":
                        event = AgentStateEvent.from_mapping(body)
                        result = gateway.state_store.append_event(event)
                        self._write(200, to_jsonable(result))
                        return
                    if parsed.path in {"/v1/chat/completions", "/v1/responses", "/v1/messages"}:
                        request = normalize_request(parsed.path, body, self.headers)
                        response = gateway.handle(request)
                        self._write(response.status_code, response.payload, response.headers)
                        return
                    self._write(404, {"error": {"message": "not found"}})
                except ValueError as exc:
                    self._write(400, {"error": {"type": "invalid_request", "message": str(exc)}})
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
