"""Minimal urllib-based OpenAI-compatible backend adapter."""

from __future__ import annotations

import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ..base import BackendError, BackendResponse
from ...gateway.normalizer.request import ProviderNeutralRequest
from ...state.schema import TargetCandidate


class OpenAICompatibleBackend:
    def __init__(self, base_url: str, *, api_key: str = "", timeout_seconds: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    def send(self, request: ProviderNeutralRequest, target: TargetCandidate) -> BackendResponse:
        path = target.metadata.get("path")
        if not path:
            path = {
                "openai_chat": "/v1/chat/completions",
                "openai_responses": "/v1/responses",
                "anthropic_messages": "/v1/messages",
            }[request.protocol]
        body = json.dumps(request.backend_payload(target.model_id)).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        req = Request(self.base_url + path, data=body, headers=headers, method="POST")
        try:
            with urlopen(req, timeout=self.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
                payload = json.loads(raw) if raw else {}
                return BackendResponse(
                    status_code=response.status,
                    payload=payload,
                    output_tokens=_output_tokens(payload),
                )
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise BackendError(f"backend HTTP {exc.code}: {detail[:500]}") from exc
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise BackendError(f"backend request failed: {exc}") from exc


def _output_tokens(payload: dict) -> int:
    usage = payload.get("usage") if isinstance(payload, dict) else None
    if not isinstance(usage, dict):
        return 0
    for key in ("output_tokens", "completion_tokens"):
        if key in usage:
            try:
                return max(0, int(usage[key]))
            except (TypeError, ValueError):
                return 0
    return 0


__all__ = ["OpenAICompatibleBackend"]
