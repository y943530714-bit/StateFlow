"""Deterministic backend used by tests, examples, and local smoke runs."""

from __future__ import annotations

from contextlib import contextmanager
import json
from typing import Iterator

from stateflow.interface.backend import BackendResponse
from stateflow.interface.request import ProviderNeutralRequest
from stateflow.state_manager.schema import TargetCandidate


class InMemoryBackend:
    @contextmanager
    def open_stream(
        self, request: ProviderNeutralRequest, target: TargetCandidate
    ) -> Iterator[Iterator[bytes]]:
        message = f"StateFlow routed request to {target.model_id}@{target.replica_id}."
        payload = {
            "id": request.request_id,
            "object": "chat.completion.chunk",
            "model": target.model_id,
            "choices": [{"index": 0, "delta": {"content": message}, "finish_reason": "stop"}],
        }
        yield iter((
            f"data: {json.dumps(payload)}\n\n".encode(),
            b"data: [DONE]\n\n",
        ))

    def send(self, request: ProviderNeutralRequest, target: TargetCandidate) -> BackendResponse:
        message = f"StateFlow routed request to {target.model_id}@{target.replica_id}."
        if request.protocol == "anthropic_messages":
            payload = {
                "id": request.request_id,
                "type": "message",
                "role": "assistant",
                "model": target.model_id,
                "content": [{"type": "text", "text": message}],
                "stop_reason": "end_turn",
                "usage": {"input_tokens": request.prompt_tokens, "output_tokens": 8},
            }
        elif request.protocol == "openai_responses":
            payload = {
                "id": request.request_id,
                "object": "response",
                "model": target.model_id,
                "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": message}]}],
                "usage": {"input_tokens": request.prompt_tokens, "output_tokens": 8},
            }
        else:
            payload = {
                "id": request.request_id,
                "object": "chat.completion",
                "model": target.model_id,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": message}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": request.prompt_tokens, "completion_tokens": 8, "total_tokens": request.prompt_tokens + 8},
            }
        return BackendResponse(status_code=200, payload=payload, output_tokens=8)


__all__ = ["InMemoryBackend"]
