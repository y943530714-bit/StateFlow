"""Backend adapter boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from stateflow.interface.request import ProviderNeutralRequest
from stateflow.state_manager.schema import TargetCandidate


@dataclass
class BackendResponse:
    status_code: int
    payload: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)
    output_tokens: int = 0


class BackendAdapter(Protocol):
    def send(self, request: ProviderNeutralRequest, target: TargetCandidate) -> BackendResponse:
        ...


class BackendError(RuntimeError):
    pass


class BackendRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, BackendAdapter] = {}

    def register(self, key: str, adapter: BackendAdapter) -> None:
        self._adapters[key] = adapter

    def get(self, key: str) -> BackendAdapter | None:
        return self._adapters.get(key)

    def require(self, key: str) -> BackendAdapter:
        adapter = self.get(key)
        if adapter is None:
            raise BackendError(f"no backend adapter registered for {key!r}")
        return adapter
