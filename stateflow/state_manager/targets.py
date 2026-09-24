"""Registered model replicas and expiring health reports."""

from __future__ import annotations

from dataclasses import replace
from threading import RLock
from time import monotonic
from typing import Any, Iterable

from ..state.schema import TargetCandidate


class TargetRegistry:
    def __init__(self, targets: Iterable[TargetCandidate] = ()) -> None:
        self._targets = list(targets)
        self._overlays: dict[tuple[str, str, str], tuple[float, dict[str, Any]]] = {}
        self._lock = RLock()

    def register(self, target: TargetCandidate) -> None:
        with self._lock:
            self._targets.append(target)

    def all(self) -> list[TargetCandidate]:
        now = monotonic()
        with self._lock:
            result = []
            for target in self._targets:
                key = (target.model_id, target.endpoint_id, target.replica_id)
                overlay = self._overlays.get(key)
                if overlay and overlay[0] > now:
                    result.append(replace(target, **overlay[1]))
                else:
                    self._overlays.pop(key, None)
                    result.append(target)
            return result

    def update(self, report: dict[str, Any]) -> None:
        """Apply an expiring instance status report; do not mutate model priors."""

        key = tuple(str(report.get(part, "")) for part in (
            "model_id", "endpoint_id", "replica_id"
        ))
        allowed = {
            "available", "healthy", "queue_latency_seconds", "load_balance_score",
            "local_kv_tokens", "remote_kv_tokens",
        }
        changes = {name: report[name] for name in allowed if name in report}
        if not changes or set(report) - allowed - {
            "model_id", "endpoint_id", "replica_id", "ttl_seconds"
        }:
            raise ValueError("target report contains no status or unsupported fields")
        for name in ("available", "healthy"):
            if name in changes and not isinstance(changes[name], bool):
                raise ValueError(f"{name} must be a boolean")
        for name in allowed - {"available", "healthy"}:
            if name in changes and (
                isinstance(changes[name], bool)
                or not isinstance(changes[name], (int, float))
                or changes[name] < 0
            ):
                raise ValueError(f"{name} must be non-negative")
        ttl = float(report.get("ttl_seconds", 30))
        if not 0 < ttl <= 3600:
            raise ValueError("ttl_seconds must be between 0 and 3600")
        with self._lock:
            if key not in {
                (item.model_id, item.endpoint_id, item.replica_id) for item in self._targets
            }:
                raise KeyError(f"unknown target {key}")
            current = self._overlays.get(key)
            previous = current[1] if current and current[0] > monotonic() else {}
            self._overlays[key] = (monotonic() + ttl, {**previous, **changes})
