"""Failure-isolated polling loop for out-of-process source adapters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import threading
from typing import Callable

from ..state.schema import utcnow
from .clients import CollectionReport


@dataclass(frozen=True)
class RunnerState:
    attempts: int = 0
    successes: int = 0
    failures: int = 0
    consecutive_failures: int = 0
    last_success_at: datetime | None = None
    last_error: str = ""
    last_report: CollectionReport | None = None


class PollingAdapterRunner:
    """Run a source collector outside the request path with bounded backoff."""

    def __init__(
        self,
        operation: Callable[[], CollectionReport],
        *,
        interval_seconds: float = 1.0,
        max_backoff_seconds: float = 30.0,
    ) -> None:
        if interval_seconds <= 0 or max_backoff_seconds <= 0:
            raise ValueError("poll intervals must be positive")
        self.operation = operation
        self.interval_seconds = interval_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self._state = RunnerState()
        self._lock = threading.Lock()

    @property
    def state(self) -> RunnerState:
        with self._lock:
            return self._state

    def run_once(self) -> CollectionReport | None:
        with self._lock:
            current = self._state
        try:
            report = self.operation()
        except Exception as exc:
            with self._lock:
                self._state = RunnerState(
                    attempts=current.attempts + 1,
                    successes=current.successes,
                    failures=current.failures + 1,
                    consecutive_failures=current.consecutive_failures + 1,
                    last_success_at=current.last_success_at,
                    last_error=type(exc).__name__,
                    last_report=current.last_report,
                )
            return None
        with self._lock:
            self._state = RunnerState(
                attempts=current.attempts + 1,
                successes=current.successes + 1,
                failures=current.failures,
                consecutive_failures=0,
                last_success_at=utcnow(),
                last_report=report,
            )
        return report

    def serve(self, stop: threading.Event) -> None:
        while not stop.is_set():
            self.run_once()
            failures = self.state.consecutive_failures
            delay = min(
                self.max_backoff_seconds,
                self.interval_seconds * (2 ** min(failures, 10)),
            )
            stop.wait(delay)


__all__ = ["PollingAdapterRunner", "RunnerState"]
