"""Asynchronous state event reporting."""

from __future__ import annotations

from dataclasses import dataclass
import queue
import threading
from typing import Callable

from ...state.event import AgentStateEvent


@dataclass
class ReporterStats:
    published: int = 0
    dropped: int = 0
    failed: int = 0


class AsyncStateReporter:
    """Bounded, best-effort reporter isolated from the model request path."""

    def __init__(self, sink: Callable[[AgentStateEvent], object], max_queue: int = 1024) -> None:
        self._sink = sink
        self._queue: queue.Queue[AgentStateEvent | None] = queue.Queue(maxsize=max_queue)
        self.stats = ReporterStats()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="stateflow-state-reporter", daemon=True)
        self._thread.start()

    def publish(self, event: AgentStateEvent) -> bool:
        try:
            self._queue.put_nowait(event)
            self.stats.published += 1
            return True
        except queue.Full:
            self.stats.dropped += 1
            return False

    def close(self, timeout: float = 1.0) -> None:
        self._stop.set()
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        self._thread.join(timeout=timeout)

    def _run(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            try:
                event = self._queue.get(timeout=0.05)
            except queue.Empty:
                continue
            if event is None:
                continue
            try:
                self._sink(event)
            except Exception:
                self.stats.failed += 1
