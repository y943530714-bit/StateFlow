"""Canonical key registry and schema negotiation."""

from __future__ import annotations

from datetime import datetime, timedelta
import threading
from typing import Any, Mapping

from .contracts import CanonicalKey, SourceAuthority, StateSemantic


class UnknownCanonicalKey(KeyError):
    pass


class CanonicalKeyRegistry:
    def __init__(self) -> None:
        self._definitions: dict[str, CanonicalKey] = {}
        self._aliases: dict[str, str] = {}
        self._lock = threading.RLock()

    def register(self, definition: CanonicalKey, *, replace: bool = False) -> None:
        with self._lock:
            if definition.key in self._definitions and not replace:
                raise ValueError(f"canonical key already registered: {definition.key}")
            for alias in definition.aliases:
                owner = self._aliases.get(alias)
                if owner is not None and owner != definition.key and not replace:
                    raise ValueError(f"canonical key alias already registered: {alias}")
            self._definitions[definition.key] = definition
            for alias in definition.aliases:
                self._aliases[alias] = definition.key

    def resolve(self, key: str) -> CanonicalKey:
        with self._lock:
            canonical = self._aliases.get(key, key)
            try:
                return self._definitions[canonical]
            except KeyError as exc:
                raise UnknownCanonicalKey(key) from exc

    def canonicalize(self, key: str) -> str:
        return self.resolve(key).key

    def list(self, *, include_deprecated: bool = False) -> tuple[CanonicalKey, ...]:
        with self._lock:
            values = self._definitions.values()
            return tuple(
                sorted(
                    (item for item in values if include_deprecated or not item.deprecated),
                    key=lambda item: item.key,
                )
            )

    @staticmethod
    def validate_value(definition: CanonicalKey, value: Any) -> bool:
        """Validate the small transport-neutral type vocabulary used by keys."""

        kind = definition.value_type
        if kind == "int":
            return isinstance(value, int) and not isinstance(value, bool)
        if kind == "float":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if kind == "bool":
            return isinstance(value, bool)
        if kind in {"string", "enum", "entity_ref"}:
            return isinstance(value, str)
        if kind == "timestamp":
            return isinstance(value, (datetime, str))
        if kind == "string_list":
            return isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value)
        if kind == "map":
            return isinstance(value, Mapping)
        return True


def default_key_registry() -> CanonicalKeyRegistry:
    """Return the first canonical key set listed in design v1.1 appendix A."""

    registry = CanonicalKeyRegistry()
    dynamic = timedelta(milliseconds=500)
    lifecycle = timedelta(seconds=5)
    slow = timedelta(seconds=30)
    definitions = [
        CanonicalKey("agent.phase", ("agent",), "enum", ttl=lifecycle),
        CanonicalKey("agent.priority", ("agent", "request"), "float", ttl=slow),
        CanonicalKey("agent.deadline", ("agent", "request"), "timestamp", ttl=slow),
        CanonicalKey("agent.expected_resume_time", ("agent",), "timestamp", ttl=lifecycle),
        CanonicalKey("session.status", ("session",), "enum", ttl=lifecycle),
        CanonicalKey("task.dependencies", ("task",), "string_list", ttl=slow),
        CanonicalKey("request.context_tokens", ("request",), "int", "token", ttl=lifecycle),
        CanonicalKey("request.expected_output_tokens", ("request",), "int", "token", ttl=lifecycle),
        CanonicalKey("request.queue_time", ("request",), "float", "s", ttl=dynamic),
        CanonicalKey("request.retry_count", ("request",), "int", ttl=lifecycle),
        CanonicalKey("request.target_instance", ("request",), "entity_ref", ttl=lifecycle),
        CanonicalKey(
            "runtime.queue_depth",
            ("component",),
            "int",
            ttl=dynamic,
            aliases=("runtime.queue.waiting_requests", "num_waiting", "queue_length"),
        ),
        CanonicalKey("runtime.running", ("component",), "int", ttl=dynamic),
        CanonicalKey("runtime.ttft_p95", ("component",), "float", "s", ttl=dynamic),
        CanonicalKey("runtime.tpot_p95", ("component",), "float", "s", ttl=dynamic),
        CanonicalKey("runtime.prefill_tps", ("component",), "float", "token/s", ttl=dynamic),
        CanonicalKey("runtime.decode_tps", ("component",), "float", "token/s", ttl=dynamic),
        CanonicalKey("runtime.kv_usage", ("component",), "float", "ratio", ttl=dynamic),
        CanonicalKey("runtime.health", ("component",), "enum", ttl=dynamic),
        CanonicalKey("kv.location", ("stateful_object",), "entity_ref", ttl=lifecycle),
        CanonicalKey("kv.size", ("stateful_object",), "int", "byte", ttl=lifecycle),
        CanonicalKey("kv.replica_count", ("stateful_object",), "int", ttl=lifecycle),
        CanonicalKey("kv.cache_hit", ("stateful_object", "request"), "bool", ttl=lifecycle),
        CanonicalKey("kv.transfer_state", ("stateful_object",), "enum", ttl=lifecycle),
        CanonicalKey("kv.soft_pin", ("stateful_object",), "bool", ttl=lifecycle),
        CanonicalKey(
            "kv.reuse_probability",
            ("stateful_object",),
            "float",
            "ratio",
            semantic=StateSemantic.DERIVED,
            source_priority=(SourceAuthority.DERIVED,),
            ttl=lifecycle,
        ),
        CanonicalKey("tool.queue_depth", ("component", "tool_call"), "int", ttl=lifecycle),
        CanonicalKey("tool.available_concurrency", ("component",), "int", ttl=lifecycle),
        CanonicalKey("tool.latency_p95", ("component",), "float", "s", ttl=lifecycle),
        CanonicalKey("tool.success_rate", ("component",), "float", "ratio", ttl=lifecycle),
        CanonicalKey("node.allocatable", ("node",), "map", ttl=slow),
        CanonicalKey("node.health", ("node",), "enum", ttl=lifecycle),
        CanonicalKey("instance.ready", ("instance",), "bool", ttl=lifecycle),
        CanonicalKey("instance.allocated_gpu", ("instance",), "float", "gpu", ttl=slow),
        CanonicalKey("resource.hbm.used", ("resource",), "int", "byte", ttl=dynamic),
        CanonicalKey("resource.hbm.reserved", ("resource",), "int", "byte", ttl=dynamic),
        CanonicalKey("resource.gpu.util", ("resource",), "float", "ratio", ttl=dynamic),
        CanonicalKey(
            "link.effective_bw",
            ("link",),
            "float",
            "byte/s",
            semantic=StateSemantic.DERIVED,
            ttl=dynamic,
        ),
    ]
    for definition in definitions:
        registry.register(definition)
    return registry


__all__ = ["CanonicalKeyRegistry", "UnknownCanonicalKey", "default_key_registry"]
