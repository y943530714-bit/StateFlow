"""Stable entity references and cross-source correlation bindings."""

from __future__ import annotations

from dataclasses import dataclass
import threading
from typing import Mapping

from ..state import CorrelationIdentity, GraphKind
from .base import entity_ref


class IdentityConflict(ValueError):
    """Raised when one external identity is bound to two canonical entities."""


@dataclass(frozen=True)
class IdentityBinding:
    namespace: str
    value: str
    entity_ref: str


class CorrelationResolver:
    """Resolve trace/request/component IDs to canonical graph entities.

    Bindings are deliberately explicit. Conflicting source identities are
    rejected instead of silently merging two requests or deployment objects.
    """

    def __init__(self) -> None:
        self._bindings: dict[tuple[str, str], set[str]] = {}
        self._unique_namespaces = {
            "request_id",
            "action_id",
            "component_id",
            "instance_id",
            "kv_id",
            "node_id",
        }
        self._lock = threading.RLock()

    def bind(self, entity: str, **identities: str) -> tuple[IdentityBinding, ...]:
        if not entity:
            raise ValueError("entity ref must not be empty")
        created: list[IdentityBinding] = []
        with self._lock:
            for namespace, raw_value in identities.items():
                value = str(raw_value).strip()
                if not value:
                    continue
                key = (namespace, value)
                existing = self._bindings.get(key, set())
                if namespace in self._unique_namespaces and existing and entity not in existing:
                    raise IdentityConflict(
                        f"{namespace}={value!r} is already bound to {sorted(existing)[0]}"
                    )
                self._bindings.setdefault(key, set()).add(entity)
                created.append(IdentityBinding(namespace, value, entity))
        return tuple(created)

    def resolve(self, namespace: str, value: str) -> str | None:
        matches = self.resolve_all(namespace, value)
        return matches[0] if len(matches) == 1 else None

    def resolve_all(self, namespace: str, value: str) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._bindings.get((namespace, str(value)), set())))

    def require(self, namespace: str, value: str) -> str:
        resolved = self.resolve(namespace, value)
        if resolved is None:
            matches = self.resolve_all(namespace, value)
            reason = "ambiguous" if matches else "unresolved"
            raise KeyError(f"{reason} correlation identity: {namespace}={value!r}")
        return resolved

    def bindings(self) -> tuple[IdentityBinding, ...]:
        with self._lock:
            return tuple(
                IdentityBinding(namespace, value, ref)
                for (namespace, value), refs in sorted(self._bindings.items())
                for ref in sorted(refs)
            )

    @staticmethod
    def correlation(values: Mapping[str, str]) -> CorrelationIdentity:
        allowed = CorrelationIdentity.__dataclass_fields__
        return CorrelationIdentity(
            **{key: str(value) for key, value in values.items() if key in allowed and value}
        )

    @staticmethod
    def request_ref(request_id: str) -> str:
        return entity_ref(GraphKind.COMPONENT, "request", request_id)

    @staticmethod
    def runtime_ref(runtime_id: str) -> str:
        return entity_ref(GraphKind.COMPONENT, "component", runtime_id)

    @staticmethod
    def kv_ref(kv_id: str) -> str:
        return entity_ref(GraphKind.COMPONENT, "stateful_object", kv_id)

    @staticmethod
    def cluster_ref(cluster_id: str) -> str:
        return entity_ref(GraphKind.DEPLOYMENT, "cluster", cluster_id)

    @staticmethod
    def node_ref(node_id: str) -> str:
        return entity_ref(GraphKind.DEPLOYMENT, "node", node_id)

    @staticmethod
    def instance_ref(instance_id: str) -> str:
        return entity_ref(GraphKind.DEPLOYMENT, "instance", instance_id)

    @staticmethod
    def resource_ref(resource_id: str) -> str:
        return entity_ref(GraphKind.DEPLOYMENT, "resource", resource_id)

    @staticmethod
    def link_ref(link_id: str) -> str:
        return entity_ref(GraphKind.DEPLOYMENT, "link", link_id)


__all__ = ["CorrelationResolver", "IdentityBinding", "IdentityConflict"]
