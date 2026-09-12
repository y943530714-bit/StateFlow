"""Finite Kubernetes list-watch batches with resourceVersion recovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, Mapping
from urllib.parse import urlencode

from .clients import KubernetesSourceClient
from .kubernetes import DeploymentObservation
from .source import SourceClientError, SourceHTTPError, TextFetcher, fetch_text


@dataclass(frozen=True)
class KubernetesWatchCursor:
    node_resource_version: str = ""
    pod_resource_version: str = ""

    @property
    def initialized(self) -> bool:
        return bool(self.node_resource_version and self.pod_resource_version)


@dataclass(frozen=True)
class KubernetesWatchBatch:
    observations: tuple[DeploymentObservation, ...]
    cursor: KubernetesWatchCursor
    relisted: bool = False
    events: int = 0


class KubernetesResourceVersionExpired(SourceClientError):
    """The API server can no longer serve a requested resourceVersion."""


class KubernetesWatchClient:
    """Collect bounded Node/Pod watch windows and recover from HTTP-style 410 events.

    The injected text fetcher must return one finite newline-delimited JSON batch.
    Production runners should bound the server-side watch with ``timeoutSeconds``.
    """

    def __init__(
        self,
        source: KubernetesSourceClient,
        *,
        timeout_seconds: int = 30,
        fetcher: TextFetcher = fetch_text,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("watch timeout_seconds must be positive")
        self.source = source
        self.timeout_seconds = timeout_seconds
        self.fetcher = fetcher

    def collect(
        self,
        cursor: KubernetesWatchCursor | None = None,
        *,
        observed_at: datetime | None = None,
    ) -> KubernetesWatchBatch:
        current = cursor or KubernetesWatchCursor()
        if not current.initialized:
            return self.relist(observed_at=observed_at)
        try:
            node_observations, node_version, node_events = self._watch_resource(
                "nodes", current.node_resource_version, observed_at=observed_at
            )
            pod_observations, pod_version, pod_events = self._watch_resource(
                "pods", current.pod_resource_version, observed_at=observed_at
            )
        except KubernetesResourceVersionExpired:
            return self.relist(observed_at=observed_at)
        except SourceHTTPError as exc:
            if exc.status_code == 410:
                return self.relist(observed_at=observed_at)
            raise
        return KubernetesWatchBatch(
            node_observations + pod_observations,
            KubernetesWatchCursor(node_version, pod_version),
            events=node_events + pod_events,
        )

    def relist(
        self, *, observed_at: datetime | None = None
    ) -> KubernetesWatchBatch:
        node_payload, pod_payload = self.source.list_payloads()
        observations = self.source.decode(
            node_payload, pod_payload, observed_at=observed_at
        )
        return KubernetesWatchBatch(
            observations,
            KubernetesWatchCursor(
                _list_resource_version(node_payload, "NodeList"),
                _list_resource_version(pod_payload, "PodList"),
            ),
            relisted=True,
        )

    def _watch_resource(
        self,
        resource: str,
        resource_version: str,
        *,
        observed_at: datetime | None,
    ) -> tuple[tuple[DeploymentObservation, ...], str, int]:
        url = self.source.api_server + self.source.resource_path(resource)
        query = urlencode(
            {
                "watch": "true",
                "allowWatchBookmarks": "true",
                "resourceVersion": resource_version,
                "timeoutSeconds": str(self.timeout_seconds),
            }
        )
        payload = self.fetcher(
            f"{url}?{query}", self.source.headers, self.timeout_seconds + 2.0
        )
        version = resource_version
        observations: list[DeploymentObservation] = []
        events = 0
        for event in _watch_events(payload):
            events += 1
            event_type = str(event.get("type", "")).upper()
            raw_object = event.get("object")
            item = raw_object if isinstance(raw_object, Mapping) else {}
            if event_type == "ERROR":
                if _status_code(item) == 410:
                    raise KubernetesResourceVersionExpired(
                        f"Kubernetes {resource} resourceVersion expired"
                    )
                raise SourceClientError(
                    f"Kubernetes {resource} watch returned error {_status_code(item)}"
                )
            event_version = _object_resource_version(item)
            if event_version:
                version = event_version
            if event_type == "BOOKMARK":
                continue
            if event_type not in {"ADDED", "MODIFIED", "DELETED"}:
                raise SourceClientError(
                    f"unsupported Kubernetes watch event type: {event_type or '<empty>'}"
                )
            observations.extend(
                self.source.decode_resource(
                    resource,
                    item,
                    observed_at=observed_at,
                    deleted=event_type == "DELETED",
                )
            )
        return tuple(observations), version, events


def _watch_events(payload: str) -> tuple[Mapping[str, Any], ...]:
    events: list[Mapping[str, Any]] = []
    for line_number, raw_line in enumerate(payload.splitlines(), 1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SourceClientError(
                f"invalid Kubernetes watch JSON at line {line_number}"
            ) from exc
        if not isinstance(value, Mapping):
            raise SourceClientError(
                f"Kubernetes watch event at line {line_number} must be an object"
            )
        events.append(value)
    return tuple(events)


def _list_resource_version(payload: Any, kind: str) -> str:
    if not isinstance(payload, Mapping):
        raise SourceClientError(f"expected Kubernetes {kind}")
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping) or not metadata.get("resourceVersion"):
        raise SourceClientError(f"Kubernetes {kind} is missing metadata.resourceVersion")
    return str(metadata["resourceVersion"])


def _object_resource_version(value: Mapping[str, Any]) -> str:
    metadata = value.get("metadata")
    return (
        str(metadata.get("resourceVersion", ""))
        if isinstance(metadata, Mapping)
        else ""
    )


def _status_code(value: Mapping[str, Any]) -> int:
    try:
        return int(value.get("code", 0))
    except (TypeError, ValueError):
        return 0


__all__ = [
    "KubernetesResourceVersionExpired",
    "KubernetesWatchBatch",
    "KubernetesWatchClient",
    "KubernetesWatchCursor",
]
