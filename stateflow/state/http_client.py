"""Standard-library HTTP client for the State Plane southbound contract."""

from __future__ import annotations

from datetime import timedelta
import json
from typing import Any, Iterable, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .contracts import (
    ComponentDescriptor,
    GraphEntity,
    GraphKind,
    Heartbeat,
    RelationUpdate,
    StateUpdate,
    WriteResult,
)
from .schema import to_jsonable


class StatePlaneHTTPError(RuntimeError):
    def __init__(self, message: str, *, status_code: int = 0) -> None:
        super().__init__(message)
        self.status_code = status_code


class StatePlaneHTTPClient:
    """Expose the writer subset used by semantic adapters over HTTP."""

    def __init__(self, base_url: str, *, timeout: float = 3.0) -> None:
        if timeout <= 0:
            raise ValueError("State Plane HTTP timeout must be positive")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.prefix = "/v1/state-plane"

    def list_components(self) -> tuple[ComponentDescriptor, ...]:
        payload = self._request("GET", "/components")
        return tuple(
            decode_component(item)
            for item in decode_objects(payload.get("components"), "components")
        )

    def register_component(self, descriptor: ComponentDescriptor) -> None:
        self._request("POST", "/components/register", to_jsonable(descriptor))

    def upsert_entity(self, entity: GraphEntity) -> None:
        self._request("POST", "/entities/upsert", {"entities": [to_jsonable(entity)]})

    def get_entity(self, entity_ref: str) -> GraphEntity | None:
        payload = self._request(
            "POST", "/graph/query", {"roots": [entity_ref], "depth": 0}
        )
        entities = decode_objects(payload.get("entities"), "entities")
        return decode_entity(entities[0]) if entities else None

    def publish_state(self, updates: Iterable[StateUpdate]) -> list[WriteResult]:
        payload = self._request(
            "POST",
            "/state/publish",
            {"updates": [encode_state_update(item) for item in updates]},
            accepted_statuses=(200, 409),
        )
        return decode_write_results(payload)

    def upsert_relations(self, updates: Iterable[RelationUpdate]) -> list[WriteResult]:
        payload = self._request(
            "POST",
            "/relations/upsert",
            {"relations": [encode_relation_update(item) for item in updates]},
            accepted_statuses=(200, 409),
        )
        return decode_write_results(payload)

    def heartbeat(self, heartbeat: Heartbeat) -> None:
        self._request("POST", "/heartbeat", encode_heartbeat(heartbeat))

    def _request(
        self,
        method: str,
        path: str,
        body: Mapping[str, Any] | None = None,
        *,
        accepted_statuses: tuple[int, ...] = (200, 201),
    ) -> dict[str, Any]:
        raw = None if body is None else json.dumps(body).encode("utf-8")
        request = Request(
            self.base_url + self.prefix + path,
            data=raw,
            headers={"content-type": "application/json"} if raw is not None else {},
            method=method,
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                status = response.status
                payload = _json_object(response.read())
        except HTTPError as exc:
            status = exc.code
            payload = _json_object(exc.read())
        except (URLError, TimeoutError, OSError) as exc:
            reason = exc.reason if isinstance(exc, URLError) else exc
            raise StatePlaneHTTPError(
                f"State Plane request failed: {type(reason).__name__}"
            ) from exc
        if status not in accepted_statuses:
            error = payload.get("error")
            message = (
                str(error.get("message", ""))
                if isinstance(error, Mapping)
                else ""
            )
            raise StatePlaneHTTPError(
                message or f"State Plane returned HTTP {status}", status_code=status
            )
        return payload


def encode_state_update(value: StateUpdate) -> dict[str, Any]:
    result = to_jsonable(value)
    result["ttl_ms"] = _milliseconds(value.ttl)
    result.pop("ttl", None)
    return result


def encode_relation_update(value: RelationUpdate) -> dict[str, Any]:
    result = to_jsonable(value)
    result["ttl_ms"] = _milliseconds(value.ttl)
    result.pop("ttl", None)
    return result


def encode_heartbeat(value: Heartbeat) -> dict[str, Any]:
    result = to_jsonable(value)
    result["ttl_ms"] = _milliseconds(value.ttl)
    result.pop("ttl", None)
    return result


def _milliseconds(value: timedelta | None) -> int | None:
    return None if value is None else max(0, int(value.total_seconds() * 1000))


def decode_write_results(payload: Mapping[str, Any]) -> list[WriteResult]:
    return [
        WriteResult(
            accepted=bool(item.get("accepted", False)),
            duplicate=bool(item.get("duplicate", False)),
            conflict=bool(item.get("conflict", False)),
            entity_ref=str(item.get("entity_ref", "")),
            key=str(item.get("key", "")),
            version=int(item.get("version", 0)),
            reason=str(item.get("reason", "")),
        )
        for item in decode_objects(payload.get("results"), "results")
    ]


def decode_component(value: Mapping[str, Any]) -> ComponentDescriptor:
    return ComponentDescriptor(
        component_id=str(value.get("component_id", "")),
        kind=str(value.get("kind", "")),
        implementation=str(value.get("implementation", "")),
        version=str(value.get("version", "")),
        schema_versions=_strings(value.get("schema_versions")),
        produces_state=_strings(value.get("produces_state")),
        consumes_state=_strings(value.get("consumes_state")),
        produces_metrics=_strings(value.get("produces_metrics")),
        consumes_metrics=_strings(value.get("consumes_metrics")),
        manages_types=_strings(value.get("manages_types")),
        relation_capabilities=_strings(value.get("relation_capabilities")),
        update_mode=str(value.get("update_mode", "event")),
        endpoint=str(value.get("endpoint", "")),
    )


def decode_entity(value: Mapping[str, Any]) -> GraphEntity:
    labels = value.get("labels")
    return GraphEntity(
        ref=str(value.get("ref", "")),
        graph=GraphKind(str(value.get("graph", ""))),
        entity_type=str(value.get("entity_type", "")),
        lifecycle=str(value.get("lifecycle", "")),
        owner_component_ref=str(value.get("owner_component_ref", "")),
        parent_ref=str(value.get("parent_ref", "")),
        labels={str(key): str(item) for key, item in labels.items()}
        if isinstance(labels, Mapping)
        else {},
        schema_version=str(value.get("schema_version", "1.1")),
    )


def decode_objects(value: Any, name: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list) or not all(
        isinstance(item, Mapping) for item in value
    ):
        raise StatePlaneHTTPError(f"State Plane {name} response is invalid")
    return tuple(value)


def _strings(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in value) if isinstance(value, list) else ()


def _json_object(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StatePlaneHTTPError("State Plane returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise StatePlaneHTTPError("State Plane response must be an object")
    return value


__all__ = [
    "StatePlaneHTTPClient",
    "StatePlaneHTTPError",
    "decode_component",
    "decode_entity",
    "decode_objects",
    "decode_write_results",
    "encode_heartbeat",
    "encode_relation_update",
    "encode_state_update",
]
