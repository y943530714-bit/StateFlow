"""Dependency-free HTTP-facing facade for the canonical State Plane."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from .contracts import (
    ComponentDescriptor,
    CorrelationIdentity,
    GraphEntity,
    GraphKind,
    Heartbeat,
    MetricSample,
    RelationUpdate,
    SnapshotRequest,
    SourceAuthority,
    SourceRef,
    StateSemantic,
    StateEvent,
    StateUpdate,
)
from .plane import InMemoryStatePlane
from .schema import to_jsonable


@dataclass(frozen=True)
class StatePlaneAPIResponse:
    status_code: int
    payload: dict[str, Any]


class StatePlaneAPI:
    """Map stable Southbound/Northbound operations to an in-process store."""

    prefix = "/v1/state-plane"

    def __init__(self, plane: InMemoryStatePlane) -> None:
        self.plane = plane

    def handles(self, path: str) -> bool:
        return path == self.prefix or path.startswith(self.prefix + "/")

    def get(self, path: str, query: Mapping[str, list[str]]) -> StatePlaneAPIResponse:
        relative = self._relative(path)
        if relative == "/components":
            return self._ok({"components": to_jsonable(self.plane.list_components())})
        if relative == "/heartbeats":
            return self._ok({"heartbeats": to_jsonable(self.plane.heartbeats())})
        if relative == "/schema":
            key = _first(query, "key")
            if key:
                return self._ok({"key": to_jsonable(self.plane.registry.resolve(key))})
            return self._ok({"keys": to_jsonable(self.plane.registry.list())})
        if relative == "/state":
            entity_ref = _required_query(query, "entity_ref")
            keys = _csv_query(query, "keys")
            if not keys:
                raise ValueError("keys query parameter is required")
            snapshot = _first(query, "snapshot_token")
            max_age_ms = _optional_int(query, "max_age_ms")
            authority = _authority(
                _first(query, "min_authority") or SourceAuthority.LOG_HINT
            )
            values = self.plane.get_state(
                entity_ref,
                keys,
                max_age_ms={"": max_age_ms} if max_age_ms is not None else None,
                min_authority=authority,
                snapshot=snapshot,
            )
            return self._ok({"values": to_jsonable(values), "snapshot_token": snapshot})
        if relative.startswith("/snapshots/"):
            token = relative.removeprefix("/snapshots/")
            if not token:
                raise ValueError("snapshot token is required")
            return self._ok(self.plane.read_snapshot(token).to_dict())
        if relative == "/changes":
            changes = self.plane.changes(
                after_cursor=_optional_int(query, "after_cursor") or 0,
                entity_prefix=_first(query, "entity_prefix"),
                keys=_csv_query(query, "keys"),
                limit=_optional_int(query, "limit") or 100,
            )
            next_cursor = changes[-1].cursor if changes else (_optional_int(query, "after_cursor") or 0)
            return self._ok({"changes": to_jsonable(changes), "next_cursor": next_cursor})
        if relative == "/metrics":
            samples = self.plane.query_metrics(
                entity_refs=_csv_query(query, "entity_refs") or _csv_query(query, "entity_ref"),
                keys=_csv_query(query, "keys"),
                start=_optional_datetime(query, "start"),
                end=_optional_datetime(query, "end"),
            )
            aggregation = (_first(query, "agg") or "raw").lower()
            return self._ok(_metric_response(samples, aggregation))
        if relative in {"", "/health", "/freshness"}:
            payload = dict(self.plane.freshness())
            payload["status"] = "ok"
            return self._ok(payload)
        raise KeyError(f"unknown State Plane endpoint: {path}")

    def post(self, path: str, body: Mapping[str, Any]) -> StatePlaneAPIResponse:
        relative = self._relative(path)
        if relative == "/components/register":
            descriptor = _component_descriptor(body)
            self.plane.register_component(descriptor)
            return self._created({"component": to_jsonable(descriptor)})
        if relative == "/entities/upsert":
            entities = tuple(_graph_entity(item) for item in _batch(body, "entities"))
            for entity in entities:
                self.plane.upsert_entity(entity)
            return self._ok({"accepted": len(entities), "entities": to_jsonable(entities)})
        if relative == "/state/publish":
            updates = tuple(_state_update(item) for item in _batch(body, "updates"))
            results = self.plane.publish_state(updates)
            return self._write_response(results)
        if relative == "/state/batch-get":
            request = _snapshot_request(body)
            return self._ok(self.plane.get_snapshot(request).to_dict())
        if relative == "/relations/upsert":
            updates = tuple(_relation_update(item) for item in _batch(body, "relations"))
            results = self.plane.upsert_relations(updates)
            return self._write_response(results)
        if relative == "/metrics/publish":
            samples = tuple(_metric_sample(item) for item in _batch(body, "samples"))
            return self._write_response(self.plane.publish_metrics(samples))
        if relative == "/events/publish":
            events = tuple(_state_event(item) for item in _batch(body, "events"))
            return self._write_response(self.plane.publish_events(events))
        if relative == "/heartbeat":
            heartbeat = _heartbeat(body)
            self.plane.heartbeat(heartbeat)
            return self._ok({"heartbeat": to_jsonable(heartbeat)})
        if relative == "/snapshots":
            request = _snapshot_request(body)
            return self._created(self.plane.get_snapshot(request).to_dict())
        if relative == "/graph/query":
            roots = tuple(str(value) for value in body.get("roots", ()) or ())
            if not roots:
                raise ValueError("roots must contain at least one entity ref")
            result = self.plane.query_graph(
                roots,
                relation_types=tuple(str(value) for value in body.get("relation_types", ()) or ()),
                depth=int(body.get("depth", 1)),
                snapshot=str(body.get("snapshot_token", "")),
            )
            return self._ok(
                {
                    "snapshot_token": result.snapshot_token,
                    "entities": to_jsonable(result.entities),
                    "relations": to_jsonable(result.relations),
                }
            )
        raise KeyError(f"unknown State Plane endpoint: {path}")

    def _relative(self, path: str) -> str:
        if not self.handles(path):
            raise KeyError(f"not a State Plane path: {path}")
        return path[len(self.prefix) :] or ""

    @staticmethod
    def _ok(payload: dict[str, Any]) -> StatePlaneAPIResponse:
        return StatePlaneAPIResponse(200, payload)

    @staticmethod
    def _created(payload: dict[str, Any]) -> StatePlaneAPIResponse:
        return StatePlaneAPIResponse(201, payload)

    @staticmethod
    def _write_response(results: list[Any]) -> StatePlaneAPIResponse:
        accepted = sum(1 for result in results if result.accepted)
        payload = {
            "accepted": accepted,
            "rejected": len(results) - accepted,
            "results": to_jsonable(results),
        }
        return StatePlaneAPIResponse(200 if accepted == len(results) else 409, payload)


def _batch(body: Mapping[str, Any], key: str) -> tuple[Mapping[str, Any], ...]:
    raw = body.get(key)
    if raw is None:
        raw = (body,)
    if not isinstance(raw, (list, tuple)):
        raise ValueError(f"{key} must be a list")
    if not all(isinstance(item, Mapping) for item in raw):
        raise ValueError(f"every {key} item must be an object")
    if not raw:
        raise ValueError(f"{key} must not be empty")
    return tuple(raw)


def _component_descriptor(value: Mapping[str, Any]) -> ComponentDescriptor:
    component_id = str(value.get("component_id", ""))
    kind = str(value.get("kind", ""))
    if not component_id or not kind:
        raise ValueError("component_id and kind are required")
    return ComponentDescriptor(
        component_id=component_id,
        kind=kind,
        implementation=str(value.get("implementation", "")),
        version=str(value.get("version", "")),
        schema_versions=_strings(value.get("schema_versions", ("1.1",))),
        produces_state=_strings(value.get("produces_state", ())),
        consumes_state=_strings(value.get("consumes_state", ())),
        produces_metrics=_strings(value.get("produces_metrics", ())),
        consumes_metrics=_strings(value.get("consumes_metrics", ())),
        manages_types=_strings(value.get("manages_types", ())),
        relation_capabilities=_strings(value.get("relation_capabilities", ())),
        update_mode=str(value.get("update_mode", "event")),
        endpoint=str(value.get("endpoint", "")),
    )


def _graph_entity(value: Mapping[str, Any]) -> GraphEntity:
    return GraphEntity(
        ref=_required(value, "ref"),
        graph=GraphKind(_required(value, "graph")),
        entity_type=_required(value, "entity_type"),
        lifecycle=str(value.get("lifecycle", "")),
        owner_component_ref=str(value.get("owner_component_ref", "")),
        parent_ref=str(value.get("parent_ref", "")),
        labels=_string_map(value.get("labels"), "labels"),
        schema_version=str(value.get("schema_version", "1.1")),
    )


def _state_update(value: Mapping[str, Any]) -> StateUpdate:
    source = value.get("source_ref")
    correlation = value.get("correlation") or {}
    return StateUpdate(
        entity_ref=_required(value, "entity_ref"),
        key=_required(value, "key"),
        value=value.get("value"),
        producer=_required(value, "producer"),
        timestamp=_datetime(value.get("timestamp")),
        ttl=_duration_ms(value.get("ttl_ms")),
        authority=_authority(value.get("authority", SourceAuthority.DIRECT_TELEMETRY)),
        version=int(value.get("version", 0)),
        semantic=StateSemantic(value.get("semantic", StateSemantic.OBSERVED)),
        confidence=float(value.get("confidence", 1.0)),
        source_ref=_source_ref(source),
        correlation=CorrelationIdentity(
            **{key: str(item) for key, item in correlation.items() if key in CorrelationIdentity.__dataclass_fields__}
        ),
        expected_version=_int_or_none(value.get("expected_version")),
        idempotency_key=str(value.get("idempotency_key", "")),
    )


def _relation_update(value: Mapping[str, Any]) -> RelationUpdate:
    return RelationUpdate(
        source_ref=_required(value, "source_ref"),
        relation_type=_required(value, "relation_type"),
        target_ref=_required(value, "target_ref"),
        producer=_required(value, "producer"),
        timestamp=_datetime(value.get("timestamp")),
        authority=_authority(value.get("authority", SourceAuthority.STRUCTURED_LIFECYCLE)),
        attributes=_mapping(value.get("attributes"), "attributes"),
        ttl=_duration_ms(value.get("ttl_ms")),
        version=int(value.get("version", 0)),
        expected_version=_int_or_none(value.get("expected_version")),
        idempotency_key=str(value.get("idempotency_key", "")),
    )


def _metric_sample(value: Mapping[str, Any]) -> MetricSample:
    correlation = value.get("correlation") or {}
    return MetricSample(
        entity_ref=_required(value, "entity_ref"),
        metric_key=_required(value, "metric_key"),
        value=_number(value, "value"),
        unit=str(value.get("unit", "")),
        producer=_required(value, "producer"),
        timestamp=_datetime(value.get("timestamp")),
        labels=_string_map(value.get("labels"), "labels"),
        correlation=CorrelationIdentity(
            **{key: str(item) for key, item in correlation.items() if key in CorrelationIdentity.__dataclass_fields__}
        ),
    )


def _state_event(value: Mapping[str, Any]) -> StateEvent:
    correlation = value.get("correlation") or {}
    source = value.get("source_ref")
    return StateEvent(
        event_id=_required(value, "event_id"),
        event_type=_required(value, "event_type"),
        subject_ref=_required(value, "subject_ref"),
        producer=_required(value, "producer"),
        timestamp=_datetime(value.get("timestamp")),
        correlation=CorrelationIdentity(
            **{key: str(item) for key, item in correlation.items() if key in CorrelationIdentity.__dataclass_fields__}
        ),
        payload=_mapping(value.get("payload"), "payload"),
        source_ref=_source_ref(source),
    )


def _heartbeat(value: Mapping[str, Any]) -> Heartbeat:
    return Heartbeat(
        subject_ref=_required(value, "subject_ref"),
        producer=_required(value, "producer"),
        source_watermark=str(value.get("source_watermark", "")),
        schema_version=str(value.get("schema_version", "1.1")),
        timestamp=_datetime(value.get("timestamp")),
        ttl=_duration_ms(value.get("ttl_ms")) or timedelta(seconds=10),
    )


def _snapshot_request(value: Mapping[str, Any]) -> SnapshotRequest:
    entities = _strings(value.get("entities", ()))
    keys = _strings(value.get("keys", ()))
    if not entities or not keys:
        raise ValueError("entities and keys must both be non-empty")
    return SnapshotRequest(
        entities=entities,
        keys=keys,
        max_age_ms={str(key): int(item) for key, item in _mapping(value.get("max_age_ms"), "max_age_ms").items()},
        min_authority=_authority(value.get("min_authority", SourceAuthority.LOG_HINT)),
        include_relations=bool(value.get("include_relations", True)),
    )


def _required(value: Mapping[str, Any], key: str) -> str:
    result = str(value.get(key, ""))
    if not result:
        raise ValueError(f"{key} is required")
    return result


def _number(value: Mapping[str, Any], key: str) -> float:
    if key not in value or isinstance(value[key], bool):
        raise ValueError(f"{key} must be numeric")
    try:
        return float(value[key])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be numeric") from exc


def _authority(value: Any) -> SourceAuthority:
    try:
        return SourceAuthority.parse(value)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"invalid source authority: {value}") from exc


def _source_ref(value: Any) -> SourceRef | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("source_ref must be an object")
    backend = str(value.get("backend", ""))
    reference = str(value.get("reference", ""))
    if not backend or not reference:
        raise ValueError("source_ref.backend and source_ref.reference are required")
    return SourceRef(
        backend=backend,
        reference=reference,
        trace_id=str(value.get("trace_id", "")),
        span_id=str(value.get("span_id", "")),
    )


def _strings(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(item.strip() for item in value.split(",") if item.strip())
    if not isinstance(value, (list, tuple, set, frozenset)):
        raise ValueError("expected a string or list of strings")
    return tuple(str(item) for item in value)


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    return dict(value)


def _string_map(value: Any, name: str) -> dict[str, str]:
    return {str(key): str(item) for key, item in _mapping(value, name).items()}


def _datetime(value: Any) -> datetime:
    if value is None or value == "":
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _duration_ms(value: Any) -> timedelta | None:
    return None if value is None else timedelta(milliseconds=max(0, int(value)))


def _int_or_none(value: Any) -> int | None:
    return None if value is None else int(value)


def _first(query: Mapping[str, list[str]], key: str) -> str:
    values = query.get(key, ())
    return str(values[0]) if values else ""


def _required_query(query: Mapping[str, list[str]], key: str) -> str:
    value = _first(query, key)
    if not value:
        raise ValueError(f"{key} query parameter is required")
    return value


def _csv_query(query: Mapping[str, list[str]], key: str) -> tuple[str, ...]:
    result: list[str] = []
    for value in query.get(key, ()):
        result.extend(item.strip() for item in value.split(",") if item.strip())
    return tuple(result)


def _optional_int(query: Mapping[str, list[str]], key: str) -> int | None:
    value = _first(query, key)
    return int(value) if value else None


def _optional_datetime(query: Mapping[str, list[str]], key: str) -> datetime | None:
    value = _first(query, key)
    return _datetime(value) if value else None


def _metric_response(samples: tuple[MetricSample, ...], aggregation: str) -> dict[str, Any]:
    if aggregation == "raw":
        return {"aggregation": "raw", "samples": to_jsonable(samples)}
    supported = {"avg", "max", "min", "sum", "count", "last", "p95"}
    if aggregation not in supported:
        raise ValueError(f"unsupported metric aggregation: {aggregation}")
    grouped: dict[tuple[str, str, str], list[MetricSample]] = {}
    for sample in samples:
        grouped.setdefault((sample.entity_ref, sample.metric_key, sample.unit), []).append(sample)
    values: list[dict[str, Any]] = []
    for (entity_ref, key, unit), items in sorted(grouped.items()):
        numbers = sorted(item.value for item in items)
        if aggregation == "avg":
            value: float | int = sum(numbers) / len(numbers)
        elif aggregation == "max":
            value = max(numbers)
        elif aggregation == "min":
            value = min(numbers)
        elif aggregation == "sum":
            value = sum(numbers)
        elif aggregation == "count":
            value = len(numbers)
        elif aggregation == "last":
            value = max(items, key=lambda item: item.timestamp).value
        else:
            index = max(0, min(len(numbers) - 1, (95 * len(numbers) + 99) // 100 - 1))
            value = numbers[index]
        values.append(
            {
                "entity_ref": entity_ref,
                "metric_key": key,
                "unit": unit,
                "aggregation": aggregation,
                "value": value,
                "sample_count": len(items),
            }
        )
    return {"aggregation": aggregation, "values": values}


__all__ = ["StatePlaneAPI", "StatePlaneAPIResponse"]
