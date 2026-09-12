"""Thread-safe reference implementation of the v1.1 read-only State Plane."""

from __future__ import annotations

from collections import OrderedDict, deque
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from fnmatch import fnmatchcase
import threading
from typing import Callable, Iterable, Mapping

from .contracts import (
    ComponentDescriptor,
    GraphEntity,
    GraphKind,
    GraphQueryResult,
    GraphRelation,
    Heartbeat,
    MetricSample,
    RelationUpdate,
    Snapshot,
    SnapshotRequest,
    SourceAuthority,
    StateChange,
    StateEvent,
    StateUpdate,
    StateValue,
    WriteResult,
    normalize_confidence,
    snapshot_token,
)
from .registry import CanonicalKeyRegistry, UnknownCanonicalKey, default_key_registry
from .schema import utcnow


StatePlaneCallback = Callable[[StateChange], None]

class InMemoryStatePlane:
    """Canonical hot state, relation indexes, snapshots, and change cursors.

    The store is deliberately small but preserves the contracts needed to
    replace it with a durable/distributed backend: source authority, TTL,
    optimistic CAS, idempotency, immutable snapshot tokens, and resumable
    change cursors.
    """

    def __init__(
        self,
        registry: CanonicalKeyRegistry | None = None,
        *,
        max_snapshots: int = 128,
        max_changes: int = 10_000,
        max_metrics: int = 100_000,
        max_events: int = 100_000,
    ) -> None:
        self.registry = registry or default_key_registry()
        self._components: dict[str, ComponentDescriptor] = {}
        self._entities: dict[str, GraphEntity] = {}
        self._state: dict[tuple[str, str], StateValue] = {}
        self._relations: dict[tuple[str, str, str], GraphRelation] = {}
        self._metrics: deque[MetricSample] = deque(maxlen=max(1, max_metrics))
        self._events: OrderedDict[str, StateEvent] = OrderedDict()
        self._heartbeats: dict[str, Heartbeat] = {}
        self._idempotency: dict[str, WriteResult] = {}
        self._snapshots: OrderedDict[str, Snapshot] = OrderedDict()
        self._changes: deque[StateChange] = deque(maxlen=max_changes)
        self._subscribers: list[StatePlaneCallback] = []
        self._revision = 0
        self._max_snapshots = max(1, max_snapshots)
        self._max_events = max(1, max_events)
        self._lock = threading.RLock()

    @property
    def revision(self) -> int:
        with self._lock:
            return self._revision

    def register_component(self, descriptor: ComponentDescriptor) -> None:
        with self._lock:
            existing = self._components.get(descriptor.component_id)
            if existing is not None and existing != descriptor:
                raise ValueError(f"component already registered with different descriptor: {descriptor.component_id}")
            self._components[descriptor.component_id] = descriptor

    def list_components(self) -> tuple[ComponentDescriptor, ...]:
        with self._lock:
            return tuple(sorted(deepcopy(list(self._components.values())), key=lambda item: item.component_id))

    def upsert_entity(self, entity: GraphEntity) -> None:
        callbacks: list[StatePlaneCallback]
        with self._lock:
            previous = self._entities.get(entity.ref)
            if previous is not None and previous.graph != entity.graph:
                raise ValueError(f"entity graph cannot change: {entity.ref}")
            if previous == entity:
                return
            self._entities[entity.ref] = deepcopy(entity)
            change = self._record_change("entity", entity.ref, entity.entity_type, 1)
            callbacks = list(self._subscribers)
        self._notify(callbacks, change)

    def get_entity(self, entity_ref: str) -> GraphEntity | None:
        with self._lock:
            value = self._entities.get(entity_ref)
            return deepcopy(value) if value is not None else None

    def publish_state(self, updates: Iterable[StateUpdate]) -> list[WriteResult]:
        results: list[WriteResult] = []
        notifications: list[StateChange] = []
        with self._lock:
            for update in updates:
                result, change = self._publish_one(update)
                results.append(result)
                if change is not None:
                    notifications.append(change)
            callbacks = list(self._subscribers)
        for change in notifications:
            self._notify(callbacks, change)
        return results

    def _publish_one(self, update: StateUpdate) -> tuple[WriteResult, StateChange | None]:
        if update.idempotency_key and update.idempotency_key in self._idempotency:
            previous = self._idempotency[update.idempotency_key]
            return replace(previous, accepted=False, duplicate=True, reason="duplicate idempotency key"), None
        entity = self._entities.get(update.entity_ref)
        if entity is None:
            return self._rejected(update, "entity is not registered"), None
        try:
            definition = self.registry.resolve(update.key)
        except UnknownCanonicalKey:
            return self._rejected(update, "canonical key is not registered"), None
        if entity.entity_type not in definition.entity_types:
            return self._rejected(update, f"key is invalid for entity type {entity.entity_type}"), None
        if not self.registry.validate_value(definition, update.value):
            return self._rejected(update, f"value does not match declared type {definition.value_type}"), None

        key = (update.entity_ref, definition.key)
        current = self._state.get(key)
        if update.expected_version is not None:
            actual = current.version if current is not None else 0
            if actual != update.expected_version:
                return self._rejected(update, f"CAS conflict: expected {update.expected_version}, got {actual}", conflict=True), None
        authority = SourceAuthority.parse(update.authority)
        timestamp = _aware(update.timestamp)
        if current is not None:
            if authority < current.authority and current.is_fresh(timestamp):
                return self._rejected(update, "lower-authority source cannot replace fresh state", conflict=True), None
            if authority == current.authority and timestamp < _aware(current.timestamp):
                return self._rejected(update, "out-of-order state update", conflict=True), None
            if update.version and update.version <= current.version:
                return self._rejected(update, "state version must increase", conflict=True), None

        version = update.version or ((current.version + 1) if current is not None else 1)
        value = StateValue(
            entity_ref=update.entity_ref,
            key=definition.key,
            value=deepcopy(update.value),
            producer=update.producer,
            timestamp=timestamp,
            ttl=update.ttl if update.ttl is not None else definition.ttl,
            authority=authority,
            version=version,
            semantic=update.semantic,
            confidence=normalize_confidence(update.confidence),
            source_ref=deepcopy(update.source_ref),
            correlation=deepcopy(update.correlation),
        )
        self._state[key] = value
        change = self._record_change("state", update.entity_ref, definition.key, version, timestamp)
        result = WriteResult(True, entity_ref=update.entity_ref, key=definition.key, version=version, reason="accepted")
        if update.idempotency_key:
            self._idempotency[update.idempotency_key] = result
        return result, change

    def upsert_relations(self, updates: Iterable[RelationUpdate]) -> list[WriteResult]:
        results: list[WriteResult] = []
        notifications: list[StateChange] = []
        with self._lock:
            for update in updates:
                result, change = self._upsert_relation(update)
                results.append(result)
                if change is not None:
                    notifications.append(change)
            callbacks = list(self._subscribers)
        for change in notifications:
            self._notify(callbacks, change)
        return results

    def publish_metrics(self, samples: Iterable[MetricSample]) -> list[WriteResult]:
        results: list[WriteResult] = []
        with self._lock:
            for sample in samples:
                entity = self._entities.get(sample.entity_ref)
                if entity is None:
                    results.append(
                        WriteResult(False, entity_ref=sample.entity_ref, key=sample.metric_key, reason="entity is not registered")
                    )
                    continue
                try:
                    definition = self.registry.resolve(sample.metric_key)
                except UnknownCanonicalKey:
                    results.append(
                        WriteResult(False, entity_ref=sample.entity_ref, key=sample.metric_key, reason="canonical key is not registered")
                    )
                    continue
                if entity.entity_type not in definition.entity_types:
                    results.append(
                        WriteResult(False, entity_ref=sample.entity_ref, key=definition.key, reason="metric is invalid for entity type")
                    )
                    continue
                if not isinstance(sample.value, (int, float)) or isinstance(sample.value, bool):
                    results.append(
                        WriteResult(False, entity_ref=sample.entity_ref, key=definition.key, reason="metric value must be numeric")
                    )
                    continue
                normalized = replace(
                    sample,
                    metric_key=definition.key,
                    value=float(sample.value),
                    timestamp=_aware(sample.timestamp),
                )
                self._metrics.append(deepcopy(normalized))
                results.append(
                    WriteResult(True, entity_ref=sample.entity_ref, key=definition.key, version=self._revision, reason="accepted")
                )
        return results

    def query_metrics(
        self,
        *,
        entity_refs: Iterable[str] = (),
        keys: Iterable[str] = (),
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[MetricSample, ...]:
        entities = set(entity_refs)
        patterns = tuple(keys)
        start_at = _aware(start) if start is not None else None
        end_at = _aware(end) if end is not None else None
        with self._lock:
            return tuple(
                deepcopy(sample)
                for sample in self._metrics
                if (not entities or sample.entity_ref in entities)
                and (not patterns or _matches(sample.metric_key, patterns))
                and (start_at is None or _aware(sample.timestamp) >= start_at)
                and (end_at is None or _aware(sample.timestamp) <= end_at)
            )

    def publish_events(self, events: Iterable[StateEvent]) -> list[WriteResult]:
        results: list[WriteResult] = []
        notifications: list[StateChange] = []
        with self._lock:
            for event in events:
                if event.event_id in self._events:
                    results.append(
                        WriteResult(False, duplicate=True, entity_ref=event.subject_ref, key=event.event_type, reason="duplicate event_id")
                    )
                    continue
                if event.subject_ref not in self._entities:
                    results.append(
                        WriteResult(False, entity_ref=event.subject_ref, key=event.event_type, reason="subject entity is not registered")
                    )
                    continue
                normalized = replace(event, timestamp=_aware(event.timestamp))
                self._events[event.event_id] = deepcopy(normalized)
                while len(self._events) > self._max_events:
                    self._events.popitem(last=False)
                change = self._record_change("event", event.subject_ref, event.event_type, 1, normalized.timestamp)
                notifications.append(change)
                results.append(
                    WriteResult(True, entity_ref=event.subject_ref, key=event.event_type, version=change.cursor, reason="accepted")
                )
            callbacks = list(self._subscribers)
        for change in notifications:
            self._notify(callbacks, change)
        return results

    def heartbeat(self, heartbeat: Heartbeat) -> None:
        with self._lock:
            if heartbeat.subject_ref not in self._entities and heartbeat.subject_ref not in self._components:
                raise KeyError(f"heartbeat subject is not registered: {heartbeat.subject_ref}")
            self._heartbeats[heartbeat.subject_ref] = replace(heartbeat, timestamp=_aware(heartbeat.timestamp))

    def heartbeats(self) -> tuple[Heartbeat, ...]:
        with self._lock:
            return tuple(deepcopy(self._heartbeats[key]) for key in sorted(self._heartbeats))

    def _upsert_relation(self, update: RelationUpdate) -> tuple[WriteResult, StateChange | None]:
        if update.idempotency_key and update.idempotency_key in self._idempotency:
            previous = self._idempotency[update.idempotency_key]
            return replace(previous, accepted=False, duplicate=True, reason="duplicate idempotency key"), None
        source = self._entities.get(update.source_ref)
        target = self._entities.get(update.target_ref)
        if source is None or target is None:
            return self._relation_rejected(update, "relation endpoints must be registered"), None
        if not self._relation_allowed(source, target, update.relation_type):
            return self._relation_rejected(update, "relation type is not allowed for these graphs"), None

        key = (update.source_ref, update.relation_type, update.target_ref)
        current = self._relations.get(key)
        if update.expected_version is not None:
            actual = current.version if current is not None else 0
            if actual != update.expected_version:
                return self._relation_rejected(update, f"CAS conflict: expected {update.expected_version}, got {actual}", True), None
        authority = SourceAuthority.parse(update.authority)
        timestamp = _aware(update.timestamp)
        if current is not None:
            if authority < current.authority and current.is_fresh(timestamp):
                return self._relation_rejected(update, "lower-authority source cannot replace fresh relation", True), None
            if authority == current.authority and timestamp < _aware(current.timestamp):
                return self._relation_rejected(update, "out-of-order relation update", True), None
            if update.version and update.version <= current.version:
                return self._relation_rejected(update, "relation version must increase", True), None
        version = update.version or ((current.version + 1) if current is not None else 1)
        relation = GraphRelation(
            update.source_ref,
            update.relation_type,
            update.target_ref,
            update.producer,
            timestamp,
            authority,
            deepcopy(update.attributes),
            update.ttl,
            version,
        )
        self._relations[key] = relation
        change = self._record_change("relation", update.source_ref, update.relation_type, version, timestamp)
        result = WriteResult(True, entity_ref=update.source_ref, key=update.relation_type, version=version, reason="accepted")
        if update.idempotency_key:
            self._idempotency[update.idempotency_key] = result
        return result, change

    @staticmethod
    def _relation_allowed(source: GraphEntity, target: GraphEntity, relation_type: str) -> bool:
        if source.graph == target.graph == GraphKind.COMPONENT:
            if relation_type == "manages":
                return source.entity_type == "component"
            if relation_type in {"parent_of", "depends_on"}:
                return source.entity_type in {"agent", "session", "task", "request", "tool_call"}
            if relation_type in {"serves", "executes"}:
                return source.entity_type == "component" and target.entity_type in {"request", "tool_call"}
            if relation_type in {"owns", "uses"}:
                return target.entity_type == "stateful_object"
            if relation_type == "produces_state":
                return source.entity_type == "component" and target.entity_type == "state_key_catalog"
            if relation_type == "executing_on":
                return source.entity_type in {"request", "tool_call"} and target.entity_type == "component"
            return False
        if source.graph == target.graph == GraphKind.DEPLOYMENT:
            if relation_type == "contains":
                return source.entity_type in {"cluster", "node"} and target.entity_type in {
                    "node",
                    "instance",
                    "resource",
                }
            if relation_type == "allocated_to":
                return source.entity_type == "instance" and target.entity_type == "resource"
            if relation_type == "connected_to":
                return source.entity_type in {"node", "resource"} and target.entity_type in {
                    "node",
                    "resource",
                }
            return False
        if source.graph != GraphKind.COMPONENT or target.graph != GraphKind.DEPLOYMENT:
            return False
        if relation_type == "deployed_on":
            return source.entity_type == "component" and target.entity_type in {"instance", "node"}
        if relation_type == "executing_on":
            return source.entity_type in {"request", "tool_call"} and target.entity_type == "instance"
        if relation_type == "located_on":
            return source.entity_type == "stateful_object" and target.entity_type in {"resource", "node"}
        return False

    def get_state(
        self,
        entity_ref: str,
        keys: Iterable[str],
        *,
        max_age_ms: Mapping[str, int] | None = None,
        min_authority: SourceAuthority = SourceAuthority.LOG_HINT,
        snapshot: str = "",
    ) -> tuple[StateValue, ...]:
        if snapshot:
            frozen = self.read_snapshot(snapshot)
            requested = tuple(self._canonical_pattern(key) for key in keys)
            return tuple(
                deepcopy(value)
                for value in frozen.values
                if value.entity_ref == entity_ref and _matches(value.key, requested)
            )
        result = self.get_snapshot(
            SnapshotRequest((entity_ref,), tuple(keys), max_age_ms or {}, min_authority, False)
        )
        return result.values

    def get_snapshot(self, request: SnapshotRequest) -> Snapshot:
        now = utcnow()
        with self._lock:
            requested_entities = tuple(dict.fromkeys(request.entities))
            requested_keys = tuple(dict.fromkeys(self._canonical_pattern(key) for key in request.keys))
            selected: list[StateValue] = []
            stale: list[str] = []
            missing: list[str] = []
            for entity_ref in requested_entities:
                entity = self._entities.get(entity_ref)
                if entity is None:
                    missing.append(entity_ref)
                    continue
                for key_pattern in requested_keys:
                    applicable = self._applicable_keys(entity.entity_type, key_pattern)
                    if applicable is None:
                        missing.append(f"{entity_ref}:{key_pattern}")
                        continue
                    for canonical_key in applicable:
                        identity = f"{entity_ref}:{canonical_key}"
                        value = self._state.get((entity_ref, canonical_key))
                        if value is None:
                            missing.append(identity)
                            continue
                        max_age = self._max_age_for(value.key, request.max_age_ms)
                        if value.authority < SourceAuthority.parse(request.min_authority):
                            missing.append(identity)
                        elif not value.is_fresh(now, max_age):
                            stale.append(identity)
                        else:
                            selected.append(deepcopy(value))

            selected = list({(item.entity_ref, item.key): item for item in selected}.values())
            relations: tuple[GraphRelation, ...] = ()
            if request.include_relations:
                scope = set(requested_entities)
                relations = tuple(
                    deepcopy(relation)
                    for relation in self._relations.values()
                    if relation.is_fresh(now)
                    and (relation.source_ref in scope or relation.target_ref in scope)
                )
            expected = len(selected) + len(set(missing)) + len(set(stale))
            completeness = len(selected) / expected if expected else 1.0
            token = snapshot_token(self._revision)
            result = Snapshot(
                token=token,
                logical_time=self._revision,
                created_at=now,
                completeness=completeness,
                values=tuple(sorted(selected, key=lambda item: (item.entity_ref, item.key))),
                relations=tuple(sorted(relations, key=lambda item: (item.source_ref, item.relation_type, item.target_ref))),
                missing=tuple(sorted(set(missing))),
                stale=tuple(sorted(set(stale))),
            )
            self._snapshots[token] = deepcopy(result)
            self._snapshots.move_to_end(token)
            while len(self._snapshots) > self._max_snapshots:
                self._snapshots.popitem(last=False)
            return deepcopy(result)

    def read_snapshot(self, token: str) -> Snapshot:
        with self._lock:
            try:
                return deepcopy(self._snapshots[token])
            except KeyError as exc:
                raise KeyError(f"unknown or expired snapshot token: {token}") from exc

    def query_graph(
        self,
        roots: Iterable[str],
        *,
        relation_types: Iterable[str] = (),
        depth: int = 1,
        snapshot: str = "",
    ) -> GraphQueryResult:
        with self._lock:
            relation_source = self.read_snapshot(snapshot).relations if snapshot else tuple(self._relations.values())
            allowed = set(relation_types)
            visited = set(roots)
            frontier = set(roots)
            included: dict[tuple[str, str, str], GraphRelation] = {}
            for _ in range(max(0, depth)):
                next_frontier: set[str] = set()
                for relation in relation_source:
                    if allowed and relation.relation_type not in allowed:
                        continue
                    if relation.source_ref in frontier or relation.target_ref in frontier:
                        key = (relation.source_ref, relation.relation_type, relation.target_ref)
                        included[key] = relation
                        next_frontier.update((relation.source_ref, relation.target_ref))
                next_frontier -= visited
                if not next_frontier:
                    break
                visited.update(next_frontier)
                frontier = next_frontier
            entities = tuple(
                deepcopy(self._entities[ref]) for ref in sorted(visited) if ref in self._entities
            )
            relations = tuple(deepcopy(included[key]) for key in sorted(included))
            return GraphQueryResult(snapshot, entities, relations)

    def subscribe(self, callback: StatePlaneCallback) -> None:
        with self._lock:
            self._subscribers.append(callback)

    def changes(
        self,
        *,
        after_cursor: int = 0,
        entity_prefix: str = "",
        keys: Iterable[str] = (),
        limit: int = 100,
    ) -> tuple[StateChange, ...]:
        patterns = tuple(keys)
        with self._lock:
            values = [
                deepcopy(item)
                for item in self._changes
                if item.cursor > after_cursor
                and (not entity_prefix or item.subject_ref.startswith(entity_prefix))
                and (not patterns or _matches(item.key_or_relation, patterns))
            ]
            return tuple(values[: max(0, limit)])

    def freshness(self, *, now: datetime | None = None) -> dict[str, float | int]:
        now = now or utcnow()
        with self._lock:
            total = len(self._state)
            stale = sum(1 for value in self._state.values() if not value.is_fresh(now))
            return {
                "revision": self._revision,
                "state_count": total,
                "stale_count": stale,
                "stale_ratio": stale / total if total else 0.0,
                "metric_count": len(self._metrics),
                "event_count": len(self._events),
                "adapter_count": len(self._heartbeats),
                "stale_adapter_count": sum(
                    1
                    for heartbeat in self._heartbeats.values()
                    if _aware(now) > _aware(heartbeat.timestamp) + heartbeat.ttl
                ),
            }

    def _record_change(
        self,
        kind: str,
        subject_ref: str,
        key_or_relation: str,
        version: int,
        timestamp: datetime | None = None,
    ) -> StateChange:
        self._revision += 1
        change = StateChange(self._revision, kind, subject_ref, key_or_relation, version, timestamp or utcnow())
        self._changes.append(change)
        return change

    @staticmethod
    def _notify(callbacks: list[StatePlaneCallback], change: StateChange) -> None:
        for callback in callbacks:
            try:
                callback(deepcopy(change))
            except Exception:
                continue

    @staticmethod
    def _max_age_for(key: str, policy: Mapping[str, int]) -> timedelta | None:
        if not policy:
            return None
        matches = [
            (prefix, milliseconds)
            for prefix, milliseconds in policy.items()
            if prefix in {"", "*"} or key == prefix or key.startswith(prefix + ".")
        ]
        if not matches:
            return None
        _, milliseconds = max(matches, key=lambda item: len(item[0]))
        return timedelta(milliseconds=max(0, milliseconds))

    def _canonical_pattern(self, key: str) -> str:
        if _is_pattern(key):
            return key
        try:
            return self.registry.canonicalize(key)
        except UnknownCanonicalKey:
            return key

    def _applicable_keys(self, entity_type: str, pattern: str) -> tuple[str, ...] | None:
        """Expand a key pattern only for schemas valid on the current entity."""

        if not _is_pattern(pattern):
            try:
                definition = self.registry.resolve(pattern)
            except UnknownCanonicalKey:
                return None
            return (definition.key,) if entity_type in definition.entity_types else ()
        return tuple(
            definition.key
            for definition in self.registry.list()
            if entity_type in definition.entity_types and fnmatchcase(definition.key, pattern)
        )

    @staticmethod
    def _rejected(update: StateUpdate, reason: str, conflict: bool = False) -> WriteResult:
        return WriteResult(False, conflict=conflict, entity_ref=update.entity_ref, key=update.key, reason=reason)

    @staticmethod
    def _relation_rejected(update: RelationUpdate, reason: str, conflict: bool = False) -> WriteResult:
        return WriteResult(False, conflict=conflict, entity_ref=update.source_ref, key=update.relation_type, reason=reason)


def _matches(value: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatchcase(value, pattern) for pattern in patterns)


def _is_pattern(value: str) -> bool:
    return any(character in value for character in "*?[")


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


__all__ = ["InMemoryStatePlane", "StatePlaneCallback"]
