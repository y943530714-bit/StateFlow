"""Optional Python client for the State Plane gRPC service."""

from __future__ import annotations

from datetime import timezone
import json
from typing import Any, Iterable, Iterator, Mapping

import grpc

from ..state.contracts import (
    ComponentDescriptor,
    GraphEntity,
    Heartbeat,
    RelationUpdate,
    StateChange,
    StateUpdate,
    WriteResult,
)
from ..state.http_client import (
    decode_component,
    decode_entity,
    decode_objects,
    decode_write_results,
    encode_heartbeat,
    encode_relation_update,
    encode_state_update,
)
from ..state.schema import to_jsonable
from .generated import stateflow_pb2, stateflow_pb2_grpc
from .operations import RPC_OPERATIONS


UNARY_OPERATIONS = frozenset(RPC_OPERATIONS)


class StatePlaneGRPCError(RuntimeError):
    def __init__(self, message: str, *, code: str = "") -> None:
        super().__init__(message)
        self.code = code


class StatePlaneGRPCClient:
    def __init__(
        self,
        target: str,
        *,
        timeout: float = 3.0,
        channel=None,
    ) -> None:
        if timeout <= 0:
            raise ValueError("gRPC timeout must be positive")
        self.timeout = timeout
        self.channel = channel or grpc.insecure_channel(target)
        self._owns_channel = channel is None
        self.stub = stateflow_pb2_grpc.StatePlaneServiceStub(self.channel)

    def call(
        self,
        operation: str,
        *,
        payload: Mapping[str, Any] | None = None,
        query: Mapping[str, str] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        if operation not in UNARY_OPERATIONS:
            raise ValueError(f"unsupported State Plane gRPC operation: {operation}")
        request = stateflow_pb2.StatePlaneRpcRequest(
            payload_json=json.dumps(payload or {}, ensure_ascii=False),
            query=dict(query or {}),
        )
        try:
            response = getattr(self.stub, operation)(request, timeout=self.timeout)
        except grpc.RpcError as exc:
            raise StatePlaneGRPCError(
                exc.details() or "State Plane gRPC request failed",
                code=exc.code().name if exc.code() is not None else "",
            ) from exc
        return response.status_code, _json_object(response.payload_json)

    def list_components(self) -> tuple[ComponentDescriptor, ...]:
        status, payload = self.call("ListComponents")
        _require_success(status, payload)
        return tuple(
            decode_component(item)
            for item in decode_objects(payload.get("components"), "components")
        )

    def register_component(self, descriptor: ComponentDescriptor) -> None:
        status, payload = self.call(
            "RegisterComponent", payload=to_jsonable(descriptor)
        )
        _require_success(status, payload)

    def upsert_entity(self, entity: GraphEntity) -> None:
        status, payload = self.call(
            "UpsertEntities", payload={"entities": [to_jsonable(entity)]}
        )
        _require_success(status, payload)

    def get_entity(self, entity_ref: str) -> GraphEntity | None:
        status, payload = self.call(
            "QueryGraph", payload={"roots": [entity_ref], "depth": 0}
        )
        _require_success(status, payload)
        entities = decode_objects(payload.get("entities"), "entities")
        return decode_entity(entities[0]) if entities else None

    def publish_state(self, updates: Iterable[StateUpdate]) -> list[WriteResult]:
        status, payload = self.call(
            "PublishState",
            payload={"updates": [encode_state_update(item) for item in updates]},
        )
        if status not in (200, 409):
            _require_success(status, payload)
        return decode_write_results(payload)

    def upsert_relations(
        self, updates: Iterable[RelationUpdate]
    ) -> list[WriteResult]:
        status, payload = self.call(
            "UpsertRelations",
            payload={
                "relations": [encode_relation_update(item) for item in updates]
            },
        )
        if status not in (200, 409):
            _require_success(status, payload)
        return decode_write_results(payload)

    def heartbeat(self, heartbeat: Heartbeat) -> None:
        status, payload = self.call(
            "Heartbeat", payload=encode_heartbeat(heartbeat)
        )
        _require_success(status, payload)

    def watch_state(
        self,
        *,
        after_cursor: int = 0,
        entity_prefix: str = "",
        keys: tuple[str, ...] = (),
        batch_size: int = 100,
        follow: bool = False,
        poll_interval_ms: int = 100,
    ) -> Iterator[StateChange]:
        request = stateflow_pb2.WatchStateRequest(
            after_cursor=max(0, after_cursor),
            entity_prefix=entity_prefix,
            keys=keys,
            batch_size=batch_size,
            follow=follow,
            poll_interval_ms=poll_interval_ms,
        )
        try:
            stream = self.stub.WatchState(
                request, timeout=None if follow else self.timeout
            )
            for value in stream:
                yield StateChange(
                    cursor=value.cursor,
                    kind=value.kind,
                    subject_ref=value.subject_ref,
                    key_or_relation=value.key_or_relation,
                    version=value.version,
                    timestamp=value.timestamp.ToDatetime(tzinfo=timezone.utc),
                )
        except grpc.RpcError as exc:
            raise StatePlaneGRPCError(
                exc.details() or "State Plane gRPC watch failed",
                code=exc.code().name if exc.code() is not None else "",
            ) from exc

    def close(self) -> None:
        if self._owns_channel:
            self.channel.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


def _json_object(payload: str) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise StatePlaneGRPCError(
            "State Plane gRPC response contains invalid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise StatePlaneGRPCError("State Plane gRPC response must be a JSON object")
    return value


def _require_success(status: int, payload: Mapping[str, Any]) -> None:
    if 200 <= status < 300:
        return
    error = payload.get("error")
    message = str(error.get("message", "")) if isinstance(error, Mapping) else ""
    raise StatePlaneGRPCError(
        message or f"State Plane returned status {status}", code=str(status)
    )


__all__ = ["StatePlaneGRPCClient", "StatePlaneGRPCError", "UNARY_OPERATIONS"]
