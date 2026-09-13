"""Optional gRPC transport for the canonical State Plane API."""

from __future__ import annotations

from concurrent import futures
import json
import threading
from typing import Any, Mapping

import grpc

from ..state import InMemoryStatePlane, StatePlaneAPI
from .generated import stateflow_pb2, stateflow_pb2_grpc
from .operations import RPC_OPERATIONS


class StatePlaneGRPCServicer(stateflow_pb2_grpc.StatePlaneServiceServicer):
    """Dispatch gRPC methods through the same validated API used by HTTP."""

    def __init__(self, plane: InMemoryStatePlane) -> None:
        self.plane = plane
        self.api = StatePlaneAPI(plane)

    def RegisterComponent(self, request, context):
        return self._call("RegisterComponent", request, context)

    def UpsertEntities(self, request, context):
        return self._call("UpsertEntities", request, context)

    def PublishState(self, request, context):
        return self._call("PublishState", request, context)

    def PublishMetrics(self, request, context):
        return self._call("PublishMetrics", request, context)

    def PublishEvents(self, request, context):
        return self._call("PublishEvents", request, context)

    def UpsertRelations(self, request, context):
        return self._call("UpsertRelations", request, context)

    def Heartbeat(self, request, context):
        return self._call("Heartbeat", request, context)

    def GetState(self, request, context):
        return self._call("GetState", request, context)

    def GetSnapshot(self, request, context):
        return self._call("GetSnapshot", request, context)

    def ReadSnapshot(self, request, context):
        return self._call("ReadSnapshot", request, context)

    def QueryGraph(self, request, context):
        return self._call("QueryGraph", request, context)

    def QueryMetrics(self, request, context):
        return self._call("QueryMetrics", request, context)

    def GetFreshness(self, request, context):
        return self._call("GetFreshness", request, context)

    def ListSchema(self, request, context):
        return self._call("ListSchema", request, context)

    def ListComponents(self, request, context):
        return self._call("ListComponents", request, context)

    def ListHeartbeats(self, request, context):
        return self._call("ListHeartbeats", request, context)

    def WatchState(self, request, context):
        cursor = max(0, request.after_cursor)
        batch_size = max(1, min(request.batch_size or 100, 1000))
        interval = max(0.01, (request.poll_interval_ms or 100) / 1000.0)
        stop = threading.Event()
        context.add_callback(stop.set)
        while context.is_active():
            changes = self.plane.changes(
                after_cursor=cursor,
                entity_prefix=request.entity_prefix,
                keys=tuple(request.keys),
                limit=batch_size,
            )
            for change in changes:
                message = stateflow_pb2.StateChange(
                    cursor=change.cursor,
                    kind=change.kind,
                    subject_ref=change.subject_ref,
                    key_or_relation=change.key_or_relation,
                    version=change.version,
                )
                message.timestamp.FromDatetime(change.timestamp)
                cursor = change.cursor
                yield message
            if not request.follow:
                return
            if len(changes) < batch_size:
                stop.wait(interval)

    def _call(self, operation: str, request, context):
        try:
            method, relative_path = RPC_OPERATIONS[operation]
            payload = _payload(request)
            if "{token}" in relative_path:
                token = str(payload.get("token", ""))
                if not token:
                    raise ValueError(f"{operation} requires payload token")
                relative_path = relative_path.format(token=token)
            path = self.api.prefix + relative_path
            response = (
                self.api.get(path, _query(request))
                if method == "GET"
                else self.api.post(path, payload)
            )
            return _response(response.status_code, response.payload)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except KeyError as exc:
            context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
        except Exception as exc:  # pragma: no cover - defensive transport boundary
            context.abort(grpc.StatusCode.INTERNAL, type(exc).__name__)


def create_grpc_server(
    plane: InMemoryStatePlane,
    address: str = "127.0.0.1:50051",
    *,
    max_workers: int = 8,
):
    """Create an unstarted gRPC server and return it with its bound port."""

    if max_workers <= 0:
        raise ValueError("max_workers must be positive")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    stateflow_pb2_grpc.add_StatePlaneServiceServicer_to_server(
        StatePlaneGRPCServicer(plane), server
    )
    port = server.add_insecure_port(address)
    if port == 0:
        raise RuntimeError(f"failed to bind StateFlow gRPC server to {address}")
    return server, port


def _payload(request) -> dict[str, Any]:
    if not request.payload_json:
        return {}
    try:
        value = json.loads(request.payload_json)
    except json.JSONDecodeError as exc:
        raise ValueError("payload_json must be valid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("payload_json must contain a JSON object")
    return value


def _query(request) -> dict[str, list[str]]:
    return {str(key): [str(value)] for key, value in request.query.items()}


def _response(status_code: int, payload: Mapping[str, Any]):
    return stateflow_pb2.StatePlaneRpcResponse(
        status_code=status_code,
        payload_json=json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
    )


__all__ = ["StatePlaneGRPCServicer", "create_grpc_server"]
