"""Canonical gRPC operation names and StatePlaneAPI routes."""

from __future__ import annotations


RPC_OPERATIONS = {
    "RegisterComponent": ("POST", "/components/register"),
    "UpsertEntities": ("POST", "/entities/upsert"),
    "PublishState": ("POST", "/state/publish"),
    "PublishMetrics": ("POST", "/metrics/publish"),
    "PublishEvents": ("POST", "/events/publish"),
    "UpsertRelations": ("POST", "/relations/upsert"),
    "Heartbeat": ("POST", "/heartbeat"),
    "GetState": ("GET", "/state"),
    "GetSnapshot": ("POST", "/snapshots"),
    "ReadSnapshot": ("GET", "/snapshots/{token}"),
    "QueryGraph": ("POST", "/graph/query"),
    "QueryMetrics": ("GET", "/metrics"),
    "GetFreshness": ("GET", "/freshness"),
    "ListSchema": ("GET", "/schema"),
    "ListComponents": ("GET", "/components"),
    "ListHeartbeats": ("GET", "/heartbeats"),
}


__all__ = ["RPC_OPERATIONS"]
