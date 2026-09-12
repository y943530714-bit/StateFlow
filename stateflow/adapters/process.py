"""Independent source-adapter process for the HTTP State Plane."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import signal
import threading
from typing import Callable

from ..state import StatePlaneHTTPClient
from .clients import (
    CollectionReport,
    DCGMSourceClient,
    KVMetadataSourceClient,
    KubernetesSourceClient,
    RayServeSourceClient,
    SGLangSourceClient,
    VLLMSourceClient,
)
from .dcgm import DCGMStateAdapter
from .kubernetes import KubernetesStateAdapter
from .kubernetes_watch import KubernetesWatchClient, KubernetesWatchCursor
from .kv import KVStateAdapter
from .profiles import LMCACHE_KV_PROFILE, MOONCAKE_KV_PROFILE
from .runner import PollingAdapterRunner
from .runtime import RuntimeStateAdapter


Operation = Callable[[], CollectionReport]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one StateFlow observability source outside the request path"
    )
    parser.add_argument(
        "--source",
        required=True,
        choices=(
            "vllm",
            "sglang",
            "ray-serve",
            "dcgm",
            "kubernetes",
            "mooncake",
            "lmcache",
        ),
    )
    parser.add_argument("--source-endpoint", required=True)
    parser.add_argument("--state-plane-url", required=True)
    parser.add_argument("--runtime-id", default="")
    parser.add_argument("--instance-id", default="")
    parser.add_argument("--node-id", default="")
    parser.add_argument("--cluster-id", default="")
    parser.add_argument("--namespace", default="")
    parser.add_argument("--metric-label", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--bearer-token-file", default="")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--max-backoff", type=float, default=30.0)
    parser.add_argument("--source-timeout", type=float, default=3.0)
    parser.add_argument("--state-plane-timeout", type=float, default=3.0)
    parser.add_argument("--watch-timeout", type=int, default=30)
    parser.add_argument("--once", action="store_true")
    return parser


def build_operation(args: argparse.Namespace, plane=None) -> Operation:
    target = (
        plane
        if plane is not None
        else StatePlaneHTTPClient(
            args.state_plane_url, timeout=args.state_plane_timeout
        )
    )
    source_name = args.source
    if source_name in {"vllm", "sglang", "ray-serve"}:
        if not args.runtime_id or not args.instance_id:
            raise ValueError("runtime sources require --runtime-id and --instance-id")
        client_type = {
            "vllm": VLLMSourceClient,
            "sglang": SGLangSourceClient,
            "ray-serve": RayServeSourceClient,
        }[source_name]
        client = client_type(
            args.source_endpoint,
            runtime_id=args.runtime_id,
            instance_id=args.instance_id,
            node_id=args.node_id,
            metric_labels=_labels(args.metric_label),
            timeout=args.source_timeout,
        )
        adapter = RuntimeStateAdapter(
            target, component_id=f"stateflow-{source_name}-adapter"
        )
        return lambda: client.collect_and_publish(adapter)
    if source_name == "dcgm":
        client = DCGMSourceClient(
            args.source_endpoint,
            default_node_id=args.node_id,
            timeout=args.source_timeout,
        )
        adapter = DCGMStateAdapter(target)
        return lambda: client.collect_and_publish(adapter)
    if source_name in {"mooncake", "lmcache"}:
        profile = (
            MOONCAKE_KV_PROFILE
            if source_name == "mooncake"
            else LMCACHE_KV_PROFILE
        )
        client = KVMetadataSourceClient(
            args.source_endpoint,
            profile=profile,
            timeout=args.source_timeout,
        )
        adapter = KVStateAdapter(target, component_id=f"stateflow-{source_name}-adapter")
        return lambda: client.collect_and_publish(adapter)
    if not args.cluster_id:
        raise ValueError("kubernetes source requires --cluster-id")
    bearer_token = _bearer_token(args.bearer_token_file)
    client = KubernetesSourceClient(
        args.source_endpoint,
        cluster_id=args.cluster_id,
        namespace=args.namespace,
        bearer_token=bearer_token,
        timeout=args.source_timeout,
    )
    watcher = KubernetesWatchClient(client, timeout_seconds=args.watch_timeout)
    adapter = KubernetesStateAdapter(target)
    cursor = KubernetesWatchCursor()

    def collect_kubernetes() -> CollectionReport:
        nonlocal cursor
        batch = watcher.collect(cursor)
        accepted = rejected = 0
        for observation in batch.observations:
            report = adapter.publish(observation)
            accepted += report.accepted
            rejected += report.rejected
        cursor = batch.cursor
        watermark = (
            f"nodes={cursor.node_resource_version};pods={cursor.pod_resource_version}"
        )
        return CollectionReport(
            "kubernetes-watch",
            len(batch.observations),
            accepted,
            rejected,
            watermark,
        )

    return collect_kubernetes


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        operation = build_operation(args)
        runner = PollingAdapterRunner(
            operation,
            interval_seconds=args.interval,
            max_backoff_seconds=args.max_backoff,
        )
    except ValueError as exc:
        parser.error(str(exc))
    if args.once:
        report = runner.run_once()
        if report is None:
            print(f"StateFlow adapter failed: {runner.state.last_error}")
            return 1
        print(
            f"source={report.source} observations={report.observations} "
            f"accepted={report.accepted} rejected={report.rejected} "
            f"watermark={report.watermark}"
        )
        return 0

    stop = threading.Event()

    def request_stop(signum, frame) -> None:
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    runner.serve(stop)
    return 0


def _labels(values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key.strip():
            raise ValueError("--metric-label must use KEY=VALUE")
        result[key.strip()] = item
    return result


def _bearer_token(path: str) -> str:
    if path:
        try:
            return Path(path).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ValueError(
                f"unable to read bearer token file: {type(exc).__name__}"
            ) from exc
    return os.environ.get("STATEFLOW_KUBERNETES_BEARER_TOKEN", "").strip()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["build_operation", "build_parser", "main"]
