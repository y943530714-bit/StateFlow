"""Source-specific clients that feed the semantic observability adapters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from ..state import SourceRef
from ..state.schema import utcnow
from .dcgm import DCGMStateAdapter, HardwareObservation
from .kubernetes import DeploymentObservation, KubernetesStateAdapter
from .kv import KVLocation, KVObservation, KVStateAdapter
from .runtime import RuntimeObservation, RuntimeStateAdapter
from .source import (
    JSONFetcher,
    SourceClientError,
    TextFetcher,
    fetch_json,
    fetch_text,
    parse_prometheus,
    stable_watermark,
)


@dataclass(frozen=True)
class CollectionReport:
    source: str
    observations: int
    accepted: int
    rejected: int
    watermark: str


class VLLMSourceClient:
    """Read vLLM's Prometheus endpoint and publish one runtime window."""

    def __init__(
        self,
        endpoint: str,
        *,
        runtime_id: str,
        instance_id: str,
        node_id: str = "",
        timeout: float = 2.0,
        fetcher: TextFetcher = fetch_text,
    ) -> None:
        self.endpoint = _endpoint(endpoint, "/metrics")
        self.runtime_id = runtime_id
        self.instance_id = instance_id
        self.node_id = node_id
        self.timeout = timeout
        self.fetcher = fetcher

    def collect(
        self,
        *,
        request_id: str = "",
        trace_id: str = "",
        observed_at: datetime | None = None,
    ) -> RuntimeObservation:
        payload = self.fetcher(self.endpoint, {}, self.timeout)
        metrics = parse_prometheus(payload)
        watermark = stable_watermark(payload)
        return RuntimeObservation(
            runtime_id=self.runtime_id,
            instance_id=self.instance_id,
            node_id=self.node_id,
            request_id=request_id,
            trace_id=trace_id,
            observed_at=observed_at or utcnow(),
            queue_depth=_integer(metrics.aggregate("vllm:num_requests_waiting")),
            running=_integer(metrics.aggregate("vllm:num_requests_running")),
            ttft_p95_seconds=metrics.histogram_quantile(
                "vllm:time_to_first_token_seconds", 0.95
            ),
            tpot_p95_seconds=metrics.histogram_quantile(
                "vllm:inter_token_latency_seconds", 0.95
            ),
            kv_usage_ratio=metrics.aggregate(
                "vllm:kv_cache_usage_perc", mode="max"
            ),
            health="healthy",
            ready=True,
            implementation="vllm",
            watermark=watermark,
            observation_id=watermark,
            source_ref=SourceRef("vllm-prometheus", self.endpoint, trace_id),
        )

    def collect_and_publish(
        self,
        adapter: RuntimeStateAdapter,
        *,
        request_id: str = "",
        trace_id: str = "",
    ) -> CollectionReport:
        observation = self.collect(request_id=request_id, trace_id=trace_id)
        report = adapter.publish(observation)
        return CollectionReport(
            "vllm", 1, report.accepted, report.rejected, observation.watermark
        )


class DCGMSourceClient:
    """Read dcgm-exporter metrics and emit one hardware window per GPU."""

    def __init__(
        self,
        endpoint: str,
        *,
        default_node_id: str = "",
        timeout: float = 2.0,
        fetcher: TextFetcher = fetch_text,
    ) -> None:
        self.endpoint = _endpoint(endpoint, "/metrics")
        self.default_node_id = default_node_id
        self.timeout = timeout
        self.fetcher = fetcher

    def collect(self, *, observed_at: datetime | None = None) -> tuple[HardwareObservation, ...]:
        payload = self.fetcher(self.endpoint, {}, self.timeout)
        snapshot = parse_prometheus(payload)
        watermark = stable_watermark(payload)
        grouped: dict[tuple[str, str], dict[str, float]] = {}
        supported = {
            "DCGM_FI_DEV_FB_USED",
            "DCGM_FI_DEV_GPU_UTIL",
            "DCGM_FI_DEV_XID_ERRORS",
        }
        for sample in snapshot.samples:
            if sample.name not in supported:
                continue
            node_id = (
                sample.labels.get("Hostname")
                or sample.labels.get("hostname")
                or sample.labels.get("node")
                or self.default_node_id
            )
            gpu_id = sample.labels.get("gpu") or sample.labels.get("UUID", "")
            if not node_id or not gpu_id:
                continue
            grouped.setdefault((node_id, gpu_id), {})[sample.name] = sample.value
        timestamp = observed_at or utcnow()
        node_health = {
            node_id: (
                "unhealthy"
                if any(
                    values.get("DCGM_FI_DEV_XID_ERRORS", 0.0) > 0
                    for (candidate_node, _), values in grouped.items()
                    if candidate_node == node_id
                )
                else "healthy"
            )
            for node_id, _ in grouped
        }
        return tuple(
            HardwareObservation(
                node_id=node_id,
                gpu_id=gpu_id,
                observed_at=timestamp,
                hbm_used_bytes=_mib_to_bytes(values.get("DCGM_FI_DEV_FB_USED")),
                gpu_util_ratio=_percent_to_ratio(values.get("DCGM_FI_DEV_GPU_UTIL")),
                node_health=node_health[node_id],
                watermark=watermark,
                observation_id=f"{watermark}:{node_id}:{gpu_id}",
                source_ref=SourceRef(
                    "dcgm-exporter",
                    f"{self.endpoint}#{reference_suffix(node_id, gpu_id)}",
                ),
            )
            for (node_id, gpu_id), values in sorted(grouped.items())
        )

    def collect_and_publish(self, adapter: DCGMStateAdapter) -> CollectionReport:
        observations = self.collect()
        accepted = rejected = 0
        for observation in observations:
            report = adapter.publish(observation)
            accepted += report.accepted
            rejected += report.rejected
        watermark = observations[0].watermark if observations else ""
        return CollectionReport("dcgm", len(observations), accepted, rejected, watermark)


class KubernetesSourceClient:
    """List Node/Pod state using Kubernetes API resourceVersion watermarks."""

    def __init__(
        self,
        api_server: str,
        *,
        cluster_id: str,
        namespace: str = "",
        bearer_token: str = "",
        timeout: float = 5.0,
        fetcher: JSONFetcher = fetch_json,
    ) -> None:
        self.api_server = api_server.rstrip("/")
        self.cluster_id = cluster_id
        self.namespace = namespace
        self.timeout = timeout
        self.fetcher = fetcher
        self.headers = {"Authorization": f"Bearer {bearer_token}"} if bearer_token else {}

    def collect(self, *, observed_at: datetime | None = None) -> tuple[DeploymentObservation, ...]:
        node_payload = self.fetcher(
            self.api_server + "/api/v1/nodes", self.headers, self.timeout
        )
        pod_path = (
            f"/api/v1/namespaces/{self.namespace}/pods"
            if self.namespace
            else "/api/v1/pods"
        )
        pod_payload = self.fetcher(self.api_server + pod_path, self.headers, self.timeout)
        return self.decode(node_payload, pod_payload, observed_at=observed_at)

    def decode(
        self,
        node_payload: Any,
        pod_payload: Any,
        *,
        observed_at: datetime | None = None,
    ) -> tuple[DeploymentObservation, ...]:
        nodes = _items(node_payload, "NodeList")
        pods = _items(pod_payload, "PodList")
        timestamp = observed_at or utcnow()
        observations: list[DeploymentObservation] = []
        for node in nodes:
            metadata = _mapping(node.get("metadata"), "node.metadata")
            status = _mapping(node.get("status"), "node.status")
            node_id = str(metadata.get("name", ""))
            if not node_id:
                continue
            observations.append(
                DeploymentObservation(
                    cluster_id=self.cluster_id,
                    node_id=node_id,
                    observed_at=timestamp,
                    node_health="healthy" if _ready(status) else "unhealthy",
                    node_allocatable=_allocatable(status.get("allocatable")),
                    labels=_string_mapping(metadata.get("labels")),
                    watermark=str(metadata.get("resourceVersion", "")),
                    observation_id=f"node:{metadata.get('uid', node_id)}:{metadata.get('resourceVersion', '')}",
                    source_ref=SourceRef(
                        "kubernetes", f"nodes/{node_id}@{metadata.get('resourceVersion', '')}"
                    ),
                )
            )
        for pod in pods:
            metadata = _mapping(pod.get("metadata"), "pod.metadata")
            spec = _mapping(pod.get("spec"), "pod.spec")
            status = _mapping(pod.get("status"), "pod.status")
            node_id = str(spec.get("nodeName", ""))
            if not node_id:
                continue
            labels = _string_mapping(metadata.get("labels"))
            annotations = _string_mapping(metadata.get("annotations"))
            instance_id = (
                labels.get("stateflow.io/instance-id")
                or str(metadata.get("uid", ""))
                or str(metadata.get("name", ""))
            )
            runtime_id = (
                labels.get("stateflow.io/runtime-id")
                or annotations.get("stateflow.io/runtime-id")
                or labels.get("app.kubernetes.io/name", "")
            )
            observations.append(
                DeploymentObservation(
                    cluster_id=self.cluster_id,
                    node_id=node_id,
                    observed_at=timestamp,
                    instance_id=instance_id,
                    runtime_id=runtime_id,
                    namespace=str(metadata.get("namespace", self.namespace)),
                    pod_uid=str(metadata.get("uid", "")),
                    instance_ready=_ready(status),
                    allocated_gpu=_pod_gpu(spec),
                    labels=labels,
                    watermark=str(metadata.get("resourceVersion", "")),
                    observation_id=f"pod:{instance_id}:{metadata.get('resourceVersion', '')}",
                    source_ref=SourceRef(
                        "kubernetes",
                        "pods/"
                        f"{metadata.get('namespace', '')}/{metadata.get('name', '')}"
                        f"@{metadata.get('resourceVersion', '')}",
                    ),
                )
            )
        return tuple(observations)

    def collect_and_publish(self, adapter: KubernetesStateAdapter) -> CollectionReport:
        observations = self.collect()
        accepted = rejected = 0
        for observation in observations:
            report = adapter.publish(observation)
            accepted += report.accepted
            rejected += report.rejected
        watermark = max((item.watermark for item in observations), default="")
        return CollectionReport(
            "kubernetes", len(observations), accepted, rejected, watermark
        )


class KVMetadataSourceClient:
    """Read a Mooncake/LMCache sidecar's metadata-only JSON projection."""

    def __init__(
        self,
        endpoint: str,
        *,
        backend: str = "kv-metadata",
        timeout: float = 2.0,
        fetcher: JSONFetcher = fetch_json,
    ) -> None:
        self.endpoint = endpoint
        self.backend = backend
        self.timeout = timeout
        self.fetcher = fetcher

    def collect(self, *, observed_at: datetime | None = None) -> tuple[KVObservation, ...]:
        payload = self.fetcher(self.endpoint, {}, self.timeout)
        raw_items = payload.get("items", ()) if isinstance(payload, Mapping) else payload
        if not isinstance(raw_items, (list, tuple)):
            raise SourceClientError("KV metadata response must be a list or contain items")
        timestamp = observed_at or utcnow()
        observations: list[KVObservation] = []
        for raw in raw_items:
            item = _mapping(raw, "kv item")
            kv_id = str(item.get("kv_id") or item.get("id") or item.get("key") or "")
            if not kv_id:
                raise SourceClientError("KV metadata item is missing kv_id")
            locations = tuple(_kv_location(value) for value in item.get("locations", ()) or ())
            watermark = str(item.get("version") or item.get("watermark") or "")
            request_ids = item.get("request_ids", ()) or ()
            if isinstance(request_ids, str):
                request_ids = (request_ids,)
            observations.append(
                KVObservation(
                    kv_id=kv_id,
                    observed_at=timestamp,
                    request_ids=tuple(str(value) for value in request_ids),
                    locations=locations,
                    size_bytes=_optional_int(item.get("size_bytes", item.get("size"))),
                    replica_count=_optional_int(item.get("replica_count")),
                    cache_hit=_optional_bool(item.get("cache_hit")),
                    transfer_state=_optional_string(item.get("transfer_state")),
                    soft_pin=_optional_bool(item.get("soft_pin")),
                    lifecycle=str(item.get("lifecycle", "active")),
                    trace_id=str(item.get("trace_id", "")),
                    watermark=watermark,
                    observation_id=f"{kv_id}:{watermark}" if watermark else "",
                    source_ref=SourceRef(self.backend, f"{self.endpoint}#{kv_id}"),
                )
            )
        return tuple(observations)

    def collect_and_publish(self, adapter: KVStateAdapter) -> CollectionReport:
        observations = self.collect()
        accepted = rejected = 0
        for observation in observations:
            report = adapter.publish(observation)
            accepted += report.accepted
            rejected += report.rejected
        watermark = max((item.watermark for item in observations), default="")
        return CollectionReport("kv", len(observations), accepted, rejected, watermark)


def _endpoint(endpoint: str, default_path: str) -> str:
    value = endpoint.rstrip("/")
    return value if value.endswith(default_path) else value + default_path


def _integer(value: float | None) -> int | None:
    return None if value is None else int(round(value))


def _mib_to_bytes(value: float | None) -> int | None:
    return None if value is None else int(value * 1024 * 1024)


def _percent_to_ratio(value: float | None) -> float | None:
    return None if value is None else max(0.0, min(1.0, value / 100.0))


def reference_suffix(node_id: str, gpu_id: str) -> str:
    return f"{node_id}/gpu/{gpu_id}"


def _items(payload: Any, expected_kind: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("items"), list):
        raise SourceClientError(f"expected Kubernetes {expected_kind}")
    return tuple(_mapping(item, expected_kind + ".items") for item in payload["items"])


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SourceClientError(f"{name} must be an object")
    return value


def _string_mapping(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): str(item) for key, item in value.items()}


def _ready(status: Mapping[str, Any]) -> bool:
    for condition in status.get("conditions", ()) or ():
        if isinstance(condition, Mapping) and condition.get("type") == "Ready":
            return str(condition.get("status", "")).lower() == "true"
    return str(status.get("phase", "")).lower() == "running"


def _allocatable(value: Any) -> dict[str, object]:
    raw = _string_mapping(value)
    result: dict[str, object] = dict(raw)
    if "cpu" in raw:
        result["cpu_cores"] = _cpu(raw["cpu"])
    if "memory" in raw:
        result["memory_bytes"] = _bytes(raw["memory"])
    if "nvidia.com/gpu" in raw:
        result["gpu"] = float(raw["nvidia.com/gpu"])
    return result


def _pod_gpu(spec: Mapping[str, Any]) -> float | None:
    total = 0.0
    found = False
    for container in spec.get("containers", ()) or ():
        if not isinstance(container, Mapping):
            continue
        resources = container.get("resources", {})
        if not isinstance(resources, Mapping):
            continue
        values = resources.get("limits") or resources.get("requests") or {}
        if isinstance(values, Mapping) and "nvidia.com/gpu" in values:
            total += float(values["nvidia.com/gpu"])
            found = True
    return total if found else None


def _cpu(value: str) -> float:
    return float(value[:-1]) / 1000.0 if value.endswith("m") else float(value)


def _bytes(value: str) -> int:
    suffixes = {
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "K": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
    }
    for suffix, multiplier in suffixes.items():
        if value.endswith(suffix):
            return int(float(value[: -len(suffix)]) * multiplier)
    return int(float(value))


def _kv_location(value: Any) -> KVLocation:
    if isinstance(value, str):
        return KVLocation(value)
    item = _mapping(value, "kv location")
    identifier = str(item.get("identifier") or item.get("id") or item.get("ref") or "")
    if not identifier:
        raise SourceClientError("KV location is missing identifier")
    return KVLocation(
        identifier,
        entity_type=str(item.get("entity_type", "resource")),
        tier=str(item.get("tier", "")),
        primary=bool(item.get("primary", False)),
    )


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    raise SourceClientError(f"invalid boolean value: {value!r}")


def _optional_string(value: Any) -> str | None:
    return None if value is None else str(value)


__all__ = [
    "CollectionReport",
    "DCGMSourceClient",
    "KVMetadataSourceClient",
    "KubernetesSourceClient",
    "VLLMSourceClient",
]
