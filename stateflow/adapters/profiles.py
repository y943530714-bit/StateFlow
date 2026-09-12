"""Declarative source profiles for runtime metrics and KV metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .source import PrometheusSnapshot


@dataclass(frozen=True)
class PrometheusMetricRule:
    """Resolve the first available metric alias into one semantic value."""

    names: tuple[str, ...]
    mode: str = "sum"
    quantile: float | None = None
    scale: float = 1.0

    def read(
        self,
        snapshot: PrometheusSnapshot,
        labels: Mapping[str, str] | None = None,
    ) -> float | None:
        for name in self.names:
            value = (
                snapshot.histogram_quantile(name, self.quantile, labels=labels)
                if self.quantile is not None
                else snapshot.aggregate(name, labels=labels, mode=self.mode)
            )
            if value is not None:
                return value * self.scale
        return None


@dataclass(frozen=True)
class RuntimeMetricsProfile:
    """Map implementation-specific Prometheus metrics to RuntimeObservation."""

    name: str
    source_backend: str
    queue_depth: PrometheusMetricRule | None = None
    running: PrometheusMetricRule | None = None
    ttft_p95_seconds: PrometheusMetricRule | None = None
    tpot_p95_seconds: PrometheusMetricRule | None = None
    prefill_tokens_per_second: PrometheusMetricRule | None = None
    decode_tokens_per_second: PrometheusMetricRule | None = None
    kv_usage_ratio: PrometheusMetricRule | None = None

    def project(
        self,
        snapshot: PrometheusSnapshot,
        labels: Mapping[str, str] | None = None,
    ) -> dict[str, float | None]:
        return {
            field_name: rule.read(snapshot, labels) if rule is not None else None
            for field_name, rule in (
                ("queue_depth", self.queue_depth),
                ("running", self.running),
                ("ttft_p95_seconds", self.ttft_p95_seconds),
                ("tpot_p95_seconds", self.tpot_p95_seconds),
                ("prefill_tokens_per_second", self.prefill_tokens_per_second),
                ("decode_tokens_per_second", self.decode_tokens_per_second),
                ("kv_usage_ratio", self.kv_usage_ratio),
            )
        }


VLLM_RUNTIME_PROFILE = RuntimeMetricsProfile(
    name="vllm",
    source_backend="vllm-prometheus",
    queue_depth=PrometheusMetricRule(("vllm:num_requests_waiting",)),
    running=PrometheusMetricRule(("vllm:num_requests_running",)),
    ttft_p95_seconds=PrometheusMetricRule(
        ("vllm:time_to_first_token_seconds",), quantile=0.95
    ),
    tpot_p95_seconds=PrometheusMetricRule(
        ("vllm:inter_token_latency_seconds",), quantile=0.95
    ),
    kv_usage_ratio=PrometheusMetricRule(("vllm:kv_cache_usage_perc",), mode="max"),
)


SGLANG_RUNTIME_PROFILE = RuntimeMetricsProfile(
    name="sglang",
    source_backend="sglang-prometheus",
    queue_depth=PrometheusMetricRule(
        ("sglang:num_queue_reqs", "sglang:num_requests_waiting")
    ),
    running=PrometheusMetricRule(
        ("sglang:num_running_reqs", "sglang:num_requests_running")
    ),
    ttft_p95_seconds=PrometheusMetricRule(
        ("sglang:time_to_first_token_seconds",), quantile=0.95
    ),
    tpot_p95_seconds=PrometheusMetricRule(
        (
            "sglang:time_per_output_token_seconds",
            "sglang:inter_token_latency_seconds",
        ),
        quantile=0.95,
    ),
    decode_tokens_per_second=PrometheusMetricRule(
        ("sglang:gen_throughput", "sglang:generation_throughput")
    ),
    kv_usage_ratio=PrometheusMetricRule(
        ("sglang:token_usage", "sglang:kv_cache_usage_perc"), mode="max"
    ),
)


RAY_SERVE_RUNTIME_PROFILE = RuntimeMetricsProfile(
    name="ray-serve",
    source_backend="ray-serve-prometheus",
    queue_depth=PrometheusMetricRule(
        ("ray_serve_deployment_queued_queries", "ray_serve_num_queued_queries")
    ),
    running=PrometheusMetricRule(("ray_serve_replica_processing_queries",)),
)


@dataclass(frozen=True)
class KVMetadataProfile:
    """Describe containers and field aliases in a metadata-only KV snapshot."""

    name: str
    item_paths: tuple[tuple[str, ...], ...]
    fields: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    def items(self, payload: Any) -> tuple[Any, ...]:
        if isinstance(payload, (list, tuple)):
            return tuple(payload)
        for path in self.item_paths:
            value = payload
            for part in path:
                if not isinstance(value, Mapping) or part not in value:
                    value = None
                    break
                value = value[part]
            if isinstance(value, (list, tuple)):
                return tuple(value)
        return ()

    def value(self, item: Mapping[str, Any], field_name: str, default: Any = None) -> Any:
        for alias in self.fields.get(field_name, (field_name,)):
            if alias in item and item[alias] is not None:
                return item[alias]
        return default


_COMMON_KV_FIELDS = {
    "kv_id": ("kv_id", "id", "key"),
    "request_ids": ("request_ids",),
    "locations": ("locations",),
    "size_bytes": ("size_bytes", "size"),
    "replica_count": ("replica_count",),
    "cache_hit": ("cache_hit",),
    "transfer_state": ("transfer_state",),
    "soft_pin": ("soft_pin",),
    "lifecycle": ("lifecycle",),
    "trace_id": ("trace_id",),
    "watermark": ("version", "watermark"),
}


NORMALIZED_KV_PROFILE = KVMetadataProfile(
    name="kv-metadata",
    item_paths=(("items",),),
    fields=_COMMON_KV_FIELDS,
)


MOONCAKE_KV_PROFILE = KVMetadataProfile(
    name="mooncake",
    item_paths=(("objects",), ("data", "objects"), ("items",)),
    fields={
        **_COMMON_KV_FIELDS,
        "kv_id": ("object_id", "kv_id", "id", "key"),
        "request_ids": ("request_ids", "requests"),
        "locations": ("locations", "replicas"),
        "replica_count": ("replica_count", "num_replicas"),
        "watermark": ("version", "revision", "watermark"),
    },
)


LMCACHE_KV_PROFILE = KVMetadataProfile(
    name="lmcache",
    item_paths=(("chunks",), ("entries",), ("data", "entries"), ("items",)),
    fields={
        **_COMMON_KV_FIELDS,
        "kv_id": ("chunk_id", "kv_id", "key", "id"),
        "request_ids": ("request_ids", "requests"),
        "locations": ("locations", "location"),
        "size_bytes": ("size_bytes", "chunk_size", "size"),
        "replica_count": ("replica_count", "num_locations"),
        "watermark": ("version", "revision", "watermark"),
    },
)


__all__ = [
    "KVMetadataProfile",
    "LMCACHE_KV_PROFILE",
    "MOONCAKE_KV_PROFILE",
    "NORMALIZED_KV_PROFILE",
    "PrometheusMetricRule",
    "RAY_SERVE_RUNTIME_PROFILE",
    "RuntimeMetricsProfile",
    "SGLANG_RUNTIME_PROFILE",
    "VLLM_RUNTIME_PROFILE",
]
