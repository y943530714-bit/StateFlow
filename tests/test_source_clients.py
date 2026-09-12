from __future__ import annotations

from datetime import datetime, timezone
import json
import unittest

from stateflow.adapters import (
    CollectionReport,
    DCGMSourceClient,
    KVMetadataSourceClient,
    KubernetesWatchClient,
    KubernetesWatchCursor,
    KubernetesSourceClient,
    LMCACHE_KV_PROFILE,
    MOONCAKE_KV_PROFILE,
    ObservabilityBridge,
    PollingAdapterRunner,
    RayServeSourceClient,
    SGLangSourceClient,
    SourceClientError,
    SourceHTTPError,
    VLLMSourceClient,
    parse_prometheus,
)
from stateflow.state import InMemoryStatePlane


VLLM_METRICS = """
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="agent"} 3
vllm:num_requests_running{model_name="agent"} 7
vllm:kv_cache_usage_perc{model_name="agent"} 0.61
vllm:time_to_first_token_seconds_bucket{model_name="agent",le="0.1"} 50
vllm:time_to_first_token_seconds_bucket{model_name="agent",le="0.2"} 95
vllm:time_to_first_token_seconds_bucket{model_name="agent",le="+Inf"} 100
vllm:inter_token_latency_seconds_bucket{model_name="agent",le="0.01"} 80
vllm:inter_token_latency_seconds_bucket{model_name="agent",le="0.02"} 95
vllm:inter_token_latency_seconds_bucket{model_name="agent",le="+Inf"} 100
"""

DCGM_METRICS = """
DCGM_FI_DEV_FB_USED{gpu="0",UUID="GPU-a",Hostname="node-a"} 32768
DCGM_FI_DEV_GPU_UTIL{gpu="0",UUID="GPU-a",Hostname="node-a"} 72
DCGM_FI_DEV_XID_ERRORS{gpu="0",UUID="GPU-a",Hostname="node-a"} 0
DCGM_FI_DEV_FB_USED{gpu="1",UUID="GPU-b",Hostname="node-a"} 16384
DCGM_FI_DEV_GPU_UTIL{gpu="1",UUID="GPU-b",Hostname="node-a"} 25
DCGM_FI_DEV_XID_ERRORS{gpu="1",UUID="GPU-b",Hostname="node-a"} 31
"""

SGLANG_METRICS = """
sglang:num_queue_reqs 4
sglang:num_running_reqs 6
sglang:token_usage 0.72
sglang:gen_throughput 123.5
sglang:time_to_first_token_seconds_bucket{le="0.1"} 80
sglang:time_to_first_token_seconds_bucket{le="0.2"} 95
sglang:time_to_first_token_seconds_bucket{le="+Inf"} 100
sglang:time_per_output_token_seconds_bucket{le="0.01"} 95
sglang:time_per_output_token_seconds_bucket{le="+Inf"} 100
"""

RAY_SERVE_METRICS = """
ray_serve_deployment_queued_queries{application="agent",deployment="Runtime"} 2
ray_serve_replica_processing_queries{application="agent",deployment="Runtime",replica="a"} 3
ray_serve_replica_processing_queries{application="agent",deployment="Runtime",replica="b"} 4
ray_serve_deployment_queued_queries{application="other",deployment="Runtime"} 50
ray_serve_replica_processing_queries{application="other",deployment="Runtime",replica="c"} 60
"""


def text_fetcher(payload: str):
    def fetch(url, headers, timeout):
        return payload

    return fetch


class PrometheusSourceTests(unittest.TestCase):
    def test_vllm_histograms_and_gauges_publish_runtime_state(self) -> None:
        plane = InMemoryStatePlane()
        bridge = ObservabilityBridge(plane)
        client = VLLMSourceClient(
            "http://runtime-a:8000",
            runtime_id="runtime-a",
            instance_id="replica-a",
            node_id="node-a",
            fetcher=text_fetcher(VLLM_METRICS),
        )

        observation = client.collect(
            request_id="request-a",
            trace_id="trace-a",
            observed_at=datetime.now(timezone.utc),
        )
        self.assertEqual(observation.queue_depth, 3)
        self.assertEqual(observation.running, 7)
        self.assertAlmostEqual(observation.ttft_p95_seconds or 0.0, 0.2)
        self.assertAlmostEqual(observation.tpot_p95_seconds or 0.0, 0.02)
        self.assertEqual(observation.kv_usage_ratio, 0.61)
        report = bridge.runtime.publish(observation)
        self.assertEqual(report.rejected, 0)
        runtime_ref = bridge.resolver.require("component_id", "runtime-a")
        self.assertEqual(plane.get_state(runtime_ref, ("runtime.queue_depth",))[0].value, 3)

    def test_dcgm_units_health_and_multiple_gpu_windows(self) -> None:
        client = DCGMSourceClient(
            "http://dcgm-exporter:9400/metrics",
            fetcher=text_fetcher(DCGM_METRICS),
        )
        observations = client.collect()

        self.assertEqual(len(observations), 2)
        self.assertEqual(observations[0].hbm_used_bytes, 32768 * 1024 * 1024)
        self.assertEqual(observations[0].gpu_util_ratio, 0.72)
        self.assertEqual(observations[0].node_health, "unhealthy")
        self.assertEqual(observations[1].node_health, "unhealthy")

    def test_prometheus_parser_rejects_malformed_samples(self) -> None:
        with self.assertRaises(SourceClientError):
            parse_prometheus("not a prometheus sample")

    def test_sglang_profile_projects_runtime_metrics(self) -> None:
        client = SGLangSourceClient(
            "http://sglang:30000",
            runtime_id="runtime-sglang",
            instance_id="replica-sglang",
            fetcher=text_fetcher(SGLANG_METRICS),
        )

        observation = client.collect()

        self.assertEqual(observation.queue_depth, 4)
        self.assertEqual(observation.running, 6)
        self.assertAlmostEqual(observation.ttft_p95_seconds or 0.0, 0.2)
        self.assertAlmostEqual(observation.tpot_p95_seconds or 0.0, 0.01)
        self.assertEqual(observation.decode_tokens_per_second, 123.5)
        self.assertEqual(observation.kv_usage_ratio, 0.72)
        self.assertEqual(observation.source_ref.backend, "sglang-prometheus")

    def test_ray_serve_profile_aggregates_replica_metrics(self) -> None:
        client = RayServeSourceClient(
            "http://ray-head:8080/metrics",
            runtime_id="serve-app",
            instance_id="serve-deployment",
            metric_labels={"application": "agent", "deployment": "Runtime"},
            fetcher=text_fetcher(RAY_SERVE_METRICS),
        )

        observation = client.collect()

        self.assertEqual(observation.queue_depth, 2)
        self.assertEqual(observation.running, 7)
        self.assertEqual(observation.implementation, "ray-serve")


class JSONSourceTests(unittest.TestCase):
    def test_kubernetes_node_and_pod_list_decode_resource_versions(self) -> None:
        payloads = {
            "nodes": {
                "kind": "NodeList",
                "items": [
                    {
                        "metadata": {
                            "name": "node-a",
                            "uid": "node-uid",
                            "resourceVersion": "101",
                            "labels": {"topology.kubernetes.io/zone": "z1"},
                        },
                        "status": {
                            "conditions": [{"type": "Ready", "status": "True"}],
                            "allocatable": {
                                "cpu": "8",
                                "memory": "64Gi",
                                "nvidia.com/gpu": "2",
                            },
                        },
                    }
                ],
            },
            "pods": {
                "kind": "PodList",
                "items": [
                    {
                        "metadata": {
                            "name": "runtime-a-0",
                            "namespace": "serving",
                            "uid": "pod-uid",
                            "resourceVersion": "202",
                            "labels": {
                                "stateflow.io/instance-id": "replica-a",
                                "stateflow.io/runtime-id": "runtime-a",
                            },
                        },
                        "spec": {
                            "nodeName": "node-a",
                            "containers": [
                                {"resources": {"limits": {"nvidia.com/gpu": "1"}}}
                            ],
                        },
                        "status": {
                            "phase": "Running",
                            "conditions": [{"type": "Ready", "status": "True"}],
                        },
                    }
                ],
            },
        }

        def fetch(url, headers, timeout):
            return payloads["nodes" if url.endswith("/nodes") else "pods"]

        client = KubernetesSourceClient(
            "https://kubernetes.default.svc",
            cluster_id="prod-a",
            namespace="serving",
            bearer_token="redacted-token",
            fetcher=fetch,
        )
        observations = client.collect()

        self.assertEqual(len(observations), 2)
        node, pod = observations
        self.assertEqual(node.node_allocatable["memory_bytes"], 64 * 1024**3)
        self.assertEqual(node.watermark, "101")
        self.assertEqual(pod.instance_id, "replica-a")
        self.assertEqual(pod.runtime_id, "runtime-a")
        self.assertEqual(pod.allocated_gpu, 1.0)
        self.assertEqual(pod.watermark, "202")

    def test_kv_metadata_projection_supports_mooncake_sidecar_shape(self) -> None:
        payload = {
            "items": [
                {
                    "id": "kv-a",
                    "request_ids": ["request-a"],
                    "locations": [
                        {
                            "id": "node-a/gpu/0",
                            "entity_type": "resource",
                            "tier": "hbm",
                            "primary": True,
                        }
                    ],
                    "size": 4096,
                    "replica_count": 1,
                    "cache_hit": "true",
                    "transfer_state": "resident",
                    "soft_pin": False,
                    "version": "44",
                }
            ]
        }
        client = KVMetadataSourceClient(
            "http://mooncake-sidecar/metadata",
            backend="mooncake",
            fetcher=lambda url, headers, timeout: payload,
        )
        observation = client.collect()[0]

        self.assertEqual(observation.kv_id, "kv-a")
        self.assertEqual(observation.size_bytes, 4096)
        self.assertTrue(observation.cache_hit)
        self.assertEqual(observation.locations[0].tier, "hbm")
        self.assertEqual(observation.source_ref.backend, "mooncake")

    def test_native_mooncake_and_lmcache_profiles(self) -> None:
        mooncake = KVMetadataSourceClient(
            "http://mooncake/metadata",
            profile=MOONCAKE_KV_PROFILE,
            fetcher=lambda url, headers, timeout: {
                "data": {
                    "objects": [
                        {
                            "object_id": "moon-a",
                            "requests": ["request-a"],
                            "replicas": [{"node_id": "node-a", "entity_type": "node"}],
                            "size": 8192,
                            "num_replicas": 1,
                            "revision": "51",
                        }
                    ]
                }
            },
        ).collect()[0]
        lmcache = KVMetadataSourceClient(
            "http://lmcache/metadata",
            profile=LMCACHE_KV_PROFILE,
            fetcher=lambda url, headers, timeout: {
                "chunks": [
                    {
                        "chunk_id": "chunk-a",
                        "request_ids": "request-b",
                        "location": "node-b/gpu/0",
                        "chunk_size": 16384,
                        "num_locations": 1,
                        "revision": 9,
                    }
                ]
            },
        ).collect()[0]

        self.assertEqual(mooncake.kv_id, "moon-a")
        self.assertEqual(mooncake.locations[0].identifier, "node-a")
        self.assertEqual(mooncake.source_ref.backend, "mooncake")
        self.assertEqual(lmcache.kv_id, "chunk-a")
        self.assertEqual(lmcache.size_bytes, 16384)
        self.assertEqual(lmcache.request_ids, ("request-b",))
        self.assertEqual(lmcache.source_ref.backend, "lmcache")

    def test_kubernetes_watch_advances_bookmarks_and_decodes_updates(self) -> None:
        source = KubernetesSourceClient(
            "https://kubernetes.default.svc",
            cluster_id="prod-a",
            namespace="serving",
            fetcher=lambda url, headers, timeout: {},
        )
        urls = []

        def watch_fetcher(url, headers, timeout):
            urls.append(url)
            if "/nodes?" in url:
                return "\n".join(
                    (
                        json.dumps(
                            {
                                "type": "MODIFIED",
                                "object": {
                                    "metadata": {
                                        "name": "node-a",
                                        "uid": "node-a-uid",
                                        "resourceVersion": "102",
                                    },
                                    "status": {
                                        "conditions": [{"type": "Ready", "status": "True"}]
                                    },
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "type": "BOOKMARK",
                                "object": {"metadata": {"resourceVersion": "105"}},
                            }
                        ),
                    )
                )
            return "\n".join(
                (
                    json.dumps(
                        {
                            "type": "DELETED",
                            "object": {
                                "metadata": {
                                    "name": "runtime-a",
                                    "namespace": "serving",
                                    "uid": "pod-a",
                                    "resourceVersion": "204",
                                },
                                "spec": {"nodeName": "node-a", "containers": []},
                                "status": {"phase": "Running"},
                            },
                        }
                    ),
                    json.dumps(
                        {
                            "type": "BOOKMARK",
                            "object": {"metadata": {"resourceVersion": "205"}},
                        }
                    ),
                )
            )

        batch = KubernetesWatchClient(source, fetcher=watch_fetcher).collect(
            KubernetesWatchCursor("101", "201")
        )

        self.assertFalse(batch.relisted)
        self.assertEqual(len(batch.observations), 2)
        self.assertEqual(batch.observations[0].node_id, "node-a")
        self.assertTrue(batch.observations[1].deleted)
        self.assertFalse(batch.observations[1].instance_ready)
        self.assertEqual(batch.cursor, KubernetesWatchCursor("105", "205"))
        self.assertEqual(batch.events, 4)
        self.assertTrue(all("allowWatchBookmarks=true" in url for url in urls))

    def test_kubernetes_watch_relists_after_resource_version_410(self) -> None:
        node_list = {
            "kind": "NodeList",
            "metadata": {"resourceVersion": "301"},
            "items": [
                {
                    "metadata": {"name": "node-a", "resourceVersion": "300"},
                    "status": {
                        "conditions": [{"type": "Ready", "status": "True"}]
                    },
                }
            ],
        }
        pod_list = {
            "kind": "PodList",
            "metadata": {"resourceVersion": "401"},
            "items": [],
        }

        def list_fetcher(url, headers, timeout):
            return node_list if url.endswith("/nodes") else pod_list

        source = KubernetesSourceClient(
            "https://kubernetes.default.svc",
            cluster_id="prod-a",
            fetcher=list_fetcher,
        )
        error = json.dumps(
            {
                "type": "ERROR",
                "object": {"kind": "Status", "code": 410, "reason": "Expired"},
            }
        )
        batch = KubernetesWatchClient(
            source, fetcher=lambda url, headers, timeout: error
        ).collect(KubernetesWatchCursor("100", "200"))

        self.assertTrue(batch.relisted)
        self.assertEqual(batch.cursor, KubernetesWatchCursor("301", "401"))
        self.assertEqual(len(batch.observations), 1)

        def expired_fetcher(url, headers, timeout):
            raise SourceHTTPError(url, 410)

        http_batch = KubernetesWatchClient(
            source, fetcher=expired_fetcher
        ).collect(KubernetesWatchCursor("100", "200"))
        self.assertTrue(http_batch.relisted)
        self.assertEqual(http_batch.cursor, KubernetesWatchCursor("301", "401"))


class RunnerTests(unittest.TestCase):
    def test_runner_isolates_source_failure_and_recovers(self) -> None:
        attempts = 0

        def operation():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise SourceClientError("temporarily unavailable")
            return CollectionReport("fixture", 1, 2, 0, "w2")

        runner = PollingAdapterRunner(operation, interval_seconds=0.01)
        self.assertIsNone(runner.run_once())
        self.assertEqual(runner.state.last_error, "SourceClientError")
        self.assertIsNotNone(runner.run_once())
        self.assertEqual(runner.state.successes, 1)
        self.assertEqual(runner.state.failures, 1)
        self.assertEqual(runner.state.consecutive_failures, 0)


if __name__ == "__main__":
    unittest.main()
