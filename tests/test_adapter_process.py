from __future__ import annotations

from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import threading
import unittest

from stateflow.adapters import component_ref
from stateflow.adapters.process import build_operation, build_parser, main as adapter_main
from stateflow.demo import build_demo_gateway
from stateflow.gateway.server import StateFlowHTTPServer
from stateflow.state import InMemoryStatePlane, StatePlaneHTTPClient


METRICS = """
vllm:num_requests_waiting 3
vllm:num_requests_running 7
vllm:kv_cache_usage_perc 0.61
"""


class _MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/metrics":
            self.send_error(404)
            return
        payload = METRICS.encode("utf-8")
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args) -> None:
        return


class AdapterProcessTests(unittest.TestCase):
    def test_vllm_process_publishes_over_http_and_is_idempotent(self) -> None:
        gateway = build_demo_gateway()
        state_server = StateFlowHTTPServer(gateway, port=0)
        metrics_server = ThreadingHTTPServer(("127.0.0.1", 0), _MetricsHandler)
        threads = [
            threading.Thread(target=state_server.serve_forever, daemon=True),
            threading.Thread(target=metrics_server.serve_forever, daemon=True),
        ]
        for thread in threads:
            thread.start()
        state_host, state_port = state_server.address
        metrics_host, metrics_port = metrics_server.server_address
        arguments = [
            "--source",
            "vllm",
            "--source-endpoint",
            f"http://{metrics_host}:{metrics_port}",
            "--state-plane-url",
            f"http://{state_host}:{state_port}",
            "--runtime-id",
            "runtime-process",
            "--instance-id",
            "replica-process",
            "--node-id",
            "node-process",
            "--once",
        ]
        try:
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(adapter_main(arguments), 0)
                self.assertEqual(adapter_main(arguments), 0)

            runtime_ref = component_ref("runtime-process")
            values = gateway.state_plane.get_state(
                runtime_ref, ("runtime.queue_depth", "runtime.running")
            )
            self.assertEqual([value.value for value in values], [3, 7])
            self.assertIn("rejected=", output.getvalue())
            heartbeat = {
                item.subject_ref: item for item in gateway.state_plane.heartbeats()
            }["stateflow-vllm-adapter"]
            self.assertEqual(heartbeat.ttl.total_seconds(), 10.0)

            remote = StatePlaneHTTPClient(
                f"http://{state_host}:{state_port}", timeout=2.0
            )
            self.assertIsNotNone(remote.get_entity(runtime_ref))
            self.assertIn(
                "stateflow-vllm-adapter",
                {item.component_id for item in remote.list_components()},
            )
        finally:
            metrics_server.shutdown()
            metrics_server.server_close()
            state_server.shutdown()
            for thread in threads:
                thread.join(timeout=2)

    def test_process_validates_source_specific_identity_and_labels(self) -> None:
        parser = build_parser()
        missing_identity = parser.parse_args(
            [
                "--source",
                "ray-serve",
                "--source-endpoint",
                "http://ray/metrics",
                "--state-plane-url",
                "http://stateflow",
            ]
        )
        with self.assertRaisesRegex(ValueError, "runtime sources require"):
            build_operation(missing_identity, InMemoryStatePlane())

        invalid_label = parser.parse_args(
            [
                "--source",
                "ray-serve",
                "--source-endpoint",
                "http://ray/metrics",
                "--state-plane-url",
                "http://stateflow",
                "--runtime-id",
                "app",
                "--instance-id",
                "deployment",
                "--metric-label",
                "invalid",
            ]
        )
        with self.assertRaisesRegex(ValueError, "KEY=VALUE"):
            build_operation(invalid_label, InMemoryStatePlane())


if __name__ == "__main__":
    unittest.main()
