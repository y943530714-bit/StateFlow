"""Local demo gateway for smoke tests and development."""

from __future__ import annotations

import argparse

from .backend import BackendRegistry
from .backend.openai_compatible import InMemoryBackend
from .gateway import RequestGateway, TargetRegistry
from .gateway.server import StateFlowHTTPServer
from .scheduler.harness.success_first import SuccessFirstScheduler
from .state.schema import TargetCandidate
from .state.store import InMemoryStateStore


def build_demo_gateway() -> RequestGateway:
    """Build a dependency-free gateway with three representative targets."""

    targets = TargetRegistry(
        [
            TargetCandidate(
                model_id="fast-small",
                endpoint_id="demo-cluster-a",
                replica_id="a-0",
                tier="efficient",
                capabilities={"tool_calling"},
                base_success=0.84,
                base_uncertainty=0.01,
                inference_cost=0.01,
                fallback_path_cost=0.05,
                queue_latency_seconds=0.015,
                load_balance_score=0.9,
                backend_key="demo",
            ),
            TargetCandidate(
                model_id="reliable-medium",
                endpoint_id="demo-cluster-a",
                replica_id="a-1",
                tier="capable",
                capabilities={"tool_calling", "reasoning"},
                base_success=0.95,
                base_uncertainty=0.01,
                inference_cost=0.04,
                fallback_path_cost=0.05,
                queue_latency_seconds=0.035,
                load_balance_score=0.7,
                backend_key="demo",
            ),
            TargetCandidate(
                model_id="frontier-reliable",
                endpoint_id="demo-cluster-b",
                replica_id="b-0",
                tier="frontier",
                capabilities={"tool_calling", "reasoning", "multimodal"},
                base_success=0.98,
                base_uncertainty=0.01,
                inference_cost=0.12,
                fallback_path_cost=0.05,
                queue_latency_seconds=0.06,
                load_balance_score=0.5,
                backend_key="demo",
            ),
        ]
    )
    store = InMemoryStateStore()
    backends = BackendRegistry()
    backends.register("demo", InMemoryBackend())
    scheduler = SuccessFirstScheduler(decision_sink=store.record_routing_decision)
    return RequestGateway(scheduler, store, targets, backends)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the StateFlow local demo gateway")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--grpc-host", default="127.0.0.1")
    parser.add_argument("--grpc-port", type=int)
    args = parser.parse_args(argv)

    grpc_factory = None
    if args.grpc_port is not None:
        try:
            from .rpc import create_grpc_server
        except ImportError:
            parser.error(
                "gRPC support is not installed; install the package with [grpc]"
            )
        grpc_factory = create_grpc_server

    gateway = build_demo_gateway()
    server = None
    grpc_server = None
    try:
        server = StateFlowHTTPServer(gateway, args.host, args.port)
        host, port = server.address
        print(f"StateFlow demo listening on http://{host}:{port}")
        print("POST /v1/chat/completions, /v1/responses, or /v1/messages")
        print("State Plane API: /v1/state-plane/*")
        if grpc_factory is not None:
            grpc_server, grpc_port = grpc_factory(
                gateway.state_plane, f"{args.grpc_host}:{args.grpc_port}"
            )
            grpc_server.start()
            print(f"State Plane gRPC: {args.grpc_host}:{grpc_port}")
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        if server is not None:
            server.shutdown()
        if grpc_server is not None:
            grpc_server.stop(grace=1).wait()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["build_demo_gateway", "main"]
