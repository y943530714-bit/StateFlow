"""Small JSON configuration for real backends; no provider SDK is required."""

from __future__ import annotations

from dataclasses import fields
import json
import os
from pathlib import Path

from stateflow.interface.backend import BackendRegistry
from stateflow.interface.openai_backend import OpenAICompatibleBackend
from stateflow.interface.gateway import RequestGateway
from stateflow.state_manager import TargetRegistry
from stateflow.planner.scheduler import SuccessFirstScheduler
from stateflow.planner.types import SchedulerConfig
from stateflow.state_manager.schema import TargetCandidate
from stateflow.state_manager.store import InMemoryStateStore


def build_configured_gateway(path: str | Path) -> RequestGateway:
    """Build a gateway from backend endpoints, candidate models and policy."""

    config = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError("configuration must be a JSON object")
    backend_specs = config.get("backends")
    target_specs = config.get("targets")
    if not isinstance(backend_specs, dict) or not backend_specs:
        raise ValueError("backends must be a non-empty object")
    if not isinstance(target_specs, list) or not target_specs:
        raise ValueError("targets must be a non-empty array")

    backends = BackendRegistry()
    for key, spec in backend_specs.items():
        if not isinstance(spec, dict) or not spec.get("base_url"):
            raise ValueError(f"backend {key!r} requires base_url")
        api_key_env = spec.get("api_key_env", "")
        if api_key_env and api_key_env not in os.environ:
            raise ValueError(f"backend {key!r} requires environment variable {api_key_env}")
        backends.register(key, OpenAICompatibleBackend(
            str(spec["base_url"]),
            api_key=os.environ.get(api_key_env, "") if api_key_env else "",
            timeout_seconds=float(spec.get("timeout_seconds", 60)),
        ))

    valid_fields = {item.name for item in fields(TargetCandidate)}
    targets = []
    identities = set()
    for spec in target_specs:
        if not isinstance(spec, dict):
            raise ValueError("target must be an object")
        unknown = set(spec) - valid_fields - {"protocols"}
        if unknown:
            raise ValueError(f"unknown target fields: {', '.join(sorted(unknown))}")
        data = {key: value for key, value in spec.items() if key in valid_fields}
        if not all(data.get(key) for key in ("model_id", "endpoint_id", "replica_id")):
            raise ValueError("target requires model_id, endpoint_id and replica_id")
        if data.get("backend_key") not in backend_specs:
            raise ValueError(f"target {data.get('model_id')!r} has unknown backend_key")
        protocols = spec.get("protocols", ["openai_chat"])
        if not isinstance(protocols, list) or not protocols or not set(protocols) <= {
            "openai_chat", "openai_responses", "anthropic_messages"
        }:
            raise ValueError("target protocols must list supported request APIs")
        if not isinstance(data.get("metadata", {}), dict):
            raise ValueError("target metadata must be an object")
        data["capabilities"] = set(data.get("capabilities", []))
        data["metadata"] = {**data.get("metadata", {}), "protocols": protocols}
        target = TargetCandidate(**data)
        identity = (target.model_id, target.endpoint_id, target.replica_id)
        if identity in identities:
            raise ValueError(f"duplicate target: {identity}")
        identities.add(identity)
        targets.append(target)

    policy = config.get("policy", {})
    if not isinstance(policy, dict) or set(policy) - {item.name for item in fields(SchedulerConfig)}:
        raise ValueError("policy must contain only SchedulerConfig fields")
    store = InMemoryStateStore()
    scheduler = SuccessFirstScheduler(SchedulerConfig(**policy), decision_sink=store.record_routing_decision)
    return RequestGateway(scheduler, store, TargetRegistry(targets), backends)


__all__ = ["build_configured_gateway"]
