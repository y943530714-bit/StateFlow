"""OpenAI Chat/Responses and Anthropic Messages request normalization."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any, Mapping
import uuid


def _header(headers: Mapping[str, str], *names: str) -> str:
    lowered = {str(key).lower(): value for key, value in headers.items()}
    for name in names:
        if name.lower() in lowered:
            return str(lowered[name.lower()])
    return ""


def _csv_values(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def _bool_value(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def estimate_tokens(value: Any) -> int:
    """Portable token estimate used when a tokenizer is not registered.

    Runtime integrations should replace this with the target tokenizer.  The
    scheduler only needs a safe context-window estimate at the normalization
    boundary.
    """

    if value is None:
        return 0
    if isinstance(value, str):
        return max(1, (len(value) + 3) // 4)
    if isinstance(value, (int, float, bool)):
        return 1
    if isinstance(value, Mapping):
        return sum(estimate_tokens(key) + estimate_tokens(item) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return sum(estimate_tokens(item) for item in value)
    return estimate_tokens(str(value))


@dataclass
class ProviderNeutralRequest:
    protocol: str
    request_id: str
    session_id: str
    task_id: str = "default-task"
    turn_id: int = 0
    step_id: str = ""
    attempt_id: str = ""
    trace_id: str = ""
    model: str = ""
    prompt_tokens: int = 0
    predicted_output_tokens: int = 0
    required_capabilities: set[str] = field(default_factory=set)
    tenant_id: str = "default"
    security_domain: str = "default"
    allowed_models: set[str] = field(default_factory=set)
    allowed_regions: set[str] = field(default_factory=set)
    allowed_cache_domains: set[str] = field(default_factory=set)
    remote_kv_allowed: bool = True
    cost_budget: float | None = None
    stream: bool = False
    body: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)

    def backend_payload(self, target_model: str | None = None) -> dict[str, Any]:
        payload = dict(self.body)
        # These are StateFlow routing hints, not fields of the provider API.
        for key in (
            "request_id", "session_id", "program_id", "task_id", "turn_id", "step_id",
            "attempt_id", "trace_id", "prompt_tokens", "required_capabilities",
            "cost_budget", "protocol",
        ):
            payload.pop(key, None)
        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            metadata = dict(metadata)
            for key in (
                "session_id", "task_id", "tenant_id", "security_domain",
                "allowed_models", "allowed_regions", "allowed_cache_domains",
                "remote_kv_allowed", "cost_budget",
            ):
                metadata.pop(key, None)
            if metadata:
                payload["metadata"] = metadata
            else:
                payload.pop("metadata", None)
        if target_model:
            payload["model"] = target_model
        return payload

    def to_state_payload(self) -> dict[str, Any]:
        """Metadata-only request projection for Agent State."""

        payload = {
            "model": self.model,
            "logical_model": self.model,
            "prompt_tokens": self.prompt_tokens,
            "predicted_output_tokens": self.predicted_output_tokens,
            "required_capabilities": sorted(self.required_capabilities),
            "request_id": self.request_id,
            "identity": {"tenant_id": self.tenant_id},
            "security": {
                "tenant_id": self.tenant_id,
                "security_domain": self.security_domain,
                "allowed_models": sorted(self.allowed_models),
                "allowed_regions": sorted(self.allowed_regions),
                "allowed_cache_domains": sorted(self.allowed_cache_domains),
                "remote_kv_allowed": self.remote_kv_allowed,
            },
        }
        if self.cost_budget is not None:
            payload["qos"] = {"task_cost_budget": self.cost_budget}
        return payload


def normalize_request(
    path: str,
    body: Mapping[str, Any],
    headers: Mapping[str, str] | None = None,
) -> ProviderNeutralRequest:
    headers = dict(headers or {})
    payload = dict(body)
    path = path.rstrip("/")
    if path.endswith("/chat/completions"):
        protocol = "openai_chat"
        messages = payload.get("messages", [])
        prompt_tokens = int(payload.get("prompt_tokens") or estimate_tokens(messages))
        predicted_output = int(
            payload.get("max_tokens")
            or payload.get("max_completion_tokens")
            or 256
        )
        required = set(payload.get("required_capabilities", []) or [])
        if payload.get("tools"):
            required.add("tool_calling")
    elif path.endswith("/responses"):
        protocol = "openai_responses"
        prompt = payload.get("input", payload.get("messages", []))
        prompt_tokens = int(payload.get("prompt_tokens") or estimate_tokens(prompt))
        predicted_output = int(payload.get("max_output_tokens") or 256)
        required = set(payload.get("required_capabilities", []) or [])
        if payload.get("tools"):
            required.add("tool_calling")
    elif path.endswith("/messages"):
        protocol = "anthropic_messages"
        messages = payload.get("messages", [])
        prompt_tokens = int(payload.get("prompt_tokens") or estimate_tokens(messages) + estimate_tokens(payload.get("system", "")))
        predicted_output = int(payload.get("max_tokens") or 256)
        required = set(payload.get("required_capabilities", []) or [])
        if payload.get("tools"):
            required.add("tool_calling")
    else:
        raise ValueError(f"unsupported model API path: {path}")

    request_id = (
        _header(headers, "x-request-id", "request-id")
        or str(payload.get("request_id", ""))
        or f"req-{uuid.uuid4().hex[:16]}"
    )
    session_id = (
        _header(headers, "x-stateflow-program-id", "x-stateflow-session-id", "x-session-id", "session-id")
        or str(payload.get("program_id", ""))
        or str(payload.get("session_id", ""))
        or str((payload.get("metadata") or {}).get("session_id", ""))
        or f"session-{request_id}"
    )
    task_id = (
        _header(headers, "x-stateflow-task-id", "x-task-id", "task-id")
        or str(payload.get("task_id", ""))
        or str((payload.get("metadata") or {}).get("task_id", "default-task"))
    )
    trace_id = _header(headers, "x-trace-id", "traceparent") or str(payload.get("trace_id", ""))
    step_id = _header(headers, "x-stateflow-step-id", "x-step-id") or str(payload.get("step_id", ""))
    attempt_id = _header(headers, "x-stateflow-attempt-id") or str(payload.get("attempt_id", ""))
    metadata = payload.get("metadata") or {}
    required.update(_csv_values(_header(headers, "x-stateflow-required-capabilities")))
    tenant_id = _header(headers, "x-stateflow-tenant-id", "x-tenant-id") or str(
        metadata.get("tenant_id", "default")
    )
    security_domain = _header(headers, "x-stateflow-security-domain") or str(
        metadata.get("security_domain", "default")
    )
    allowed_models = set(metadata.get("allowed_models", []) or [])
    allowed_regions = set(metadata.get("allowed_regions", []) or [])
    allowed_cache_domains = set(metadata.get("allowed_cache_domains", []) or [])
    allowed_models.update(_csv_values(_header(headers, "x-stateflow-allowed-models")))
    allowed_regions.update(_csv_values(_header(headers, "x-stateflow-allowed-regions")))
    allowed_cache_domains.update(_csv_values(_header(headers, "x-stateflow-allowed-cache-domains")))
    remote_kv_raw = _header(headers, "x-stateflow-remote-kv-allowed")
    remote_kv_allowed = _bool_value(
        remote_kv_raw if remote_kv_raw else metadata.get("remote_kv_allowed"),
        True,
    )
    budget_raw = payload.get("cost_budget", metadata.get("cost_budget"))
    cost_budget = float(budget_raw) if budget_raw is not None else None
    turn_raw = _header(headers, "x-stateflow-turn-id", "x-turn-id") or payload.get("turn_id", 0)

    return ProviderNeutralRequest(
        protocol=protocol,
        request_id=request_id,
        session_id=session_id,
        task_id=task_id or "default-task",
        turn_id=int(turn_raw or 0),
        step_id=step_id,
        attempt_id=attempt_id,
        trace_id=trace_id,
        model=str(payload.get("model", "")),
        prompt_tokens=max(0, prompt_tokens),
        predicted_output_tokens=max(0, predicted_output),
        required_capabilities=required,
        tenant_id=tenant_id,
        security_domain=security_domain,
        allowed_models=allowed_models,
        allowed_regions=allowed_regions,
        allowed_cache_domains=allowed_cache_domains,
        remote_kv_allowed=remote_kv_allowed,
        cost_budget=cost_budget,
        stream=bool(payload.get("stream", False)),
        body=payload,
        headers=headers,
    )


__all__ = ["ProviderNeutralRequest", "estimate_tokens", "normalize_request"]
