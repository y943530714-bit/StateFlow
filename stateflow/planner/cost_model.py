"""KV-aware effective cost model."""

from __future__ import annotations

from stateflow.state_manager.schema import HarnessSchedulingView, TargetCandidate
from stateflow.planner.types import CostBreakdown, SchedulerConfig, SuccessEstimate
from stateflow.planner.kv_value import future_kv_value


def _kv_breakdown(view: HarnessSchedulingView, target: TargetCandidate) -> tuple[int, int, int]:
    prompt_tokens = max(0, view.prompt_tokens)
    local = min(prompt_tokens, max(0, target.local_kv_tokens))
    remote = min(prompt_tokens - local, max(0, target.remote_kv_tokens))
    if not view.remote_kv_allowed or not target.supports_remote_kv:
        remote = 0
    miss = max(0, prompt_tokens - local - remote)
    return local, remote, miss


def effective_cost(
    view: HarnessSchedulingView,
    target: TargetCandidate,
    estimate: SuccessEstimate,
    config: SchedulerConfig,
) -> CostBreakdown:
    """Compute C_effective from the design document.

    C_effective = inference + KV fetch + KV recompute + expected retry
                   + routing overhead - lambda_future * future KV value
    """

    local, remote, miss = _kv_breakdown(view, target)
    prompt_tokens = max(0, view.prompt_tokens)
    output_tokens = max(0, view.predicted_output_tokens)
    inference = max(0.0, target.inference_cost)
    inference += prompt_tokens * max(0.0, target.input_cost_per_token)
    inference += output_tokens * max(0.0, target.output_cost_per_token)
    fetch = remote * max(0.0, target.kv_fetch_cost_per_token)
    recompute = miss * max(0.0, target.kv_recompute_cost_per_token)
    expected_retry = max(0.0, 1.0 - estimate.predicted_success) * max(
        0.0,
        target.fallback_path_cost,
    )
    future = future_kv_value(
        view,
        target,
        config,
        local_kv_tokens=local,
        remote_kv_tokens=remote,
    )
    raw_effective = (
        inference
        + fetch
        + recompute
        + expected_retry
        + max(0.0, target.routing_overhead_cost)
        - config.lambda_future * future
    )
    return CostBreakdown(
        inference_cost=inference,
        kv_fetch_cost=fetch,
        kv_recompute_cost=recompute,
        expected_retry_cost=expected_retry,
        routing_overhead_cost=max(0.0, target.routing_overhead_cost),
        future_kv_value=future,
        effective_cost=max(0.0, raw_effective),
        local_kv_tokens=local,
        remote_kv_tokens=remote,
        miss_tokens=miss,
    )


__all__ = ["effective_cost"]
