"""Future KV valuation for multi-turn Agent sessions."""

from __future__ import annotations

import math

from stateflow.state_manager.schema import HarnessSchedulingView, TargetCandidate, clamp
from stateflow.planner.types import SchedulerConfig


def future_kv_value(
    view: HarnessSchedulingView,
    target: TargetCandidate,
    config: SchedulerConfig,
    *,
    local_kv_tokens: int | None = None,
    remote_kv_tokens: int | None = None,
) -> float:
    """Estimate the value of retaining reusable KV for future turns.

    The value is deliberately bounded by the candidate's recompute cost.  It
    is a cost credit, never a negative service bill.
    """

    local = max(0, target.local_kv_tokens if local_kv_tokens is None else local_kv_tokens)
    remote = max(0, target.remote_kv_tokens if remote_kv_tokens is None else remote_kv_tokens)
    effective_reusable_tokens = local + remote * clamp(
        target.remote_kv_reuse_factor,
        0.0,
        1.0,
    )
    p_continue = clamp(view.continuation_probability)
    p_reuse = clamp(view.kv_reuse_probability)
    remaining_turns = max(0, view.predicted_remaining_turns)
    recompute_cost = max(0.0, target.kv_recompute_cost_per_token)
    gap = max(0.0, view.predicted_time_until_next_model_call_seconds)
    decay_scale = max(1e-6, config.future_value_decay_seconds)
    decay = math.exp(-gap / decay_scale)
    return max(
        0.0,
        p_continue
        * p_reuse
        * effective_reusable_tokens
        * remaining_turns
        * recompute_cost
        * decay,
    )


__all__ = ["future_kv_value"]
