"""End-to-end request latency estimate."""

from __future__ import annotations

from ....state.schema import HarnessSchedulingView, TargetCandidate
from ...types import LatencyBreakdown
from .cost_model import _kv_breakdown


def predicted_latency(
    view: HarnessSchedulingView,
    target: TargetCandidate,
) -> LatencyBreakdown:
    local, remote, miss = _kv_breakdown(view, target)
    fetch = remote * max(0.0, target.kv_fetch_latency_per_token)
    recompute = miss * max(0.0, target.kv_recompute_latency_per_token)
    kv_latency = fetch + recompute
    prefill_tps = max(1e-9, target.prefill_tokens_per_second)
    decode_tps = max(1e-9, target.decode_tokens_per_second)
    prefill = miss / prefill_tps
    decode = max(0, view.predicted_output_tokens) / decode_tps
    total = max(0.0, target.queue_latency_seconds) + kv_latency + prefill + decode

    # This is the v0.1 request estimate.  A future model can replace it with
    # a critical-path delta while preserving the same output contract.
    critical_factor = 1.0 + 0.0 * max(0.0, view.critical_path_urgency)
    return LatencyBreakdown(
        queue_latency_seconds=max(0.0, target.queue_latency_seconds),
        kv_latency_seconds=kv_latency,
        prefill_latency_seconds=prefill,
        decode_latency_seconds=decode,
        predicted_latency_seconds=total,
        critical_path_latency_seconds=total * critical_factor,
    )


__all__ = ["predicted_latency"]
