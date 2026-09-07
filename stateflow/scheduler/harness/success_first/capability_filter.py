"""Hard, non-scored candidate filtering."""

from __future__ import annotations

from ...types import FilterRejection, FilterResult
from ....state.schema import HarnessSchedulingView, TargetCandidate


def hard_filter(view: HarnessSchedulingView, targets: list[TargetCandidate]) -> FilterResult:
    """Remove targets that cannot safely serve the request.

    These predicates intentionally happen before success/cost/latency scoring;
    a target that violates a capability or security constraint is not allowed
    to win through a favorable score.
    """

    eligible: list[TargetCandidate] = []
    rejected: list[FilterRejection] = []
    required_tokens = max(0, view.prompt_tokens) + max(0, view.predicted_output_tokens)

    for target in targets:
        reasons: list[str] = []
        if not target.available:
            reasons.append("endpoint_unavailable")
        if not target.healthy:
            reasons.append("endpoint_unhealthy")
        if not target.compatible:
            reasons.append("adapter_tokenizer_or_revision_incompatible")
        if required_tokens > target.context_window_tokens:
            reasons.append("context_window_exceeded")
        missing = set(view.required_capabilities) - set(target.capabilities)
        if missing:
            reasons.append("missing_capabilities:" + ",".join(sorted(missing)))
        if view.allowed_models and target.model_id not in view.allowed_models:
            reasons.append("model_not_allowed")
        if view.allowed_regions and target.region not in view.allowed_regions:
            reasons.append("region_not_allowed")
        if view.allowed_cache_domains and target.cache_domain not in view.allowed_cache_domains:
            reasons.append("cache_domain_not_allowed")
        if view.security_domain and target.security_domain != view.security_domain:
            reasons.append("security_domain_mismatch")

        # A target may expose remote-cache metadata even when this request is
        # forbidden from using it.  That is a cost-model decision, not a target
        # eligibility failure.  Only reject targets explicitly requiring remote
        # KV to serve this request.
        if target.metadata.get("requires_remote_kv") and not view.remote_kv_allowed:
            reasons.append("remote_kv_required_but_forbidden")

        if view.cost_budget is not None:
            local = min(max(0, view.prompt_tokens), max(0, target.local_kv_tokens))
            remote = min(
                max(0, view.prompt_tokens) - local,
                max(0, target.remote_kv_tokens),
            )
            if not view.remote_kv_allowed or not target.supports_remote_kv:
                remote = 0
            miss = max(0, max(0, view.prompt_tokens) - local - remote)
            lower_bound = (
                max(0.0, target.inference_cost)
                + max(0, view.prompt_tokens) * max(0.0, target.input_cost_per_token)
                + max(0, view.predicted_output_tokens) * max(0.0, target.output_cost_per_token)
                + remote * max(0.0, target.kv_fetch_cost_per_token)
                + miss * max(0.0, target.kv_recompute_cost_per_token)
                + max(0.0, target.routing_overhead_cost)
            )
            if lower_bound > view.cost_budget:
                reasons.append("hard_cost_budget_exceeded")

        if reasons:
            rejected.append(
                FilterRejection(
                    target.model_id,
                    target.endpoint_id,
                    target.replica_id,
                    tuple(reasons),
                )
            )
        else:
            eligible.append(target)

    return FilterResult(eligible=eligible, rejected=rejected)


__all__ = ["hard_filter"]
