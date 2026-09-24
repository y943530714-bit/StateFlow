"""Deterministic safety overrides for high-risk Agent states."""

from __future__ import annotations

from dataclasses import dataclass

from stateflow.state_manager.schema import HarnessSchedulingView, TargetCandidate
from stateflow.planner.types import SchedulerConfig


@dataclass(frozen=True)
class CriticalOverride:
    active: bool
    reasons: tuple[str, ...] = ()


def critical_override(view: HarnessSchedulingView, config: SchedulerConfig) -> CriticalOverride:
    reasons: list[str] = []
    if view.critical_error:
        reasons.append("critical_error")
    if view.error_severity >= config.error_severity_override:
        reasons.append("high_error_severity")
    if view.failure_streak >= config.failure_streak_override:
        reasons.append("failure_streak")
    if view.consecutive_failures >= config.failure_streak_override:
        reasons.append("consecutive_failures")
    if view.spinning_score >= config.spinning_score_override:
        reasons.append("spinning")
    if view.recovery_score >= config.error_severity_override:
        reasons.append("recovery_or_replanning")
    if view.context_compaction_needed:
        reasons.append("context_compaction")
    if view.critical_step:
        reasons.append("critical_path_step")
    if view.security_sensitive:
        reasons.append("security_sensitive")
    if view.phase.value in {"REPLANNING", "BLOCKED"}:
        reasons.append("replanning_or_blocked")
    if view.state_completeness < config.min_state_completeness_for_downgrade:
        reasons.append("state_unknown_safe_fallback")
    return CriticalOverride(active=bool(reasons), reasons=tuple(dict.fromkeys(reasons)))


def capable_only(targets: list[TargetCandidate]) -> list[TargetCandidate]:
    """Keep high-capability tiers while preserving availability as a fallback."""

    capable = [
        target
        for target in targets
        if target.tier.lower() in {"capable", "frontier", "strong", "premium"}
        or int(target.metadata.get("capability_rank", 0)) >= 2
    ]
    return capable or targets


__all__ = ["CriticalOverride", "capable_only", "critical_override"]
