"""Session-aware replica placement after success and cost gates."""

from __future__ import annotations

from dataclasses import dataclass

from stateflow.state_manager.schema import HarnessSchedulingView, TargetCandidate
from stateflow.planner.types import CandidateEvaluation


@dataclass(frozen=True)
class PlacementResult:
    evaluation: CandidateEvaluation
    reason: str


def _is_local(view: HarnessSchedulingView, target: TargetCandidate) -> bool:
    if view.preferred_replica and target.replica_id == view.preferred_replica:
        return True
    if view.preferred_endpoint and target.endpoint_id == view.preferred_endpoint:
        return True
    if target.metadata.get("session_local") is True:
        return True
    return False


def choose_placement(
    view: HarnessSchedulingView,
    candidates: list[tuple[TargetCandidate, CandidateEvaluation]],
) -> PlacementResult:
    if not candidates:
        raise ValueError("placement requires at least one candidate")

    if view.session_turn <= 0:
        ranked = sorted(
            candidates,
            key=lambda item: (
                -item[0].load_balance_score,
                item[1].predicted_latency_seconds,
                -item[1].local_kv_tokens,
                item[0].endpoint_id,
                item[0].replica_id,
            ),
        )
        best = ranked[0]
        latency_best = min(candidates, key=lambda item: item[1].predicted_latency_seconds)
        reason = "LOAD_BALANCE" if best is not latency_best else "LATENCY_TIEBREAK"
        return PlacementResult(best[1], reason)

    local = [item for item in candidates if _is_local(view, item[0])]
    remote = [item for item in candidates if not _is_local(view, item[0])]
    if local and remote:
        local_best = min(local, key=lambda item: item[1].predicted_latency_seconds)
        remote_best = min(remote, key=lambda item: item[1].predicted_latency_seconds)
        # Stay local only while local queue pressure is cheaper than moving the
        # session.  Fetch/recompute latency is already in the remote estimate;
        # the explicit migration penalty captures state movement not represented
        # by the current request's KV bytes.
        move_penalty = max(0.0, view.migration_penalty)
        if local_best[1].predicted_latency_seconds <= remote_best[1].predicted_latency_seconds + move_penalty:
            return PlacementResult(local_best[1], "KV_AFFINITY")
        return PlacementResult(remote_best[1], "LATENCY_TIEBREAK")
    if local:
        return PlacementResult(
            min(local, key=lambda item: item[1].predicted_latency_seconds)[1],
            "KV_AFFINITY",
        )
    return PlacementResult(
        min(candidates, key=lambda item: item[1].predicted_latency_seconds)[1],
        "LATENCY_TIEBREAK",
    )


__all__ = ["PlacementResult", "choose_placement"]
