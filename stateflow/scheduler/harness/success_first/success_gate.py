"""Conservative success gate for the lexicographic scheduler."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ...types import CandidateEvaluation


@dataclass(frozen=True)
class SuccessGateResult:
    """Model IDs that survive the success-equivalence gate."""

    model_ids: frozenset[str]
    best_lcb: float
    fallback: bool = False


def success_gate(
    evaluations: Iterable[CandidateEvaluation],
    *,
    s_min: float,
    epsilon_success: float,
) -> SuccessGateResult:
    """Apply ``LCB >= S_min`` and ``LCB >= S_best - epsilon``.

    Success is grouped by logical model so that replica queue/cost/KV data is
    not allowed to change the model-level success decision.  If no model
    clears ``s_min``, the highest-LCB model set is returned as a safe
    availability fallback and ``fallback`` is marked for observability.
    """

    by_model: dict[str, float] = {}
    for item in evaluations:
        by_model[item.model_id] = max(by_model.get(item.model_id, 0.0), item.success_lcb)
    if not by_model:
        return SuccessGateResult(frozenset(), 0.0, fallback=True)

    best_lcb = max(by_model.values())
    margin = max(0.0, epsilon_success)
    accepted = frozenset(
        model_id
        for model_id, lcb in by_model.items()
        if lcb >= s_min and lcb >= best_lcb - margin
    )
    if accepted:
        return SuccessGateResult(accepted, best_lcb, fallback=False)

    fallback_models = frozenset(
        model_id for model_id, lcb in by_model.items() if lcb >= best_lcb - 1e-12
    )
    return SuccessGateResult(fallback_models, best_lcb, fallback=True)


__all__ = ["SuccessGateResult", "success_gate"]
