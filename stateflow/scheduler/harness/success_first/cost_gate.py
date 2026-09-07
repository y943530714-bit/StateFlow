"""Cost equivalence gate."""

from __future__ import annotations

from typing import Iterable

from ...types import CandidateEvaluation


def cost_gate(
    evaluations: Iterable[CandidateEvaluation],
    epsilon_cost: float,
) -> list[CandidateEvaluation]:
    values = list(evaluations)
    if not values:
        return []
    best = min(item.effective_cost for item in values)
    threshold = best * (1.0 + max(0.0, epsilon_cost)) + 1e-12
    return [item for item in values if item.effective_cost <= threshold]


__all__ = ["cost_gate"]
