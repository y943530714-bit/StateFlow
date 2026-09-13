"""Deterministic candidate-action prediction baselines."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Protocol

from ..state.contracts import Snapshot, StateValue
from ..state.schema import clamp
from .contracts import (
    CandidateAction,
    CostPrediction,
    FutureStatePrediction,
    PerformancePrediction,
    Prediction,
    ReliabilityPrediction,
)


class PredictionModel(Protocol):
    version: str

    def predict(self, snapshot: Snapshot, candidate: CandidateAction) -> Prediction:
        ...


@dataclass(frozen=True)
class AnalyticalPredictionModel:
    """Explainable v0.1 model over canonical snapshot values.

    Candidate parameters describe the proposed action and may provide static
    target capabilities or accounting rates. Dynamic values are resolved from
    the immutable snapshot. Missing critical inputs propagate as unknown.
    """

    version: str = "analytical-0.1"

    def predict(self, snapshot: Snapshot, candidate: CandidateAction) -> Prediction:
        features = _FeatureResolver(snapshot, candidate)

        queue_seconds = features.number(
            "queue_seconds", "request.queue_time", default=0.0
        )
        transfer_bytes = features.number(
            "transfer_bytes", "kv.size", default=0.0
        )
        transfer_setup = features.number("transfer_setup_seconds", default=0.0)
        effective_bw = features.number(
            "effective_bw_bytes_per_second",
            "link.effective_bw",
            required=transfer_bytes > 0,
            positive=transfer_bytes > 0,
        )
        transfer_seconds = _rate_duration(
            transfer_bytes,
            effective_bw,
            setup=transfer_setup,
        )

        prompt_tokens = features.number(
            "prompt_tokens", "request.context_tokens", default=0.0
        )
        cached_tokens = min(
            prompt_tokens,
            features.number("cached_tokens", default=0.0),
        )
        prefill_tokens = max(0.0, prompt_tokens - cached_tokens)
        prefill_tps = features.number(
            "prefill_tokens_per_second",
            "runtime.prefill_tps",
            required=prefill_tokens > 0,
            positive=prefill_tokens > 0,
        )
        prefill_seconds = _rate_duration(prefill_tokens, prefill_tps)

        output_tokens = features.number(
            "expected_output_tokens",
            "request.expected_output_tokens",
            default=0.0,
        )
        decode_tps = features.number(
            "decode_tokens_per_second",
            "runtime.decode_tps",
            required=output_tokens > 0,
            positive=output_tokens > 0,
        )
        decode_seconds = _rate_duration(output_tokens, decode_tps)

        latency_parts = (
            queue_seconds,
            transfer_seconds,
            prefill_seconds,
            decode_seconds,
        )
        e2e_seconds = (
            sum(latency_parts) if all(item is not None for item in latency_parts) else None
        )
        ttft_parts = (queue_seconds, transfer_seconds, prefill_seconds)
        ttft_seconds = (
            sum(ttft_parts) if all(item is not None for item in ttft_parts) else None
        )

        hbm_used = features.number(
            "hbm_used_bytes", "resource.hbm.used", default=0.0
        )
        hbm_reserved = features.number(
            "hbm_reserved_bytes", "resource.hbm.reserved", default=0.0
        )
        predicted_kv_growth = features.number(
            "predicted_kv_growth_bytes", default=0.0
        )
        hbm_release = features.number("hbm_release_bytes", default=0.0)
        predicted_hbm = max(
            0.0,
            hbm_used + hbm_reserved + predicted_kv_growth - hbm_release,
        )
        hbm_capacity = features.number(
            "hbm_capacity_bytes",
            required=predicted_hbm > 0,
            positive=predicted_hbm > 0,
        )
        hbm_pressure = (
            predicted_hbm / hbm_capacity
            if hbm_capacity is not None and hbm_capacity > 0
            else None
        )

        deadline = features.number("deadline_seconds")
        oom_probability = (
            clamp(max(0.0, hbm_pressure - 0.9) * 2.0)
            if hbm_pressure is not None
            else None
        )
        timeout_probability = (
            _deadline_risk(e2e_seconds, deadline)
            if e2e_seconds is not None and deadline is not None
            else None
        )
        base_success = features.number("base_success_probability")
        success_probability = _success_probability(
            base_success,
            oom_probability,
            timeout_probability,
        )
        risks = tuple(
            value
            for value in (oom_probability, timeout_probability)
            if value is not None
        )

        allocated_gpu = features.number(
            "allocated_gpu", "instance.allocated_gpu", default=0.0
        )
        gpu_seconds = (
            e2e_seconds * allocated_gpu if e2e_seconds is not None else None
        )
        memory_time = (
            predicted_hbm * e2e_seconds if e2e_seconds is not None else None
        )
        storage_io = features.number("storage_io_bytes", default=0.0)
        monetary_cost = _monetary_cost(
            gpu_seconds,
            transfer_bytes,
            storage_io,
            fixed=features.number("fixed_cost", default=0.0),
            gpu_rate=features.number("gpu_second_cost", default=0.0),
            network_rate=features.number("network_byte_cost", default=0.0),
            storage_rate=features.number("storage_io_byte_cost", default=0.0),
        )

        queue_depth = features.number("queue_depth", "runtime.queue_depth")
        queue_delta = features.number("queue_delta", default=0.0)
        gpu_util = features.number("gpu_utilization", "resource.gpu.util")
        kv_location = features.string("target_kv_location", "kv.location")

        confidence = features.confidence()
        applicability = _applicability(candidate.action_type)
        fallback = (
            "none"
            if confidence >= 0.5 and features.required_features_complete
            else "baseline"
        )
        return Prediction(
            snapshot_id=snapshot.token,
            candidate_id=candidate.candidate_id,
            performance=PerformancePrediction(
                ttft_seconds=ttft_seconds,
                tpot_seconds=(1.0 / decode_tps if decode_tps else None),
                e2e_seconds=e2e_seconds,
                throughput=decode_tps,
                transfer_eta_seconds=transfer_seconds,
            ),
            reliability=ReliabilityPrediction(
                success_probability=success_probability,
                oom_probability=oom_probability,
                timeout_probability=timeout_probability,
                slo_violation_probability=max(risks) if risks else None,
            ),
            cost=CostPrediction(
                gpu_seconds=gpu_seconds,
                memory_time=memory_time,
                network_bytes=int(transfer_bytes),
                storage_io_bytes=int(storage_io),
                monetary_cost=monetary_cost,
            ),
            future_state=FutureStatePrediction(
                queue_depth=(queue_depth + queue_delta if queue_depth is not None else None),
                hbm_pressure=hbm_pressure,
                kv_location=kv_location,
                demand=queue_depth,
                contention=gpu_util,
                values={"predicted_hbm_bytes": predicted_hbm},
            ),
            confidence=confidence,
            model_version=self.version,
            applicability=applicability,
            feature_freshness_ms=features.freshness,
            fallback=fallback,
            notes=tuple(features.notes),
        )


class _FeatureResolver:
    def __init__(self, snapshot: Snapshot, candidate: CandidateAction) -> None:
        self.snapshot = snapshot
        self.candidate = candidate
        self.freshness: dict[str, int] = {}
        self.notes: list[str] = []
        self.required = 0
        self.available = 0

    def number(
        self,
        parameter: str,
        canonical_key: str = "",
        *,
        default: float | None = None,
        required: bool = False,
        positive: bool = False,
    ) -> float | None:
        value = self._resolve(parameter, canonical_key)
        if required:
            self.required += 1
        if value is None:
            if required:
                self.notes.append(f"missing_feature:{canonical_key or parameter}")
            return default
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"prediction feature {parameter} must be numeric")
        number = float(value)
        if not math.isfinite(number) or number < 0:
            raise ValueError(f"prediction feature {parameter} must be finite and non-negative")
        if positive and number == 0:
            if required:
                self.notes.append(f"invalid_feature:{canonical_key or parameter}")
            return default
        if required:
            self.available += 1
        return number

    def string(self, parameter: str, canonical_key: str = "") -> str | None:
        value = self._resolve(parameter, canonical_key)
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"prediction feature {parameter} must be a string")
        return value

    def confidence(self) -> float:
        coverage = self.available / self.required if self.required else 0.0
        return clamp(self.snapshot.completeness * (0.35 + 0.65 * coverage))

    @property
    def required_features_complete(self) -> bool:
        return self.available == self.required

    def _resolve(self, parameter: str, canonical_key: str) -> Any:
        if parameter in self.candidate.parameters:
            return self.candidate.parameters[parameter]
        if not canonical_key:
            return None
        matches = [item for item in self.snapshot.values if item.key == canonical_key]
        exact = [
            item
            for item in matches
            if item.entity_ref == self.candidate.target_component
        ]
        selected: StateValue | None = None
        if len(exact) == 1:
            selected = exact[0]
        elif len(matches) == 1:
            selected = matches[0]
        elif len(matches) > 1:
            self.notes.append(f"ambiguous_feature:{canonical_key}")
        if selected is None:
            return None
        age = max(
            0,
            int((self.snapshot.created_at - selected.timestamp).total_seconds() * 1000),
        )
        self.freshness[canonical_key] = age
        return selected.value


def _rate_duration(
    amount: float,
    rate: float | None,
    *,
    setup: float = 0.0,
) -> float | None:
    if amount <= 0:
        return setup
    if rate is None or rate <= 0:
        return None
    return setup + amount / rate


def _deadline_risk(duration: float, deadline: float) -> float:
    if deadline <= 0:
        return 1.0
    return clamp((duration / deadline - 0.8) / 0.2)


def _success_probability(
    base: float | None,
    oom: float | None,
    timeout: float | None,
) -> float | None:
    if base is None and oom is None and timeout is None:
        return None
    result = clamp(base if base is not None else 1.0)
    for risk in (oom, timeout):
        if risk is not None:
            result *= 1.0 - clamp(risk)
    return clamp(result)


def _monetary_cost(
    gpu_seconds: float | None,
    network_bytes: float,
    storage_bytes: float,
    *,
    fixed: float,
    gpu_rate: float,
    network_rate: float,
    storage_rate: float,
) -> float | None:
    if gpu_seconds is None:
        return None
    return (
        fixed
        + gpu_seconds * gpu_rate
        + network_bytes * network_rate
        + storage_bytes * storage_rate
    )


def _applicability(action_type: str) -> str:
    normalized = action_type.lower()
    if normalized in {"route", "admit", "fallback"}:
        return "routing"
    if normalized in {"keep", "offload", "prefetch", "migrate"}:
        return "kv"
    return normalized or "unknown"


__all__ = ["AnalyticalPredictionModel", "PredictionModel"]
