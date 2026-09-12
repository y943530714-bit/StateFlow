"""Dependency-free HTTP and Prometheus source-client primitives."""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
import re
from typing import Any, Callable, Mapping
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class SourceClientError(RuntimeError):
    pass


class SourceHTTPError(SourceClientError):
    def __init__(self, url: str, status_code: int) -> None:
        super().__init__(f"source request failed for {url}: HTTP {status_code}")
        self.url = url
        self.status_code = status_code


TextFetcher = Callable[[str, Mapping[str, str], float], str]
JSONFetcher = Callable[[str, Mapping[str, str], float], Any]


def fetch_text(
    url: str,
    headers: Mapping[str, str] | None = None,
    timeout: float = 2.0,
) -> str:
    request = Request(url, headers=dict(headers or {}), method="GET")
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8")
    except HTTPError as exc:
        raise SourceHTTPError(url, exc.code) from exc
    except Exception as exc:
        raise SourceClientError(f"source request failed for {url}: {type(exc).__name__}") from exc


def fetch_json(
    url: str,
    headers: Mapping[str, str] | None = None,
    timeout: float = 2.0,
) -> Any:
    try:
        return json.loads(fetch_text(url, headers, timeout))
    except json.JSONDecodeError as exc:
        raise SourceClientError(f"source returned invalid JSON for {url}") from exc


@dataclass(frozen=True)
class PrometheusSample:
    name: str
    labels: Mapping[str, str]
    value: float


@dataclass(frozen=True)
class PrometheusSnapshot:
    samples: tuple[PrometheusSample, ...]

    def values(
        self,
        name: str,
        labels: Mapping[str, str] | None = None,
    ) -> tuple[float, ...]:
        required = labels or {}
        return tuple(
            sample.value
            for sample in self.samples
            if sample.name == name
            and all(sample.labels.get(key) == value for key, value in required.items())
        )

    def aggregate(
        self,
        name: str,
        *,
        labels: Mapping[str, str] | None = None,
        mode: str = "sum",
    ) -> float | None:
        values = self.values(name, labels)
        if not values:
            return None
        if mode == "sum":
            return sum(values)
        if mode == "max":
            return max(values)
        if mode == "avg":
            return sum(values) / len(values)
        raise ValueError(f"unsupported aggregation mode: {mode}")

    def histogram_quantile(
        self,
        base_name: str,
        quantile: float,
        *,
        labels: Mapping[str, str] | None = None,
    ) -> float | None:
        if not 0.0 <= quantile <= 1.0:
            raise ValueError("quantile must be in [0, 1]")
        buckets: dict[float, float] = {}
        required = labels or {}
        for sample in self.samples:
            if sample.name != base_name + "_bucket":
                continue
            if not all(sample.labels.get(key) == value for key, value in required.items()):
                continue
            raw_bound = sample.labels.get("le")
            if raw_bound is None:
                continue
            bound = math.inf if raw_bound in {"+Inf", "Inf"} else float(raw_bound)
            buckets[bound] = buckets.get(bound, 0.0) + sample.value
        if not buckets:
            return None
        ordered = sorted(buckets.items())
        total = ordered[-1][1]
        if total <= 0:
            return 0.0
        target = total * quantile
        previous_bound = 0.0
        previous_count = 0.0
        for bound, count in ordered:
            if count < target:
                if math.isfinite(bound):
                    previous_bound = bound
                previous_count = count
                continue
            if not math.isfinite(bound):
                return previous_bound
            bucket_count = count - previous_count
            if bucket_count <= 0:
                return bound
            fraction = (target - previous_count) / bucket_count
            return previous_bound + (bound - previous_bound) * fraction
        return previous_bound


_SAMPLE = re.compile(
    r"^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{(.*)\})?\s+([^\s]+)(?:\s+\d+)?$"
)
_LABEL = re.compile(r'(\w+)\s*=\s*"((?:\\.|[^"\\])*)"(?:\s*,\s*|$)')


def parse_prometheus(text: str) -> PrometheusSnapshot:
    samples: list[PrometheusSample] = []
    for line_number, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _SAMPLE.match(line)
        if match is None:
            raise SourceClientError(f"invalid Prometheus sample at line {line_number}")
        name, raw_labels, raw_value = match.groups()
        try:
            value = float(raw_value)
        except ValueError as exc:
            raise SourceClientError(
                f"invalid Prometheus value at line {line_number}"
            ) from exc
        labels = _parse_labels(raw_labels or "", line_number)
        samples.append(PrometheusSample(name, labels, value))
    return PrometheusSnapshot(tuple(samples))


def _parse_labels(raw: str, line_number: int) -> dict[str, str]:
    if not raw:
        return {}
    labels: dict[str, str] = {}
    position = 0
    while position < len(raw):
        match = _LABEL.match(raw, position)
        if match is None:
            raise SourceClientError(f"invalid Prometheus labels at line {line_number}")
        key, value = match.groups()
        labels[key] = (
            value.replace(r"\n", "\n").replace(r'\"', '"').replace(r"\\", "\\")
        )
        position = match.end()
    return labels


def stable_watermark(payload: str) -> str:
    import hashlib

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


__all__ = [
    "JSONFetcher",
    "PrometheusSample",
    "PrometheusSnapshot",
    "SourceClientError",
    "SourceHTTPError",
    "TextFetcher",
    "fetch_json",
    "fetch_text",
    "parse_prometheus",
    "stable_watermark",
]
