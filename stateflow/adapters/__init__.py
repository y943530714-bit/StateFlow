"""Semantic adapters that project existing components into State Plane."""

from .base import AdapterPublishReport, StatePlaneAdapter, component_ref, entity_ref
from .bridge import MaterializedRequestView, ObservabilityBridge
from .clients import (
    CollectionReport,
    DCGMSourceClient,
    KVMetadataSourceClient,
    KubernetesSourceClient,
    VLLMSourceClient,
)
from .dcgm import DCGMStateAdapter, HardwareObservation
from .gateway import GatewayStateProjector
from .identity import CorrelationResolver, IdentityBinding, IdentityConflict
from .kubernetes import DeploymentObservation, KubernetesStateAdapter
from .kv import KVLocation, KVObservation, KVStateAdapter
from .runtime import RuntimeObservation, RuntimeStateAdapter
from .runner import PollingAdapterRunner, RunnerState
from .source import PrometheusSample, PrometheusSnapshot, SourceClientError, parse_prometheus

__all__ = [
    "AdapterPublishReport",
    "CorrelationResolver",
    "CollectionReport",
    "DCGMStateAdapter",
    "DCGMSourceClient",
    "DeploymentObservation",
    "GatewayStateProjector",
    "HardwareObservation",
    "IdentityBinding",
    "IdentityConflict",
    "KVLocation",
    "KVMetadataSourceClient",
    "KVObservation",
    "KVStateAdapter",
    "KubernetesStateAdapter",
    "KubernetesSourceClient",
    "MaterializedRequestView",
    "ObservabilityBridge",
    "PollingAdapterRunner",
    "PrometheusSample",
    "PrometheusSnapshot",
    "RuntimeObservation",
    "RuntimeStateAdapter",
    "RunnerState",
    "SourceClientError",
    "StatePlaneAdapter",
    "component_ref",
    "entity_ref",
    "parse_prometheus",
    "VLLMSourceClient",
]
