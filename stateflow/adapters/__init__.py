"""Semantic adapters that project existing components into State Plane."""

from .base import AdapterPublishReport, StatePlaneAdapter, component_ref, entity_ref
from .bridge import MaterializedRequestView, ObservabilityBridge
from .dcgm import DCGMStateAdapter, HardwareObservation
from .gateway import GatewayStateProjector
from .identity import CorrelationResolver, IdentityBinding, IdentityConflict
from .kubernetes import DeploymentObservation, KubernetesStateAdapter
from .kv import KVLocation, KVObservation, KVStateAdapter
from .runtime import RuntimeObservation, RuntimeStateAdapter

__all__ = [
    "AdapterPublishReport",
    "CorrelationResolver",
    "DCGMStateAdapter",
    "DeploymentObservation",
    "GatewayStateProjector",
    "HardwareObservation",
    "IdentityBinding",
    "IdentityConflict",
    "KVLocation",
    "KVObservation",
    "KVStateAdapter",
    "KubernetesStateAdapter",
    "MaterializedRequestView",
    "ObservabilityBridge",
    "RuntimeObservation",
    "RuntimeStateAdapter",
    "StatePlaneAdapter",
    "component_ref",
    "entity_ref",
]
