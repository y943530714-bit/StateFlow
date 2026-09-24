"""StateFlow v1: four control-plane modules and a provider-facing gateway.

Older research APIs are still importable from their original subpackages and
resolved lazily here for callers that used the old root-level exports.
"""

from importlib import import_module

from .action_catalog import Action, ActionCatalog, ActionDefinition, ActionDispatchError
from .control_plane import ControlPlane
from .interface import UnifiedStateInterface
from .planner import Planner
from .state_manager import StateManager, TargetRegistry

__all__ = [
    "Action", "ActionCatalog", "ActionDefinition", "ActionDispatchError",
    "ControlPlane", "Planner", "StateManager", "TargetRegistry",
    "UnifiedStateInterface",
]

_LEGACY_EXPORTS = {
    "gateway": ("GatewayResponse", "RequestGateway", "normalize_request"),
    "adapters": ("CorrelationResolver", "DeploymentObservation", "HardwareObservation",
                 "KVLocation", "KVObservation", "ObservabilityBridge", "RuntimeObservation"),
    "scheduler": ("NoFeasibleTarget", "RoutingControlBundle", "RoutingDecision",
                  "SchedulerConfig", "routing_control_bundle"),
    "state": ("AgentPhase", "AgentState", "CanonicalKeyRegistry", "GraphEntity",
              "GraphKind", "InMemoryStatePlane", "HarnessSchedulingView", "RelationUpdate",
              "SnapshotRequest", "SourceAuthority", "StateUpdate", "StatePlaneHTTPClient",
              "TargetCandidate", "build_scheduling_view", "new_agent_state"),
}


def __getattr__(name: str):
    for module, exports in _LEGACY_EXPORTS.items():
        if name in exports:
            return getattr(import_module(f".{module}", __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
