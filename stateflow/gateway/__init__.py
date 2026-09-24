"""Provider-facing request gateway; keep protocol types importable independently."""

from .normalizer import ProviderNeutralRequest, normalize_request
from ..state_manager import TargetRegistry

__all__ = ["GatewayResponse", "ProviderNeutralRequest", "RequestGateway", "TargetRegistry", "normalize_request"]


def __getattr__(name: str):
    if name in {"GatewayResponse", "RequestGateway"}:
        from .service import GatewayResponse, RequestGateway
        return {"GatewayResponse": GatewayResponse, "RequestGateway": RequestGateway}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
