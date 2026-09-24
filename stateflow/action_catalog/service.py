"""Action contracts and validation. This module never invokes components."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from stateflow.state_manager.schema import to_jsonable


@dataclass(frozen=True)
class Action:
    decision_id: str
    program_id: str
    action_id: str
    target: str
    params: dict[str, str]
    state_version: int

    def to_dict(self) -> dict:
        return to_jsonable(self)


@dataclass(frozen=True)
class ActionDefinition:
    action_id: str
    target_type: str
    required_params: tuple[str, ...]

    def to_dict(self) -> dict:
        return to_jsonable(self)


class ActionDispatchError(RuntimeError):
    """An action is invalid or has already been dispatched."""


class ActionCatalog:
    """List supported actions; execution remains with the component adapter."""

    def __init__(self, definitions: Iterable[ActionDefinition] | None = None) -> None:
        self._definitions = {
            item.action_id: item
            for item in (definitions if definitions is not None else (
                ActionDefinition("route_model", "request_gateway", ("model_id", "endpoint_id", "replica_id")),
            ))
        }

    def all(self) -> list[dict]:
        return [item.to_dict() for item in self._definitions.values()]

    def supports(self, action_id: str) -> bool:
        return action_id in self._definitions

    def validate(self, action: Action) -> None:
        definition = self._definitions.get(action.action_id)
        if definition is None:
            raise ActionDispatchError(f"unsupported action: {action.action_id}")
        if not action.decision_id or not action.program_id or not action.target:
            raise ActionDispatchError("decision_id, program_id and target are required")
        missing = [key for key in definition.required_params if not action.params.get(key)]
        if missing:
            raise ActionDispatchError(f"missing action parameters: {', '.join(missing)}")
