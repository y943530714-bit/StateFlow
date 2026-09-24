"""Verify the four public modules cooperate without a live inference engine."""

import unittest

from stateflow.action_catalog import ActionCatalog, ActionDispatchError
from stateflow.demo import build_demo_gateway
from stateflow.gateway.normalizer.request import normalize_request
from stateflow.interface import UnifiedStateInterface
from stateflow.planner import Planner
from stateflow.state.event import AgentStateEvent
from stateflow.state_manager import StateManager


class ModuleBoundaryTests(unittest.TestCase):
    def test_planning_is_pure_and_dispatch_feedback_returns_to_state_manager(self):
        gateway = build_demo_gateway()
        control = gateway.control_plane
        self.assertIsInstance(control.state_manager, StateManager)
        self.assertIsInstance(control.planner, Planner)
        self.assertIsInstance(control.catalog, ActionCatalog)
        self.assertIsInstance(control.interface, UnifiedStateInterface)

        request = normalize_request(
            "/v1/chat/completions",
            {"model": "logical-agent", "messages": [{"role": "user", "content": "hello"}]},
            {"x-stateflow-program-id": "program-1"},
        )
        control.ingest(AgentStateEvent(
            event_type="TASK_STARTED", session_id="program-1", source="harness",
            payload={"identity": {"harness_type": "coding-agent"}},
        ))
        decision, action = control.plan_route(request, gateway.targets.all())
        self.assertEqual(action.decision_id, decision.decision_id)
        self.assertEqual(action.action_id, "route_model")
        self.assertFalse(any(e.event_type.startswith("ACTION_") for e in gateway.state_store.events_for("program-1")))

        invoked = []
        result = control.dispatch(action, lambda: invoked.append(action.target) or "ok", request=request)
        self.assertEqual(result, "ok")
        self.assertEqual(invoked, [action.target])
        self.assertEqual(
            [e.event_type for e in gateway.state_store.events_for("program-1") if e.event_type.startswith("ACTION_")],
            ["ACTION_DISPATCHED", "ACTION_SUCCEEDED"],
        )
        with self.assertRaises(ActionDispatchError):
            control.dispatch(action, lambda: invoked.append("duplicate"), request=request)
        self.assertEqual(invoked, [action.target])

    def test_catalog_without_route_action_cannot_plan(self):
        from stateflow.scheduler.types import NoFeasibleTarget

        gateway = build_demo_gateway()
        gateway.control_plane.catalog = ActionCatalog([])
        gateway.control_plane.planner.catalog = gateway.control_plane.catalog
        request = normalize_request(
            "/v1/chat/completions", {"model": "agent", "messages": []},
            {"x-stateflow-program-id": "program-2"},
        )
        with self.assertRaises(NoFeasibleTarget):
            gateway.control_plane.plan_route(request, gateway.targets.all())
