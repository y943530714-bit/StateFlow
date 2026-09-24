from __future__ import annotations

from datetime import timedelta
import unittest

from stateflow.state_manager.event import AgentStateEvent
from stateflow.state_manager.schema import AgentPhase, ContextSegment, ExecutionNode, ToolStatus, utcnow
from stateflow.state_manager.store import InMemoryStateStore


class StatePlaneTests(unittest.TestCase):
    def test_events_update_snapshot_and_hot_view(self) -> None:
        store = InMemoryStateStore()
        events = [
            AgentStateEvent(
                "TASK_STARTED",
                "s1",
                task_id="t1",
                sequence=1,
                payload={
                    "identity": {"tenant_id": "tenant-a", "required_capabilities": ["reasoning"]},
                    "task": {"task_type": "coding"},
                },
            ),
            AgentStateEvent(
                "TURN_STARTED",
                "s1",
                task_id="t1",
                turn_id=1,
                sequence=2,
                payload={"context": {"prompt_tokens": 120}, "prediction": {"continuation_probability": 0.8}},
            ),
            AgentStateEvent(
                "MODEL_REQUESTED",
                "s1",
                task_id="t1",
                turn_id=1,
                sequence=3,
                payload={"logical_model": "logical-agent", "predicted_output_tokens": 32},
            ),
            AgentStateEvent(
                "TOOL_STARTED",
                "s1",
                task_id="t1",
                turn_id=1,
                sequence=4,
                payload={"tool_id": "tool-1", "tool_name": "search", "status": "RUNNING"},
            ),
            AgentStateEvent(
                "TOOL_RESULT_READY",
                "s1",
                task_id="t1",
                turn_id=1,
                sequence=5,
                payload={"tool_id": "tool-1", "result_tokens": 15},
            ),
        ]
        results = store.append_events(events)

        self.assertTrue(all(result.accepted for result in results))
        state = store.get_state("s1", "t1")
        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state.identity.tenant_id, "tenant-a")
        self.assertEqual(state.task.task_type, "coding")
        self.assertEqual(state.lifecycle.phase, AgentPhase.TOOL_RESULT_READY)
        self.assertEqual(state.context.total_tokens, 135)
        self.assertEqual(state.tools.active_tools[0].status, ToolStatus.RESULT_READY)

        view = store.get_scheduling_view("s1", "t1")
        self.assertEqual(view.prompt_tokens, 135)
        self.assertEqual(view.required_capabilities, {"reasoning"})
        self.assertEqual(view.session_turn, 1)

    def test_event_order_is_idempotent_and_event_mapping_is_safe(self) -> None:
        store = InMemoryStateStore()
        first = AgentStateEvent.from_mapping(
            {
                "event_type": "TASK_STARTED",
                "session_id": "s1",
                "sequence": 1,
                "payload": {},
            }
        )
        self.assertEqual(first.event_type, "TASK_STARTED")
        accepted = store.append_event(first)
        duplicate = store.append_event(
            AgentStateEvent.from_mapping(
                {"event": "TASK_STARTED", "session_id": "s1", "sequence": 1, "payload": {}}
            )
        )
        stale = store.append_event(
            AgentStateEvent("TASK_STARTED", "s1", epoch=-1, sequence=1, payload={})
        )

        self.assertTrue(accepted.accepted)
        self.assertTrue(duplicate.duplicate)
        self.assertTrue(stale.stale)

    def test_patch_converts_enums_and_empty_list_dataclasses(self) -> None:
        store = InMemoryStateStore()
        store.append_event(
            AgentStateEvent(
                "STATE_PATCH",
                "s1",
                sequence=1,
                payload={
                    "lifecycle": {"phase": "BLOCKED"},
                    "execution": {"nodes": [{"node_id": "n1", "critical": True}]},
                    "context": {"segments": [{"segment_id": "seg-1", "token_count": 8}]},
                },
            )
        )
        state = store.get_state("s1")
        assert state is not None
        self.assertEqual(state.lifecycle.phase, AgentPhase.BLOCKED)
        self.assertIsInstance(state.execution.nodes[0], ExecutionNode)
        self.assertIsInstance(state.context.segments[0], ContextSegment)
        self.assertTrue(state.execution.nodes[0].critical)

    def test_freshness_metadata_is_preserved_and_observer_failure_is_nonfatal(self) -> None:
        store = InMemoryStateStore()
        store.subscribe(lambda event, state: (_ for _ in ()).throw(RuntimeError("observer")))
        result = store.append_event(
            AgentStateEvent(
                "STATE_PATCH",
                "s1",
                sequence=1,
                observed_at=utcnow() - timedelta(seconds=10),
                ttl=timedelta(seconds=1),
                confidence=0.8,
                payload={"prediction": {"continuation_probability": 0.9}},
            )
        )

        self.assertTrue(result.accepted)
        state = store.get_state("s1")
        assert state is not None
        self.assertFalse(state.field_meta["prediction.continuation_probability"].is_fresh())
        self.assertLess(stateflow_view_freshness(store), 1.0)


def stateflow_view_freshness(store: InMemoryStateStore) -> float:
    return store.get_scheduling_view("s1").state_freshness


if __name__ == "__main__":
    unittest.main()
