from __future__ import annotations

import unittest

from stateflow.harness import AsyncStateReporter, GenericProxyHarnessAdapter


class HarnessTests(unittest.TestCase):
    def test_generic_proxy_adapter_keeps_state_metadata_only(self) -> None:
        adapter = GenericProxyHarnessAdapter()
        snapshot = adapter.snapshot(
            {
                "session_id": "s1",
                "task_id": "t1",
                "prompt": "secret prompt",
                "messages": [{"role": "user", "content": "secret"}],
                "agent_type": "coding",
            }
        )
        event = adapter.on_task_start({"session_id": "s1", "task_id": "t1", "agent_type": "coding"})

        self.assertNotIn("prompt", snapshot)
        self.assertNotIn("messages", snapshot)
        self.assertEqual(event.session_id, "s1")
        self.assertEqual(event.event_type, "TASK_STARTED")

    def test_reporter_failures_do_not_escape_publish(self) -> None:
        def failing_sink(event):
            raise RuntimeError("sink unavailable")

        reporter = AsyncStateReporter(failing_sink, max_queue=4)
        try:
            self.assertTrue(reporter.publish(GenericProxyHarnessAdapter().on_task_start({"session_id": "s1"})))
        finally:
            reporter.close(timeout=2)
        self.assertGreaterEqual(reporter.stats.failed, 1)


if __name__ == "__main__":
    unittest.main()
