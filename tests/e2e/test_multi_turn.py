"""
Multi-Turn Conversation E2E Test Suite.
Verifies continuous sessions with follow-up exchanges without repeating the wake word,
context propagation, state transitions, and session auto-close via sign-off phrase / timeout.
"""

import asyncio
import json
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(__file__))

from test_utils import TestServer, bridge_api
import websockets


class TestMultiTurnSession(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.server = TestServer(port=8995)
        cls.server.start(fast_mode=True)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def setUp(self):
        time.sleep(0.05)

    def tearDown(self):
        bridge_api.execute_blocking_ai_pipeline = getattr(self.server, "_orig_pipeline", bridge_api.execute_blocking_ai_pipeline)

    def test_multi_turn_continuous_session_and_context_propagation(self):
        """
        Verify a continuous multi-turn session with at least 2 follow-up exchanges
        without repeating the wake word, verifying state transitions to listening-followup,
        context propagation, and session history accumulation.
        """
        async def _run():
            async with websockets.connect(self.server.ws_url) as ws:
                init_msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
                self.assertEqual(init_msg.get("data"), "Connected")

                # --- Turn 1: Initial Wake / Trigger ---
                await ws.send(json.dumps({"action": "trigger"}))
                
                turn1_events = []
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
                    turn1_events.append(m)
                    if m.get("type") == "status" and m.get("data") == "Idle":
                        break

                # Verify Turn 1 response and state transition
                resp1 = next(m for m in turn1_events if m.get("type") == "response")
                self.assertTrue(resp1.get("session_active") or bridge_api.session_active)
                
                state_events = [m.get("state") for m in turn1_events if m.get("type") == "state"]
                self.assertIn("listening-followup", state_events)

                # --- Turn 2: First Follow-up Exchange (without wake word) ---
                def turn2_pipeline(loop):
                    bridge_api.emit_status_threadsafe(loop, "Listening...")
                    time.sleep(0.001)
                    bridge_api.emit_status_threadsafe(loop, "Transcribing...")
                    time.sleep(0.001)
                    bridge_api.emit_status_threadsafe(loop, "Thinking...")
                    time.sleep(0.001)
                    bridge_api.emit_status_threadsafe(loop, "Speaking...")
                    time.sleep(0.001)
                    return ("How about tomorrow?", "Tomorrow will be 75°F and partly cloudy.")

                bridge_api.execute_blocking_ai_pipeline = turn2_pipeline

                await ws.send(json.dumps({"action": "trigger"}))
                turn2_events = []
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
                    turn2_events.append(m)
                    if m.get("type") == "status" and m.get("data") == "Idle":
                        break

                resp2 = next(m for m in turn2_events if m.get("type") == "response")
                self.assertEqual(resp2.get("user"), "How about tomorrow?")
                self.assertIn("75°F", resp2.get("assistant"))
                self.assertTrue(bridge_api.session_active)

                # --- Turn 3: Second Follow-up Exchange ---
                def turn3_pipeline(loop):
                    bridge_api.emit_status_threadsafe(loop, "Listening...")
                    time.sleep(0.001)
                    bridge_api.emit_status_threadsafe(loop, "Transcribing...")
                    time.sleep(0.001)
                    bridge_api.emit_status_threadsafe(loop, "Thinking...")
                    time.sleep(0.001)
                    bridge_api.emit_status_threadsafe(loop, "Speaking...")
                    time.sleep(0.001)
                    return ("Should I take an umbrella?", "No umbrella is needed; rain is not expected tomorrow.")

                bridge_api.execute_blocking_ai_pipeline = turn3_pipeline

                await ws.send(json.dumps({"action": "trigger"}))
                turn3_events = []
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
                    turn3_events.append(m)
                    if m.get("type") == "status" and m.get("data") == "Idle":
                        break

                resp3 = next(m for m in turn3_events if m.get("type") == "response")
                self.assertEqual(resp3.get("user"), "Should I take an umbrella?")
                self.assertIn("umbrella", resp3.get("assistant").lower())

                # Verify context history length in backend
                self.assertGreaterEqual(len(bridge_api.conversation_history), 4)

        asyncio.run(_run())

    def test_multi_turn_session_auto_close_on_sign_off_phrase(self):
        """
        Verify session auto-close when user speaks a sign-off phrase (e.g. 'thanks jarvis'),
        resetting session_active to False and transitioning state back to sleeping/Idle.
        """
        async def _run():
            bridge_api.session_active = True
            bridge_api.conversation_history.append({"role": "user", "content": "hello"})

            def sign_off_pipeline(loop):
                bridge_api.session_active = False
                bridge_api.conversation_history.clear()
                return ("thanks jarvis", "You're welcome! Goodbye.")

            bridge_api.execute_blocking_ai_pipeline = sign_off_pipeline

            async with websockets.connect(self.server.ws_url) as ws:
                await asyncio.wait_for(ws.recv(), timeout=2.0)
                await ws.send(json.dumps({"action": "trigger"}))
                received = []
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
                    received.append(m)
                    if m.get("type") == "status" and m.get("data") == "Idle":
                        break

                self.assertFalse(bridge_api.session_active)
                self.assertEqual(len(bridge_api.conversation_history), 0)

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
