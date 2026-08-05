"""
Automated E2E Test Suite for Multi-Turn Conversational Session Management & Silence Timeout.
Verifies:
1. Multi-turn conversational session with at least 2 follow-up exchanges without wake word.
2. Context retention across multiple turns in conversation history.
3. Auto-closure of session to 'sleeping' after silence timeout (8-12s).
4. Immediate session termination upon receiving sign-off phrase.
"""

import asyncio
import json
import os
import sys
import time
import unittest

# Ensure test_utils and backend can be imported
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "backend"))

from test_utils import TestServer, bridge_api
import websockets


class TestMultiTurnSession(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.server = TestServer(port=8996)
        cls.server.start(fast_mode=True)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def setUp(self):
        time.sleep(0.05)
        bridge_api.session_active = False
        bridge_api.conversation_history.clear()

    def test_multiturn_at_least_two_followups_and_context_retention(self):
        """Test multi-turn conversational session with at least 2 follow-up exchanges without wake word and verify context retention."""
        async def _run():
            turn_idx = 0
            queries = [
                ("What's the weather in Seattle?", "It's 65°F and rainy in Seattle."),
                ("What about tomorrow?", "Tomorrow in Seattle it will be 68°F and cloudy."),
                ("Should I bring an umbrella?", "Yes, bring an umbrella as rain is expected in Seattle.")
            ]

            def multi_turn_pipeline(loop):
                nonlocal turn_idx
                q, a = queries[turn_idx]
                turn_idx += 1
                bridge_api.emit_status_threadsafe(loop, "Listening...")
                time.sleep(0.001)
                bridge_api.emit_status_threadsafe(loop, "Transcribing...")
                time.sleep(0.001)
                bridge_api.emit_status_threadsafe(loop, "Thinking...")
                time.sleep(0.001)
                bridge_api.emit_status_threadsafe(loop, "Speaking...")
                time.sleep(0.001)
                return q, a

            bridge_api.execute_blocking_ai_pipeline = multi_turn_pipeline

            async with websockets.connect(self.server.ws_url) as ws:
                init_msg = json.loads(await ws.recv())
                self.assertEqual(init_msg.get("type"), "status")
                self.assertEqual(init_msg.get("data"), "Connected")

                # Turn 1: Initial query (starts session)
                await ws.send(json.dumps({"action": "trigger"}))
                t1_events = []
                while True:
                    msg = json.loads(await ws.recv())
                    t1_events.append(msg)
                    if msg.get("type") == "state" and msg.get("state") == "listening-followup":
                        break

                self.assertTrue(bridge_api.session_active)
                self.assertEqual(len(bridge_api.conversation_history), 2)
                self.assertEqual(bridge_api.conversation_history[0]["content"], queries[0][0])
                self.assertEqual(bridge_api.conversation_history[1]["content"], queries[0][1])

                # Turn 2: Follow-up 1 (without wake word)
                await ws.send(json.dumps({"action": "trigger"}))
                t2_events = []
                while True:
                    msg = json.loads(await ws.recv())
                    t2_events.append(msg)
                    if msg.get("type") == "state" and msg.get("state") == "listening-followup":
                        break

                self.assertTrue(bridge_api.session_active)
                self.assertEqual(len(bridge_api.conversation_history), 4)
                self.assertEqual(bridge_api.conversation_history[2]["content"], queries[1][0])
                self.assertEqual(bridge_api.conversation_history[3]["content"], queries[1][1])

                # Turn 3: Follow-up 2 (without wake word)
                await ws.send(json.dumps({"action": "trigger"}))
                t3_events = []
                while True:
                    msg = json.loads(await ws.recv())
                    t3_events.append(msg)
                    if msg.get("type") == "state" and msg.get("state") == "listening-followup":
                        break

                self.assertTrue(bridge_api.session_active)
                self.assertEqual(len(bridge_api.conversation_history), 6)
                self.assertEqual(bridge_api.conversation_history[4]["content"], queries[2][0])
                self.assertEqual(bridge_api.conversation_history[5]["content"], queries[2][1])

                # Verify responses carried session_active flag
                resp_t2 = [m for m in t2_events if m.get("type") == "response"][0]
                resp_t3 = [m for m in t3_events if m.get("type") == "response"][0]
                self.assertTrue(resp_t2.get("session_active"))
                self.assertTrue(resp_t3.get("session_active"))

        try:
            asyncio.run(_run())
        finally:
            bridge_api.execute_blocking_ai_pipeline = getattr(self.server, "_orig_pipeline", bridge_api.execute_blocking_ai_pipeline)

    def test_multiturn_silence_timeout_autocloses_session(self):
        """Verify session auto-closes to 'sleeping' state after silence timeout (8-12s)."""
        async def _run():
            async with websockets.connect(self.server.ws_url) as ws:
                await ws.recv()  # Connected frame

                # Start Turn 1
                await ws.send(json.dumps({"action": "trigger"}))
                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("type") == "state" and msg.get("state") == "listening-followup":
                        break

                self.assertTrue(bridge_api.session_active)

                # Wait for silence timeout event without sending any trigger
                timeout_state = None
                start_t = time.time()
                while time.time() - start_t < 15.0:
                    try:
                        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
                        if msg.get("type") == "state" and msg.get("state") == "sleeping":
                            timeout_state = "sleeping"
                            break
                    except asyncio.TimeoutError:
                        if not bridge_api.session_active:
                            timeout_state = "sleeping"
                            break

                self.assertEqual(timeout_state, "sleeping")
                self.assertFalse(bridge_api.session_active)

        asyncio.run(_run())

    def test_multiturn_sign_off_phrase_resets_session(self):
        """Verify sign-off phrase ('that's all', 'goodbye') terminates multi-turn session immediately."""
        async def _run():
            turn_count = 0

            def sign_off_pipeline(loop):
                nonlocal turn_count
                turn_count += 1
                bridge_api.emit_status_threadsafe(loop, "Listening...")
                time.sleep(0.001)
                bridge_api.emit_status_threadsafe(loop, "Transcribing...")
                time.sleep(0.001)
                bridge_api.emit_status_threadsafe(loop, "Thinking...")
                time.sleep(0.001)
                bridge_api.emit_status_threadsafe(loop, "Speaking...")
                time.sleep(0.001)

                if turn_count == 1:
                    return ("How are you?", "I am doing well, thank you!")
                else:
                    return ("that's all", "You're welcome! Goodbye.")

            bridge_api.execute_blocking_ai_pipeline = sign_off_pipeline

            async with websockets.connect(self.server.ws_url) as ws:
                await ws.recv()  # Connected frame

                # Turn 1: Regular query
                await ws.send(json.dumps({"action": "trigger"}))
                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("type") == "state" and msg.get("state") == "listening-followup":
                        break

                self.assertTrue(bridge_api.session_active)

                # Turn 2: Sign-off phrase
                await ws.send(json.dumps({"action": "trigger"}))
                turn2_states = []
                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("type") == "state":
                        turn2_states.append(msg.get("state"))
                        if msg.get("state") == "sleeping":
                            break

                self.assertIn("sleeping", turn2_states)
                self.assertFalse(bridge_api.session_active)
                self.assertEqual(len(bridge_api.conversation_history), 0)

        try:
            asyncio.run(_run())
        finally:
            bridge_api.execute_blocking_ai_pipeline = getattr(self.server, "_orig_pipeline", bridge_api.execute_blocking_ai_pipeline)


if __name__ == "__main__":
    unittest.main()
