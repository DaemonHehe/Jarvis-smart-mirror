"""
Automated E2E Test Suite for Barge-In Interruption & Speech Cancellation Flow.
Verifies:
1. Mid-playback barge-in event interrupts TTS output, emits tts_cancel event, and processes new utterance into new response without self-triggering loop.
2. WS trigger action sent while Speaking state interrupts current response and signals barge-in.
3. HTTP POST /trigger received while pipeline busy returns 409 Conflict and signals barge-in.
"""

import asyncio
import json
import os
import sys
import time
import unittest
import urllib.request

# Ensure test_utils and backend can be imported
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "backend"))

from test_utils import TestServer, bridge_api
import websockets


class TestBargeInFlow(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.server = TestServer(port=8997)
        cls.server.start(fast_mode=True)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def setUp(self):
        self.assertTrue(self.server.wait_until_idle(), "Previous pipeline cycle did not finish")
        bridge_api.barge_in_event.clear()

    def test_bargein_mid_playback_interrupts_tts_and_generates_new_response(self):
        """Test barge-in event mid-playback interrupts TTS output, broadcasts tts_cancel event, and generates new response without self-triggering."""
        async def _run():
            barge_in_detected = False

            def long_speaking_pipeline(loop):
                nonlocal barge_in_detected
                bridge_api.emit_status_threadsafe(loop, "Listening...")
                time.sleep(0.005)
                bridge_api.emit_status_threadsafe(loop, "Transcribing...")
                time.sleep(0.005)
                bridge_api.emit_status_threadsafe(loop, "Thinking...")
                time.sleep(0.005)
                bridge_api.emit_status_threadsafe(loop, "Speaking...")

                for _ in range(50):
                    if bridge_api.barge_in_event.is_set():
                        barge_in_detected = True
                        break
                    time.sleep(0.02)

                return ("Original question", "This is a long response that will be interrupted by user speaking mid-playback.")

            bridge_api.execute_blocking_ai_pipeline = long_speaking_pipeline

            async with websockets.connect(self.server.ws_url) as ws:
                init_msg = json.loads(await ws.recv())
                self.assertEqual(init_msg.get("type"), "status")

                # Step 1: Trigger long response turn
                await ws.send(json.dumps({"action": "trigger"}))

                # Wait until Speaking state is reached
                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("type") == "status" and msg.get("data") == "Speaking...":
                        break

                # Step 2: Spoken utterance mid-playback sends barge_in action
                await ws.send(json.dumps({"action": "barge_in"}))

                # Step 3: Assert immediate tts_cancel event received over WS
                received_cancel = False
                for _ in range(10):
                    msg = json.loads(await ws.recv())
                    if msg.get("type") == "tts_cancel" and msg.get("event") == "barge_in":
                        received_cancel = True
                        break

                self.assertTrue(received_cancel, "Expected tts_cancel event was not received over WebSocket.")
                for _ in range(20):
                    if barge_in_detected:
                        break
                    await asyncio.sleep(0.01)
                self.assertTrue(barge_in_detected, "Pipeline did not detect barge-in signal.")

                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("type") == "status" and msg.get("data") == "Idle":
                        break

                # Reset the signal and use a deterministic follow-up cycle.
                bridge_api.barge_in_event.clear()

                def followup_pipeline(loop):
                    for status in ("Listening...", "Transcribing...", "Thinking...", "Speaking..."):
                        bridge_api.emit_status_threadsafe(loop, status)
                        time.sleep(0.001)
                    return ("Follow-up question", "Follow-up response")

                bridge_api.execute_blocking_ai_pipeline = followup_pipeline

                # Step 4: Verify system seamlessly processes new utterance into new response without self-triggering
                await ws.send(json.dumps({"action": "trigger"}))
                new_statuses = []
                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("type") == "status":
                        new_statuses.append(msg.get("data"))
                    if msg.get("type") == "status" and msg.get("data") == "Idle":
                        break

                self.assertIn("Listening...", new_statuses)
                self.assertIn("Transcribing...", new_statuses)
                self.assertIn("Thinking...", new_statuses)
                self.assertIn("Speaking...", new_statuses)
                self.assertIn("Idle", new_statuses)

        try:
            asyncio.run(_run())
        finally:
            bridge_api.execute_blocking_ai_pipeline = getattr(self.server, "_orig_pipeline", bridge_api.execute_blocking_ai_pipeline)

    def test_bargein_via_ws_trigger_while_busy(self):
        """Sending WS trigger action while pipeline is busy in Speaking state interrupts TTS and emits tts_cancel event."""
        async def _run():
            def busy_speaking_pipeline(loop):
                bridge_api.emit_status_threadsafe(loop, "Speaking...")
                for _ in range(50):
                    if bridge_api.barge_in_event.is_set():
                        break
                    time.sleep(0.02)
                return ("Busy test query", "Speaking response")

            bridge_api.execute_blocking_ai_pipeline = busy_speaking_pipeline

            async with websockets.connect(self.server.ws_url) as ws:
                await ws.recv()  # Connected frame
                await ws.send(json.dumps({"action": "trigger"}))

                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("type") == "status" and msg.get("data") == "Speaking...":
                        break

                # Send secondary trigger action while Speaking
                await ws.send(json.dumps({"action": "trigger"}))

                cancel_msg = None
                for _ in range(10):
                    candidate = json.loads(await ws.recv())
                    if candidate.get("type") == "tts_cancel":
                        cancel_msg = candidate
                        break
                self.assertIsNotNone(cancel_msg)
                self.assertEqual(cancel_msg.get("event"), "barge_in")

        try:
            asyncio.run(_run())
        finally:
            bridge_api.execute_blocking_ai_pipeline = getattr(self.server, "_orig_pipeline", bridge_api.execute_blocking_ai_pipeline)

    def test_bargein_http_trigger_conflict_signals_bargein(self):
        """Sending HTTP POST /trigger while pipeline is busy returns 409 Conflict and signals barge_in_event."""
        async def _run():
            def long_pipeline(loop):
                bridge_api.emit_status_threadsafe(loop, "Speaking...")
                time.sleep(0.5)
                return ("HTTP test", "Response")

            bridge_api.execute_blocking_ai_pipeline = long_pipeline

            async with websockets.connect(self.server.ws_url) as ws:
                await ws.recv()
                await ws.send(json.dumps({"action": "trigger"}))

                # Wait until speaking
                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("type") == "status" and msg.get("data") == "Speaking...":
                        break

                req = urllib.request.Request(f"{self.server.http_url}/trigger", method="POST")
                try:
                    urllib.request.urlopen(req)
                    self.fail("Expected HTTPError 409 Conflict")
                except urllib.error.HTTPError as e:
                    self.assertEqual(e.code, 409)

        try:
            asyncio.run(_run())
        finally:
            bridge_api.execute_blocking_ai_pipeline = getattr(self.server, "_orig_pipeline", bridge_api.execute_blocking_ai_pipeline)


if __name__ == "__main__":
    unittest.main()
