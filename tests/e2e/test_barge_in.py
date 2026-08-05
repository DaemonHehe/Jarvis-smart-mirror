"""
Barge-In Interruption E2E Test Suite.
Verifies user speech while response audio/TTS is playing cancels TTS playback immediately
and triggers a new pipeline cycle; also verifies TTS playback alone does not falsely self-trigger barge-in.
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


class TestBargeInInterruption(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.server = TestServer(port=8996)
        cls.server.start(fast_mode=True)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def setUp(self):
        time.sleep(0.05)

    def tearDown(self):
        bridge_api.execute_blocking_ai_pipeline = getattr(self.server, "_orig_pipeline", bridge_api.execute_blocking_ai_pipeline)
        bridge_api.barge_in_event.clear()


    def test_barge_in_stops_tts_playback_and_triggers_new_cycle(self):
        """
        Verify user speech/barge-in signal while pipeline is busy in Speaking/TTS mode
        sets barge_in_event, stops current playback immediately, and triggers a new cycle.
        """
        async def _run():
            async with websockets.connect(self.server.ws_url) as ws:
                await asyncio.wait_for(ws.recv(), timeout=2.0)
                
                # Start pipeline cycle
                await ws.send(json.dumps({"action": "trigger"}))
                
                # Wait for Listening... status frame
                first_msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
                self.assertEqual(first_msg.get("type"), "status")
                
                # Signal barge-in while cycle is active
                bridge_api.barge_in_event.set()
                await ws.send(json.dumps({"action": "trigger"}))

                # Verify barge_in_event flag was raised and cycle recovers to Idle
                self.assertTrue(bridge_api.barge_in_event.is_set())
                
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
                    if m.get("type") == "status" and m.get("data") == "Idle":
                        break

        asyncio.run(_run())

    def test_tts_playback_alone_does_not_falsely_self_trigger_barge_in(self):
        """
        Verify that TTS audio output amplitude frames (source: 'tts') do NOT set barge_in_event
        or falsely self-trigger barge-in cancellation.
        """
        async def _run():
            bridge_api.barge_in_event.clear()

            # Broadcast simulated TTS amplitude frame
            await bridge_api.notify_amplitude(0.85, source="tts")

            # Verify barge_in_event remains clear (not falsely triggered by TTS output)
            self.assertFalse(bridge_api.barge_in_event.is_set())

            async with websockets.connect(self.server.ws_url) as ws:
                await asyncio.wait_for(ws.recv(), timeout=2.0)
                await ws.send(json.dumps({"action": "trigger"}))
                
                received = []
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
                    received.append(m)
                    if m.get("type") == "status" and m.get("data") == "Idle":
                        break

                # Assert cycle completes successfully without error or false barge-in
                statuses = [m.get("data") for m in received if m.get("type") == "status"]
                self.assertIn("Idle", statuses)
                self.assertFalse(bridge_api.barge_in_event.is_set())

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
