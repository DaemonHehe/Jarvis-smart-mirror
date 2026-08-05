"""
Adversarial Test Harness for FastAPI WebSocket Bridge (bridge_api.py)
Milestone 1 Protocol Edge-Case & Payload Resilience Suite

Tests:
1. Malformed WebSocket frames (raw bytes, non-JSON strings, arrays [1,2,3], unexpected types).
2. Abrupt socket disconnects mid-pipeline cycle (Listening, Transcribing, Speaking). Confirm connection manager cleanup.
3. Event sequence completeness & JSON payload schema compliance across 10 consecutive full pipeline cycles.
"""

import asyncio
import json
import logging
import os
import sys
import time
import urllib.request
import websockets
import uvicorn

# Unbuffer stdout/stderr logging
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# The backend currently uses flat sibling imports, so add its directory directly.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "backend"))
os.environ.setdefault("SKIP_MODEL_LOADING", "1")
import bridge_api

HOST = "127.0.0.1"
PORT = 8998
WS_URL = f"ws://{HOST}:{PORT}/ws"
HEALTH_URL = f"http://{HOST}:{PORT}/health"
TRIGGER_URL = f"http://{HOST}:{PORT}/trigger"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("AdversarialHarness")


async def fetch_health() -> dict:
    """Fetch GET /health JSON dict asynchronously without blocking the event loop."""
    def _do_get():
        req = urllib.request.Request(HEALTH_URL, method="GET")
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))
    return await asyncio.to_thread(_do_get)


async def wait_for_server_idle(timeout: float = 10.0) -> bool:
    """Wait until pipeline_busy is False."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            health = await fetch_health()
            if not health["pipeline_busy"]:
                await asyncio.sleep(0.3)
                return True
        except Exception:
            pass
        await asyncio.sleep(0.1)
    return False


async def receive_core_cycle(ws, timeout: float = 10.0) -> list[dict]:
    """Collect status/response frames through Idle, ignoring typed side-channel events."""
    core_events = []
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        remaining = deadline - asyncio.get_running_loop().time()
        event = json.loads(await asyncio.wait_for(ws.recv(), timeout=remaining))
        assert isinstance(event, dict) and isinstance(event.get("type"), str)
        if event["type"] in ("status", "response"):
            core_events.append(event)
        if event.get("type") == "status" and event.get("data") == "Idle":
            return core_events
    raise asyncio.TimeoutError("Pipeline cycle did not reach Idle")


async def run_suite_1_malformed_frames() -> dict:
    """Suite 1: Test Malformed WebSocket frames and connection resilience."""
    logger.info("=== SUITE 1: Malformed WebSocket Frames & Schema Edge Cases ===")
    results = {"subtests": [], "vulnerabilities": []}

    await wait_for_server_idle()

    # 1. Non-JSON strings, Arrays, Unexpected Types on a single persistent connection
    async with websockets.connect(WS_URL) as ws:
        init_frame = json.loads(await ws.recv())
        assert init_frame == {"type": "status", "data": "Connected"}
        logger.info("[PASS] Initial connection handshake verified ('Connected').")

        malformed_inputs = [
            ("Non-JSON string 'hello world'", "hello world"),
            ("Non-JSON string 'not json'", "NOT JSON AT ALL"),
            ("Unclosed JSON string", '{"action": "trigger"'),
            ("Empty string", ""),
            ("JSON Array [1, 2, 3]", "[1, 2, 3]"),
            ("JSON Array ['trigger']", '["trigger"]'),
            ("JSON Primitive int 12345", "12345"),
            ("JSON Primitive float 3.14159", "3.14159"),
            ("JSON Primitive bool true", "true"),
            ("JSON Primitive null", "null"),
            ("JSON Empty Object {}", "{}"),
            ("JSON Object with numeric action", '{"action": 123}'),
            ("JSON Object with null action", '{"action": null}'),
            ("JSON Object with bool action", '{"action": true}'),
            ("JSON Object with array action", '{"action": ["trigger"]}'),
            ("JSON Object with unexpected key", '{"foo": "bar"}'),
            ("JSON Object with unicode & nulls", '{"action": "trigger\u0000\uffff"}'),
        ]

        for desc, payload in malformed_inputs:
            await ws.send(payload)
            logger.info(f"  [PASS] Sent malformed payload: {desc}")
            # The hardened server acknowledges invalid JSON/actions. Drain any
            # such response so it cannot pollute the following valid sequence.
            try:
                response = json.loads(await asyncio.wait_for(ws.recv(), timeout=0.1))
                assert response.get("type") == "error"
            except asyncio.TimeoutError:
                pass
            results["subtests"].append((desc, "PASS"))

        # Verify connection resilience by sending valid trigger frame
        logger.info("Verifying connection resilience by sending valid trigger frame on same socket...")
        await ws.send(json.dumps({"action": "trigger"}))

        expected_sequence = [
            ("status", "Listening..."),
            ("status", "Transcribing..."),
            ("status", "Thinking..."),
            ("status", "Speaking..."),
            ("response", None),
            ("status", "Idle"),
        ]

        core_events = await receive_core_cycle(ws)
        for idx, (exp_type, exp_status) in enumerate(expected_sequence):
            evt = core_events[idx]
            if exp_type == "status":
                assert evt == {"type": "status", "data": exp_status}, f"Expected {exp_status}, got {evt}"
            elif exp_type == "response":
                assert evt.get("type") == "response" and "user" in evt and "assistant" in evt

        logger.info("[PASS] Persistent WebSocket connection survived all malformed string/JSON payloads.")
        results["subtests"].append(("Persistent Socket Resilience", "PASS"))

    await asyncio.sleep(0.5)

    # 2. Raw Bytes (Binary Frame) Vulnerability Test
    logger.info("Testing Binary WebSocket Frame (Raw Bytes)...")
    ws_bin = await websockets.connect(WS_URL)
    await ws_bin.recv() # Handshake
    raw_bytes = b"\x00\x01\x02\x03\xff\xfe\x41\x42"
    await ws_bin.send(raw_bytes)
    
    # Expect socket close due to unhandled RuntimeError in receive_text()
    try:
        await asyncio.wait_for(ws_bin.recv(), timeout=2.0)
    except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError, Exception) as e:
        logger.info(f"Socket closed after binary frame: {type(e).__name__}")

    await asyncio.sleep(0.5)
    health_post_bin = await fetch_health()
    logger.info(f"Health check immediately post-binary frame: {health_post_bin}")
    
    if health_post_bin["active_connections"] > 0:
        vuln_msg = (
            f"CRITICAL VULNERABILITY DETECTED: Binary WebSocket frame caused unhandled RuntimeError "
            f"in receive_text(), bypassing 'except WebSocketDisconnect:' and leaving a dead connection "
            f"leaked in ConnectionManager.active_connections (count: {health_post_bin['active_connections']})."
        )
        logger.warning(vuln_msg)
        results["vulnerabilities"].append(vuln_msg)
        results["subtests"].append(("Binary Frame Socket Cleanup", "FAIL (Connection Leak)"))
    else:
        results["subtests"].append(("Binary Frame Socket Cleanup", "PASS"))

    # Force cleanup of leaked connection by triggering HTTP broadcast
    def _trigger_post():
        req = urllib.request.Request(TRIGGER_URL, method="POST")
        try:
            with urllib.request.urlopen(req) as resp:
                pass
        except Exception:
            pass
    await asyncio.to_thread(_trigger_post)
    await wait_for_server_idle(5.0)

    logger.info("Suite 1 complete.\n")
    return results


async def run_suite_2_abrupt_disconnects() -> dict:
    """Suite 2: Test Abrupt Socket Disconnects Mid-Pipeline Cycle."""
    logger.info("=== SUITE 2: Abrupt Socket Disconnects Mid-Pipeline Cycle ===")
    results = {"subtests": [], "vulnerabilities": []}

    stages_to_disconnect = ["Listening...", "Transcribing...", "Speaking..."]

    for stage in stages_to_disconnect:
        await wait_for_server_idle()
        logger.info(f"Testing abrupt disconnect at stage: '{stage}'...")
        ws = await websockets.connect(WS_URL)
        await ws.recv() # Handshake
        await ws.send(json.dumps({"action": "trigger"}))

        while True:
            evt = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
            if evt.get("type") == "status" and evt.get("data") == stage:
                logger.info(f"  Reached stage '{stage}'. Abruptly closing client socket connection!")
                await ws.close()
                break

        # Wait for background pipeline cycle to complete (~4.5s)
        await wait_for_server_idle(6.0)

        health = await fetch_health()
        logger.info(f"  Health check post disconnect at '{stage}': {health}")
        assert health["active_connections"] == 0, (
            f"LEAK DETECTED: active_connections is {health['active_connections']} after disconnect at {stage}"
        )
        assert health["pipeline_busy"] is False, (
            f"LOCK HANG DETECTED: pipeline_busy is True after disconnect at {stage}"
        )
        logger.info(f"[PASS] active_connections clean (0) and pipeline_lock released post '{stage}' disconnect.")
        results["subtests"].append((f"Abrupt Disconnect at '{stage}'", "PASS"))

    # Multi-client Disconnect & Isolation Test
    await wait_for_server_idle()
    logger.info("Testing multi-client isolation & partial disconnect...")
    ws1 = await websockets.connect(WS_URL)
    await ws1.recv()
    ws2 = await websockets.connect(WS_URL)
    await ws2.recv()

    health = await fetch_health()
    assert health["active_connections"] == 2, f"Expected 2 connections, got {health['active_connections']}"

    # ws1 triggers pipeline
    await ws1.send(json.dumps({"action": "trigger"}))

    # ws2 receives Listening...
    evt_ws2 = json.loads(await ws2.recv())
    assert evt_ws2 == {"type": "status", "data": "Listening..."}

    # Abruptly close ws1 during Listening...
    logger.info("Closing ws1 mid-cycle while ws2 remains connected...")
    await ws1.close()

    # ws2 should continue receiving all remaining events
    ws2_events = await receive_core_cycle(ws2)

    assert ws2_events[0] == {"type": "status", "data": "Transcribing..."}
    assert ws2_events[1] == {"type": "status", "data": "Thinking..."}
    assert ws2_events[2] == {"type": "status", "data": "Speaking..."}
    assert ws2_events[3].get("type") == "response"
    assert ws2_events[4] == {"type": "status", "data": "Idle"}

    await ws2.close()
    await wait_for_server_idle()

    health = await fetch_health()
    assert health["active_connections"] == 0, f"Expected 0 connections, got {health['active_connections']}"
    assert health["pipeline_busy"] is False
    logger.info("[PASS] Multi-client isolation & partial disconnect verified.")
    results["subtests"].append(("Multi-Client Isolation", "PASS"))

    logger.info("Suite 2 complete.\n")
    return results


async def run_suite_3_consecutive_cycles() -> dict:
    """Suite 3: Verify event sequence completeness and schema compliance across 10 consecutive full pipeline cycles."""
    logger.info("=== SUITE 3: 10 Consecutive Full Pipeline Cycles (Schema & Completeness) ===")
    results = {"subtests": [], "cycle_durations": []}

    EXPECTED_SEQUENCE = [
        ("status", "Listening..."),
        ("status", "Transcribing..."),
        ("status", "Thinking..."),
        ("status", "Speaking..."),
        ("response", None),
        ("status", "Idle"),
    ]

    await wait_for_server_idle()

    async with websockets.connect(WS_URL) as ws:
        init_frame = json.loads(await ws.recv())
        assert init_frame == {"type": "status", "data": "Connected"}

        for cycle in range(1, 11):
            cycle_start = time.time()
            logger.info(f"--- Starting Cycle {cycle}/10 ---")

            await ws.send(json.dumps({"action": "trigger"}))

            received_events = await receive_core_cycle(ws)

            # Sequence verification
            assert len(received_events) == 6, f"Cycle {cycle}: Expected 6 events, got {len(received_events)}"

            for idx, (exp_type, exp_status) in enumerate(EXPECTED_SEQUENCE):
                evt = received_events[idx]
                
                # Schema compliance check
                assert isinstance(evt, dict), f"Cycle {cycle} Event {idx}: Expected dict, got {type(evt)}"
                assert "type" in evt, f"Cycle {cycle} Event {idx}: Missing 'type' field"
                assert evt["type"] == exp_type, (
                    f"Cycle {cycle} Event {idx}: Expected type '{exp_type}', got '{evt['type']}'"
                )

                if exp_type == "status":
                    assert "data" in evt, f"Cycle {cycle} Event {idx}: Missing 'data' field"
                    assert isinstance(evt["data"], str), f"Cycle {cycle} Event {idx}: 'data' must be str"
                    assert evt["data"] == exp_status, (
                        f"Cycle {cycle} Event {idx}: Expected status '{exp_status}', got '{evt['data']}'"
                    )
                elif exp_type == "response":
                    assert "user" in evt, f"Cycle {cycle} Event {idx}: Missing 'user' field"
                    assert "assistant" in evt, f"Cycle {cycle} Event {idx}: Missing 'assistant' field"
                    assert isinstance(evt["user"], str) and len(evt["user"].strip()) > 0, (
                        f"Cycle {cycle} Event {idx}: Invalid 'user' text: {evt['user']!r}"
                    )
                    assert isinstance(evt["assistant"], str) and len(evt["assistant"].strip()) > 0, (
                        f"Cycle {cycle} Event {idx}: Invalid 'assistant' text: {evt['assistant']!r}"
                    )
                    # Check exact 3 keys
                    assert len(evt.keys()) == 3, f"Cycle {cycle} Event {idx}: Unexpected keys: {evt.keys()}"

            cycle_duration = time.time() - cycle_start
            results["cycle_durations"].append(cycle_duration)
            logger.info(f"  [PASS] Cycle {cycle}/10 completed in {cycle_duration:.2f}s with 100% schema compliance.")
            results["subtests"].append((f"Cycle {cycle}/10", "PASS"))

    await wait_for_server_idle()

    health = await fetch_health()
    assert health["active_connections"] == 0, f"Post 10 cycles active_connections expected 0, got {health['active_connections']}"
    assert health["pipeline_busy"] is False, f"Post 10 cycles pipeline_busy expected False, got {health['pipeline_busy']}"
    logger.info("[PASS] All 10 consecutive full pipeline cycles verified successfully!")

    logger.info("Suite 3 complete.\n")
    return results


async def main():
    logger.info(f"Starting isolated bridge_api server instance on port {PORT}...")
    original_pipeline = bridge_api.execute_blocking_ai_pipeline

    def fast_pipeline(loop):
        for pipeline_status in ("Listening...", "Transcribing...", "Thinking...", "Speaking..."):
            bridge_api.emit_status_threadsafe(loop, pipeline_status)
            time.sleep(0.002)
        return ("Test question", "Test response")

    bridge_api.execute_blocking_ai_pipeline = fast_pipeline
    config = uvicorn.Config(bridge_api.app, host=HOST, port=PORT, log_level="error")
    server = uvicorn.Server(config)
    server.install_signal_handlers = False
    server_task = asyncio.create_task(server.serve())

    # Wait for server readiness
    for _ in range(50):
        try:
            h = await fetch_health()
            if h.get("status") == "ok":
                logger.info(f"Server is up and listening on port {PORT}.")
                break
        except Exception:
            pass
        await asyncio.sleep(0.1)

    try:
        res1 = await run_suite_1_malformed_frames()
        res2 = await run_suite_2_abrupt_disconnects()
        res3 = await run_suite_3_consecutive_cycles()

        print("\n" + "=" * 65)
        print("    ADVERSARIAL & RESILIENCE HARNESS SUMMARY RESULTS    ")
        print("=" * 65)
        print("Suite 1 (Malformed WebSocket Frames):")
        for desc, status_str in res1["subtests"]:
            print(f"  - {desc:42s}: {status_str}")
        if res1["vulnerabilities"]:
            print("  Vulnerabilities Identified:")
            for v in res1["vulnerabilities"]:
                print(f"    * {v}")

        print("\nSuite 2 (Abrupt Socket Disconnects):")
        for desc, status_str in res2["subtests"]:
            print(f"  - {desc:42s}: {status_str}")

        print("\nSuite 3 (10 Consecutive Pipeline Cycles):")
        avg_dur = sum(res3["cycle_durations"]) / len(res3["cycle_durations"])
        print(f"  - Total Cycles Completed  : {len(res3['cycle_durations'])}/10")
        print(f"  - Average Cycle Duration  : {avg_dur:.2f}s")
        print(f"  - Event Sequence Order    : 100% Invariant Compliant")
        print(f"  - JSON Schema Compliance  : 100% Compliant")
        print(f"  - Memory / Socket Clean   : PASS (0 Active Connections)")
        print("=" * 65 + "\n")

    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=5.0)
        except asyncio.TimeoutError:
            server_task.cancel()
            await asyncio.gather(server_task, return_exceptions=True)
        bridge_api.execute_blocking_ai_pipeline = original_pipeline
        logger.info("Server instance shut down cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
