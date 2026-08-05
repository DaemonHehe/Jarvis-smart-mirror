"""
Standalone Test Verification Script for FastAPI WebSocket Bridge (bridge_api.py)
Milestone 1 Verification Suite

Validates:
1. Server launch / connection handshake ({"type": "status", "data": "Connected"})
2. GET /health status response
3. WS Trigger ("action": "trigger") event sequence & payload schema
   Sequence: Listening... -> Transcribing... -> Thinking... -> Speaking... -> response payload -> Idle
   Schema: {"type": "response", "user": "<non-empty str>", "assistant": "<non-empty str>"}
4. HTTP POST /trigger execution and concurrency lock (409 Conflict / busy status when locked)
"""

import asyncio
import json
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import websockets

BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = 8000
WS_URL = f"ws://{BRIDGE_HOST}:{BRIDGE_PORT}/ws"
HEALTH_URL = f"http://{BRIDGE_HOST}:{BRIDGE_PORT}/health"
TRIGGER_URL = f"http://{BRIDGE_HOST}:{BRIDGE_PORT}/trigger"


def is_server_running() -> bool:
    """Check if the bridge server port is accepting TCP connections."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        result = sock.connect_ex((BRIDGE_HOST, BRIDGE_PORT))
        return result == 0


async def verify_health_endpoint():
    """Verify GET /health returns expected fields."""
    print("[TEST] Testing GET /health...")
    req = urllib.request.Request(HEALTH_URL, method="GET")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200, f"Expected 200, got {resp.status}"
        data = json.loads(resp.read().decode("utf-8"))
        assert data.get("status") == "ok", f"Expected status 'ok', got {data.get('status')}"
        assert "active_connections" in data, "Missing 'active_connections' field"
        assert "pipeline_busy" in data, "Missing 'pipeline_busy' field"
        print(f"[PASS] GET /health response valid: {data}")


async def verify_ws_trigger_sequence():
    """Verify WebSocket handshake, trigger action, exact event sequence, and payload schema."""
    print("[TEST] Testing WebSocket trigger, event sequence, and response payload schema...")
    async with websockets.connect(WS_URL) as ws:
        # 1. Immediate connection handshake
        init_frame = json.loads(await ws.recv())
        assert init_frame == {"type": "status", "data": "Connected"}, (
            f"Handshake failed. Expected status 'Connected', got: {init_frame}"
        )
        print("[PASS] Initial connection handshake verified ('Connected').")

        # 2. Trigger cycle via WS frame
        await ws.send(json.dumps({"action": "trigger"}))
        print("[INFO] Sent WS trigger message: {'action': 'trigger'}")

        # 3. Expected sequence of events
        expected_sequence = [
            ("status", "Listening..."),
            ("status", "Transcribing..."),
            ("status", "Thinking..."),
            ("status", "Speaking..."),
            ("response", None),
            ("status", "Idle"),
        ]

        received_events = []
        for i in range(len(expected_sequence)):
            raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
            evt = json.loads(raw)
            received_events.append(evt)
            print(f"  Event [{i+1}/{len(expected_sequence)}]: {evt}")

        # 4. Assert sequence order and content
        for idx, (exp_type, exp_status) in enumerate(expected_sequence):
            evt = received_events[idx]
            assert evt.get("type") == exp_type, (
                f"Event {idx} expected type '{exp_type}', got '{evt.get('type')}'"
            )

            if exp_type == "status":
                assert evt.get("data") == exp_status, (
                    f"Event {idx} expected status '{exp_status}', got '{evt.get('data')}'"
                )
            elif exp_type == "response":
                user = evt.get("user")
                assistant = evt.get("assistant")

                assert isinstance(user, str) and len(user.strip()) > 0, (
                    f"Invalid or empty 'user' field in response: {user!r}"
                )
                assert isinstance(assistant, str) and len(assistant.strip()) > 0, (
                    f"Invalid or empty 'assistant' field in response: {assistant!r}"
                )
                print(f"[PASS] Response payload validated: user={user!r}, assistant={assistant!r}")

        print("[PASS] WebSocket event sequence verified in exact order.")


async def verify_http_trigger_and_concurrency_lock():
    """Verify HTTP POST /trigger and asyncio.Lock busy protection."""
    print("[TEST] Testing HTTP POST /trigger and concurrency lock...")
    async with websockets.connect(WS_URL) as ws:
        # Handshake
        await ws.recv()

        # 1. Send first HTTP POST trigger
        req1 = urllib.request.Request(TRIGGER_URL, method="POST")
        with urllib.request.urlopen(req1) as resp1:
            assert resp1.status == 200, f"Expected 200 OK, got {resp1.status}"
            body1 = json.loads(resp1.read().decode("utf-8"))
            assert body1.get("status") == "Pipeline cycle triggered"
        print("[PASS] Initial HTTP POST /trigger accepted.")

        # 2. Immediately send second HTTP POST trigger (should fail with 409 Conflict / busy)
        req2 = urllib.request.Request(TRIGGER_URL, method="POST")
        try:
            with urllib.request.urlopen(req2) as resp2:
                # If server returns 200 with busy JSON (fallback)
                body2 = json.loads(resp2.read().decode("utf-8"))
                assert body2.get("status") == "busy", f"Expected busy status, got: {body2}"
                print("[PASS] Concurrency lock prevented duplicate execution (returned busy status).")
        except urllib.error.HTTPError as err:
            assert err.code in (409, 429), f"Expected 409 Conflict, got HTTP {err.code}"
            err_body = json.loads(err.read().decode("utf-8"))
            assert err_body.get("status") == "busy", f"Expected 'busy' status in error body, got: {err_body}"
            print(f"[PASS] Concurrency lock prevented duplicate execution (returned HTTP {err.code} busy).")

        # 3. Wait for event cycle to complete
        idle_received = False
        while not idle_received:
            raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
            evt = json.loads(raw)
            if evt.get("type") == "status" and evt.get("data") == "Idle":
                idle_received = True

        print("[PASS] HTTP trigger pipeline cycle completed cleanly.")


async def main():
    server_process = None
    if not is_server_running():
        print("[INFO] Bridge server not running. Starting backend/bridge_api.py in background...")
        # Use python binary from .venv or current sys.executable
        python_exec = sys.executable
        server_process = subprocess.Popen(
            [python_exec, "backend/bridge_api.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        # Wait up to 5 seconds for server readiness
        for _ in range(50):
            if is_server_running():
                print("[INFO] Bridge server is up and listening on port 8000.")
                break
            time.sleep(0.1)
        else:
            if server_process.poll() is not None:
                stdout, stderr = server_process.communicate()
                print(f"[ERROR] Server failed to start.\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            sys.exit(1)

    try:
        await verify_health_endpoint()
        await verify_ws_trigger_sequence()
        await verify_http_trigger_and_concurrency_lock()
        print("\n=========================================")
        print(" ALL VERIFICATION TESTS PASSED CLEANLY! ")
        print("=========================================\n")
    finally:
        if server_process and server_process.poll() is None:
            print("[INFO] Terminating temporary server process...")
            server_process.terminate()
            server_process.wait()


if __name__ == "__main__":
    asyncio.run(main())
