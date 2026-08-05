"""
Shared utilities for the Jarvis Smart Mirror backend test suite.
"""

import asyncio
import json
import os
import socket
import sys
import threading
import time

# Ensure project root and .venv site-packages are in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

venv_site_packages = os.path.join(
    PROJECT_ROOT, ".venv", "lib", f"python{sys.version_info.major}.{sys.version_info.minor}", "site-packages"
)
if os.path.exists(venv_site_packages) and venv_site_packages not in sys.path:
    sys.path.insert(0, venv_site_packages)

backend_dir = os.path.join(PROJECT_ROOT, "backend")
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

import urllib.request
import uvicorn
import websockets
import bridge_api

class TestServer:
    """Helper to start and stop bridge_api FastAPI server in a background thread."""

    __test__ = False

    def __init__(self, host: str = "127.0.0.1", port: int = 8990):
        self.host = host
        self.port = port
        self.server = None
        self.thread = None
        self.sock = None
        self._orig_pipeline = bridge_api.execute_blocking_ai_pipeline
        self._orig_followup_timeout = bridge_api.FOLLOWUP_TIMEOUT_SECONDS

    def start(self, fast_mode: bool = True):
        os.environ["SKIP_MODEL_LOADING"] = "1"
        os.environ["SKIP_OLLAMA_WARMUP"] = "1"
        if fast_mode:
            def fast_pipeline(loop):
                bridge_api.emit_status_threadsafe(loop, "Listening...")
                time.sleep(0.001)
                bridge_api.emit_status_threadsafe(loop, "Transcribing...")
                time.sleep(0.001)
                bridge_api.emit_status_threadsafe(loop, "Thinking...")
                time.sleep(0.001)
                bridge_api.emit_status_threadsafe(loop, "Speaking...")
                time.sleep(0.001)
                return ("What's the weather like today?", "It is currently 72°F and sunny in your area.")

            bridge_api.execute_blocking_ai_pipeline = fast_pipeline
            bridge_api.FOLLOWUP_TIMEOUT_SECONDS = 0.05

        bridge_api.manager.active_connections.clear()
        bridge_api.manager.client_roles.clear()
        bridge_api.session_active = False
        bridge_api.conversation_history.clear()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except Exception:
                pass
        target_port = self.port
        bound = False
        for p in range(target_port, target_port + 20):
            try:
                sock.bind((self.host, p))
                self.port = p
                bound = True
                break
            except OSError:
                continue
        if not bound:
            sock.bind((self.host, 0))
            self.port = sock.getsockname()[1]

        self.sock = sock

        config = uvicorn.Config(bridge_api.app, host=self.host, port=self.port, log_level="error")
        self.server = uvicorn.Server(config)
        self.server.install_signal_handlers = False

        def run_server():
            try:
                self.server.run(sockets=[self.sock])
            except Exception:
                pass

        self.thread = threading.Thread(target=run_server, daemon=True)
        self.thread.start()

        # Poll health endpoint until server is ready to accept requests
        health_url = f"http://{self.host}:{self.port}/health"
        for _ in range(50):
            try:
                req = urllib.request.Request(health_url)
                with urllib.request.urlopen(req, timeout=0.2) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                time.sleep(0.05)

    def stop(self):
        if self.server:
            self.server.should_exit = True
            if self.thread and self.thread.is_alive():
                self.thread.join(timeout=3.0)
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
        self.server = None
        self.thread = None
        bridge_api.manager.active_connections.clear()
        bridge_api.manager.client_roles.clear()
        bridge_api.session_active = False
        bridge_api.conversation_history.clear()
        bridge_api.execute_blocking_ai_pipeline = self._orig_pipeline
        bridge_api.FOLLOWUP_TIMEOUT_SECONDS = self._orig_followup_timeout
        time.sleep(0.2)

    def wait_until_idle(self, timeout: float = 3.0) -> bool:
        """Wait until the running server has no active pipeline cycle."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{self.http_url}/health", timeout=0.2) as response:
                    if not json.loads(response.read().decode("utf-8"))["pipeline_busy"]:
                        return True
            except Exception:
                pass
            time.sleep(0.02)
        return False

    @property
    def ws_url(self) -> str:
        return f"ws://{self.host}:{self.port}/ws"

    @property
    def http_url(self) -> str:
        return f"http://{self.host}:{self.port}"
