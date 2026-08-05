import os
import sys
from types import SimpleNamespace

import pytest


BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "backend"))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

import bridge_api
from gemini_live import GeminiLiveError, GeminiSettings


def settings(*, requested=True, api_key="test-key", cooldown=60):
    return GeminiSettings(
        requested=requested,
        api_key=api_key,
        model="test-model",
        voice=None,
        idle_seconds=10,
        fallback_cooldown_seconds=cooldown,
    )


@pytest.mark.asyncio
async def test_cloud_success_does_not_invoke_local_pipeline(monkeypatch):
    calls = []
    app = SimpleNamespace(state=SimpleNamespace(gemini_settings=settings()))

    async def cloud(_app, _settings):
        calls.append("cloud")
        return None

    async def local(*_args, **_kwargs):
        calls.append("local")

    monkeypatch.setattr(bridge_api, "gemini_retry_after", 0.0)
    monkeypatch.setattr(bridge_api, "run_gemini_conversation", cloud)
    monkeypatch.setattr(bridge_api, "run_local_pipeline_cycle", local)

    await bridge_api.run_pipeline_cycle(app)

    assert calls == ["cloud"]


@pytest.mark.asyncio
async def test_cloud_failure_passes_captured_utterance_to_local(monkeypatch):
    captured = {}
    app = SimpleNamespace(state=SimpleNamespace(gemini_settings=settings(cooldown=30)))
    error = GeminiLiveError("network", pcm=b"pcm", transcript="สวัสดี")

    async def cloud(_app, _settings):
        return error

    async def local(_app, **kwargs):
        captured.update(kwargs)

    async def broadcast(_event):
        return None

    monkeypatch.setattr(bridge_api, "gemini_retry_after", 0.0)
    monkeypatch.setattr(bridge_api, "run_gemini_conversation", cloud)
    monkeypatch.setattr(bridge_api, "run_local_pipeline_cycle", local)
    monkeypatch.setattr(bridge_api.manager, "broadcast", broadcast)

    await bridge_api.run_pipeline_cycle(app)

    assert captured == {"fallback_pcm": b"pcm", "fallback_transcript": "สวัสดี"}
    assert bridge_api.gemini_retry_after > 0


@pytest.mark.asyncio
async def test_missing_key_uses_local_without_cloud_attempt(monkeypatch):
    calls = []
    app = SimpleNamespace(state=SimpleNamespace(gemini_settings=settings(api_key=None)))

    async def cloud(_app, _settings):
        calls.append("cloud")

    async def local(*_args, **_kwargs):
        calls.append("local")

    async def broadcast(_event):
        return None

    monkeypatch.setattr(bridge_api, "gemini_retry_after", 0.0)
    monkeypatch.setattr(bridge_api, "run_gemini_conversation", cloud)
    monkeypatch.setattr(bridge_api, "run_local_pipeline_cycle", local)
    monkeypatch.setattr(bridge_api.manager, "broadcast", broadcast)

    await bridge_api.run_pipeline_cycle(app)

    assert calls == ["local"]
