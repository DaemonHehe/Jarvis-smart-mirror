import os
import sys
from types import SimpleNamespace

import pytest


BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "backend"))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

import bridge_api


class FakeStdin:
    def __init__(self):
        self.data = bytearray()
        self.closed = False

    def write(self, data):
        self.data.extend(data)

    async def drain(self):
        pass

    def is_closing(self):
        return self.closed

    def close(self):
        self.closed = True


class FakeStdout:
    def __init__(self):
        self.chunks = [b"\x00\x10" * 100, b""]

    async def read(self, _size):
        return self.chunks.pop(0)


class FakeProcess:
    def __init__(self):
        self.stdin = FakeStdin()
        self.stdout = FakeStdout()
        self.returncode = None

    def kill(self):
        self.returncode = -1

    async def wait(self):
        self.returncode = self.returncode or 0
        return self.returncode


def test_tuning_environment_parsers_use_bounds_and_defaults(monkeypatch):
    monkeypatch.setenv("TEST_INT", "invalid")
    monkeypatch.setenv("TEST_FLOAT", "Infinity")
    assert bridge_api.env_int("TEST_INT", 10, 1, 20) == 10
    assert bridge_api.env_float("TEST_FLOAT", 5.0, 1.0, 10.0) == 5.0

    monkeypatch.setenv("TEST_INT", "999")
    monkeypatch.setenv("TEST_FLOAT", "-2")
    assert bridge_api.env_int("TEST_INT", 10, 1, 20) == 20
    assert bridge_api.env_float("TEST_FLOAT", 5.0, 1.0, 10.0) == 1.0


@pytest.mark.asyncio
async def test_local_tts_streams_without_disk_and_marks_real_pcm(monkeypatch):
    process = FakeProcess()
    events = []
    writes = []

    class Communicate:
        def __init__(self, text, voice):
            assert text == "hello"
            assert voice == bridge_api.EDGE_TTS_VOICE

        async def stream(self):
            yield {"type": "audio", "data": b"mp3-data"}

    async def create_process(*_args, **_kwargs):
        return process

    async def write_audio(chunk):
        writes.append(chunk)
        return True

    async def amplitude(_value, source="mic"):
        assert source == "tts"

    monkeypatch.setitem(sys.modules, "edge_tts", SimpleNamespace(Communicate=Communicate))
    monkeypatch.setattr(bridge_api.asyncio, "create_subprocess_exec", create_process)
    monkeypatch.setattr(bridge_api.local_audio_output, "write", write_audio)
    monkeypatch.setattr(bridge_api, "notify_amplitude", amplitude)
    monkeypatch.setattr(bridge_api.tracker, "mark_event", events.append)
    bridge_api.barge_in_event.clear()

    success = await bridge_api.synthesize_segment_and_play(
        "hello",
        1,
        monitor_barge_in=False,
    )

    assert success
    assert process.stdin.data == b"mp3-data"
    assert writes == [b"\x00\x10" * 100]
    assert events == ["first_tts_audio"]
