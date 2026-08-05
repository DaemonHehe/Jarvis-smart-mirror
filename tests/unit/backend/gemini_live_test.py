import asyncio
import threading
from types import SimpleNamespace

import pytest

from backend.gemini_live import (
    GeminiLiveError,
    GeminiLiveProvider,
    GeminiSettings,
    PcmRingBuffer,
    PyAudioDuplex,
    build_system_instruction,
    classify_provider_error,
    merge_transcript,
    pcm_rms,
)


class FakeBlockingStream:
    def __init__(self, block_writes=False):
        self.block_writes = block_writes
        self.write_started = threading.Event()
        self.release_write = threading.Event()

    def read(self, frames, exception_on_overflow=False):
        return b"\x00\x00" * frames

    def write(self, _chunk):
        self.write_started.set()
        if self.block_writes:
            self.release_write.wait(timeout=1)

    def stop_stream(self):
        pass

    def start_stream(self):
        pass

    def close(self):
        pass


class FakePyAudioModule:
    paInt16 = 8

    def __init__(self):
        self.input_stream = FakeBlockingStream()
        self.output_stream = FakeBlockingStream(block_writes=True)

    def PyAudio(self):
        module = self

        class Instance:
            def open(self, *, input=False, output=False, **_kwargs):
                return module.input_stream if input else module.output_stream

            def terminate(self):
                pass

        return Instance()


class FakeAudio:
    def __init__(self):
        self.opened = False
        self.closed = False
        self.writes = []
        self.clears = 0

    async def open(self):
        self.opened = True

    async def read(self):
        await asyncio.sleep(0.001)
        return b"\x00\x10" * 64

    async def write(self, chunk):
        self.writes.append(chunk)

    async def clear_output(self):
        self.clears += 1

    async def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, messages=None, error=None):
        self.messages = messages or []
        self.error = error
        self.audio_inputs = []
        self.tool_responses = []

    async def send_realtime_input(self, audio=None, **_kwargs):
        self.audio_inputs.append(audio)

    async def send_tool_response(self, function_responses):
        self.tool_responses.extend(function_responses)

    async def receive(self):
        for message in self.messages:
            yield message
        if self.error:
            await asyncio.sleep(0.01)
            raise self.error
        await asyncio.sleep(10)


class FakeConnect:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, *_args):
        return False


class FakeClient:
    def __init__(self, session):
        self.aio = SimpleNamespace(
            live=SimpleNamespace(connect=lambda **_kwargs: FakeConnect(session))
        )


class FakeClientSequence:
    def __init__(self, sessions):
        self.sessions = iter(sessions)
        self.configs = []

        def connect(**kwargs):
            self.configs.append(kwargs["config"])
            return FakeConnect(next(self.sessions))

        self.aio = SimpleNamespace(live=SimpleNamespace(connect=connect))


def settings(**overrides):
    values = {
        "requested": True,
        "api_key": "test-key",
        "model": "test-model",
        "voice": None,
        "idle_seconds": 0.03,
        "fallback_cooldown_seconds": 60,
    }
    values.update(overrides)
    return GeminiSettings(**values)


def content_message(**values):
    defaults = {
        "model_turn": None,
        "input_transcription": None,
        "output_transcription": None,
        "interrupted": False,
        "generation_complete": False,
        "turn_complete": False,
        "turn_complete_reason": None,
    }
    defaults.update(values)
    return SimpleNamespace(
        server_content=SimpleNamespace(**defaults),
        tool_call=None,
        session_resumption_update=None,
        go_away=None,
    )


def transcription(text, finished=True):
    return SimpleNamespace(text=text, finished=finished)


@pytest.mark.parametrize(
    ("message", "reason"),
    [
        ("429 quota exceeded", "quota"),
        ("invalid API key", "configuration"),
        ("connection reset", "network"),
    ],
)
def test_classify_provider_error(message, reason):
    assert classify_provider_error(RuntimeError(message)) == reason


def test_pcm_helpers_are_bounded():
    buffer = PcmRingBuffer(max_seconds=1)
    buffer.append(b"a" * 40000)

    assert len(buffer.bytes()) <= 32000
    assert pcm_rms(b"\x00\x00" * 10) == 0.0
    assert pcm_rms(b"\xff\x7f" * 10) > 0.99


def test_transcript_merge_accepts_delta_and_cumulative_updates():
    assert merge_transcript("hello", " world") == "hello world"
    assert merge_transcript("hello", "hello world") == "hello world"
    assert merge_transcript("hello", "hello") == "hello"


def test_system_instruction_contains_language_policy_and_bounded_history():
    prompt = build_system_instruction([
        {"role": "user", "content": "old"},
        {"role": "assistant", "content": "ล่าสุด"},
    ])

    assert "same language" in prompt
    assert "ล่าสุด" in prompt
    assert "search_memory" in prompt


def test_invalid_numeric_environment_uses_safe_defaults(monkeypatch):
    monkeypatch.setenv("JARVIS_CONVERSATION_PROVIDER", "gemini-live")
    monkeypatch.setenv("GEMINI_API_KEY", "   ")
    monkeypatch.setenv("JARVIS_FOLLOWUP_TIMEOUT_SECONDS", "not-a-number")
    monkeypatch.setenv("GEMINI_FALLBACK_COOLDOWN_SECONDS", "NaN")

    configured = GeminiSettings.from_env()

    assert configured.requested
    assert configured.api_key is None
    assert configured.idle_seconds == 10.0
    assert configured.fallback_cooldown_seconds == 60.0


@pytest.mark.asyncio
async def test_duplex_playback_does_not_block_network_receiver():
    pyaudio_module = FakePyAudioModule()
    audio = PyAudioDuplex(pyaudio_module)
    await audio.open()

    await asyncio.wait_for(audio.write(b"\x00\x10" * 100), timeout=0.05)
    await asyncio.to_thread(pyaudio_module.output_stream.write_started.wait, 0.2)

    assert pyaudio_module.output_stream.write_started.is_set()
    pyaudio_module.output_stream.release_write.set()
    await audio.close()


@pytest.mark.asyncio
async def test_missing_key_fails_without_opening_audio():
    audio = FakeAudio()
    provider = GeminiLiveProvider(
        settings(api_key=None),
        emit=lambda _event: asyncio.sleep(0),
        knowledge_search=lambda _query, _limit: asyncio.sleep(0, result=[]),
        history=[],
        audio=audio,
    )

    with pytest.raises(GeminiLiveError) as error:
        await provider.run()

    assert error.value.reason == "configuration"
    assert not audio.opened


@pytest.mark.asyncio
async def test_multilingual_turn_emits_transcripts_audio_and_response():
    audio_part = SimpleNamespace(inline_data=SimpleNamespace(data=b"\x00\x20" * 20))
    messages = [
        content_message(input_transcription=transcription("อากาศวันนี้เป็นอย่างไร")),
        content_message(
            output_transcription=transcription("วันนี้อากาศดี"),
            model_turn=SimpleNamespace(parts=[audio_part]),
        ),
        content_message(generation_complete=True),
        content_message(turn_complete=True),
    ]
    session = FakeSession(messages)
    audio = FakeAudio()
    events = []

    async def emit(event):
        events.append(event)

    provider = GeminiLiveProvider(
        settings(),
        emit=emit,
        knowledge_search=lambda _query, _limit: asyncio.sleep(0, result=[]),
        history=[],
        audio=audio,
        client_factory=lambda _key: FakeClient(session),
    )

    result = await provider.run()

    assert result.turns == [("อากาศวันนี้เป็นอย่างไร", "วันนี้อากาศดี")]
    assert audio.writes
    assert audio.closed
    assert any(event.get("type") == "provider" and event.get("status") == "active" for event in events)
    assert any(event.get("type") == "response" for event in events)
    assert sum(event.get("type") == "response" for event in events) == 1


@pytest.mark.asyncio
async def test_interruption_clears_playback_and_emits_cancel():
    session = FakeSession([content_message(interrupted=True)])
    audio = FakeAudio()
    events = []

    async def emit(event):
        events.append(event)

    provider = GeminiLiveProvider(
        settings(),
        emit=emit,
        knowledge_search=lambda _query, _limit: asyncio.sleep(0, result=[]),
        history=[],
        audio=audio,
        client_factory=lambda _key: FakeClient(session),
    )

    await provider.run()

    assert audio.clears == 1
    assert any(event.get("type") == "tts_cancel" for event in events)


@pytest.mark.asyncio
async def test_network_failure_carries_pcm_for_local_fallback():
    session = FakeSession(error=ConnectionError("socket closed"))
    audio = FakeAudio()
    provider = GeminiLiveProvider(
        settings(idle_seconds=1),
        emit=lambda _event: asyncio.sleep(0),
        knowledge_search=lambda _query, _limit: asyncio.sleep(0, result=[]),
        history=[],
        audio=audio,
        client_factory=lambda _key: FakeClient(session),
    )

    with pytest.raises(GeminiLiveError) as error:
        await provider.run()

    assert error.value.reason == "network"
    assert error.value.pcm
    assert audio.closed


@pytest.mark.asyncio
async def test_go_away_resumes_with_latest_session_handle():
    resume_update = SimpleNamespace(resumable=True, new_handle="resume-token")
    rotate_message = SimpleNamespace(
        server_content=None,
        tool_call=None,
        session_resumption_update=resume_update,
        go_away=SimpleNamespace(time_left=1),
        voice_activity=None,
        voice_activity_detection_signal=None,
    )
    client = FakeClientSequence([FakeSession([rotate_message]), FakeSession([])])
    audio = FakeAudio()
    events = []

    async def emit(event):
        events.append(event)

    provider = GeminiLiveProvider(
        settings(),
        emit=emit,
        knowledge_search=lambda _query, _limit: asyncio.sleep(0, result=[]),
        history=[],
        audio=audio,
        client_factory=lambda _key: client,
    )

    await provider.run()

    assert len(client.configs) == 2
    assert client.configs[1].session_resumption.handle == "resume-token"
    assert sum(event.get("status") == "connecting" for event in events) == 2


@pytest.mark.asyncio
async def test_memory_tool_is_read_only_and_result_limited():
    call = SimpleNamespace(
        id="call-1",
        name="search_memory",
        args={"query": "favorite city", "limit": 99},
    )
    tool_message = SimpleNamespace(
        server_content=None,
        tool_call=SimpleNamespace(function_calls=[call]),
        session_resumption_update=None,
        go_away=None,
        voice_activity=None,
        voice_activity_detection_signal=None,
    )
    session = FakeSession([tool_message])
    searches = []

    async def search(query, limit):
        searches.append((query, limit))
        return [{"id": index} for index in range(10)]

    provider = GeminiLiveProvider(
        settings(),
        emit=lambda _event: asyncio.sleep(0),
        knowledge_search=search,
        history=[],
        audio=FakeAudio(),
        client_factory=lambda _key: FakeClient(session),
    )

    await provider.run()

    assert searches == [("favorite city", 5)]
    assert len(session.tool_responses) == 1
    assert len(session.tool_responses[0].response["results"]) == 5


@pytest.mark.asyncio
async def test_stop_closes_long_idle_session_immediately():
    audio = FakeAudio()
    provider = GeminiLiveProvider(
        settings(idle_seconds=30),
        emit=lambda _event: asyncio.sleep(0),
        knowledge_search=lambda _query, _limit: asyncio.sleep(0, result=[]),
        history=[],
        audio=audio,
        client_factory=lambda _key: FakeClient(FakeSession([])),
    )

    task = asyncio.create_task(provider.run())
    while not audio.opened:
        await asyncio.sleep(0.001)
    await provider.stop()
    result = await asyncio.wait_for(task, timeout=0.2)

    assert result.ended_by == "idle"
    assert audio.closed
