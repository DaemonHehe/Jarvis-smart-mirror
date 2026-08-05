"""Gemini Live audio provider with fakeable session and audio boundaries."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
import logging
import os
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Iterable

try:
    from conversation_providers import ConversationResult, EmitEvent, KnowledgeSearch
except ImportError:
    from backend.conversation_providers import ConversationResult, EmitEvent, KnowledgeSearch


logger = logging.getLogger("JarvisGeminiLive")

DEFAULT_MODEL = "gemini-3.1-flash-live-preview"
INPUT_RATE = 16000
OUTPUT_RATE = 24000
INPUT_CHUNK_MS = 40
INPUT_CHUNK_FRAMES = INPUT_RATE * INPUT_CHUNK_MS // 1000
MAX_UTTERANCE_SECONDS = 30
PLAYBACK_QUEUE_CHUNKS = 100


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    """Read a finite numeric setting without allowing bad env values to break startup."""
    try:
        value = float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default
    if value != value or value in (float("inf"), float("-inf")):
        return default
    return max(minimum, value)


@dataclass(frozen=True)
class GeminiSettings:
    """Validated environment configuration for Gemini Live."""

    requested: bool
    api_key: str | None
    model: str
    voice: str | None
    idle_seconds: float
    fallback_cooldown_seconds: float

    @property
    def available(self) -> bool:
        return self.requested and bool(self.api_key)

    @classmethod
    def from_env(cls) -> "GeminiSettings":
        provider = os.environ.get("JARVIS_CONVERSATION_PROVIDER", "local").strip().lower()
        api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        voice = os.environ.get("GEMINI_VOICE", "").strip()
        return cls(
            requested=provider == "gemini-live",
            api_key=api_key or None,
            model=os.environ.get("GEMINI_LIVE_MODEL", DEFAULT_MODEL).strip() or DEFAULT_MODEL,
            voice=voice or None,
            idle_seconds=_env_float("JARVIS_FOLLOWUP_TIMEOUT_SECONDS", 10.0, minimum=1.0),
            fallback_cooldown_seconds=_env_float("GEMINI_FALLBACK_COOLDOWN_SECONDS", 60.0),
        )


class GeminiLiveError(RuntimeError):
    """Sanitized provider failure carrying audio needed for local fallback."""

    def __init__(
        self,
        reason: str,
        *,
        pcm: bytes = b"",
        transcript: str = "",
        partial_response: str = "",
        turns: Iterable[tuple[str, str]] = (),
    ):
        super().__init__(f"Gemini Live unavailable ({reason})")
        self.reason = reason
        self.pcm = pcm
        self.transcript = transcript
        self.partial_response = partial_response
        self.turns = list(turns)


class _ResumeGeminiSession(RuntimeError):
    """Internal signal requesting a transparent Live connection rotation."""


def classify_provider_error(error: BaseException) -> str:
    """Map SDK/network errors to stable, non-sensitive reason codes."""
    text = f"{type(error).__name__} {error}".lower()
    if any(token in text for token in ("quota", "resourceexhausted", "resource exhausted", "429")):
        return "quota"
    if any(token in text for token in ("api key", "permission", "unauth", "forbidden", "invalid argument", "model not found")):
        return "configuration"
    return "network"


def pcm_rms(pcm: bytes) -> float:
    """Calculate a dependency-free normalized RMS value for signed 16-bit PCM."""
    if len(pcm) < 2:
        return 0.0
    sample_count = len(pcm) // 2
    total = 0
    for index in range(0, sample_count * 2, 2):
        sample = int.from_bytes(pcm[index:index + 2], "little", signed=True)
        total += sample * sample
    return min(1.0, (total / sample_count) ** 0.5 / 32768.0)


def merge_transcript(current: str, update: str) -> str:
    """Merge SDK transcript deltas while tolerating cumulative updates."""
    update = update or ""
    if not current or update.startswith(current):
        return update
    if current.endswith(update):
        return current
    return current + update


class PcmRingBuffer:
    """Bounded recent microphone buffer used for same-utterance fallback."""

    def __init__(self, max_seconds: int = MAX_UTTERANCE_SECONDS):
        self.max_bytes = INPUT_RATE * 2 * max_seconds
        self._chunks: deque[bytes] = deque()
        self._size = 0

    def append(self, chunk: bytes) -> None:
        if not chunk:
            return
        self._chunks.append(bytes(chunk))
        self._size += len(chunk)
        while self._size > self.max_bytes and self._chunks:
            self._size -= len(self._chunks.popleft())

    def bytes(self) -> bytes:
        return b"".join(self._chunks)

    def clear(self) -> None:
        self._chunks.clear()
        self._size = 0


class PyAudioDuplex:
    """Blocking PyAudio streams exposed through async methods."""

    def __init__(self, pyaudio_module: Any):
        self._pyaudio_module = pyaudio_module
        self._pa = None
        self._input = None
        self._output = None
        self._output_queue: asyncio.Queue[bytes] | None = None
        self._writer_task: asyncio.Task[None] | None = None

    async def open(self) -> None:
        if self._pyaudio_module is None:
            raise GeminiLiveError("configuration")
        self._pa = self._pyaudio_module.PyAudio()
        self._input = self._pa.open(
            format=self._pyaudio_module.paInt16,
            channels=1,
            rate=INPUT_RATE,
            input=True,
            frames_per_buffer=INPUT_CHUNK_FRAMES,
        )
        self._output = self._pa.open(
            format=self._pyaudio_module.paInt16,
            channels=1,
            rate=OUTPUT_RATE,
            output=True,
            frames_per_buffer=OUTPUT_RATE * INPUT_CHUNK_MS // 1000,
        )
        self._output_queue = asyncio.Queue(maxsize=PLAYBACK_QUEUE_CHUNKS)
        self._writer_task = asyncio.create_task(self._write_output())

    async def _write_output(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            if self._output_queue is None:
                return
            chunk = await self._output_queue.get()
            try:
                if self._output is not None:
                    await loop.run_in_executor(None, self._output.write, chunk)
            finally:
                self._output_queue.task_done()

    async def read(self) -> bytes:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._input.read(INPUT_CHUNK_FRAMES, exception_on_overflow=False),
        )

    async def write(self, chunk: bytes) -> None:
        if not chunk or self._output_queue is None:
            return
        try:
            self._output_queue.put_nowait(bytes(chunk))
        except asyncio.QueueFull:
            # Prefer a small audible skip over delaying interruption/control frames.
            try:
                self._output_queue.get_nowait()
                self._output_queue.task_done()
            except asyncio.QueueEmpty:
                pass
            self._output_queue.put_nowait(bytes(chunk))

    async def clear_output(self) -> None:
        if self._output_queue is not None:
            while True:
                try:
                    self._output_queue.get_nowait()
                    self._output_queue.task_done()
                except asyncio.QueueEmpty:
                    break
        if self._output is not None:
            await asyncio.get_running_loop().run_in_executor(None, self._output.stop_stream)
            await asyncio.get_running_loop().run_in_executor(None, self._output.start_stream)

    async def close(self) -> None:
        if self._writer_task is not None:
            self._writer_task.cancel()
            await asyncio.gather(self._writer_task, return_exceptions=True)
            self._writer_task = None
        self._output_queue = None
        for stream in (self._input, self._output):
            if stream is not None:
                try:
                    stream.stop_stream()
                    stream.close()
                except Exception:
                    pass
        if self._pa is not None:
            self._pa.terminate()
        self._input = None
        self._output = None
        self._pa = None


def build_system_instruction(history: Iterable[dict[str, str]]) -> str:
    """Build bounded multilingual behavior and recent context instructions."""
    recent = list(history)[-10:]
    history_text = "\n".join(
        f"{item.get('role', 'user')}: {str(item.get('content', ''))[:500]}"
        for item in recent
    )
    instruction = (
        "You are Jarvis, a concise smart-mirror voice assistant. Detect the user's spoken "
        "language and answer naturally in the same language, including code-switching. "
        "Never claim a tool succeeded unless its response confirms success. Use search_memory "
        "only when prior personal context would materially improve the answer."
    )
    if history_text:
        instruction += f"\n\nRecent conversation context:\n{history_text}"
    return instruction


class GeminiLiveProvider:
    """Runs a multi-turn Gemini Live session and emits provider-neutral events."""

    name = "gemini-live"

    def __init__(
        self,
        settings: GeminiSettings,
        *,
        emit: EmitEvent,
        knowledge_search: KnowledgeSearch,
        history: Iterable[dict[str, str]],
        audio: Any,
        client_factory: Callable[[str], Any] | None = None,
        sign_off: Callable[[str], bool] | None = None,
    ):
        self.settings = settings
        self.emit = emit
        self.knowledge_search = knowledge_search
        self.history = list(history)
        self.audio = audio
        self.client_factory = client_factory or self._default_client_factory
        self.sign_off = sign_off or (lambda _text: False)
        self._stop = asyncio.Event()
        self._resume_handle: str | None = None
        self._input_buffer = PcmRingBuffer()
        self._input_text = ""
        self._output_text = ""
        self._turns: list[tuple[str, str]] = []
        self._output_started = False
        self._synthesis_started = False
        self._turn_finalized = False

    @staticmethod
    def _default_client_factory(api_key: str) -> Any:
        from google import genai

        return genai.Client(api_key=api_key)

    async def stop(self) -> None:
        self._stop.set()

    async def interrupt(self) -> None:
        """Stop queued local playback while Gemini processes the new speech turn."""
        await self.audio.clear_output()

    def _build_config(self) -> Any:
        from google.genai import types

        speech_config = None
        if self.settings.voice:
            speech_config = types.SpeechConfig(
                voice_config=types.VoiceConfig(
                    prebuilt_voice_config=types.PrebuiltVoiceConfig(
                        voice_name=self.settings.voice,
                    )
                )
            )
        tool = types.Tool(
            function_declarations=[
                types.FunctionDeclaration(
                    name="search_memory",
                    description="Search the user's private Jarvis knowledge memory.",
                    parameters_json_schema={
                        "type": "object",
                        "properties": {
                            "query": {"type": "string", "maxLength": 500},
                            "limit": {"type": "integer", "minimum": 1, "maximum": 5},
                        },
                        "required": ["query"],
                    },
                )
            ]
        )
        return types.LiveConnectConfig(
            response_modalities=["AUDIO"],
            speech_config=speech_config,
            system_instruction=build_system_instruction(self.history),
            tools=[tool],
            input_audio_transcription=types.AudioTranscriptionConfig(),
            output_audio_transcription=types.AudioTranscriptionConfig(),
            session_resumption=types.SessionResumptionConfig(handle=self._resume_handle),
            context_window_compression=types.ContextWindowCompressionConfig(
                sliding_window=types.SlidingWindow(target_tokens=8000),
            ),
        )

    async def run(self) -> ConversationResult:
        if not self.settings.available:
            raise GeminiLiveError("configuration")

        await self.emit({"type": "provider", "provider": self.name, "status": "connecting", "reason": None})
        try:
            await self.audio.open()
            client = self.client_factory(self.settings.api_key or "")
            reconnect_attempts = 0
            while not self._stop.is_set():
                try:
                    async with client.aio.live.connect(
                        model=self.settings.model,
                        config=self._build_config(),
                    ) as session:
                        await self.emit({"type": "provider", "provider": self.name, "status": "active", "reason": None})
                        if reconnect_attempts == 0:
                            await self.emit({"type": "status", "data": "Listening..."})
                            await self.emit({"type": "state", "state": "listening", "timestamp": time.time()})
                        await self._run_connected(session)
                        break
                except _ResumeGeminiSession:
                    if not self._resume_handle or reconnect_attempts >= 2:
                        raise ConnectionError("Gemini Live resumption unavailable")
                    reconnect_attempts += 1
                    await self.emit({"type": "provider", "provider": self.name, "status": "connecting", "reason": None})
                    continue
                except Exception as error:
                    if (
                        self._resume_handle
                        and reconnect_attempts < 2
                        and classify_provider_error(error) == "network"
                    ):
                        reconnect_attempts += 1
                        await self.emit({"type": "provider", "provider": self.name, "status": "connecting", "reason": None})
                        continue
                    raise
        except GeminiLiveError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning("Gemini Live session failed category=%s", classify_provider_error(error))
            raise GeminiLiveError(
                classify_provider_error(error),
                pcm=self._input_buffer.bytes(),
                transcript=self._input_text,
                partial_response=self._output_text,
                turns=self._turns,
            ) from error
        finally:
            await self.audio.close()

        ended_by = "sign-off" if self._input_text and self.sign_off(self._input_text) else "idle"
        return ConversationResult(provider=self.name, turns=self._turns, ended_by=ended_by)

    async def _run_connected(self, session: Any) -> None:
        message_queue: asyncio.Queue[Any] = asyncio.Queue()

        async def send_audio() -> None:
            from google.genai import types

            while not self._stop.is_set():
                chunk = await self.audio.read()
                self._input_buffer.append(chunk)
                await self.emit({
                    "type": "amplitude",
                    "value": round(pcm_rms(chunk), 3),
                    "source": "mic",
                    "timestamp": time.time(),
                })
                await session.send_realtime_input(
                    audio=types.Blob(data=chunk, mime_type=f"audio/pcm;rate={INPUT_RATE}")
                )

        async def receive_messages() -> None:
            try:
                async for message in session.receive():
                    await message_queue.put(message)
                await message_queue.put(None)
            except Exception as error:
                await message_queue.put(error)

        sender = asyncio.create_task(send_audio())
        receiver = asyncio.create_task(receive_messages())
        try:
            while not self._stop.is_set():
                message_task = asyncio.create_task(message_queue.get())
                stop_task = asyncio.create_task(self._stop.wait())
                done, pending = await asyncio.wait(
                    (message_task, stop_task),
                    timeout=self.settings.idle_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                if not done:
                    self._stop.set()
                    break
                if stop_task in done and stop_task.result():
                    message_task.cancel()
                    await asyncio.gather(message_task, return_exceptions=True)
                    break
                message = message_task.result()
                if message is None:
                    if self._stop.is_set():
                        break
                    raise ConnectionError("Gemini Live connection closed")
                if isinstance(message, BaseException):
                    raise message
                await self._handle_message(session, message)
        finally:
            sender.cancel()
            receiver.cancel()
            await asyncio.gather(sender, receiver, return_exceptions=True)

    async def _handle_message(self, session: Any, message: Any) -> None:
        resume = getattr(message, "session_resumption_update", None)
        if resume and getattr(resume, "resumable", True):
            self._resume_handle = getattr(resume, "new_handle", None) or getattr(resume, "handle", None)

        if getattr(message, "go_away", None):
            logger.info("Gemini Live requested connection rotation")
            raise _ResumeGeminiSession()

        activity = getattr(message, "voice_activity", None) or getattr(
            message,
            "voice_activity_detection_signal",
            None,
        )
        activity_name = str(activity or "").lower()
        if "start" in activity_name:
            self._input_buffer.clear()
            self._input_text = ""
            self._output_text = ""
            self._output_started = False
            self._synthesis_started = False
            self._turn_finalized = False
            await self.emit({"type": "status", "data": "Listening..."})
            await self.emit({"type": "state", "state": "listening-followup", "timestamp": time.time()})

        tool_call = getattr(message, "tool_call", None)
        if tool_call:
            await self._handle_tool_calls(session, getattr(tool_call, "function_calls", []) or [])

        content = getattr(message, "server_content", None)
        if content is None:
            return

        input_transcription = getattr(content, "input_transcription", None)
        if input_transcription and getattr(input_transcription, "text", None):
            self._turn_finalized = False
            self._input_text = merge_transcript(
                self._input_text,
                getattr(input_transcription, "text", ""),
            ).strip()
            await self.emit({
                "type": "transcript",
                "role": "user",
                "text": self._input_text,
                "is_final": bool(getattr(input_transcription, "finished", False)),
                "timestamp": time.time(),
            })
            if getattr(input_transcription, "finished", False):
                await self.emit({"type": "status", "data": "Transcribing..."})
                await self.emit({"type": "status", "data": "Thinking..."})
                await self.emit({"type": "state", "state": "thinking", "timestamp": time.time()})

        output_transcription = getattr(content, "output_transcription", None)
        if output_transcription and getattr(output_transcription, "text", None):
            if not self._synthesis_started:
                self._synthesis_started = True
                await self.emit({"type": "metric", "event": "tts_start", "timestamp": time.time()})
            self._output_text = merge_transcript(
                self._output_text,
                getattr(output_transcription, "text", ""),
            ).strip()
            await self.emit({
                "type": "transcript",
                "role": "assistant",
                "text": self._output_text,
                "is_final": bool(getattr(output_transcription, "finished", False)),
                "timestamp": time.time(),
            })
            await self.emit({"type": "metric", "event": "first_llm_token", "timestamp": time.time()})

        model_turn = getattr(content, "model_turn", None)
        for part in getattr(model_turn, "parts", []) or []:
            inline = getattr(part, "inline_data", None)
            audio_data = getattr(inline, "data", None) if inline else None
            if audio_data:
                if not self._synthesis_started:
                    self._synthesis_started = True
                    await self.emit({"type": "metric", "event": "tts_start", "timestamp": time.time()})
                if not self._output_started:
                    self._output_started = True
                    await self.emit({"type": "status", "data": "Speaking..."})
                    await self.emit({"type": "state", "state": "speaking", "timestamp": time.time()})
                    await self.emit({"type": "metric", "event": "first_tts_audio", "timestamp": time.time()})
                await self.audio.write(audio_data)
                await self.emit({
                    "type": "amplitude",
                    "value": round(pcm_rms(audio_data), 3),
                    "source": "tts",
                    "timestamp": time.time(),
                })

        if getattr(content, "interrupted", False):
            await self.audio.clear_output()
            await self.emit({"type": "tts_cancel", "event": "barge_in", "timestamp": time.time()})
            await self.emit({"type": "state", "state": "listening-followup", "timestamp": time.time()})

        if getattr(content, "generation_complete", False) or getattr(content, "turn_complete", False):
            await self._complete_turn(content)

    async def _complete_turn(self, content: Any) -> None:
        if self._turn_finalized:
            return
        self._turn_finalized = True
        reason = str(getattr(content, "turn_complete_reason", "") or "").lower()
        if any(token in reason for token in ("safety", "blocked", "prohibited")):
            await self.emit({
                "type": "transcript",
                "role": "assistant",
                "text": "I cannot help with that request.",
                "is_final": True,
                "timestamp": time.time(),
            })
        elif self._input_text or self._output_text:
            self._turns.append((self._input_text, self._output_text))
            await self.emit({
                "type": "response",
                "user": self._input_text,
                "assistant": self._output_text,
                "session_active": not self.sign_off(self._input_text),
            })

        if self.sign_off(self._input_text):
            self._stop.set()
            return

        self._input_buffer.clear()
        self._input_text = ""
        self._output_text = ""
        self._output_started = False
        self._synthesis_started = False
        await self.emit({"type": "state", "state": "listening-followup", "timestamp": time.time()})
        await self.emit({"type": "status", "data": "Idle"})

    async def _handle_tool_calls(self, session: Any, calls: Iterable[Any]) -> None:
        from google.genai import types

        responses = []
        for call in calls:
            if getattr(call, "name", None) != "search_memory":
                continue
            args = getattr(call, "args", {}) or {}
            query = str(args.get("query", ""))[:500]
            limit = max(1, min(5, int(args.get("limit", 3))))
            results = await self.knowledge_search(query, limit)
            responses.append(types.FunctionResponse(
                id=getattr(call, "id", None),
                name="search_memory",
                response={"results": results[:limit]},
            ))
        if responses:
            await session.send_tool_response(function_responses=responses)
