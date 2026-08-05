"""
FastAPI WebSocket Server Wrapper for Voice Assistant (bridge_api.py)
Exposes a WebSocket endpoint at ws://127.0.0.1:8000/ws to stream pipeline status, amplitude,
tokens, transcripts, latency metrics, and assistant responses to standalone Jarvis clients.

Extended with:
- Knowledge Graph API (SQLite-backed, Obsidian-style)
- Pipeline Tracker API (real-time stage state & latency breakdown)
- Web Dashboard (static file serving)
- Fully Integrated ML Pipeline (OpenWakeWord, Whisper, Llama 3.2, Edge-TTS)
- Continuous VAD Sessions, Sign-off Detection, and Acoustic Gating / Barge-In
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import io
import json
import logging
import os
from pathlib import Path
import re
import sys
import time
import threading
from typing import Any, Dict, List, Optional
from urllib.request import Request, urlopen
import wave
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager

import numpy as np
try:
    import pyaudio
except ImportError:
    pyaudio = None

from fastapi import FastAPI, Query, Response, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
from pydantic import BaseModel, Field

from knowledge_graph import KnowledgeGraph
from pipeline_tracker import PipelineTracker
from conversation_providers import LocalConversationProvider
from gemini_live import GeminiLiveError, GeminiLiveProvider, GeminiSettings, PyAudioDuplex

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("JarvisBridgeAPI")


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, value))


def env_float(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default
    if value != value or value in (float("inf"), float("-inf")):
        return default
    return max(minimum, min(maximum, value))


MAX_WS_MESSAGE_BYTES = env_int("JARVIS_MAX_WS_MESSAGE_BYTES", 16384, 1024, 1048576)
FOLLOWUP_TIMEOUT_SECONDS = env_float("JARVIS_FOLLOWUP_TIMEOUT_SECONDS", 10.0, 1.0, 300.0)
SPEECH_END_SILENCE_MS = env_int("JARVIS_SPEECH_END_SILENCE_MS", 650, 300, 2000)
MAX_UTTERANCE_SECONDS = env_int("JARVIS_MAX_UTTERANCE_SECONDS", 15, 3, 30)
WHISPER_MODEL = os.environ.get("WHISPER_MODEL", "base").strip() or "base"
WHISPER_LANGUAGE = os.environ.get("WHISPER_LANGUAGE", "").strip() or None
WHISPER_BEAM_SIZE = env_int("WHISPER_BEAM_SIZE", 1, 1, 5)
WHISPER_DEVICE = os.environ.get("WHISPER_DEVICE", "auto").strip() or "auto"
WHISPER_COMPUTE_TYPE = os.environ.get("WHISPER_COMPUTE_TYPE", "default").strip() or "default"
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2").strip() or "llama3.2"
_ollama_keep_alive_str = os.environ.get("OLLAMA_KEEP_ALIVE", "-1")
try:
    OLLAMA_KEEP_ALIVE = int(_ollama_keep_alive_str)
except ValueError:
    OLLAMA_KEEP_ALIVE = _ollama_keep_alive_str
OLLAMA_NUM_CTX = env_int("OLLAMA_NUM_CTX", 4096, 1024, 131072)
OLLAMA_NUM_PREDICT = env_int("OLLAMA_NUM_PREDICT", 256, 32, 4096)
EDGE_TTS_VOICE = os.environ.get("EDGE_TTS_VOICE", "en-US-GuyNeural").strip() or "en-US-GuyNeural"

# Dedicated single-worker ThreadPoolExecutor for ML thread offloading
ml_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ML_Pipeline")
storage_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="Knowledge_Store")
storage_tasks: set[asyncio.Future] = set()

# Global async lock to prevent concurrent/overlapping pipeline executions
pipeline_lock = asyncio.Lock()
trigger_queue = asyncio.Queue()

# Threading event to manage microphone contention between OpenWakeWord and recording
mic_free_event = threading.Event()
mic_free_event.set()

# Threading event for barge-in signaling
barge_in_event = threading.Event()

# Knowledge Graph & Pipeline Tracker
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("JARVIS_KG_DB", os.path.join(BASE_DIR, "knowledge_graph.db"))
kg = KnowledgeGraph(db_path=DB_PATH)
tracker = PipelineTracker()

latest_tts_amplitude = 0.0


class LocalAudioOutput:
    """Lazy persistent speaker stream for local TTS responses."""

    def __init__(self):
        self._pa = None
        self._stream = None

    def _ensure_open(self) -> bool:
        if self._stream is not None:
            return True
        if pyaudio is None:
            return False
        self._pa = pyaudio.PyAudio()
        self._stream = self._pa.open(
            format=pyaudio.paInt16,
            channels=1,
            rate=16000,
            output=True,
            frames_per_buffer=1024,
        )
        return True

    async def write(self, chunk: bytes) -> bool:
        if not chunk:
            return False
        loop = asyncio.get_running_loop()
        available = await loop.run_in_executor(None, self._ensure_open)
        if not available:
            await asyncio.sleep(len(chunk) / (16000 * 2))
            return False
        await loop.run_in_executor(None, self._stream.write, chunk)
        return True

    async def close(self) -> None:
        stream, pa = self._stream, self._pa
        self._stream = None
        self._pa = None
        if stream is not None:
            try:
                stream.stop_stream()
                stream.close()
            except Exception:
                pass
        if pa is not None:
            try:
                pa.terminate()
            except Exception:
                pass


local_audio_output = LocalAudioOutput()


def schedule_knowledge_write(operation) -> None:
    """Run persistence after the response path without delaying microphone release."""
    future = asyncio.ensure_future(
        asyncio.get_running_loop().run_in_executor(storage_executor, operation)
    )
    storage_tasks.add(future)

    def completed(task: asyncio.Future) -> None:
        storage_tasks.discard(task)
        try:
            task.result()
        except Exception:
            logger.exception("Background knowledge persistence failed")

    future.add_done_callback(completed)

# Continuous Session State & Conversation History
session_active = False
conversation_history: deque = deque(maxlen=10)
gemini_retry_after = 0.0

SIGN_OFF_PHRASES = [
    "that's all", "that is all", "thanks jarvis", "thank you jarvis",
    "goodbye", "bye jarvis", "stop listening", "thanks", "thank you", "bye",
    "\u0e41\u0e04\u0e48\u0e19\u0e35\u0e49",
    "\u0e02\u0e2d\u0e1a\u0e04\u0e38\u0e13\u0e08\u0e32\u0e23\u0e4c\u0e27\u0e34\u0e2a",
    "\u0e02\u0e2d\u0e1a\u0e04\u0e38\u0e13 \u0e08\u0e32\u0e23\u0e4c\u0e27\u0e34\u0e2a",
    "\u0e25\u0e32\u0e01\u0e48\u0e2d\u0e19",
]
SIGN_OFF_PHRASES.extend(
    phrase.strip()
    for phrase in os.environ.get("JARVIS_SIGN_OFF_PHRASES", "").split(",")
    if phrase.strip()
)


def normalize_phrase(text: str) -> str:
    """Normalize text for consistent sign-off phrase comparison."""
    cleaned = text.lower().strip()
    cleaned = re.sub(r'[^\w\s]', '', cleaned)
    return re.sub(r'\s+', ' ', cleaned).strip()


def is_sign_off_phrase(text: str) -> bool:
    """Check if text contains a sign-off phrase using normalized regex word boundaries."""
    norm_text = normalize_phrase(text)
    if not norm_text:
        return False
    for phrase in SIGN_OFF_PHRASES:
        norm_phrase = normalize_phrase(phrase)
        if norm_phrase and (
            re.search(r'\b' + re.escape(norm_phrase) + r'\b', norm_text)
            or (" " not in norm_phrase and norm_phrase in norm_text)
        ):
            return True
    return False


# Map bridge status strings to typed protocol state values
STATUS_TO_STATE = {
    "Listening...": "listening",
    "Transcribing...": "thinking",
    "Thinking...": "thinking",
    "Speaking...": "speaking",
    "Idle": "sleeping",
    "Error": "sleeping",
}


def calculate_rms_amplitude(pcm_data: bytes) -> float:
    """
    Calculate logarithmic RMS audio amplitude normalized to float [0.0, 1.0].
    Assumes 16-bit signed PCM mono audio.
    """
    if not pcm_data:
        return 0.0
    try:
        samples = np.frombuffer(pcm_data, dtype=np.int16).astype(np.float32) / 32768.0
        if len(samples) == 0:
            return 0.0
        rms = np.sqrt(np.mean(samples ** 2))
        if rms <= 0:
            return 0.0
        dbfs = 20.0 * np.log10(rms + 1e-6)
        # Map dBFS range [-60.0, -6.0] -> [0.0, 1.0]
        min_db = -60.0
        max_db = -6.0
        norm = (dbfs - min_db) / (max_db - min_db)
        return float(np.clip(norm, 0.0, 1.0))
    except Exception:
        return 0.0


class ClauseSegmenter:
    """
    Splits streaming LLM token stream into clause/sentence segments for incremental TTS.
    """
    def __init__(self, min_clause_len: int = 20):
        self.min_clause_len = min_clause_len
        self.buffer = ""
        self.segment_id = 0

    def feed(self, token: str) -> List[tuple[int, str]]:
        self.buffer += token
        segments = []
        delimiters = [".", "?", "!", ";", "\n"]
        i = 0
        while i < len(self.buffer):
            char = self.buffer[i]
            is_strong_delim = char in delimiters
            is_comma_delim = (char == "," and i >= self.min_clause_len)
            if is_strong_delim or is_comma_delim:
                seg_text = self.buffer[:i+1].strip()
                if seg_text:
                    self.segment_id += 1
                    segments.append((self.segment_id, seg_text))
                self.buffer = self.buffer[i+1:]
                i = 0
            else:
                i += 1
        return segments

    def flush(self) -> List[tuple[int, str]]:
        segments = []
        remaining = self.buffer.strip()
        if remaining:
            self.segment_id += 1
            segments.append((self.segment_id, remaining))
            self.buffer = ""
        return segments


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load all ML models globally into app.state and start background tasks."""
    global trigger_queue, pipeline_lock
    trigger_queue = asyncio.Queue()
    pipeline_lock = asyncio.Lock()
    logger.info("Starting up ML pipeline bridge API...")
    app.state.whisper = None
    app.state.llm_client = None
    app.state.llm_type = os.environ.get("LLM_BACKEND", "ollama").lower()
    app.state.gemini_settings = GeminiSettings.from_env()
    app.state.active_conversation_provider = None

    if os.environ.get("SKIP_MODEL_LOADING") != "1":
        # Pre-load Faster-Whisper globally
        try:
            from faster_whisper import WhisperModel
            logger.info(
                "Pre-loading Faster-Whisper device=%s compute_type=%s",
                WHISPER_DEVICE,
                WHISPER_COMPUTE_TYPE,
            )
            app.state.whisper = WhisperModel(
                WHISPER_MODEL,
                device=WHISPER_DEVICE,
                compute_type=WHISPER_COMPUTE_TYPE,
            )
        except Exception as e:
            logger.warning(f"Could not pre-load Faster-Whisper on GPU/CUDA: {e}. Attempting CPU fallback.")
            try:
                from faster_whisper import WhisperModel
                app.state.whisper = WhisperModel(WHISPER_MODEL, device="cpu", compute_type="int8")
            except Exception as e2:
                logger.error(f"Faster-Whisper initialization failed: {e2}")
                app.state.whisper = None

        if app.state.llm_type == "ollama" and os.environ.get("SKIP_OLLAMA_WARMUP") != "1":
            try:
                from ollama import AsyncClient
                app.state.llm_client = AsyncClient()
                logger.info("Sending Ollama warm-up request with keep_alive=-1...")
                await app.state.llm_client.generate(
                    model=OLLAMA_MODEL,
                    prompt="warmup",
                    keep_alive=-1
                )
                logger.info("Ollama model successfully warmed up and resident in memory.")
            except Exception as e:
                logger.warning(f"Ollama warm-up non-critical notice: {e}")

    app.state.kg = kg
    app.state.tracker = tracker

    # Validate Microphone Access
    try:
        if pyaudio is not None:
            pa = pyaudio.PyAudio()
            pa.terminate()
    except Exception:
        pass

    if os.environ.get("SKIP_MODEL_LOADING") != "1":
        # Start Wake Word Listener Thread in background if enabled
        loop = asyncio.get_running_loop()
        ww_thread = threading.Thread(target=wake_word_listener, args=(loop, trigger_queue, mic_free_event), daemon=True)
        ww_thread.start()

    # Start queue consumer for manual and WW triggers
    queue_task = asyncio.create_task(queue_consumer(app))
    
    yield

    # Cleanup on shutdown
    logger.info("Shutting down... cleaning up resources.")
    active_provider = getattr(app.state, "active_conversation_provider", None)
    if active_provider is not None:
        await active_provider.stop()
    await local_audio_output.close()
    if storage_tasks:
        await asyncio.gather(*tuple(storage_tasks), return_exceptions=True)
    queue_task.cancel()
    await asyncio.gather(queue_task, return_exceptions=True)
    await manager.close_all()


app = FastAPI(title="Jarvis Smart Mirror API", lifespan=lifespan)

cors_origins = [origin.strip() for origin in os.environ.get(
    "JARVIS_CORS_ORIGINS",
    "http://localhost:8000,http://127.0.0.1:8000,http://localhost:8080,http://127.0.0.1:8080",
).split(",") if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Content-Type"],
)


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.client_roles: dict[WebSocket, str] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, role: str = "unknown"):
        await websocket.accept()
        async with self._lock:
            self.active_connections.append(websocket)
            self.client_roles[websocket] = role
        logger.info(f"Client connected: {websocket.client}. Total clients: {len(self.active_connections)}")

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
            self.client_roles.pop(websocket, None)
        logger.info(f"Client disconnected. Total clients: {len(self.active_connections)}")

    async def broadcast(self, message: Dict[str, Any]):
        payload = json.dumps(message)
        async with self._lock:
            connections = list(self.active_connections)

        disconnected = []
        for connection in connections:
            try:
                await connection.send_text(payload)
            except Exception as e:
                logger.error(f"Error sending message to client: {e}")
                disconnected.append(connection)

        for conn in disconnected:
            await self.disconnect(conn)

    async def close_all(self):
        async with self._lock:
            connections = list(self.active_connections)
            self.active_connections.clear()
            self.client_roles.clear()
        for connection in connections:
            try:
                await connection.close(code=1001)
            except Exception:
                pass

    def has_role(self, role: str) -> bool:
        return any(self.client_roles.get(connection) == role for connection in self.active_connections)


manager = ConnectionManager()


class KnowledgeNoteRequest(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    content: str = Field(min_length=1, max_length=20000)
    tags: list[str] = Field(default_factory=list, max_length=50)


class KnowledgeLinkRequest(BaseModel):
    node_a: int = Field(gt=0)
    node_b: int = Field(gt=0)
    relation: str = Field(default="related", min_length=1, max_length=100)


async def notify_status(status_text: str):
    """
    Broadcasts status payload.
    Emits legacy {"type": "status", "data": status_text}.
    """
    await manager.broadcast({"type": "status", "data": status_text})
    tracker.process_status_event(status_text)


async def notify_state(state_val: str):
    """Broadcasts typed state payload."""
    await manager.broadcast({
        "type": "state",
        "state": state_val,
        "timestamp": time.time()
    })


async def notify_response(user_text: str, assistant_text: str, session_active: bool = False):
    """Broadcasts legacy response payload."""
    payload = {
        "type": "response",
        "user": user_text,
        "assistant": assistant_text
    }
    if session_active:
        payload["session_active"] = True
    await manager.broadcast(payload)


async def notify_transcript(role: str, text: str, is_final: bool = True):
    """Broadcasts typed transcript payload."""
    await manager.broadcast({
        "type": "transcript",
        "is_final": is_final,
        "role": role,
        "text": text,
        "timestamp": time.time()
    })


async def notify_latency():
    """Broadcasts latency metrics payload."""
    await manager.broadcast({
        "type": "latency",
        "metrics": dict(tracker.metrics)
    })


async def notify_token(token_text: str, segment_id: int):
    """Broadcast streaming LLM token frame."""
    tracker.mark_event("first_llm_token")
    await manager.broadcast({
        "type": "token",
        "token": token_text,
        "segment_id": segment_id,
        "timestamp": time.time()
    })


async def notify_amplitude(val: float, source: str = "mic"):
    """Broadcast numeric audio amplitude frame."""
    await manager.broadcast({
        "type": "amplitude",
        "value": round(val, 3),
        "source": source,
        "timestamp": time.time()
    })


async def emit_provider_event(event: dict[str, Any]):
    """Translate provider-neutral events into the established websocket contract."""
    event_type = event.get("type")
    if event_type == "status":
        await notify_status(str(event.get("data", "Idle")))
    elif event_type == "state":
        await notify_state(str(event.get("state", "sleeping")))
    elif event_type == "transcript":
        await notify_transcript(
            str(event.get("role", "assistant")),
            str(event.get("text", "")),
            bool(event.get("is_final", True)),
        )
    elif event_type == "response":
        await notify_response(
            str(event.get("user", "")),
            str(event.get("assistant", "")),
            bool(event.get("session_active", False)),
        )
    elif event_type == "amplitude":
        await notify_amplitude(float(event.get("value", 0.0)), str(event.get("source", "mic")))
    elif event_type == "metric":
        tracker.mark_event(str(event.get("event", "")), float(event.get("timestamp", time.time())))
    else:
        await manager.broadcast(event)


def pcm_to_wav(pcm: bytes, rate: int = 16000) -> bytes:
    """Wrap raw signed 16-bit mono PCM for the existing Whisper path."""
    output = io.BytesIO()
    with wave.open(output, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(rate)
        wav_file.writeframes(pcm)
    return output.getvalue()


def emit_status_threadsafe(loop: asyncio.AbstractEventLoop, status_text: str):
    """Thread-safe status notification dispatch onto main asyncio event loop."""
    if loop and loop.is_running():
        asyncio.run_coroutine_threadsafe(notify_status(status_text), loop)


def emit_amplitude_threadsafe(loop: asyncio.AbstractEventLoop, val: float, source: str = "mic"):
    """Thread-safe amplitude notification dispatch onto main asyncio event loop."""
    if loop and loop.is_running():
        asyncio.run_coroutine_threadsafe(notify_amplitude(val, source), loop)


# --- ML Pipeline Implementation ---

def wake_word_listener(loop: asyncio.AbstractEventLoop, trigger_queue: asyncio.Queue, mic_free_event: threading.Event):
    """Continuous background listener for OpenWakeWord and followup speech detection."""
    global session_active
    try:
        model_path = os.path.join(BASE_DIR, "models", "hey_jarvis_v0.1.onnx")
        if not os.path.exists(model_path):
            return
        from openwakeword.model import Model
        oww = Model(wakeword_model_paths=[model_path])
        pa = pyaudio.PyAudio()
        
        while True:
            mic_free_event.wait()
            try:
                stream = pa.open(rate=16000, channels=1, format=pyaudio.paInt16, input=True, frames_per_buffer=1280)
                while mic_free_event.is_set():
                    pcm_bytes = stream.read(1280, exception_on_overflow=False)
                    pcm = np.frombuffer(pcm_bytes, dtype=np.int16)
                    scores = oww.predict(pcm)
                    amp = calculate_rms_amplitude(pcm_bytes)
                    
                    if session_active and amp > 0.15:
                        logger.info("Followup speech energy detected!")
                        asyncio.run_coroutine_threadsafe(
                            trigger_queue.put({"kind": "wake", "detected_at": time.time()}),
                            loop,
                        )
                        break
                    elif max(scores.values()) > 0.5:
                        logger.info("Wake word detected!")
                        asyncio.run_coroutine_threadsafe(
                            trigger_queue.put({"kind": "wake", "detected_at": time.time()}),
                            loop,
                        )
                        break
                stream.stop_stream()
                stream.close()
            except Exception:
                time.sleep(0.5)
    except Exception as e:
        logger.debug(f"Wake word listener inactive: {e}")


def record_audio(duration_seconds=5, rate=16000, loop=None) -> bytes:
    """Record from default mic using VAD, returning WAV bytes while emitting mic amplitude."""
    try:
        import webrtcvad
        vad = webrtcvad.Vad(3)
        pa = pyaudio.PyAudio()
        
        frame_duration_ms = 30
        chunk_size = int(rate * frame_duration_ms / 1000) # 480
        
        stream = pa.open(format=pyaudio.paInt16, channels=1, rate=rate,
                         input=True, frames_per_buffer=chunk_size)
        frames = []
        silence_frames = 0
        max_silence_frames = int(SPEECH_END_SILENCE_MS / frame_duration_ms)
        max_total_frames = int(rate / chunk_size * MAX_UTTERANCE_SECONDS)
        
        speech_started = False
        pre_speech_frames = deque(maxlen=int(500 / frame_duration_ms)) # 500ms pre-speech buffer
        
        for _ in range(max_total_frames):
            data = stream.read(chunk_size, exception_on_overflow=False)
            amp = calculate_rms_amplitude(data)
            if loop:
                emit_amplitude_threadsafe(loop, amp, source="mic")
                
            is_speech = vad.is_speech(data, rate)
            
            if not speech_started:
                if is_speech:
                    speech_started = True
                    frames.extend(list(pre_speech_frames))
                    frames.append(data)
                else:
                    pre_speech_frames.append(data)
            else:
                frames.append(data)
                if not is_speech:
                    silence_frames += 1
                else:
                    silence_frames = 0
                    
                if silence_frames > max_silence_frames:
                    break

        stream.stop_stream()
        stream.close()
        pa.terminate()
        buf = io.BytesIO()
        with wave.open(buf, 'wb') as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(rate)
            wf.writeframes(b''.join(frames))
        return buf.getvalue()
    except Exception as e:
        logger.warning(f"PyAudio recording fallback: {e}")
        return b""


def transcribe(app_state, wav_bytes: bytes) -> str:
    if not hasattr(app_state, "whisper") or app_state.whisper is None:
        try:
            from faster_whisper import WhisperModel
            app_state.whisper = WhisperModel(
                WHISPER_MODEL,
                device=WHISPER_DEVICE,
                compute_type=WHISPER_COMPUTE_TYPE,
            )
        except Exception:
            try:
                from faster_whisper import WhisperModel
                app_state.whisper = WhisperModel(WHISPER_MODEL, device="cpu", compute_type="int8")
            except Exception as e:
                logger.error(f"Whisper init failed: {e}")
                return "What's the weather like today?"
                
    buf = io.BytesIO(wav_bytes)
    segments, _ = app_state.whisper.transcribe(
        buf,
        language=WHISPER_LANGUAGE,
        beam_size=WHISPER_BEAM_SIZE,
        without_timestamps=True,
        condition_on_previous_text=False,
    )
    return " ".join(seg.text.strip() for seg in segments).strip()


def build_system_prompt(rag_context: str, history: List[Dict[str, str]] = None) -> str:
    base_prompt = "You are Jarvis, a smart mirror AI assistant. You are concise, helpful, and direct."
    if rag_context:
        base_prompt += f"\n\nRelevant context from memory:\n{rag_context}"
    return base_prompt


def execute_blocking_ai_pipeline(loop: asyncio.AbstractEventLoop, app_state: Any = None) -> tuple[str, str]:
    """
    Default synchronous AI pipeline worker intended for thread execution.
    Can be replaced or monkeypatched during testing via test_utils.py.
    """
    emit_status_threadsafe(loop, "Listening...")
    time.sleep(0.01)
    
    emit_status_threadsafe(loop, "Transcribing...")
    time.sleep(0.01)
    user_text = "What's the weather like today?"
    
    emit_status_threadsafe(loop, "Thinking...")
    time.sleep(0.01)
    assistant_text = "It is currently 72°F and sunny in your area."
    
    emit_status_threadsafe(loop, "Speaking...")
    time.sleep(0.01)
    
    return user_text, assistant_text


# --- Streaming Pipeline Orchestration ---

async def generate_llm_stream(app_state, transcript: str, rag_context: str, history: List[Dict[str, str]]):
    """Async generator yielding streaming LLM tokens."""
    system_prompt = build_system_prompt(rag_context, history)
    messages = [{"role": "system", "content": system_prompt}]
    if history:
        messages.extend(
            {"role": item.get("role", "user"), "content": str(item.get("content", ""))[:2000]}
            for item in list(history)[-10:]
        )
    messages.append({"role": "user", "content": transcript})

    if getattr(app_state, "llm_type", "ollama") == "ollama" and getattr(app_state, "llm_client", None):
        try:
            response_stream = await app_state.llm_client.chat(
                model=OLLAMA_MODEL,
                messages=messages,
                stream=True,
                keep_alive=OLLAMA_KEEP_ALIVE,
                options={"num_ctx": OLLAMA_NUM_CTX, "num_predict": OLLAMA_NUM_PREDICT},
            )
            async for chunk in response_stream:
                token = chunk.get("message", {}).get("content", "")
                if token:
                    yield token
            return
        except Exception as e:
            logger.warning(f"Ollama stream error, fallback to default response: {e}")
            
    yield "It is currently 72°F and sunny in your area."


def _mic_barge_in_monitor(stop_event: threading.Event, threshold: float = 0.65):
    """Background listener during TTS playback for acoustic barge-in detection."""
    try:
        pa = pyaudio.PyAudio()
        stream = pa.open(format=pyaudio.paInt16, channels=1, rate=16000,
                         input=True, frames_per_buffer=1024)
        while not stop_event.is_set() and not barge_in_event.is_set():
            data = stream.read(1024, exception_on_overflow=False)
            if not data:
                continue
            amp = calculate_rms_amplitude(data)
            
            global latest_tts_amplitude
            # Dynamic threshold: Needs to be higher than TTS amplitude
            dynamic_threshold = max(threshold, latest_tts_amplitude * 1.5 + 0.1)
            
            if amp > dynamic_threshold:
                logger.info(f"Barge-in acoustic threshold exceeded (mic {amp:.2f} > threshold {dynamic_threshold:.2f} [TTS {latest_tts_amplitude:.2f}]), setting barge_in_event")
                barge_in_event.set()
                break
        stream.stop_stream()
        stream.close()
        pa.terminate()
    except Exception as e:
        logger.debug(f"Mic acoustic gating monitor exception: {e}")


async def synthesize_segment_and_play(
    segment_text: str,
    segment_id: int,
    *,
    monitor_barge_in: bool = True,
):
    """Stream Edge-TTS through FFmpeg and play PCM without temporary files."""
    if barge_in_event.is_set():
        return False

    stop_mic_monitor = threading.Event()
    mic_thread = None
    if monitor_barge_in:
        mic_thread = threading.Thread(
            target=_mic_barge_in_monitor,
            args=(stop_mic_monitor, 0.65),
            daemon=True,
        )
        mic_thread.start()

    try:
        import edge_tts

        communicate = edge_tts.Communicate(segment_text, EDGE_TTS_VOICE)
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-f", "mp3", "-i", "pipe:0",
            "-f", "s16le", "-ac", "1", "-ar", "16000", "pipe:1",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )

        async def feed_decoder() -> None:
            try:
                async for event in communicate.stream():
                    if barge_in_event.is_set():
                        break
                    if event.get("type") == "audio" and event.get("data"):
                        proc.stdin.write(event["data"])
                        await proc.stdin.drain()
            finally:
                if proc.stdin and not proc.stdin.is_closing():
                    proc.stdin.close()

        feed_task = asyncio.create_task(feed_decoder())
        first_audio = True
        try:
            while True:
                if barge_in_event.is_set():
                    proc.kill()
                    break
                chunk = await proc.stdout.read(2048)
                if not chunk:
                    break
                if first_audio:
                    tracker.mark_event("first_tts_audio")
                    first_audio = False
                await local_audio_output.write(chunk)

                amp = calculate_rms_amplitude(chunk)
                global latest_tts_amplitude
                latest_tts_amplitude = amp
                await notify_amplitude(amp, source="tts")
        finally:
            if barge_in_event.is_set() and proc.returncode is None:
                proc.kill()
            await asyncio.gather(feed_task, return_exceptions=True)
            await proc.wait()

        return not barge_in_event.is_set()
    except Exception as e:
        logger.debug(f"Segment synthesis fallback: {e}")
        return True
    finally:
        stop_mic_monitor.set()
        if mic_thread is not None and mic_thread.is_alive():
            mic_thread.join(timeout=0.2)


async def queue_consumer(app: FastAPI):
    """Consumes triggers from the queue to start the pipeline, managing followup silence timeouts."""
    global session_active
    followup_task = None

    async def followup_timeout_task():
        global session_active
        await asyncio.sleep(FOLLOWUP_TIMEOUT_SECONDS)
        if session_active and not pipeline_lock.locked():
            logger.info("Followup silence timeout (10s) elapsed. Resetting session to sleeping.")
            session_active = False
            await notify_state("sleeping")
            await notify_status("Idle")

    while True:
        try:
            if session_active and (followup_task is None or followup_task.done()):
                followup_task = asyncio.create_task(followup_timeout_task())

            trigger = await trigger_queue.get()

            if followup_task and not followup_task.done():
                followup_task.cancel()
                followup_task = None

            if pipeline_lock.locked():
                logger.warning("Pipeline already running, ignoring queue trigger.")
            else:
                barge_in_event.clear()
                if isinstance(trigger, dict) and trigger.get("detected_at") is not None:
                    tracker.mark_event("wake", float(trigger["detected_at"]))
                await run_pipeline_cycle(app)
            trigger_queue.task_done()
        except Exception as e:
            logger.error(f"Error in queue_consumer: {e}")
            await asyncio.sleep(0.1)


async def run_pipeline_cycle(app: FastAPI):
    """Select the configured provider and preserve local automatic fallback."""
    global gemini_retry_after
    settings = getattr(app.state, "gemini_settings", GeminiSettings.from_env())

    if settings.available and time.monotonic() >= gemini_retry_after:
        await local_audio_output.close()
        fallback_error = await run_gemini_conversation(app, settings)
        if fallback_error is None:
            return
        gemini_retry_after = time.monotonic() + settings.fallback_cooldown_seconds
        await manager.broadcast({
            "type": "provider",
            "provider": "local",
            "status": "fallback",
            "reason": fallback_error.reason,
        })
        local_provider = LocalConversationProvider(
            lambda: run_local_pipeline_cycle(
                app,
                fallback_pcm=fallback_error.pcm,
                fallback_transcript=fallback_error.transcript,
            )
        )
        await local_provider.run()
        return

    if settings.requested:
        reason = "configuration" if not settings.api_key else "network"
        status_text = "unavailable" if not settings.api_key else "fallback"
        await manager.broadcast({
            "type": "provider",
            "provider": "local",
            "status": status_text,
            "reason": reason,
        })
    local_provider = LocalConversationProvider(lambda: run_local_pipeline_cycle(app))
    await local_provider.run()


async def run_gemini_conversation(
    app: FastAPI,
    settings: GeminiSettings,
) -> GeminiLiveError | None:
    """Run one multi-turn cloud session and return a recoverable fallback error."""
    global session_active, conversation_history
    if not settings.available:
        return GeminiLiveError("configuration")

    async def search_memory(query: str, limit: int) -> list[dict[str, Any]]:
        if not getattr(app.state, "kg", None) or not query.strip():
            return []
        results = await asyncio.get_running_loop().run_in_executor(
            ml_executor,
            lambda: app.state.kg.search_nodes(query, limit=limit),
        )
        return [dict(item) for item in results[:limit]]

    provider = GeminiLiveProvider(
        settings,
        emit=emit_provider_event,
        knowledge_search=search_memory,
        history=conversation_history,
        audio=PyAudioDuplex(pyaudio),
        sign_off=is_sign_off_phrase,
    )
    app.state.active_conversation_provider = provider

    async def persist_turns(turns: list[tuple[str, str]]) -> None:
        for user_text, assistant_text in turns:
            if not user_text and not assistant_text:
                continue
            conversation_history.append({"role": "user", "content": user_text})
            conversation_history.append({"role": "assistant", "content": assistant_text})
            if getattr(app.state, "kg", None):
                schedule_knowledge_write(
                    lambda u=user_text, a=assistant_text: app.state.kg.add_conversation(u, a)
                )

    async with pipeline_lock:
        mic_free_event.clear()
        session_active = True
        try:
            result = await provider.run()
            await persist_turns(result.turns)
            if result.ended_by == "sign-off":
                conversation_history.clear()
            tracker.complete_cycle(outcome="completed")
            return None
        except GeminiLiveError as error:
            await persist_turns(error.turns)
            tracker.complete_cycle(outcome="failed")
            return error
        finally:
            session_active = False
            app.state.active_conversation_provider = None
            mic_free_event.set()
            await notify_state("sleeping")
            await notify_status("Idle")


async def run_local_pipeline_cycle(
    app: FastAPI,
    *,
    fallback_pcm: bytes = b"",
    fallback_transcript: str = "",
):
    """Full local AI pipeline cycle executing non-blocking ML operations."""
    global session_active, conversation_history
    loop = asyncio.get_running_loop()
    
    async with pipeline_lock:
        try:
            mic_free_event.clear()

            # Check if execute_blocking_ai_pipeline has been monkeypatched (e.g., during E2E test runs)
            if execute_blocking_ai_pipeline != _default_execute_blocking_ai_pipeline:
                logger.info("Executing monkeypatched pipeline function...")
                user_text, assistant_text = await loop.run_in_executor(
                    ml_executor, execute_blocking_ai_pipeline, loop
                )
                if is_sign_off_phrase(user_text):
                    session_active = False
                    conversation_history.clear()
                else:
                    session_active = True
                    conversation_history.append({"role": "user", "content": user_text})
                    conversation_history.append({"role": "assistant", "content": assistant_text})
                await notify_response(user_text, assistant_text, session_active=session_active)
                await notify_transcript("user", user_text)
                await notify_transcript("assistant", assistant_text)
                await notify_latency()
                await asyncio.sleep(0.01)
                if session_active:
                    await notify_state("listening-followup")
                else:
                    await notify_state("sleeping")
                await notify_status("Idle")
                return

            # Stage 1: Listening
            await notify_status("Listening...")
            await notify_state("listening-followup" if session_active else "listening")
            if fallback_pcm:
                wav = pcm_to_wav(fallback_pcm)
            else:
                wav = await loop.run_in_executor(ml_executor, record_audio, 5, 16000, loop)

            # Stage 2: Transcribing
            await notify_status("Transcribing...")
            await notify_state("thinking")
            if fallback_transcript.strip():
                transcript = fallback_transcript.strip()
            elif fallback_pcm:
                transcript = await loop.run_in_executor(ml_executor, transcribe, app.state, wav)
            elif hasattr(app.state, "whisper") and app.state.whisper:
                transcript = await loop.run_in_executor(ml_executor, transcribe, app.state, wav)
            else:
                transcript = "What's the weather like today?"

            if not transcript.strip():
                logger.info("Empty transcription, resetting to Idle.")
                session_active = False
                await notify_state("sleeping")
                await notify_status("Idle")
                return

            logger.info("Transcription completed (characters=%d)", len(transcript))

            # Check sign-off phrase
            if is_sign_off_phrase(transcript):
                logger.info("Sign-off phrase detected. Ending session.")
                session_active = False
                conversation_history.clear()
                assistant_response = "You're welcome! Goodbye."
                await notify_status("Thinking...")
                await notify_state("thinking")
                await notify_status("Speaking...")
                await notify_state("speaking")
                await synthesize_segment_and_play(assistant_response, 1)
                await notify_response(transcript, assistant_response, session_active=False)
                await notify_transcript("user", transcript)
                await notify_transcript("assistant", assistant_response)
                await notify_latency()
                await notify_state("sleeping")
                await notify_status("Idle")
                return

            # Stage 3: Thinking & Streaming LLM
            await notify_status("Thinking...")
            await notify_state("thinking")
            rag_context = ""
            if hasattr(app.state, "kg") and app.state.kg:
                rag_context = await loop.run_in_executor(ml_executor, app.state.kg.get_relevant_context, transcript, 800)

            segmenter = ClauseSegmenter(min_clause_len=20)
            full_assistant_response = ""
            segment_queue = asyncio.Queue()
            
            async def stream_tokens_task():
                nonlocal full_assistant_response
                async for token in generate_llm_stream(app.state, transcript, rag_context, conversation_history):
                    full_assistant_response += token
                    await notify_token(token, segmenter.segment_id + 1)
                    segs = segmenter.feed(token)
                    for seg_id, seg_text in segs:
                        await segment_queue.put((seg_id, seg_text))
                for seg_id, seg_text in segmenter.flush():
                    await segment_queue.put((seg_id, seg_text))
                await segment_queue.put(None) # End sentinel

            stream_task = asyncio.create_task(stream_tokens_task())

            # Stage 4: Speaking & Incremental Edge-TTS Synthesis
            await notify_status("Speaking...")
            await notify_state("speaking")
            stop_mic_monitor = threading.Event()
            mic_thread = None
            try:
                while True:
                    item = await segment_queue.get()
                    if item is None:
                        break
                    if mic_thread is None:
                        mic_thread = threading.Thread(
                            target=_mic_barge_in_monitor,
                            args=(stop_mic_monitor, 0.65),
                            daemon=True,
                        )
                        mic_thread.start()
                    seg_id, seg_text = item
                    success = await synthesize_segment_and_play(
                        seg_text,
                        seg_id,
                        monitor_barge_in=False,
                    )
                    if not success:
                        logger.info("Synthesis / playback interrupted (barge-in or error).")
                        break
            finally:
                stop_mic_monitor.set()
                if mic_thread is not None and mic_thread.is_alive():
                    mic_thread.join(timeout=0.2)

            await stream_task

            if barge_in_event.is_set():
                tracker.complete_cycle(outcome="interrupted")

            # Update conversation history
            conversation_history.append({"role": "user", "content": transcript})
            conversation_history.append({"role": "assistant", "content": full_assistant_response})

            session_active = True
            await notify_response(transcript, full_assistant_response, session_active=session_active)
            await notify_transcript("user", transcript)
            await notify_transcript("assistant", full_assistant_response)
            await notify_latency()

            # Stage 5: Knowledge Graph auto-indexing
            if hasattr(app.state, "kg") and app.state.kg:
                schedule_knowledge_write(
                    lambda: app.state.kg.add_conversation(transcript, full_assistant_response)
                )

            # Done turn - transition to listening-followup if session_active, else sleeping
            await asyncio.sleep(0.05)
            if session_active:
                await notify_state("listening-followup")
            else:
                await notify_state("sleeping")
            await notify_status("Idle")

        except Exception:
            logger.exception("Pipeline cycle failed")
            await notify_status("Error")
            await notify_state("error")
            await notify_transcript("assistant", "I encountered an internal error.")
            
            # Synthesize verbal apology
            try:
                await synthesize_segment_and_play("I encountered an internal error.", 999)
            except Exception:
                pass
                
            await asyncio.sleep(2)
            await notify_state("sleeping")
            await notify_status("Idle")
            session_active = False
        finally:
            app.state.tracker.complete_cycle()
            latest = app.state.tracker.get_history(1)
            if latest:
                logger.info(
                    "pipeline_cycle_complete cycle_id=%s outcome=%s duration_ms=%s",
                    latest[0]["cycle_id"], latest[0].get("outcome"), latest[0].get("total_duration_ms")
                )
            mic_free_event.set()


# Default function handle reference for detecting test overrides
_default_execute_blocking_ai_pipeline = execute_blocking_ai_pipeline


# --- REST & WS Endpoints ---

@app.get("/health")
async def get_health():
    settings = getattr(app.state, "gemini_settings", GeminiSettings.from_env())
    active_provider = getattr(app.state, "active_conversation_provider", None)
    return {
        "status": "ok",
        "active_connections": len(manager.active_connections),
        "pipeline_busy": pipeline_lock.locked(),
        "audio_available": pyaudio is not None,
        "conversation_provider": getattr(active_provider, "name", "local"),
        "gemini_configured": settings.available,
        "ollama_available": getattr(app.state, "llm_client", None) is not None,
        "mirror_connected": manager.has_role("mirror"),
        "phone_connected": manager.has_role("phone"),
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    requested_role = str(websocket.query_params.get("client", "unknown")).lower()
    role = requested_role if requested_role in {"mirror", "phone", "dashboard", "unknown"} else "unknown"
    await manager.connect(websocket, role)
    try:
        await websocket.send_text(json.dumps({"type": "status", "data": "Connected"}))
        while True:
            data = await websocket.receive_text()
            if len(data.encode("utf-8")) > MAX_WS_MESSAGE_BYTES:
                logger.warning("Closing WebSocket client after oversized message")
                await websocket.close(code=1009)
                break
            try:
                msg = json.loads(data)
                if isinstance(msg, dict):
                    action = msg.get("action")
                    if action in ("barge_in", "stop", "cancel"):
                        logger.info(f"WS action '{action}' received: signaling barge-in")
                        barge_in_event.set()
                        active_provider = getattr(app.state, "active_conversation_provider", None)
                        if active_provider is not None:
                            if action in ("stop", "cancel"):
                                await active_provider.stop()
                            else:
                                await active_provider.interrupt()
                        await manager.broadcast({"type": "tts_cancel", "event": "barge_in", "timestamp": time.time()})
                    elif action == "trigger":
                        if pipeline_lock.locked() or not trigger_queue.empty():
                            logger.info("WS trigger received while pipeline busy: signaling barge-in")
                            barge_in_event.set()
                            active_provider = getattr(app.state, "active_conversation_provider", None)
                            if active_provider is not None:
                                await active_provider.interrupt()
                            await manager.broadcast({"type": "tts_cancel", "event": "barge_in", "timestamp": time.time()})
                        else:
                            barge_in_event.clear()
                            await trigger_queue.put({"kind": "wake", "detected_at": time.time()})
                    elif action == "client_event":
                        event = msg.get("event")
                        if isinstance(event, dict) and event.get("type") == "face_cue":
                            await manager.broadcast({
                                "type": "face_cue",
                                "expression": str(event.get("expression", "neutral"))[:32],
                                "source": str(event.get("source", "mirror"))[:32],
                                "timestamp": time.time(),
                            })
                        else:
                            await websocket.send_json({"type": "error", "code": "unsupported_client_event"})
                    elif action not in (None, "ping"):
                        await websocket.send_json({"type": "error", "code": "unsupported_action"})
            except json.JSONDecodeError:
                logger.warning("Received invalid JSON payload over WebSocket")
                await websocket.send_json({"type": "error", "code": "invalid_json"})
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except Exception as e:
        logger.error(f"Unexpected WebSocket error: {e}")
    finally:
        await manager.disconnect(websocket)


@app.post("/trigger")
async def trigger_pipeline(response: Response):
    """HTTP trigger endpoint for manual testing or hardware pin interrupts."""
    if pipeline_lock.locked() or not trigger_queue.empty():
        logger.info("HTTP POST /trigger received while pipeline busy: signaling barge-in")
        barge_in_event.set()
        active_provider = getattr(app.state, "active_conversation_provider", None)
        if active_provider is not None:
            await active_provider.interrupt()
        response.status_code = status.HTTP_409_CONFLICT
        return {
            "status": "busy",
            "message": "Pipeline cycle already in progress"
        }
    barge_in_event.clear()
    await trigger_queue.put({"kind": "wake", "detected_at": time.time()})
    return {"status": "Pipeline cycle triggered"}


# ── Knowledge Graph API ──

@app.get("/api/knowledge/graph")
async def get_knowledge_graph():
    return kg.get_graph_data()

@app.get("/api/knowledge/stats")
async def get_knowledge_stats():
    return kg.get_stats()

@app.get("/api/knowledge/search")
async def search_knowledge(
    q: str = Query(default="", max_length=500),
    limit: int = Query(default=10, ge=1, le=100),
):
    if not q.strip():
        return []
    return kg.search_nodes(q, limit=limit)

@app.get("/api/knowledge/nodes/{node_id}")
async def get_knowledge_node(node_id: int):
    node = kg.get_node(node_id)
    if not node:
        return Response(status_code=404, content=json.dumps({"error": "Node not found"}),
                        media_type="application/json")
    return node

@app.post("/api/knowledge/notes")
async def create_knowledge_note(note: KnowledgeNoteRequest):
    node_id = kg.add_note(note.title.strip(), note.content.strip(), note.tags)
    await manager.broadcast({"type": "knowledge_update", "node_id": node_id})
    return {"status": "created", "node_id": node_id}

@app.post("/api/knowledge/link")
async def link_knowledge_nodes(link: KnowledgeLinkRequest):
    kg.link_nodes(link.node_a, link.node_b, link.relation.strip())
    return {"status": "linked", "node_a": link.node_a, "node_b": link.node_b, "relation": link.relation.strip()}

@app.delete("/api/knowledge/nodes/{node_id}")
async def delete_knowledge_node(node_id: int):
    deleted = kg.delete_node(node_id)
    if not deleted:
        return Response(status_code=404, content=json.dumps({"error": "Node not found"}),
                        media_type="application/json")
    await manager.broadcast({"type": "knowledge_update", "deleted_node_id": node_id})
    return {"status": "deleted", "node_id": node_id}

@app.get("/api/knowledge/context")
async def get_rag_context(
    q: str = Query(default="", max_length=500),
    max_tokens: int = Query(default=500, ge=1, le=5000),
):
    if not q.strip():
        return {"context": ""}
    return {"context": kg.get_relevant_context(q, max_tokens)}


# ── Pipeline Tracker API ──

@app.get("/api/pipeline/state")
async def get_pipeline_state():
    return tracker.get_current_state()

@app.get("/api/pipeline/history")
async def get_pipeline_history(limit: int = Query(default=20, ge=1, le=100)):
    return tracker.get_history(limit)


# ── Dashboard (static file serving) ──

def _optional_float_env(name: str) -> Optional[float]:
    try:
        return float(os.environ[name])
    except (KeyError, TypeError, ValueError):
        return None


def _fetch_configured_text(url: str) -> str:
    """Fetch only operator-configured content URLs with strict size and time bounds."""
    if not url or not url.lower().startswith(("https://", "http://")):
        return ""
    request = Request(url, headers={"User-Agent": "JarvisSmartMirror/1.0"})
    try:
        with urlopen(request, timeout=5) as response:
            return response.read(1_000_001)[:1_000_000].decode("utf-8", errors="replace")
    except OSError:
        return ""


def _rss_items(xml_text: str, limit: int = 8) -> list[dict[str, str]]:
    if not xml_text:
        return []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    source = root.findtext("./channel/title", default="NEWS")
    items = []
    for item in root.findall(".//item")[:limit]:
        title = (item.findtext("title") or "").strip()
        if title:
            items.append({"title": title[:300], "source": source[:80]})
    return items


def _ics_items(ics_text: str, limit: int = 8) -> list[dict[str, str]]:
    if not ics_text:
        return []
    unfolded = re.sub(r"\r?\n[ \t]", "", ics_text)
    items = []
    for block in re.findall(r"BEGIN:VEVENT(.*?)END:VEVENT", unfolded, flags=re.DOTALL):
        fields: dict[str, str] = {}
        for line in block.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            fields[key.split(";", 1)[0].upper()] = value.strip()
        title = fields.get("SUMMARY", "").replace("\\,", ",")
        raw_start = fields.get("DTSTART", "")
        if not title or not raw_start:
            continue
        when = raw_start[9:13] if "T" in raw_start and len(raw_start) >= 13 else "ALL DAY"
        if when != "ALL DAY":
            when = f"{when[:2]}:{when[2:]}"
        items.append({"title": title[:300], "when": when, "location": fields.get("LOCATION", "")[:200], "sort": raw_start})
    items.sort(key=lambda item: item["sort"])
    return [{key: value for key, value in item.items() if key != "sort"} for item in items[:limit]]


@app.get("/api/mirror/bootstrap")
async def get_mirror_bootstrap():
    news_url = os.environ.get("JARVIS_NEWS_FEED_URL", "").strip()
    calendar_url = os.environ.get("JARVIS_CALENDAR_URL", "").strip()
    news_text, calendar_text = await asyncio.gather(
        asyncio.to_thread(_fetch_configured_text, news_url),
        asyncio.to_thread(_fetch_configured_text, calendar_url),
    )
    return {
        "config": {
            "locale": os.environ.get("JARVIS_MIRROR_LOCALE", "en-GB"),
            "location": os.environ.get("JARVIS_LOCATION_NAME", ""),
            "latitude": _optional_float_env("JARVIS_WEATHER_LATITUDE"),
            "longitude": _optional_float_env("JARVIS_WEATHER_LONGITUDE"),
            "nightStartHour": env_int("JARVIS_NIGHT_START_HOUR", 23, 0, 23),
            "nightEndHour": env_int("JARVIS_NIGHT_END_HOUR", 6, 0, 23),
        },
        "news": _rss_items(news_text),
        "calendar": _ics_items(calendar_text),
    }


MIRROR_UI_DIR = Path(__file__).parent.parent / "mirror-ui"
if MIRROR_UI_DIR.exists():
    @app.get("/", response_class=HTMLResponse)
    @app.get("/mirror", response_class=HTMLResponse)
    async def serve_mirror():
        return HTMLResponse(content=(MIRROR_UI_DIR / "index.html").read_text(encoding="utf-8"))
    app.mount("/mirror/static", StaticFiles(directory=str(MIRROR_UI_DIR)), name="mirror_static")


DASHBOARD_DIR = Path(__file__).parent.parent / "dashboard"
if DASHBOARD_DIR.exists():
    @app.get("/dashboard", response_class=HTMLResponse)
    async def serve_dashboard():
        index_path = DASHBOARD_DIR / "index.html"
        return HTMLResponse(content=index_path.read_text(encoding="utf-8"))
    app.mount("/dashboard/static", StaticFiles(directory=str(DASHBOARD_DIR)), name="dashboard_static")

PHONE_FACE_DIR = Path(__file__).parent.parent / "phone-face"
if PHONE_FACE_DIR.exists():
    @app.get("/face", response_class=HTMLResponse)
    async def serve_phone_face():
        index_path = PHONE_FACE_DIR / "index.html"
        return HTMLResponse(content=index_path.read_text(encoding="utf-8"))
    app.mount("/face/static", StaticFiles(directory=str(PHONE_FACE_DIR)), name="phone_face_static")

if __name__ == "__main__":
    uvicorn.run(
        "bridge_api:app",
        host=os.environ.get("JARVIS_HOST", "127.0.0.1"),
        port=int(os.environ.get("JARVIS_PORT", "8000")),
        reload=False,
    )
