"""
Audio Manager (audio_manager.py)
Handles PyAudio streaming, VAD, microphone event locking, and acoustic gating.
"""
import io
import wave
import logging
from collections import deque
import threading
import numpy as np
import pyaudio

logger = logging.getLogger("JarvisAudioManager")

# Global PyAudio instance to avoid 100-200ms latency on repeated initializations
_pa_instance = None

def get_pyaudio_instance():
    global _pa_instance
    if _pa_instance is None:
        _pa_instance = pyaudio.PyAudio()
    return _pa_instance

def calculate_rms_amplitude(pcm_data: bytes) -> float:
    """Calculate logarithmic RMS audio amplitude normalized to float [0.0, 1.0]."""
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
        min_db = -60.0
        max_db = -6.0
        norm = (dbfs - min_db) / (max_db - min_db)
        return float(np.clip(norm, 0.0, 1.0))
    except Exception:
        return 0.0

def record_audio(emit_amplitude_cb, duration_seconds=5, rate=16000) -> bytes:
    """Record from default mic using VAD, returning WAV bytes."""
    try:
        import webrtcvad
        vad = webrtcvad.Vad(3)
        pa = get_pyaudio_instance()
        
        frame_duration_ms = 30
        chunk_size = int(rate * frame_duration_ms / 1000)
        
        stream = pa.open(format=pyaudio.paInt16, channels=1, rate=rate,
                         input=True, frames_per_buffer=chunk_size)
        frames = []
        silence_frames = 0
        max_silence_frames = int(1500 / frame_duration_ms)
        max_total_frames = int(rate / chunk_size * 15)
        
        speech_started = False
        pre_speech_frames = deque(maxlen=int(500 / frame_duration_ms))
        
        for _ in range(max_total_frames):
            data = stream.read(chunk_size, exception_on_overflow=False)
            amp = calculate_rms_amplitude(data)
            if emit_amplitude_cb:
                emit_amplitude_cb(amp, "mic")
                
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
