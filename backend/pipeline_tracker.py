"""
Pipeline tracker for Jarvis Smart Mirror.

Tracks real-time state and timing of each AI pipeline stage.
Provides current state snapshots and historical cycle data for visualization.
"""

import time
import uuid
from collections import deque
from typing import Optional


# Pipeline stage definitions in execution order
PIPELINE_STAGES = [
    {"id": "wake_word", "name": "Wake Word Detection"},
    {"id": "recording", "name": "Audio Recording"},
    {"id": "transcription", "name": "Speech Transcription"},
    {"id": "reasoning", "name": "Reasoning Model"},
    {"id": "synthesis", "name": "Speech Synthesis"},
]

# Pipeline edges (directed flow)
PIPELINE_EDGES = [
    {"source": "wake_word", "target": "recording"},
    {"source": "recording", "target": "transcription"},
    {"source": "transcription", "target": "reasoning"},
    {"source": "reasoning", "target": "synthesis"},
]

# Valid state values
VALID_STATES = {"idle", "active", "completed", "error", "interrupted"}

# Mapping from bridge_api.py status strings to pipeline stage IDs
STATUS_TO_STAGE = {
    "Listening...": "recording",
    "Transcribing...": "transcription",
    "Thinking...": "reasoning",
    "Speaking...": "synthesis",
}


class StageState:
    """Tracks the state and timing of a single pipeline stage."""

    __slots__ = ("id", "name", "state", "started_at", "completed_at")

    def __init__(self, stage_id: str, name: str):
        self.id = stage_id
        self.name = name
        self.state: str = "idle"
        self.started_at: Optional[float] = None
        self.completed_at: Optional[float] = None

    def set_state(self, state: str) -> None:
        """Update this stage's state and record timestamps.

        Args:
            state: One of 'idle', 'active', 'completed', 'error'.
        """
        if state not in VALID_STATES:
            raise ValueError(f"Invalid state '{state}'. Must be one of: {VALID_STATES}")

        now = time.time()
        if state == "active" and self.state != "active":
            self.started_at = now
            self.completed_at = None
        elif state in ("completed", "error", "interrupted") and self.state == "active":
            self.completed_at = now
        elif state == "idle":
            self.started_at = None
            self.completed_at = None

        self.state = state

    @property
    def duration_ms(self) -> Optional[float]:
        """Duration in milliseconds if stage has both start and end timestamps."""
        if self.started_at is not None and self.completed_at is not None:
            return round((self.completed_at - self.started_at) * 1000, 1)
        elif self.started_at is not None and self.state == "active":
            return round((time.time() - self.started_at) * 1000, 1)
        return None

    def to_dict(self) -> dict:
        """Serialize stage state to a dict."""
        return {
            "id": self.id,
            "name": self.name,
            "state": self.state,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_ms": self.duration_ms,
        }

    def reset(self) -> None:
        """Reset stage to idle state."""
        self.state = "idle"
        self.started_at = None
        self.completed_at = None


class PipelineTracker:
    """Tracks the real-time state and timing of the AI voice pipeline stages.

    Provides state snapshots for live visualization and maintains a history
    of completed cycles for the dashboard timeline view.
    """

    def __init__(self, max_history: int = 100):
        """Initialize the pipeline tracker.

        Args:
            max_history: Maximum number of completed cycles to retain in history.
        """
        self.stages: dict[str, StageState] = {}
        self._stage_order: list[str] = []
        self.current_cycle_id: Optional[str] = None
        self.cycle_start_time: Optional[float] = None
        self.cycle_end_time: Optional[float] = None
        self._history: deque[dict] = deque(maxlen=max_history)

        self.metrics: dict[str, Optional[float]] = {
            "wake_to_stt_start_ms": None,
            "stt_duration_ms": None,
            "ttft_llm_token_ms": None,
            "ttft_tts_audio_ms": None,
            "total_roundtrip_ms": None,
        }
        self.t_wake_detected: Optional[float] = None
        self.t_stt_start: Optional[float] = None
        self.t_stt_end: Optional[float] = None
        self.t_llm_start: Optional[float] = None
        self.t_first_llm_token: Optional[float] = None
        self.t_tts_start: Optional[float] = None
        self.t_first_tts_audio: Optional[float] = None
        self._pending_wake_timestamp: Optional[float] = None

        # Initialize stages from pipeline definition
        for stage_def in PIPELINE_STAGES:
            stage = StageState(stage_def["id"], stage_def["name"])
            self.stages[stage_def["id"]] = stage
            self._stage_order.append(stage_def["id"])

    def mark_event(self, event_name: str, timestamp: Optional[float] = None) -> None:
        """Record timestamp for explicit pipeline latency events."""
        now = timestamp if timestamp is not None else time.time()
        if event_name == "wake":
            if self.current_cycle_id is None or self.cycle_end_time is not None:
                self._pending_wake_timestamp = now
            else:
                self.t_wake_detected = now
        elif event_name == "stt_start":
            self.t_stt_start = now
            if self.t_wake_detected:
                self.metrics["wake_to_stt_start_ms"] = round((now - self.t_wake_detected) * 1000, 1)
        elif event_name == "stt_end":
            self.t_stt_end = now
            if self.t_stt_start:
                self.metrics["stt_duration_ms"] = round((now - self.t_stt_start) * 1000, 1)
        elif event_name == "llm_start":
            self.t_llm_start = now
        elif event_name == "first_llm_token":
            if self.t_first_llm_token is None:
                self.t_first_llm_token = now
                if self.t_llm_start:
                    self.metrics["ttft_llm_token_ms"] = round((now - self.t_llm_start) * 1000, 1)
        elif event_name == "tts_start":
            if self.t_tts_start is None:
                self.t_tts_start = now
        elif event_name == "first_tts_audio":
            if self.t_first_tts_audio is None:
                self.t_first_tts_audio = now
                if self.t_tts_start:
                    self.metrics["ttft_tts_audio_ms"] = round((now - self.t_tts_start) * 1000, 1)

    def set_stage_state(self, stage_id: str, state: str) -> None:
        """Update a pipeline stage's state.

        When a stage transitions to 'active', the previous stage is
        automatically marked as 'completed'.

        Args:
            stage_id: The stage identifier (e.g., 'recording', 'transcription').
            state: Target state ('idle', 'active', 'completed', 'error').

        Raises:
            KeyError: If stage_id is not a valid pipeline stage.
            ValueError: If state is not a valid state value.
        """
        if stage_id not in self.stages:
            raise KeyError(f"Unknown pipeline stage: '{stage_id}'. Valid stages: {list(self.stages.keys())}")

        # Auto-complete previous stages when a new one becomes active
        if state == "active":
            stage_idx = self._stage_order.index(stage_id)
            for prev_id in self._stage_order[:stage_idx]:
                prev = self.stages[prev_id]
                if prev.state == "active":
                    prev.set_state("completed")

        self.stages[stage_id].set_state(state)

    def start_cycle(self) -> str:
        """Reset all stages and begin a new pipeline cycle.

        Returns:
            A unique cycle ID (UUID4 string).
        """
        self.current_cycle_id = str(uuid.uuid4())
        now = time.time()
        self.cycle_start_time = self._pending_wake_timestamp or now
        self.cycle_end_time = None

        self.metrics = {
            "wake_to_stt_start_ms": None,
            "stt_duration_ms": None,
            "ttft_llm_token_ms": None,
            "ttft_tts_audio_ms": None,
            "total_roundtrip_ms": None,
        }
        self.t_wake_detected = self._pending_wake_timestamp
        self._pending_wake_timestamp = None
        self.t_stt_start = None
        self.t_stt_end = None
        self.t_llm_start = None
        self.t_first_llm_token = None
        self.t_tts_start = None
        self.t_first_tts_audio = None

        for stage in self.stages.values():
            stage.reset()

        # Mark wake_word as active at cycle start
        self.stages["wake_word"].set_state("active")

        return self.current_cycle_id

    def complete_cycle(self, cycle_id: Optional[str] = None, outcome: str = "completed") -> None:
        """Mark the current cycle as completed and archive it to history.

        Args:
            cycle_id: Optional cycle ID for verification. If provided and doesn't
                      match current cycle, the call is ignored.
        """
        if not self.current_cycle_id or self.cycle_end_time is not None:
            return
        if cycle_id and cycle_id != self.current_cycle_id:
            return

        self.cycle_end_time = time.time()
        if self.cycle_start_time:
            self.metrics["total_roundtrip_ms"] = round((self.cycle_end_time - self.cycle_start_time) * 1000, 1)

        # Complete any still-active stages
        for stage in self.stages.values():
            if stage.state == "active":
                stage.set_state("completed")

        # Archive to history
        cycle_record = {
            "cycle_id": self.current_cycle_id,
            "started_at": self.cycle_start_time,
            "completed_at": self.cycle_end_time,
            "total_duration_ms": self.metrics.get("total_roundtrip_ms"),
            "outcome": outcome,
            "metrics": dict(self.metrics),
            "stages": {
                stage_id: stage.to_dict() for stage_id, stage in self.stages.items()
            },
        }
        self._history.appendleft(cycle_record)

    def get_current_state(self) -> dict:
        """Return the current state of the entire pipeline.

        Returns:
            Dict with cycle_id, stages list, and edges list for visualization.
        """
        return {
            "type": "pipeline_state",
            "cycle_id": self.current_cycle_id,
            "started_at": self.cycle_start_time,
            "metrics": dict(self.metrics),
            "stages": [self.stages[sid].to_dict() for sid in self._stage_order],
            "edges": [dict(e) for e in PIPELINE_EDGES],
        }

    def get_history(self, limit: int = 20) -> list[dict]:
        """Return recent completed pipeline cycles.

        Args:
            limit: Maximum number of history entries to return.

        Returns:
            List of cycle records, most recent first.
        """
        return list(self._history)[:limit]

    def map_status_to_stage(self, status_text: str) -> Optional[str]:
        """Map a bridge_api.py status string to a pipeline stage ID.

        Args:
            status_text: Status string (e.g., 'Listening...', 'Transcribing...').

        Returns:
            Stage ID string, or None if the status doesn't map to a stage.
        """
        return STATUS_TO_STAGE.get(status_text)

    def process_status_event(self, status_text: str) -> Optional[dict]:
        """Process a status event from bridge_api.py and update pipeline state.

        Handles the full lifecycle: starts a new cycle on 'Listening...',
        transitions stages through the pipeline, and completes the cycle on 'Idle'.

        Args:
            status_text: The status string from bridge_api.py.

        Returns:
            Updated pipeline state dict if state changed, None otherwise.
        """
        if status_text == "Connected":
            return self.get_current_state()

        stage_id = self.map_status_to_stage(status_text)

        # Start a new cycle if this is the first stage event
        if stage_id == "recording" and (
            self.current_cycle_id is None
            or all(s.state == "idle" for s in self.stages.values())
        ):
            self.start_cycle()
            # Wake word is already completed if we're recording
            self.stages["wake_word"].set_state("completed")

        if status_text == "Listening...":
            # start_cycle records wake detection; STT starts after recording.
            pass
        elif status_text == "Transcribing...":
            self.mark_event("stt_start")
        elif status_text == "Thinking...":
            self.mark_event("stt_end")
            self.mark_event("llm_start")
        elif status_text == "Speaking...":
            self.mark_event("tts_start")

        if status_text == "Idle":
            if self.current_cycle_id:
                self.complete_cycle()
                # Reset all to idle
                for stage in self.stages.values():
                    stage.reset()
            return self.get_current_state()

        if status_text.startswith("Error"):
            # Mark current active stage as error
            for stage in self.stages.values():
                if stage.state == "active":
                    stage.set_state("error")
            if self.current_cycle_id:
                self.complete_cycle(outcome="error")
            return self.get_current_state()

        if stage_id is None:
            return None

        self.set_stage_state(stage_id, "active")
        return self.get_current_state()
