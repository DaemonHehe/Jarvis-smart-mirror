from unittest.mock import patch

from backend.pipeline_tracker import PipelineTracker


def test_pipeline_metrics_follow_real_stage_boundaries():
    tracker = PipelineTracker()
    now = [10.0]

    with patch("backend.pipeline_tracker.time.time", side_effect=lambda: now[0]):
        tracker.mark_event("wake")
        now[0] = 10.1
        tracker.process_status_event("Listening...")
        now[0] = 12.0
        tracker.process_status_event("Transcribing...")
        now[0] = 13.5
        tracker.process_status_event("Thinking...")
        now[0] = 14.0
        tracker.mark_event("first_llm_token")
        now[0] = 15.0
        tracker.process_status_event("Speaking...")

    assert tracker.metrics["wake_to_stt_start_ms"] == 2000.0
    assert tracker.metrics["stt_duration_ms"] == 1500.0
    assert tracker.metrics["ttft_llm_token_ms"] == 500.0


def test_missing_and_repeated_events_do_not_invent_metrics():
    tracker = PipelineTracker()
    tracker.start_cycle()
    tracker.mark_event("stt_end")
    tracker.mark_event("first_llm_token")

    assert tracker.metrics["stt_duration_ms"] is None
    assert tracker.metrics["ttft_llm_token_ms"] is None


def test_missing_wake_event_does_not_invent_wake_latency():
    tracker = PipelineTracker()
    tracker.process_status_event("Listening...")
    tracker.process_status_event("Transcribing...")

    assert tracker.metrics["wake_to_stt_start_ms"] is None


def test_completion_is_idempotent_and_errors_are_archived():
    tracker = PipelineTracker()
    tracker.process_status_event("Listening...")
    tracker.process_status_event("Error: microphone unavailable")
    tracker.complete_cycle()

    history = tracker.get_history()
    assert len(history) == 1
    assert history[0]["outcome"] == "error"
