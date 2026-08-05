"""Source contracts for the standalone Pi and phone frontends."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_mirror_has_no_magicmirror_runtime_contract():
    source = (ROOT / "mirror-ui" / "app.js").read_text(encoding="utf-8")
    assert "Module.register" not in source
    assert "sendNotification" not in source
    assert "new WebSocket" in source


def test_mirror_implements_coordinated_features():
    source = (ROOT / "mirror-ui" / "app.js").read_text(encoding="utf-8")
    for contract in (
        "resolveScene",
        "addCard",
        "consumeTimerEvent",
        "applyMedia",
        "applyPresence",
        "recordMetrics",
        "applyHealth",
    ):
        assert contract in source


def test_frontends_do_not_request_camera_access():
    sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (ROOT / "mirror-ui" / "app.js", ROOT / "phone-face" / "app.js")
    )
    assert "getUserMedia" not in sources
    assert "mediaDevices" not in sources


def test_mirror_uses_glass_design_with_accessible_motion_fallback():
    styles = (ROOT / "mirror-ui" / "styles.css").read_text(encoding="utf-8")
    assert "backdrop-filter: blur(" in styles
    assert ".glass-panel" in styles
    assert "--signal: #e83a2f" in styles
    assert "prefers-reduced-motion" in styles


def test_phone_face_has_local_personality_without_replacing_backend_state():
    source = (ROOT / "phone-face" / "app.js").read_text(encoding="utf-8")
    for contract in (
        "scheduleBlink",
        "scheduleIdleExpression",
        "setExpression",
        "pointermove",
        "backendExpressionUntil",
        "prefers-reduced-motion",
    ):
        assert contract in source
    assert 'message.type === "state"' in source
    assert 'message.type === "face_cue"' in source


def test_phone_face_supports_expressive_eye_states():
    styles = (ROOT / "phone-face" / "styles.css").read_text(encoding="utf-8")
    for expression in ("curious", "happy", "sleepy", "wink", "surprised", "focused", "excited", "music"):
        assert f'data-expression="{expression}"' in styles
    assert "--gaze-x" in styles
    assert "prefers-reduced-motion" in styles


def test_mirror_defaults_to_portrait_information_hierarchy():
    styles = (ROOT / "mirror-ui" / "styles.css").read_text(encoding="utf-8")
    assert "grid-template-rows: minmax(220px, 27vh) minmax(0, 1fr) auto auto" in styles
    assert ".clock-panel { text-align: center; }" in styles
    assert "grid-row: 1 / -1" in styles
    assert "top: 50%" in styles
    assert "left: 50%" in styles
    assert "bottom: clamp(62px, 7vh, 120px)" in styles
    assert '@media (orientation: landscape)' in styles


def test_phone_idle_behavior_is_choreographed_not_random():
    source = (ROOT / "phone-face" / "app.js").read_text(encoding="utf-8")
    assert "idleSequence" in source
    assert "blinkSequence" in source
    assert "behaviorStep" in source
    assert "Math.random" not in source


def test_mirror_frontend_has_bounded_recovery_paths():
    source = (ROOT / "mirror-ui" / "app.js").read_text(encoding="utf-8")
    for contract in (
        "AbortController",
        "fetchJson",
        "reconnectTimer",
        "reportFrontendError",
        "unhandledrejection",
        "pagehide",
        "131072",
        'mediaArt.addEventListener("error"',
    ):
        assert contract in source


def test_phone_face_handles_protocol_and_runtime_failures():
    source = (ROOT / "phone-face" / "app.js").read_text(encoding="utf-8")
    for contract in (
        "protocolFaults",
        "reportDisplayError",
        "unhandledrejection",
        "pagehide",
        "shuttingDown",
        "131072",
        "motionPreference.addListener",
    ):
        assert contract in source
