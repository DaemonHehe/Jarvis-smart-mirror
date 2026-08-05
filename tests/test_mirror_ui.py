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
