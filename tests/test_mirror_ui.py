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
