"""Unit contracts for standalone RSS and ICS bootstrap parsing."""

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

import bridge_api


def test_rss_parser_returns_bounded_sanitized_items():
    feed = "<rss><channel><title>Source</title><item><title>Headline</title></item></channel></rss>"
    assert bridge_api._rss_items(feed) == [{"title": "Headline", "source": "Source"}]
    assert bridge_api._rss_items("not xml") == []


def test_ics_parser_extracts_display_fields():
    calendar = "BEGIN:VCALENDAR\nBEGIN:VEVENT\nDTSTART:20260805T143000Z\nSUMMARY:Demo\nLOCATION:Lab\nEND:VEVENT\nEND:VCALENDAR"
    assert bridge_api._ics_items(calendar) == [{"title": "Demo", "when": "14:30", "location": "Lab"}]


def test_bootstrap_urls_are_configuration_only():
    source = (ROOT / "backend" / "bridge_api.py").read_text(encoding="utf-8")
    assert 'os.environ.get("JARVIS_NEWS_FEED_URL"' in source
    assert 'os.environ.get("JARVIS_CALENDAR_URL"' in source
    assert "Query(" not in source[source.index("def get_mirror_bootstrap"):source.index("MIRROR_UI_DIR")]
