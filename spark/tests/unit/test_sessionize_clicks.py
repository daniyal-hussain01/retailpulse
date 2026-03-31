"""
spark/tests/unit/test_sessionize_clicks.py
Tests for the clickstream sessionisation logic.
No Spark JVM, no external dependencies.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest

from spark.jobs.sessionize_clicks import (
    parse_event,
    sessionise_events,
    SESSION_TIMEOUT_MINUTES,
)


def _ts(offset_minutes: int = 0) -> str:
    base = datetime(2024, 4, 25, 10, 0, 0, tzinfo=timezone.utc)
    return (base + timedelta(minutes=offset_minutes)).isoformat()


def _event(session_id: str, offset: int, event_type: str = "page_view",
           user_id: str | None = None, product_id: str | None = None) -> dict:
    return {
        "event_id":    f"evt-{session_id}-{offset}",
        "session_id":  session_id,
        "user_id":     user_id,
        "event_type":  event_type,
        "product_id":  product_id,
        "page_url":    f"/page/{offset}",
        "occurred_at": _ts(offset),
    }


# ── parse_event ────────────────────────────────────────────────────────────────

class TestParseEvent:
    def test_flat_json_parsed(self):
        raw = {
            "event_id": "e1", "session_id": "s1", "user_id": "u1",
            "event_type": "page_view", "occurred_at": _ts(),
        }
        result = parse_event(raw)
        assert result is not None
        assert result["session_id"] == "s1"

    def test_debezium_envelope_unwrapped(self):
        raw = {
            "op": "c",
            "after": {
                "event_id": "e1", "session_id": "s1", "user_id": "u1",
                "event_type": "product_view", "occurred_at": _ts(),
            },
        }
        result = parse_event(raw)
        assert result is not None
        assert result["event_type"] == "product_view"

    def test_user_id_is_hashed(self):
        raw = {"event_id": "e1", "session_id": "s1", "user_id": "SENSITIVE_USER_ID",
               "event_type": "page_view", "occurred_at": _ts()}
        result = parse_event(raw)
        assert result is not None
        assert "user_id" not in result
        assert "user_id_hash" in result
        assert "SENSITIVE_USER_ID" not in str(result)
        assert len(result["user_id_hash"]) == 64

    def test_anonymous_user_gives_none_hash(self):
        raw = {"event_id": "e1", "session_id": "s1", "user_id": None,
               "event_type": "page_view", "occurred_at": _ts()}
        result = parse_event(raw)
        assert result is not None
        assert result["user_id_hash"] is None

    def test_record_without_session_id_skipped(self):
        result = parse_event({"event_type": "page_view"})
        assert result is None

    def test_empty_record_skipped(self):
        assert parse_event({}) is None


# ── sessionise_events ─────────────────────────────────────────────────────────

class TestSessioniseEvents:
    def test_empty_input_returns_empty(self):
        assert sessionise_events([]) == []

    def test_single_event_creates_one_session(self):
        events = [_event("s1", 0)]
        sessions = sessionise_events(events)
        assert len(sessions) == 1
        assert sessions[0]["session_id"] == "s1"

    def test_multiple_events_same_session(self):
        events = [_event("s1", i) for i in range(5)]
        sessions = sessionise_events(events)
        assert len(sessions) == 1
        assert sessions[0]["event_count"] == 5

    def test_two_distinct_session_ids(self):
        events = [_event("s1", 0), _event("s1", 1), _event("s2", 2), _event("s2", 3)]
        sessions = sessionise_events(events)
        assert len(sessions) == 2
        session_ids = {s["session_id"] for s in sessions}
        assert session_ids == {"s1", "s2"}

    def test_timeout_splits_session(self):
        """Two events with same session_id but >30 min gap → two sessions."""
        events = [
            _event("s1", 0),                          # 10:00
            _event("s1", SESSION_TIMEOUT_MINUTES + 5), # 10:35 → timeout
        ]
        sessions = sessionise_events(events)
        assert len(sessions) == 2

    def test_no_timeout_within_window(self):
        """Events within the timeout window stay in one session."""
        events = [
            _event("s1", 0),
            _event("s1", SESSION_TIMEOUT_MINUTES - 1),  # just inside window
        ]
        sessions = sessionise_events(events)
        assert len(sessions) == 1

    def test_duration_calculated_correctly(self):
        events = [_event("s1", 0), _event("s1", 10)]  # 10 minutes apart
        sessions = sessionise_events(events)
        assert sessions[0]["duration_seconds"] == 600  # 10 min = 600 sec

    def test_zero_duration_single_event(self):
        sessions = sessionise_events([_event("s1", 0)])
        assert sessions[0]["duration_seconds"] == 0

    def test_event_type_counts(self):
        events = [
            _event("s1", 0, "page_view"),
            _event("s1", 1, "product_view"),
            _event("s1", 2, "product_view"),
            _event("s1", 3, "add_to_cart"),
            _event("s1", 4, "search"),
            _event("s1", 5, "purchase"),
        ]
        sessions = sessionise_events(events)
        s = sessions[0]
        assert s["page_views"]         == 1
        assert s["product_views"]      == 2
        assert s["add_to_cart_count"]  == 1
        assert s["search_count"]       == 1
        assert s["converted"]          is True

    def test_no_purchase_not_converted(self):
        events = [_event("s1", 0, "page_view"), _event("s1", 1, "product_view")]
        sessions = sessionise_events(events)
        assert sessions[0]["converted"] is False

    def test_entry_and_exit_pages(self):
        events = [
            _event("s1", 0, "page_view"),   # entry: /page/0
            _event("s1", 1, "page_view"),   # middle
            _event("s1", 2, "page_view"),   # exit: /page/2
        ]
        sessions = sessionise_events(events)
        assert sessions[0]["entry_page"] == "/page/0"
        assert sessions[0]["exit_page"]  == "/page/2"

    def test_total_event_count(self):
        events = [_event("s1", i) for i in range(12)]
        sessions = sessionise_events(events)
        assert sessions[0]["event_count"] == 12

    def test_all_sessions_have_required_fields(self):
        events = [_event("s1", i) for i in range(3)]
        sessions = sessionise_events(events)
        required = [
            "session_id", "user_id_hash", "session_start", "session_end",
            "duration_seconds", "event_count", "page_views", "product_views",
            "add_to_cart_count", "search_count", "converted",
            "entry_page", "exit_page",
        ]
        for field in required:
            assert field in sessions[0], f"Missing field: {field}"

    @pytest.mark.parametrize("n_sessions", [1, 5, 20])
    def test_scales_to_many_sessions(self, n_sessions):
        events = []
        for i in range(n_sessions):
            for j in range(3):
                events.append(_event(f"session-{i}", i * 10 + j))
        sessions = sessionise_events(events)
        assert len(sessions) == n_sessions
