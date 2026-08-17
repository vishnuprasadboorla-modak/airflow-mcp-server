"""Tests for automatic Airflow JWT refresh."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from typing import Any

import pytest

from airflow_mcp_server.token_refresher import (
    MIN_REFRESH_INTERVAL,
    TokenRefresher,
    decode_jwt_exp,
    fetch_token,
)


def _make_jwt(exp: float) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).rstrip(b"=").decode()
    return f"{header}.{payload}.signature"


def test_decode_jwt_exp_reads_expiry():
    token = _make_jwt(1234567890.0)
    assert decode_jwt_exp(token) == 1234567890.0


def test_decode_jwt_exp_returns_none_for_malformed_token():
    assert decode_jwt_exp("not-a-jwt") is None
    assert decode_jwt_exp("") is None


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        return None

    async def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, payload: dict[str, Any]):
        self.headers: dict[str, str] = {}
        self.post_calls: list[tuple[str, dict[str, Any]]] = []
        self._payload = payload

    def post(self, path, json):
        self.post_calls.append((path, json))
        return _FakeResponse(self._payload)


@pytest.mark.asyncio
async def test_fetch_token_reads_access_token():
    session = _FakeSession({"access_token": "abc123"})
    token = await fetch_token(session, "user", "pass")

    assert token == "abc123"
    assert session.post_calls == [("/auth/token", {"username": "user", "password": "pass"})]


@pytest.mark.asyncio
async def test_fetch_token_raises_on_unexpected_response():
    session = _FakeSession({"unexpected": "shape"})
    with pytest.raises(ValueError, match="no access token found"):
        await fetch_token(session, "user", "pass")


@pytest.mark.asyncio
async def test_start_fetches_token_and_sets_header():
    token = _make_jwt(time.time() + 3600)
    session = _FakeSession({"access_token": token})
    refresher = TokenRefresher(session, "user", "pass")

    await refresher.start()
    try:
        assert session.headers["Authorization"] == f"Bearer {token}"
    finally:
        await refresher.stop()


@pytest.mark.asyncio
async def test_stop_cancels_background_loop_cleanly():
    token = _make_jwt(time.time() + 3600)
    session = _FakeSession({"access_token": token})
    refresher = TokenRefresher(session, "user", "pass")

    await refresher.start()
    task = refresher._task
    assert task is not None and not task.done()

    await refresher.stop()
    assert task.done()
    assert refresher._task is None


def test_sleep_duration_respects_minimum_interval():
    # Token expiring almost immediately should still back off to the floor, not 0/negative.
    assert TokenRefresher._sleep_duration(time.time() + 1) == MIN_REFRESH_INTERVAL


def test_sleep_duration_falls_back_when_exp_unknown():
    from airflow_mcp_server.token_refresher import FALLBACK_REFRESH_INTERVAL

    assert TokenRefresher._sleep_duration(None) == FALLBACK_REFRESH_INTERVAL


@pytest.mark.asyncio
async def test_loop_retries_after_failed_refresh(monkeypatch):
    """If a scheduled refresh fails, the loop should keep running and retry, not crash."""
    call_count = 0

    async def flaky_fetch_token(session, username, password):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("network blip")
        return _make_jwt(time.time() + 3600)

    monkeypatch.setattr("airflow_mcp_server.token_refresher.fetch_token", flaky_fetch_token)
    # Shrink the retry backoff so the test doesn't wait on real minute-scale timers.
    monkeypatch.setattr("airflow_mcp_server.token_refresher.MIN_REFRESH_INTERVAL", 0.01)

    session = _FakeSession({})
    refresher = TokenRefresher(session, "user", "pass")
    refresher._task = asyncio.create_task(refresher._loop(exp=time.time()))

    async def _wait_for_second_call():
        while call_count < 2:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_wait_for_second_call(), timeout=2)
    await refresher.stop()

    assert call_count >= 2
    assert session.headers["Authorization"].startswith("Bearer ")
