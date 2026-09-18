"""Tests for per-connection Airflow authentication (streamable-http, no startup credentials)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from airflow_mcp_server.per_connection_auth import (
    MissingCredentialsError,
    connection_lifespan,
    resolve_connection_session,
)


class _FakeSession:
    """Stands in for the aiohttp.ClientSession created inside resolve_connection_session."""

    def __init__(self):
        self.headers: dict[str, str] = {}
        self.closed = False

    async def close(self):
        self.closed = True


class _FakeRequest:
    def __init__(self, headers: dict[str, str]):
        self.headers = headers


class _FakeRequestContext:
    def __init__(self, request, lifespan_context):
        self.request = request
        self.lifespan_context = lifespan_context


class _FakeServer:
    def __init__(self, request, lifespan_context):
        self._ctx = _FakeRequestContext(request, lifespan_context)

    @property
    def request_context(self):
        return self._ctx


@pytest.mark.asyncio
async def test_resolve_connection_session_forwards_bearer_token(monkeypatch):
    fake_session = _FakeSession()
    monkeypatch.setattr("airflow_mcp_server.per_connection_auth.aiohttp.ClientSession", lambda **_: fake_session)

    request = _FakeRequest({"authorization": "Bearer client-supplied-jwt"})
    server = _FakeServer(request, {"session": None})

    session = await resolve_connection_session(server, "http://airflow.local")

    assert session is fake_session
    assert fake_session.headers["Authorization"] == "Bearer client-supplied-jwt"
    assert server.request_context.lifespan_context["session"] is fake_session


@pytest.mark.asyncio
async def test_resolve_connection_session_reuses_cached_session(monkeypatch):
    """A second tool call on the same connection must not rebuild the session."""
    session_calls = 0

    def _make_session(**_):
        nonlocal session_calls
        session_calls += 1
        return _FakeSession()

    monkeypatch.setattr("airflow_mcp_server.per_connection_auth.aiohttp.ClientSession", _make_session)

    request = _FakeRequest({"authorization": "Bearer client-supplied-jwt"})
    server = _FakeServer(request, {"session": None})

    first = await resolve_connection_session(server, "http://airflow.local")
    second = await resolve_connection_session(server, "http://airflow.local")

    assert first is second
    assert session_calls == 1


@pytest.mark.asyncio
async def test_resolve_connection_session_requires_authorization_header():
    request = _FakeRequest({})
    server = _FakeServer(request, {"session": None})

    with pytest.raises(MissingCredentialsError, match="Authorization: Bearer"):
        await resolve_connection_session(server, "http://airflow.local")


@pytest.mark.asyncio
async def test_resolve_connection_session_rejects_non_bearer_scheme():
    request = _FakeRequest({"authorization": "Basic dXNlcjpwYXNz"})
    server = _FakeServer(request, {"session": None})

    with pytest.raises(MissingCredentialsError, match="Authorization: Bearer"):
        await resolve_connection_session(server, "http://airflow.local")


@pytest.mark.asyncio
async def test_resolve_connection_session_rejects_empty_token():
    request = _FakeRequest({"authorization": "Bearer "})
    server = _FakeServer(request, {"session": None})

    with pytest.raises(MissingCredentialsError, match="Authorization: Bearer"):
        await resolve_connection_session(server, "http://airflow.local")


@pytest.mark.asyncio
async def test_connection_lifespan_closes_session_created_during_the_connection():
    closed_sessions = []

    class _TrackedSession:
        async def close(self):
            closed_sessions.append(self)

    async with connection_lifespan(SimpleNamespace()) as state:
        assert state == {"session": None}
        state["session"] = _TrackedSession()

    assert len(closed_sessions) == 1


@pytest.mark.asyncio
async def test_connection_lifespan_noop_when_no_session_was_ever_created():
    async with connection_lifespan(SimpleNamespace()) as state:
        assert state["session"] is None
    # No exception means cleanup tolerated the "never authenticated" case cleanly.
