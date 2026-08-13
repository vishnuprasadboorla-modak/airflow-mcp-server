"""Tests for AirflowOpenAPIToolset.call_tool content/structuredContent handling."""

from __future__ import annotations

import json
from typing import Any

import pytest
from mcp import types

from airflow_mcp_server.toolset import AirflowOpenAPIToolset


class FakeResponse:
    def __init__(self, status: int, headers: dict[str, str], body: bytes) -> None:
        self.status = status
        self.headers = headers
        self._body = body

    async def read(self) -> bytes:
        return self._body

    async def __aenter__(self) -> "FakeResponse":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self._response = response
        self.last_call: tuple[str, str, dict, Any] | None = None

    def request(self, method: str, path: str, params=None, json=None):
        self.last_call = (method, path, params, json)
        return self._response


@pytest.fixture
def toolset_with_fake_session(sample_openapi_spec):
    def _make(response: FakeResponse) -> AirflowOpenAPIToolset:
        session = FakeSession(response)
        return AirflowOpenAPIToolset(sample_openapi_spec, allow_mutations=False, session=session)

    return _make


@pytest.mark.asyncio
async def test_json_response_populates_content_and_structured_content(toolset_with_fake_session) -> None:
    """A successful JSON response must be visible in BOTH `content` and the
    structured payload — MCP clients that only read `content` (per spec,
    the field every client is expected to read) must still see the data,
    not just clients that additionally understand `structuredContent`.
    """
    parsed = {"dags": [{"dag_id": "example"}]}
    response = FakeResponse(
        status=200,
        headers={"content-type": "application/json"},
        body=json.dumps(parsed).encode("utf-8"),
    )
    toolset = toolset_with_fake_session(response)

    content, structured = await toolset.call_tool("get_dags", {})

    assert content, "content must not be empty when the API returned real data"
    assert isinstance(content[0], types.TextContent)
    assert json.loads(content[0].text) == parsed
    assert structured == parsed


@pytest.mark.asyncio
async def test_empty_body_still_returns_empty_content(toolset_with_fake_session) -> None:
    """A genuinely empty JSON body has nothing to show — content=[] here is
    correct, not the bug (the bug was discarding real data, not this case).
    """
    response = FakeResponse(status=200, headers={"content-type": "application/json"}, body=b"")
    toolset = toolset_with_fake_session(response)

    content, structured = await toolset.call_tool("get_dags", {})

    assert content == []
    assert structured == {}
