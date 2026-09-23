"""Tests for server modules."""

from typing import Any
from unittest.mock import ANY, AsyncMock, Mock, patch

import pytest
from mcp import types

from airflow_mcp_server import server_safe, server_unsafe
from airflow_mcp_server.config import AirflowConfig
from airflow_mcp_server.per_connection_auth import MissingCredentialsError


@pytest.fixture
def mock_config():
    """Create mock configuration."""
    return AirflowConfig(base_url="http://localhost:8080", auth_token="test-token")


@pytest.fixture
def mock_openapi_response():
    """Mock OpenAPI response."""
    return {"openapi": "3.0.0", "info": {"title": "Airflow API", "version": "1.0.0"}, "paths": {"/api/v1/dags": {"get": {"operationId": "get_dags", "summary": "Get all DAGs", "tags": ["DAGs"]}}}}


@pytest.mark.asyncio
async def test_safe_server_delegates_to_runtime(mock_config):
    runtime_mock = AsyncMock()
    with patch("airflow_mcp_server.server_safe._serve_airflow", runtime_mock):
        await server_safe.serve(mock_config, static_tools=True, transport="stdio", resources_dir="/tmp/resources")

    runtime_mock.assert_awaited_once()
    await_args = runtime_mock.await_args
    assert await_args is not None
    kwargs = await_args.kwargs
    assert kwargs["allowed_methods"] == {"GET"}
    assert kwargs["static_tools"] is True
    assert kwargs["resources_dir"] == "/tmp/resources"


@pytest.mark.asyncio
async def test_unsafe_server_delegates_to_runtime(mock_config):
    runtime_mock = AsyncMock()
    with patch("airflow_mcp_server.server_unsafe._serve_airflow", runtime_mock):
        await server_unsafe.serve(mock_config, static_tools=False, transport="streamable-http")

    runtime_mock.assert_awaited_once()
    await_args = runtime_mock.await_args
    assert await_args is not None
    kwargs = await_args.kwargs
    assert kwargs["allowed_methods"] == {"GET", "POST", "PUT", "DELETE", "PATCH"}
    assert kwargs["transport"] == "streamable-http"


class _FakeResponse:
    def __init__(self, payload):
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
    def __init__(self, response: Any):
        self._response = response
        self.closed = False
        self.headers: dict[str, str] = {}
        self.requests: list[tuple[str, dict[str, Any]]] = []

    def get(self, path):
        return self._response

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_serve_airflow_static_tools(monkeypatch, mock_config, mock_openapi_response):
    fake_response = _FakeResponse(mock_openapi_response)
    fake_session = _FakeSession(fake_response)

    monkeypatch.setattr("airflow_mcp_server.server_safe.aiohttp.ClientSession", lambda **_: fake_session)

    toolset_instance = Mock()
    with patch("airflow_mcp_server.server_safe.AirflowOpenAPIToolset", return_value=toolset_instance) as toolset_cls:
        with patch("airflow_mcp_server.server_safe._register_static_tools") as register_static:
            with patch("airflow_mcp_server.server_safe.HierarchicalToolManager") as manager_cls:
                with patch("airflow_mcp_server.server_safe.register_resources") as register_resources:
                    run_stdio = AsyncMock()
                    with patch("airflow_mcp_server.server_safe._run_stdio", run_stdio):
                        await server_safe._serve_airflow(
                            config=mock_config,
                            allowed_methods={"GET"},
                            mode_label="Safe Mode",
                            static_tools=True,
                            resources_dir=None,
                            transport="stdio",
                            transport_kwargs={},
                        )

    toolset_cls.assert_called_once_with(mock_openapi_response, allow_mutations=False, session=fake_session)
    register_static.assert_called_once()
    manager_cls.assert_not_called()
    register_resources.assert_called_once()
    run_stdio.assert_awaited_once()
    assert fake_session.closed is True


@pytest.mark.asyncio
async def test_serve_airflow_hierarchical_http(monkeypatch, mock_config, mock_openapi_response):
    fake_response = _FakeResponse(mock_openapi_response)
    fake_session = _FakeSession(fake_response)

    monkeypatch.setattr("airflow_mcp_server.server_safe.aiohttp.ClientSession", lambda **_: fake_session)

    toolset_instance = Mock()
    with patch("airflow_mcp_server.server_safe.AirflowOpenAPIToolset", return_value=toolset_instance) as toolset_cls:
        register_static = patch("airflow_mcp_server.server_safe._register_static_tools").start()
        register_resources = patch("airflow_mcp_server.server_safe.register_resources").start()
        manager_cls = patch("airflow_mcp_server.server_safe.HierarchicalToolManager").start()
        run_http = AsyncMock()
        patch("airflow_mcp_server.server_safe._run_streamable_http", run_http).start()
        try:
            await server_safe._serve_airflow(
                config=mock_config,
                allowed_methods={"GET", "POST"},
                mode_label="Unsafe Mode",
                static_tools=False,
                resources_dir="/tmp/resources",
                transport="streamable-http",
                transport_kwargs={"host": "127.0.0.1", "port": 4000},
            )
        finally:
            patch.stopall()

    toolset_cls.assert_called_once_with(mock_openapi_response, allow_mutations=True, session=fake_session)
    register_static.assert_not_called()
    manager_cls.assert_called_once()
    register_resources.assert_called_once()
    run_http.assert_awaited_once_with(ANY, host="127.0.0.1", port=4000, require_connection_token=False)
    assert fake_session.closed is True


@pytest.mark.asyncio
async def test_serve_airflow_fetch_error(monkeypatch, mock_config):
    class FailingResponse:
        async def __aenter__(self):
            raise RuntimeError("boom")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FailingSession(_FakeSession):
        def get(self, path):
            return FailingResponse()

    fake_session = FailingSession(None)
    monkeypatch.setattr("airflow_mcp_server.server_safe.aiohttp.ClientSession", lambda **_: fake_session)

    with pytest.raises(RuntimeError):
        await server_safe._serve_airflow(
            config=mock_config,
            allowed_methods={"GET"},
            mode_label="Safe Mode",
            static_tools=True,
            resources_dir=None,
            transport="stdio",
            transport_kwargs={},
        )

    assert fake_session.closed is True


@pytest.mark.asyncio
async def test_serve_airflow_requires_valid_config():
    config_no_url = AirflowConfig.__new__(AirflowConfig)
    config_no_url.base_url = None
    config_no_url.auth_token = "token"

    with pytest.raises(ValueError, match="base_url is required"):
        await server_safe._serve_airflow(
            config=config_no_url,
            allowed_methods={"GET"},
            mode_label="Safe Mode",
            static_tools=True,
            resources_dir=None,
            transport="stdio",
            transport_kwargs={},
        )

    config_no_token = AirflowConfig.__new__(AirflowConfig)
    config_no_token.base_url = "http://localhost"
    config_no_token.auth_token = None
    config_no_token.username = None
    config_no_token.password = None

    with pytest.raises(ValueError, match="auth_token, or username and password, is required"):
        await server_safe._serve_airflow(
            config=config_no_token,
            allowed_methods={"GET"},
            mode_label="Safe Mode",
            static_tools=True,
            resources_dir=None,
            transport="stdio",
            transport_kwargs={},
        )


@pytest.mark.asyncio
async def test_serve_airflow_uses_token_refresher_for_credentials(monkeypatch, mock_openapi_response):
    config = AirflowConfig(base_url="http://localhost:8080", username="airflow", password="airflow")

    fake_response = _FakeResponse(mock_openapi_response)
    fake_session = _FakeSession(fake_response)
    monkeypatch.setattr("airflow_mcp_server.server_safe.aiohttp.ClientSession", lambda **_: fake_session)

    refresher_instance = AsyncMock()

    async def _fake_start():
        fake_session.headers["Authorization"] = "Bearer refreshed-token"

    refresher_instance.start.side_effect = _fake_start

    with patch("airflow_mcp_server.server_safe.TokenRefresher", return_value=refresher_instance) as refresher_cls:
        with patch("airflow_mcp_server.server_safe.AirflowOpenAPIToolset", return_value=Mock()):
            with patch("airflow_mcp_server.server_safe._register_static_tools"):
                with patch("airflow_mcp_server.server_safe.register_resources"):
                    with patch("airflow_mcp_server.server_safe._run_stdio", AsyncMock()):
                        await server_safe._serve_airflow(
                            config=config,
                            allowed_methods={"GET"},
                            mode_label="Safe Mode",
                            static_tools=True,
                            resources_dir=None,
                            transport="stdio",
                            transport_kwargs={},
                        )

    refresher_cls.assert_called_once_with(fake_session, "airflow", "airflow")
    refresher_instance.start.assert_awaited_once()
    refresher_instance.stop.assert_awaited_once()
    assert fake_session.headers["Authorization"] == "Bearer refreshed-token"
    assert fake_session.closed is True


@pytest.mark.asyncio
async def test_serve_airflow_http_without_credentials_enters_per_connection_mode(monkeypatch, mock_openapi_response):
    """Omitting auth_token/username/password entirely is only valid for streamable-http:
    it must not raise, must fetch the spec without an Authorization header, and must build
    the toolset without a bound session since each connection supplies its own later."""
    config = AirflowConfig.__new__(AirflowConfig)
    config.base_url = "http://localhost:8080"
    config.auth_token = None
    config.username = None
    config.password = None

    fake_response = _FakeResponse(mock_openapi_response)
    fake_session = _FakeSession(fake_response)
    monkeypatch.setattr("airflow_mcp_server.server_safe.aiohttp.ClientSession", lambda **_: fake_session)

    toolset_instance = Mock()
    with patch("airflow_mcp_server.server_safe.AirflowOpenAPIToolset", return_value=toolset_instance) as toolset_cls:
        with patch("airflow_mcp_server.server_safe._register_static_tools") as register_static:
            with patch("airflow_mcp_server.server_safe.register_resources"):
                run_http = AsyncMock()
                with patch("airflow_mcp_server.server_safe._run_streamable_http", run_http):
                    await server_safe._serve_airflow(
                        config=config,
                        allowed_methods={"GET"},
                        mode_label="Safe Mode",
                        static_tools=True,
                        resources_dir=None,
                        transport="streamable-http",
                        transport_kwargs={},
                    )

    toolset_cls.assert_called_once_with(mock_openapi_response, allow_mutations=False, session=None)
    assert "Authorization" not in fake_session.headers
    register_static.assert_called_once()
    call_kwargs = register_static.call_args.kwargs
    assert call_kwargs["resolve_session"] is not None
    run_http.assert_awaited_once_with(ANY, host="localhost", port=3000, require_connection_token=True)
    assert fake_session.closed is True


@pytest.mark.asyncio
async def test_serve_airflow_stdio_without_credentials_still_rejected():
    """Per-connection auth only makes sense for streamable-http; stdio/sse must keep
    requiring credentials at startup since there is no per-connection concept there."""
    config = AirflowConfig.__new__(AirflowConfig)
    config.base_url = "http://localhost:8080"
    config.auth_token = None
    config.username = None
    config.password = None

    with pytest.raises(ValueError, match="auth_token, or username and password, is required"):
        await server_safe._serve_airflow(
            config=config,
            allowed_methods={"GET"},
            mode_label="Safe Mode",
            static_tools=True,
            resources_dir=None,
            transport="stdio",
            transport_kwargs={},
        )


class _FakeStaticServer:
    """Minimal stand-in for mcp.server.lowlevel.Server, just enough to capture the
    list_tools/call_tool handlers _register_static_tools registers on it."""

    def __init__(self) -> None:
        self.list_handler = None
        self.call_handler = None

    def list_tools(self):
        def decorator(func):
            self.list_handler = func
            return func

        return decorator

    def call_tool(self):
        def decorator(func):
            self.call_handler = func
            return func

        return decorator


@pytest.mark.asyncio
async def test_register_static_tools_list_tools_requires_authentication():
    """A connection that hasn't authenticated yet must not see the tool catalog at all -
    not just fail once it tries to call one."""
    server = _FakeStaticServer()
    toolset = Mock()
    toolset.list_tools.return_value = [types.Tool(name="get_dags", description="", inputSchema={"type": "object"}, outputSchema=None)]

    async def resolve_session():
        raise MissingCredentialsError("send 'Authorization: Bearer <jwt>'")

    server_safe._register_static_tools(server, toolset, resolve_session=resolve_session)

    with pytest.raises(MissingCredentialsError):
        await server.list_handler(None)


@pytest.mark.asyncio
async def test_register_static_tools_list_tools_succeeds_once_authenticated():
    server = _FakeStaticServer()
    tool = types.Tool(name="get_dags", description="", inputSchema={"type": "object"}, outputSchema=None)
    toolset = Mock()
    toolset.list_tools.return_value = [tool]

    async def resolve_session():
        return object()

    server_safe._register_static_tools(server, toolset, resolve_session=resolve_session)

    result = await server.list_handler(None)

    assert result.tools == [tool]


def _scope_with_headers(headers: dict[str, str]) -> dict[str, Any]:
    return {"headers": [(name.lower().encode("latin-1"), value.encode("latin-1")) for name, value in headers.items()]}


def test_scope_bearer_token_extracts_valid_header():
    scope = _scope_with_headers({"authorization": "Bearer client-jwt"})
    assert server_safe._scope_bearer_token(scope) == "client-jwt"


def test_scope_bearer_token_returns_none_when_missing():
    assert server_safe._scope_bearer_token(_scope_with_headers({})) is None


def test_scope_bearer_token_returns_none_for_non_bearer_scheme():
    scope = _scope_with_headers({"authorization": "Basic dXNlcjpwYXNz"})
    assert server_safe._scope_bearer_token(scope) is None


class _RecordingManager:
    def __init__(self) -> None:
        self.handled = False

    async def handle_request(self, scope, receive, send):
        self.handled = True


async def _drive_asgi_app(app, scope: dict[str, Any]) -> list[dict[str, Any]]:
    sent_messages: list[dict[str, Any]] = []

    async def receive():
        return {"type": "http.disconnect"}

    async def send(message):
        sent_messages.append(message)

    scope.setdefault("type", "http")
    await app(scope, receive, send)
    return sent_messages


@pytest.mark.asyncio
async def test_streamable_http_app_rejects_connection_without_token_when_required():
    manager = _RecordingManager()
    app = server_safe._StreamableHTTPApp(manager, require_connection_token=True)

    messages = await _drive_asgi_app(app, _scope_with_headers({}))

    assert manager.handled is False
    start_message = next(m for m in messages if m["type"] == "http.response.start")
    assert start_message["status"] == 401


@pytest.mark.asyncio
async def test_streamable_http_app_allows_connection_with_valid_token():
    manager = _RecordingManager()
    app = server_safe._StreamableHTTPApp(manager, require_connection_token=True)

    await _drive_asgi_app(app, _scope_with_headers({"authorization": "Bearer client-jwt"}))

    assert manager.handled is True


@pytest.mark.asyncio
async def test_streamable_http_app_allows_any_connection_when_not_required():
    manager = _RecordingManager()
    app = server_safe._StreamableHTTPApp(manager, require_connection_token=False)

    await _drive_asgi_app(app, _scope_with_headers({}))

    assert manager.handled is True
