from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, Literal, cast

import aiohttp
import uvicorn
from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.models import InitializationOptions
from mcp.server.sse import SseServerTransport
from mcp.server.stdio import stdio_server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route

from airflow_mcp_server.config import AirflowConfig
from airflow_mcp_server.hierarchical_manager import HierarchicalToolManager
from airflow_mcp_server.per_connection_auth import MissingCredentialsError, connection_lifespan, parse_bearer_token, resolve_connection_session
from airflow_mcp_server.resources import register_resources
from airflow_mcp_server.token_refresher import TokenRefresher
from airflow_mcp_server.toolset import AirflowOpenAPIToolset


async def serve(
    config: AirflowConfig,
    static_tools: bool = False,
    resources_dir: str | None = None,
    transport: Literal["stdio", "streamable-http", "sse"] = "stdio",
    **transport_kwargs,
) -> None:
    """Start MCP server in safe mode (read-only operations)."""

    await _serve_airflow(
        config=config,
        allowed_methods={"GET"},
        mode_label="Safe Mode",
        static_tools=static_tools,
        resources_dir=resources_dir,
        transport=transport,
        transport_kwargs=transport_kwargs,
    )


def _requires_per_connection_auth(config: AirflowConfig, transport: str) -> bool:
    has_static_credentials = bool(config.auth_token) or bool(getattr(config, "username", None) and getattr(config, "password", None))
    if has_static_credentials:
        return False
    if transport != "streamable-http":
        raise ValueError("auth_token, or username and password, is required")
    return True


async def _authenticate_bootstrap_session(session: aiohttp.ClientSession, config: AirflowConfig, per_connection_auth: bool) -> TokenRefresher | None:
    """Log the shared bootstrap session (used to fetch /openapi.json) into Airflow, unless
    the server is running in per-connection auth mode, where it stays anonymous."""
    if per_connection_auth:
        return None
    if config.auth_token:
        session.headers["Authorization"] = f"Bearer {config.auth_token}"
        return None
    refresher = TokenRefresher(session, config.username, config.password)
    await refresher.start()
    return refresher


async def _fetch_openapi_spec(session: aiohttp.ClientSession, per_connection_auth: bool) -> dict[str, Any]:
    try:
        async with session.get("/openapi.json") as response:
            response.raise_for_status()
            return cast(dict[str, Any], await response.json())
    except aiohttp.ClientResponseError as exc:
        if per_connection_auth:
            raise ValueError(
                f"Failed to fetch the Airflow OpenAPI spec without credentials (HTTP {exc.status}). "
                "Per-connection auth mode requires '/openapi.json' to be reachable without authentication, "
                "or supply --auth-token / --username and --password at startup to use one shared identity instead."
            ) from exc
        raise


async def _serve_airflow(
    *,
    config: AirflowConfig,
    allowed_methods: set[str],
    mode_label: str,
    static_tools: bool,
    resources_dir: str | None,
    transport: Literal["stdio", "streamable-http", "sse"],
    transport_kwargs: dict[str, object],
) -> None:
    if not config.base_url:
        raise ValueError("base_url is required")

    per_connection_auth = _requires_per_connection_auth(config, transport)

    session = aiohttp.ClientSession(
        base_url=config.base_url,
        timeout=aiohttp.ClientTimeout(total=30),
    )

    refresher: TokenRefresher | None = None
    try:
        refresher = await _authenticate_bootstrap_session(session, config, per_connection_auth)
        openapi_spec = await _fetch_openapi_spec(session, per_connection_auth)

        allow_mutations = any(method != "GET" for method in allowed_methods)
        toolset = AirflowOpenAPIToolset(openapi_spec, allow_mutations=allow_mutations, session=None if per_connection_auth else session)

        if per_connection_auth:
            server = Server(
                name=f"Airflow MCP Server ({mode_label})",
                version="0.9.0",
                instructions="Interact with Apache Airflow's REST API via MCP tools.",
                lifespan=connection_lifespan,
            )
        else:
            server = Server(
                name=f"Airflow MCP Server ({mode_label})",
                version="0.9.0",
                instructions="Interact with Apache Airflow's REST API via MCP tools.",
            )

        resolve_session = functools.partial(resolve_connection_session, server, config.base_url) if per_connection_auth else None

        if static_tools:
            _register_static_tools(server, toolset, resolve_session=resolve_session)
        else:
            HierarchicalToolManager(server, toolset, openapi_spec, allowed_methods, resolve_session=resolve_session)

        register_resources(server, resources_dir)

        initialization = server.create_initialization_options()

        host_value = transport_kwargs.get("host", "localhost")
        host = cast(str, host_value) if isinstance(host_value, str) else "localhost"

        port_value = transport_kwargs.get("port", 3000)
        if isinstance(port_value, (int, str)):
            port = int(port_value)
        else:
            raise TypeError("port must be an int or string")

        if transport == "stdio":
            await _run_stdio(server, initialization)
        elif transport == "streamable-http":
            await _run_streamable_http(server, host=str(host), port=int(port), require_connection_token=per_connection_auth)
        elif transport == "sse":
            await _run_sse(server, initialization, host=str(host), port=int(port))
        else:  # pragma: no cover
            raise ValueError(f"Unsupported transport '{transport}'")
    finally:
        if refresher is not None:
            await refresher.stop()
        await session.close()


def _register_static_tools(
    server: Server,
    toolset: AirflowOpenAPIToolset,
    resolve_session: Callable[[], Awaitable[aiohttp.ClientSession]] | None = None,
) -> None:
    tools = toolset.list_tools()

    @server.list_tools()
    async def _list_tools(_: types.ListToolsRequest | None = None) -> types.ListToolsResult:
        if resolve_session:
            await resolve_session()  # raises MissingCredentialsError until this connection authenticates
        return types.ListToolsResult(tools=tools)

    @server.call_tool()
    async def _call_tool(tool_name: str, arguments: dict[str, object]):
        try:
            session = await resolve_session() if resolve_session else None
        except MissingCredentialsError as exc:
            return [types.TextContent(type="text", text=str(exc))]

        try:
            return await toolset.call_tool(tool_name, arguments or {}, session=session)
        except ValueError as exc:
            return [types.TextContent(type="text", text=str(exc))]


async def _run_stdio(server: Server, initialization: InitializationOptions) -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, initialization, stateless=False)


def _scope_bearer_token(scope: Any) -> str | None:
    headers = dict(scope.get("headers") or [])
    raw_value = headers.get(b"authorization")
    if raw_value is None:
        return None
    return parse_bearer_token(raw_value.decode("latin-1"))


class _StreamableHTTPApp:
    """ASGI app in front of the MCP session manager. When require_connection_token is set,
    a request without a syntactically valid 'Authorization: Bearer <jwt>' header is
    rejected with 401 before it ever reaches session creation - so an MCP connection (not
    just a later tool call) can't be established without a token."""

    def __init__(self, manager: StreamableHTTPSessionManager, require_connection_token: bool) -> None:
        self._manager = manager
        self._require_connection_token = require_connection_token

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if self._require_connection_token and _scope_bearer_token(scope) is None:
            response = Response(
                "Unauthorized: this server requires an 'Authorization: Bearer <jwt>' header to establish a connection.",
                status_code=401,
            )
            await response(scope, receive, send)
            return
        await self._manager.handle_request(scope, receive, send)


async def _run_streamable_http(server: Server, *, host: str, port: int, require_connection_token: bool = False) -> None:
    session_manager = StreamableHTTPSessionManager(server, stateless=False)

    @asynccontextmanager
    async def lifespan(_: Starlette):
        async with session_manager.run():
            yield

    async def root(_: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    app = Starlette(
        routes=[
            Route("/", endpoint=root, methods=["GET"]),
            Mount("/mcp", app=_StreamableHTTPApp(session_manager, require_connection_token)),
        ],
        lifespan=lifespan,
    )

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    await uvicorn.Server(config).serve()


async def _run_sse(server: Server, initialization: InitializationOptions, *, host: str, port: int) -> None:
    transport = SseServerTransport("/messages")

    async def sse(scope: Any, receive: Any, send: Any):
        async with transport.connect_sse(scope, receive, send) as (read_stream, write_stream):
            await server.run(read_stream, write_stream, initialization, stateless=True)
        return Response(status_code=204)

    async def status(_: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    app = Starlette(
        routes=[
            Route("/", endpoint=status, methods=["GET"]),
            Route("/events", endpoint=sse, methods=["GET"]),
            Mount("/messages", app=transport.handle_post_message),
        ]
    )

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    await uvicorn.Server(config).serve()
