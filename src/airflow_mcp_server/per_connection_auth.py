"""Per-connection Airflow authentication for multi-tenant streamable-http deployments.

Started without --auth-token/--username/--password, the HTTP server never logs into
Airflow itself. Instead, each MCP connection must already hold its own Airflow JWT and
send it as `Authorization: Bearer <jwt>`; the server forwards that token as-is to Airflow
for every tool call on that connection. The resulting session is cached on that
connection's lifespan context and reused for the rest of its lifetime; a new connection
must supply its own token again - tokens are never shared or cached across connections.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import aiohttp

if TYPE_CHECKING:
    from mcp.server.lowlevel import Server


class MissingCredentialsError(ValueError):
    """Raised when a connecting client hasn't supplied a usable Airflow bearer token."""


def parse_bearer_token(header_value: str | None) -> str | None:
    """Return the token carried by an `Authorization: Bearer <token>` header, or None if
    the header is missing or doesn't use the Bearer scheme."""
    if not header_value:
        return None
    scheme, _, token = header_value.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token


async def resolve_connection_session(server: Server, base_url: str) -> aiohttp.ClientSession:
    """Return the Airflow session for the current MCP connection, binding its token on first use."""
    ctx = server.request_context
    state = ctx.lifespan_context

    cached = state.get("session")
    if cached is not None:
        return cached

    request = ctx.request
    header_value = request.headers.get("authorization") if request is not None else None
    token = parse_bearer_token(header_value)
    if token is None:
        raise MissingCredentialsError("This server requires an Airflow token: send 'Authorization: Bearer <jwt>' on each request.")

    session = aiohttp.ClientSession(base_url=base_url, timeout=aiohttp.ClientTimeout(total=30))
    session.headers["Authorization"] = f"Bearer {token}"

    state["session"] = session
    return session


@asynccontextmanager
async def connection_lifespan(_: Server):
    """Per-MCP-connection lifespan: closes whatever Airflow session got cached on it."""
    state: dict[str, aiohttp.ClientSession | None] = {"session": None}
    try:
        yield state
    finally:
        session = state.get("session")
        if session is not None:
            await session.close()
