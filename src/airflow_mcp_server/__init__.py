import asyncio
import logging
import os
import sys

import click

from airflow_mcp_server.config import AirflowConfig
from airflow_mcp_server.server_safe import serve as serve_safe
from airflow_mcp_server.server_unsafe import serve as serve_unsafe


def _env_flag(name: str) -> bool | None:
    """Parse a boolean-ish env var: None if unset, else True/False."""
    value = os.environ.get(name)
    if value is None:
        return None
    return value.strip().lower() not in {"", "0", "false", "no", "off"}


def _resolve_transport(http: bool, sse: bool) -> tuple[bool, bool]:
    """AIRFLOW_MCP_TRANSPORT, when set, decides transport outright - overriding --http/--sse."""
    env_transport = os.environ.get("AIRFLOW_MCP_TRANSPORT")
    if not env_transport:
        return http, sse
    normalized = env_transport.strip().lower()
    if normalized not in {"stdio", "http", "sse"}:
        raise click.UsageError(f"AIRFLOW_MCP_TRANSPORT must be one of stdio/http/sse, got {env_transport!r}")
    return normalized == "http", normalized == "sse"


def _resolve_mode(safe: bool, unsafe: bool) -> tuple[bool, bool]:
    """AIRFLOW_MCP_MODE, when set, decides safe/unsafe outright - overriding --safe/--unsafe."""
    env_mode = os.environ.get("AIRFLOW_MCP_MODE")
    if not env_mode:
        return safe, unsafe
    normalized = env_mode.strip().lower()
    if normalized not in {"safe", "unsafe"}:
        raise click.UsageError(f"AIRFLOW_MCP_MODE must be 'safe' or 'unsafe', got {env_mode!r}")
    return normalized == "safe", normalized == "unsafe"


def _resolve_port(port: int) -> int:
    env_port = os.environ.get("AIRFLOW_MCP_PORT")
    if not env_port:
        return port
    try:
        return int(env_port)
    except ValueError as exc:
        raise click.UsageError(f"AIRFLOW_MCP_PORT must be an integer, got {env_port!r}") from exc


def _apply_runtime_env_overrides(
    *,
    http: bool,
    sse: bool,
    safe: bool,
    unsafe: bool,
    port: int,
    host: str,
    static_tools: bool,
) -> tuple[bool, bool, bool, bool, int, str, bool]:
    """Merge AIRFLOW_MCP_* runtime env vars over their CLI-flag equivalents, env winning."""
    http, sse = _resolve_transport(http, sse)
    safe, unsafe = _resolve_mode(safe, unsafe)
    port = _resolve_port(port)
    host = os.environ.get("AIRFLOW_MCP_HOST") or host
    env_static_tools = _env_flag("AIRFLOW_MCP_STATIC_TOOLS")
    if env_static_tools is not None:
        static_tools = env_static_tools
    return http, sse, safe, unsafe, port, host, static_tools


@click.command()
@click.option("-v", "--verbose", count=True, help="Increase verbosity")
@click.option("--safe", "-s", is_flag=True, help="Use only read-only tools. Env: AIRFLOW_MCP_MODE=safe (overrides this flag).")
@click.option("--unsafe", "-u", is_flag=True, help="Use all tools (default). Env: AIRFLOW_MCP_MODE=unsafe (overrides this flag).")
@click.option("--static-tools", is_flag=True, help="Use static tools instead of hierarchical discovery. Env: AIRFLOW_MCP_STATIC_TOOLS=true (overrides this flag).")
@click.option("--base-url", help="Airflow API base URL. Env: AIRFLOW_BASE_URL (overrides this flag).")
@click.option(
    "--auth-token",
    help="Authentication token (JWT). Static for the process lifetime - prefer --username/--password for long-running deployments. "
    "With --http, omit this along with --username/--password to require each connecting client to supply its own Airflow JWT via an 'Authorization: Bearer <jwt>' header instead. "
    "Env: AUTH_TOKEN (overrides this flag).",
)
@click.option("--username", help="Airflow username. With --password, enables automatic JWT refresh for the life of the process. Env: AIRFLOW_USERNAME (overrides this flag).")
@click.option("--password", help="Airflow password. Env: AIRFLOW_PASSWORD (overrides this flag).")
@click.option("--resources-dir", type=str, help="Directory of Markdown files to expose as MCP resources. Env: AIRFLOW_MCP_RESOURCES_DIR (overrides this flag).")
@click.option("--http", is_flag=True, help="Use HTTP (Streamable HTTP) transport instead of stdio. Env: AIRFLOW_MCP_TRANSPORT=http (overrides this flag).")
@click.option("--sse", is_flag=True, help="Use Server-Sent Events transport (deprecated, use --http instead). Env: AIRFLOW_MCP_TRANSPORT=sse (overrides this flag).")
@click.option("--port", type=int, default=3000, help="Port to run HTTP/SSE server on (default: 3000). Env: AIRFLOW_MCP_PORT (overrides this flag).")
@click.option("--host", type=str, default="localhost", help="Host to bind HTTP/SSE server to (default: localhost). Env: AIRFLOW_MCP_HOST (overrides this flag).")
@click.help_option("-h", "--help")
def main(
    verbose: int,
    safe: bool,
    unsafe: bool,
    static_tools: bool,
    base_url: str | None = None,
    auth_token: str | None = None,
    username: str | None = None,
    password: str | None = None,
    resources_dir: str | None = None,
    http: bool = False,
    sse: bool = False,
    port: int = 3000,
    host: str = "localhost",
) -> None:
    """MCP server for Airflow"""
    logging_level = logging.WARN
    if verbose == 1:
        logging_level = logging.INFO
    elif verbose >= 2:
        logging_level = logging.DEBUG

    logging.basicConfig(level=logging_level, stream=sys.stderr)

    http, sse, safe, unsafe, port, host, static_tools = _apply_runtime_env_overrides(
        http=http, sse=sse, safe=safe, unsafe=unsafe, port=port, host=host, static_tools=static_tools
    )

    if http and sse:
        raise click.UsageError("Cannot specify both --http and --sse")
    if sse:
        click.echo("Warning: SSE transport is deprecated. Consider using --http instead.", err=True)

    config_base_url = os.environ.get("AIRFLOW_BASE_URL") or base_url
    config_auth_token = os.environ.get("AUTH_TOKEN") or auth_token
    config_username = os.environ.get("AIRFLOW_USERNAME") or username
    config_password = os.environ.get("AIRFLOW_PASSWORD") or password
    env_resources_dir = os.environ.get("AIRFLOW_MCP_RESOURCES_DIR")
    selected_resources_dir = resources_dir if resources_dir is not None else env_resources_dir

    try:
        config = AirflowConfig(base_url=config_base_url, auth_token=config_auth_token, username=config_username, password=config_password)
    except ValueError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)

    if http or sse:
        transport_type = "streamable-http" if http else "sse"
        transport_config = {"port": port, "host": host}
    else:
        transport_type = "stdio"
        transport_config = {}

    has_static_credentials = bool(config.auth_token) or bool(config.username and config.password)
    if not has_static_credentials:
        if transport_type != "streamable-http":
            click.echo(
                "Configuration error: auth_token (JWT), or both username and password, is required for stdio/sse transport. "
                "Only --http transport can omit these, in which case each connecting client must supply its own Airflow JWT via 'Authorization: Bearer <jwt>' instead.",
                err=True,
            )
            sys.exit(1)
        click.echo(
            "Starting in per-connection auth mode: no --auth-token/--username+--password given, so each connecting "
            "client must send its own Airflow JWT via an 'Authorization: Bearer <jwt>' header.",
            err=True,
        )

    if safe and unsafe:
        raise click.UsageError("Options --safe and --unsafe are mutually exclusive")
    elif safe:
        asyncio.run(serve_safe(config, static_tools=static_tools, transport=transport_type, resources_dir=selected_resources_dir, **transport_config))
    elif unsafe:
        asyncio.run(serve_unsafe(config, static_tools=static_tools, transport=transport_type, resources_dir=selected_resources_dir, **transport_config))
    else:
        asyncio.run(serve_unsafe(config, static_tools=static_tools, transport=transport_type, resources_dir=selected_resources_dir, **transport_config))


if __name__ == "__main__":
    main()
