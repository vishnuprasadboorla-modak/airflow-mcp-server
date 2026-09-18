# airflow-mcp-server: An MCP Server for controlling Airflow 3

mcp-name: io.github.abhishekbhakat/airflow-mcp-server

### MCPHub Certification

This MCP server is certified by [MCPHub](https://mcphub.com/mcp-servers/abhishekbhakat/airflow-mcp-server). This certification ensures that airflow-mcp-server follows best practices for Model Context Protocol implementation.


### Find on Glama

<a href="https://glama.ai/mcp/servers/6gjq9w80xr">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/6gjq9w80xr/badge" />
</a>

## Overview
A [Model Context Protocol](https://modelcontextprotocol.io/) server for controlling Airflow via Airflow APIs.

## Demo Video

https://github.com/user-attachments/assets/f3e60fff-8680-4dd9-b08e-fa7db655a705

## Setup

### Usage with Claude Desktop

#### Stdio Transport (Default)
```json
{
    "mcpServers": {
        "airflow-mcp-server": {
            "command": "uvx",
            "args": [
                "airflow-mcp-server",
                "--base-url",
                "http://localhost:8080",
                "--auth-token",
                "<jwt_token>"
            ]
        }
    }
}
```

Or use username/password so the server refreshes its own JWT automatically (recommended for long-running deployments):

```json
{
    "mcpServers": {
        "airflow-mcp-server": {
            "command": "uvx",
            "args": [
                "airflow-mcp-server",
                "--base-url",
                "http://localhost:8080",
                "--username",
                "<airflow_username>",
                "--password",
                "<airflow_password>"
            ]
        }
    }
}
```

See [`CONFIG.md`](CONFIG.md) for IDE-specific configuration examples across popular MCP clients.

#### HTTP Transport
```json
{
    "mcpServers": {
        "airflow-mcp-server-http": {
            "command": "uvx",
            "args": [
                "airflow-mcp-server",
                "--http",
                "--port",
                "3000",
                "--base-url",
                "http://localhost:8080",
                "--auth-token",
                "<jwt_token>"
            ]
        }
    }
}
```

> **Note:**
> - Set `base_url` to the root Airflow URL (e.g., `http://localhost:8080`).
> - Do **not** include `/api/v2` in the base URL. The server will automatically fetch the OpenAPI spec from `${base_url}/openapi.json`.
> - You must provide either `--auth-token` (a static JWT) **or** `--username` + `--password` (auto-refreshing JWT). Cookie and basic auth are no longer supported in Airflow 3.0.
> - This is a **single shared identity**: every client connected to this server process acts as the same Airflow user. For a multi-tenant HTTP deployment where each client should use its own Airflow account, see [Per-Connection Authentication](#per-connection-authentication-multi-tenant-http) below.

#### Per-Connection Authentication (multi-tenant HTTP)

Running `--http` with **no** `--auth-token`/`--username`/`--password` (and none of `AUTH_TOKEN`, `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD` set) starts the server in per-connection auth mode: it never logs into Airflow itself, and each connecting MCP client must already hold its own Airflow JWT and send it as a Bearer token:

```json
{
    "mcpServers": {
        "airflow-mcp-server-http": {
            "url": "http://localhost:3000/mcp",
            "headers": {
                "Authorization": "Bearer <jwt>"
            }
        }
    }
}
```

```bash
airflow-mcp-server --http --port 3000 --base-url http://localhost:8080
```

Each client obtains its own `<jwt>` the same way `--auth-token` does today - by calling Airflow's `/auth/token` with its own username/password:

```bash
curl -s -X POST http://localhost:8080/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username": "<airflow_username>", "password": "<airflow_password>"}'
```

- The connection itself requires the token: every request to `/mcp` (including the initial MCP `initialize` handshake) is rejected with `HTTP 401` unless it carries a syntactically valid `Authorization: Bearer <jwt>` header. A client with no token can't even open an MCP session, let alone see or call a tool.
- Once connected, the server reads that connection's token on its first tool call and forwards it as-is to Airflow for every subsequent tool call on that same connection - it never sees or stores a username/password, only the token the client already obtained. `tools/list` requires it too, so an unauthenticated connection can't even see the tool catalog.
- Every new connection must supply its own token again - there's no shared or cached identity across connections, so two users hitting the same running server always act as themselves, never as each other.
- The server does **not** refresh this token: it's only as long-lived as Airflow's `[api_auth] jwt_expiration_time` (24h by default). If a long-running connection's token expires mid-session, the client needs to reconnect with a fresh one - there's no `--username`/`--password` equivalent auto-refresh in this mode.
- This mode requires `${base_url}/openapi.json` to be reachable **without** authentication, since the tool list is built once at startup before any client has connected. If your Airflow instance requires auth for that endpoint, use the shared-credential mode above instead.
- Not available for `stdio`/`--sse` transport - each `stdio` client already gets its own dedicated server process, so there's no multi-tenant problem to solve there.

### Transport Options

The server supports multiple transport protocols:

#### Stdio Transport (Default)
Standard input/output transport for direct process communication:
```bash
airflow-mcp-server --safe --base-url http://localhost:8080 --auth-token <jwt>
# or with auto-refreshing credentials
airflow-mcp-server --safe --base-url http://localhost:8080 --username <user> --password <pass>
```

#### HTTP Transport
Uses Streamable HTTP for better scalability and web compatibility:
```bash
airflow-mcp-server --safe --http --port 3000 --base-url http://localhost:8080 --auth-token <jwt>
```

> **Note:** SSE transport is deprecated. Use `--http` for new deployments as it provides better bidirectional communication and is the recommended approach by FastMCP.

### Operation Modes

The server supports two operation modes:

- **Safe Mode** (`--safe`): Only allows read-only operations (GET requests). This is useful when you want to prevent any modifications to your Airflow instance.
- **Unsafe Mode** (`--unsafe`): Allows all operations including modifications. This is the default mode.

To start in safe mode:
```bash
airflow-mcp-server --safe
```

To explicitly start in unsafe mode (though this is default):
```bash
airflow-mcp-server --unsafe
```

### Tool Discovery Modes

The server supports two tool discovery approaches:

- **Hierarchical Discovery** (default): Tools are organized by categories (DAGs, Tasks, Connections, etc.). Browse categories first, then select specific tools. More manageable for large APIs.
- **Static Tools** (`--static-tools`): All tools available immediately. Better for programmatic access but can be overwhelming.

To use static tools:
```bash
airflow-mcp-server --static-tools
```

### Command Line Options

```bash
Usage: airflow-mcp-server [OPTIONS]

  MCP server for Airflow

Options:
  -v, --verbose      Increase verbosity
  -s, --safe         Use only read-only tools
  -u, --unsafe       Use all tools (default)
  --static-tools     Use static tools instead of hierarchical discovery
  --base-url TEXT    Airflow API base URL
  --auth-token TEXT  Authentication token (JWT). Static for the process
                     lifetime - prefer --username/--password for long-running
                     deployments.
  --username TEXT    Airflow username. With --password, enables automatic JWT
                     refresh for the life of the process.
  --password TEXT    Airflow password.
  --http             Use HTTP (Streamable HTTP) transport instead of stdio
  --sse              Use Server-Sent Events transport (deprecated, use --http
                     instead)
  --port INTEGER     Port to run HTTP/SSE server on (default: 3000)
  --host TEXT        Host to bind HTTP/SSE server to (default: localhost)
  --help             Show this message and exit.
```

### Using Resources

Point the server at a folder of Markdown guides whenever you want agents to reference local documentation:

```bash
airflow-mcp-server --base-url http://localhost:8080 --auth-token <jwt> --resources-dir ~/airflow-resources
```

- Every top-level `.md`/`.markdown` file becomes a read-only resource (`file:///<slug>`) visible in your MCP client.
- The first `# Heading` in each file (if present) is used as the resource title; otherwise the filename stem is used.
- Set `AIRFLOW_MCP_RESOURCES_DIR=/path/to/docs` if you prefer environment-based configuration.
- Update the files on disk and restart the server to refresh the resources list.

### Considerations

**Authentication**

Two authentication methods are supported:

- `--auth-token <jwt>` — provide a pre-issued JWT directly. Simple, but the token is static: once it expires the server must be restarted. Also configurable via the `AUTH_TOKEN` environment variable.
- `--username <user> --password <pass>` — the server logs in to Airflow's `/auth/token` endpoint on startup and automatically re-fetches a fresh JWT before each token expires. No restarts needed. Also configurable via `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` environment variables.

You must supply one of the two options above. Cookie and basic auth are no longer supported in Airflow 3.0.

**Page Limit**

The default is 100 items, but you can change it using `maximum_page_limit` option in [api] section in the `airflow.cfg` file.

**Transport Selection**

- Use **stdio** transport for direct process communication (default)
- Use **HTTP** transport for web deployments, multiple clients, or when you need better scalability
- Avoid **SSE** transport as it's deprecated in favor of HTTP transport
