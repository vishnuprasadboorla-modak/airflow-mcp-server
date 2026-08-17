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
