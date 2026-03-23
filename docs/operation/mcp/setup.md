# Setup MCP Server

This guide explains how to set up and use the BanyanDB MCP server from pre-built binaries or Docker images.

## Prerequisites

- **Node.js 20+** installed (for binary usage)
- **BanyanDB** running and accessible
- **MCP client** (e.g., Claude Desktop, MCP Inspector, or other MCP-compatible clients)

## Using Pre-built Binary

The MCP server binary is included in the BanyanDB release package. After extracting the release, you can find the MCP server in the `mcp` directory.

### 1. Verify Binary

```bash
cd mcp
node dist/index.js
```

The server starts in stdio mode by default and waits for MCP client connections. Verify BanyanDB is accessible before starting (see [Verifying BanyanDB Connection](#verifying-banyandb-connection)).

### 2. Configure Environment Variables

Set the following environment variables:

- `BANYANDB_ADDRESS`: BanyanDB server address (default: `localhost:17900`). The server auto-converts gRPC port (17900) to HTTP port (17913).

**Address formats:**
- `localhost:17900` - Local BanyanDB
- `192.168.1.100:17900` - Remote server
- `banyandb.example.com:17900` - Hostname

### 3. Configure MCP Client

Create a configuration file for your MCP client. For example, for MCP Inspector, see [MCP client](inspector.md) - Basic Setup

## Using Docker Image

The MCP server is available as a Docker image for easy deployment.

### 1. Pull Docker Image

```bash
docker pull ghcr.io/apache/skywalking-banyandb-mcp:{COMMIT_ID}
```

### 2. Run the Container

```bash
docker run -d \
  --name banyandb-mcp \
  -e BANYANDB_ADDRESS=banyandb:17900 \
  ghcr.io/apache/skywalking-banyandb-mcp:{COMMIT_ID}
```

### 3. Configure MCP Client for Docker

When using Docker, configure your MCP client to connect to the container:

```json
{
  "mcpServers": {
    "banyandb": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "-e", "BANYANDB_ADDRESS=banyandb:17900",
        "--network", "host",
        "ghcr.io/apache/skywalking-banyandb-mcp:{COMMIT_ID}"
      ]
    }
  }
}
```

### 4. Docker Compose Example

You can also use Docker Compose to run both BanyanDB and the MCP server together:

```yaml
services:
  banyandb:
    image: ghcr.io/apache/skywalking-banyandb:{COMMIT_ID} # apache/skywalking-banyandb:{COMMIT_ID}
    container_name: banyandb
    command: standalone
    ports:
      - "17912:17912"  # gRPC port
      - "17913:17913"  # HTTP port
    volumes:
      - ./banyandb-data:/data

  mcp:
    image: ghcr.io/apache/skywalking-banyandb-mcp:{COMMIT_ID} # apache/skywalking-banyandb-mcp:{COMMIT_ID}
    container_name: banyandb-mcp
    environment:
      - BANYANDB_ADDRESS=banyandb:17900
    depends_on:
      - banyandb
    networks:
      - default
```

## Configuration Options

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BANYANDB_ADDRESS` | No | `localhost:17900` | BanyanDB server address. Auto-converts gRPC port (17900) to HTTP port (17913). |
| `TRANSPORT` | No | `stdio` | Transport mode: `stdio` for standard I/O (default), `http` for Streamable HTTP. |
| `MCP_PORT` | No | `3000` | HTTP port to listen on. Only used when `TRANSPORT=http`. |

### Verifying BanyanDB Connection

Before using the MCP server, verify that BanyanDB is running and accessible:

```bash
# Check HTTP endpoint
curl http://localhost:17913/api/healthz
# Should return: {"status":"SERVING"}

# Check gRPC endpoint (if grpcurl is installed)
grpcurl -plaintext localhost:17912 list
```

## HTTP Transport Mode

By default the MCP server communicates over standard I/O (`TRANSPORT=stdio`), which is suitable for desktop clients such as Claude Desktop. Set `TRANSPORT=http` to expose the server as an HTTP endpoint instead. This mode uses the MCP [Streamable HTTP](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) transport, which supports both streaming (SSE) and direct JSON responses.

### Start in HTTP Mode

```bash
# Default port 3000
TRANSPORT=http BANYANDB_ADDRESS=localhost:17900 node dist/index.js

# Custom port
TRANSPORT=http MCP_PORT=8080 BANYANDB_ADDRESS=localhost:17900 node dist/index.js
```

The server prints the listening address on startup:

```
BanyanDB MCP HTTP server listening on :3000/mcp
```

The single endpoint is `POST /mcp` (also handles `GET /mcp` for server-to-client notifications). All other paths return 404.

### Configure an MCP Client for HTTP

MCP clients that support the Streamable HTTP transport (e.g. MCP Inspector, custom integrations) connect via a URL:

```json
{
  "mcpServers": {
    "banyandb": {
      "url": "http://localhost:3000/mcp"
    }
  }
}
```

### Docker with HTTP Mode

When running in a container, expose the HTTP port and set the transport env var:

```bash
docker run -d \
  --name banyandb-mcp \
  -p 3000:3000 \
  -e TRANSPORT=http \
  -e MCP_PORT=3000 \
  -e BANYANDB_ADDRESS=banyandb:17900 \
  ghcr.io/apache/skywalking-banyandb-mcp:{COMMIT_ID}
```

Docker Compose example with HTTP mode:

```yaml
services:
  banyandb:
    image: ghcr.io/apache/skywalking-banyandb:{COMMIT_ID}
    container_name: banyandb
    command: standalone
    ports:
      - "17912:17912"
      - "17913:17913"

  mcp:
    image: ghcr.io/apache/skywalking-banyandb-mcp:{COMMIT_ID}
    container_name: banyandb-mcp
    environment:
      - TRANSPORT=http
      - MCP_PORT=3000
      - BANYANDB_ADDRESS=banyandb:17900
    ports:
      - "3000:3000"
    depends_on:
      - banyandb
```

### Choosing a Transport

| | `stdio` (default) | `http` |
|---|---|---|
| **Best for** | Claude Desktop, local CLI clients | Web integrations, remote clients, custom apps |
| **Connection** | Subprocess stdin/stdout | HTTP `POST /mcp` |
| **Session state** | Persistent (single process) | Stateless (new context per request) |

## Troubleshooting

**Connection refused:**
- Verify BanyanDB is running: `curl http://localhost:17913/api/healthz`
- Check `BANYANDB_ADDRESS` environment variable
- Verify ports 17900 (gRPC) and 17913 (HTTP) are accessible
- For Docker, ensure containers are on the same network

**"Command not found: node":**
- Install Node.js 20+ from [nodejs.org](https://nodejs.org/)
- Or use Docker image instead

**MCP server not appearing in client:**
- Verify JSON config is valid
- Use absolute path to binary
- Check environment variables are set correctly
- Restart your MCP client after configuration changes

## Next Steps

- [Test via Inspector Guide](inspector.md) - Learn how to test the MCP server using MCP Inspector
- [Build and Package](build.md) - For developers who want to build from source

