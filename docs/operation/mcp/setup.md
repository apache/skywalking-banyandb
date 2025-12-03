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
node dist/index.js --help
```

### 2. Configure Environment Variables

Set the following environment variables:

- `BANYANDB_ADDRESS`: BanyanDB server address (default: `localhost:17900`). The server auto-converts gRPC port (17900) to HTTP port (17913).
- `LLM_API_KEY`: (Optional) API key for LLM-powered query generation. Falls back to pattern-based if not set.
- `LLM_BASE_URL`: (Optional) Base URL for the LLM API (default: `https://api.openai.com/v1`). Only used when `LLM_API_KEY` is set.

**Address formats:**
- `localhost:17900` - Local BanyanDB
- `192.168.1.100:17900` - Remote server
- `banyandb.example.com:17900` - Hostname

### 3. Configure MCP Client

Create a configuration file for your MCP client. For example, for MCP Inspector, see [MCP client](inspector.md) - Basic Setup

## Using Docker Image

The MCP server is available as a Docker image for easy deployment.

```bash
docker pull apache/skywalking-banyandb-mcp:{COMMIT_ID}
```

### 2. Run the Container

```bash
docker run -d \
  --name banyandb-mcp \
  -e BANYANDB_ADDRESS=banyandb:17900 \
  -e LLM_API_KEY=sk-your-key-here \
  -e LLM_BASE_URL=your-llm-base-url \
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
        "-e", "LLM_API_KEY=sk-your-key-here",
        "-e", "LLM_BASE_URL=your-llm-base-url",
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
      - LLM_API_KEY=${LLM_API_KEY}
      - LLM_BASE_URL=${LLM_BASE_URL:-https://api.openai.com/v1}
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
| `LLM_API_KEY` | No | - | API key for LLM-powered query generation. Falls back to pattern-based if not set. |
| `LLM_BASE_URL` | No | `https://api.openai.com/v1` | Base URL for the LLM API. Only used when `LLM_API_KEY` is set. |

### Verifying BanyanDB Connection

Before using the MCP server, verify that BanyanDB is running and accessible:

```bash
# Check HTTP endpoint
curl http://localhost:17913/api/healthz
# Should return: {"status":"SERVING"}

# Check gRPC endpoint (if grpcurl is installed)
grpcurl -plaintext localhost:17912 list
```

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

