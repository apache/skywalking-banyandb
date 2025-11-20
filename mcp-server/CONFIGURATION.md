# MCP Client Configuration Guide

Configuration guide for MCP clients to use the BanyanDB MCP server.

## Prerequisites

1. **Build the server:**
   ```bash
   cd mcp-server
   npm install && npm run build
   ```

2. **Start BanyanDB:**
   ```bash
   docker-compose up -d
   # Or: docker run -d -p 17913:17913 apache/skywalking-banyandb:latest standalone
   ```

3. **Verify BanyanDB:**
   ```bash
   curl http://localhost:17913/api/healthz  # Should return {"status":"SERVING"}
   ```

4. **Node.js 18+** installed

## MCP Inspector Configuration

### Basic Setup

Edit `inspector-config.json` with absolute path:

```json
{
  "mcpServers": {
    "banyandb": {
      "command": "node",
      "args": ["/absolute/path/to/mcp-server/dist/index.js"],
      "env": {
        "BANYANDB_ADDRESS": "localhost:17900",
        "OPENAI_API_KEY": "sk-your-key-here"
      }
    }
  }
}
```

**Get absolute path:**
```bash
cd mcp-server && pwd  # Use: $(pwd)/dist/index.js
```

### Usage

**UI Mode:**
```bash
npx @modelcontextprotocol/inspector --config inspector-config.json
# Opens http://localhost:6274
```

**CLI Mode:**
```bash
npx @modelcontextprotocol/inspector --cli node dist/index.js \
  -e BANYANDB_ADDRESS=localhost:17900 \
  --method tools/call \
  --tool-name list_resources_bydbql \
  --tool-arg "description=Show me all error logs from the last hour"
```

## Configuration Options

### Environment Variables

- `BANYANDB_ADDRESS`: BanyanDB address (default: `localhost:17900`). Auto-converts gRPC port (17900) to HTTP port (17913).
- `OPENAI_API_KEY`: (Optional) For LLM-powered query generation. Falls back to pattern-based if not set.

**Address formats:**
- `localhost:17900` - Local BanyanDB
- `192.168.1.100:17900` - Remote server
- `banyandb.example.com:17900` - Hostname

### LLM Query Generation (Optional)

1. Get API key from [OpenAI](https://platform.openai.com/api-keys)
2. Add to config: `"OPENAI_API_KEY": "sk-your-key-here"`
3. Restart Inspector

**Benefits:** More accurate queries, better context understanding  
**Fallback:** Pattern-based generation if API key missing or fails

## Other MCP Clients

**MCP CLI:**
```bash
export BANYANDB_ADDRESS=localhost:17900
mcp-server-node /path/to/mcp-server/dist/index.js
```

**MCP SDK:**
```typescript
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const transport = new StdioClientTransport({
  command: "node",
  args: ["/path/to/mcp-server/dist/index.js"],
  env: { BANYANDB_ADDRESS: "localhost:17900" }
});

const client = new Client({ name: "my-client", version: "1.0.0" }, { capabilities: {} });
await client.connect(transport);
```

## Troubleshooting

**MCP server not appearing:**
- Verify JSON config is valid
- Use absolute path to `dist/index.js`
- Check Node.js in PATH: `which node`
- Run `npm run build`

**"Command not found: node":**
- Install Node.js 18+ from [nodejs.org](https://nodejs.org/)
- Or use full path: `"command": "/usr/local/bin/node"`

**Connection refused:**
- Verify BanyanDB: `curl http://localhost:17913/api/healthz`
- Start BanyanDB: `docker-compose up -d`
- Check `BANYANDB_ADDRESS` env var
- Verify ports 17900 (gRPC) and 17913 (HTTP) are accessible

**"Cannot find module":**
- Run `npm install && npm run build`
- Verify `dist/index.js` exists

**View logs:**
```bash
BANYANDB_ADDRESS=localhost:17900 node dist/index.js
```

## Example Queries

- "Show me all error logs from the last hour"
- "Get CPU metrics for service webapp"
- "Find traces from the past 2 days"
- "Show me warnings from service api-server"

## Resources

- [MCP Documentation](https://modelcontextprotocol.io/)
- [MCP Inspector](https://github.com/modelcontextprotocol/inspector)
- [BanyanDB Documentation](https://skywalking.apache.org/docs/skywalking-banyandb/)

