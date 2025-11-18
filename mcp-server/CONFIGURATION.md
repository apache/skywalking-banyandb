# MCP Client Configuration Guide

This guide explains how to configure MCP (Model Context Protocol) clients to use the BanyanDB MCP server.

## What is MCP?

MCP (Model Context Protocol) is a protocol that allows AI assistants and tools to interact with external tools and data sources. The BanyanDB MCP server enables querying BanyanDB using natural language.

## Prerequisites

1. **BanyanDB MCP Server**: Make sure you've built the server:
   ```bash
   cd mcp-server
   npm install
   npm run build
   ```

2. **BanyanDB Server**: Ensure BanyanDB is running and accessible on HTTP port 17913 (default).
   
   **Quick Start with Docker (Recommended):**
   ```bash
   docker run -d \
     -p 17912:17912 \
     -p 17913:17913 \
     --name banyandb \
     apache/skywalking-banyandb:latest \
     standalone
   ```
   
   **Verify BanyanDB is running:**
   ```bash
   # Check health endpoint
   curl http://localhost:17913/api/healthz
   # Should return: {"status":"SERVING"}
   
   # Test BydbQL endpoint (may return 404 if endpoint not available in this version)
   curl -X POST http://localhost:17913/api/v1/bydbql/query \
     -H "Content-Type: application/json" \
     -d '{"query":"SELECT * FROM stream LIMIT 1"}'
   ```
   
   **Note:** A `404 Not Found` on the BydbQL endpoint may indicate the endpoint isn't available in your BanyanDB version. However, if the health endpoint works, BanyanDB is running. The MCP server will show a clearer error message if it can't connect.

3. **Node.js**: Ensure Node.js 18+ is installed and available in your PATH.

## Configuration for MCP Inspector

The [MCP Inspector](https://github.com/modelcontextprotocol/inspector) is the recommended MCP client for testing and development. It provides both a visual web interface and CLI mode.

### Step 1: Use the Provided Configuration File

The `inspector-config.json` file is already configured with the correct settings. You can use it directly:

```bash
cd mcp-server
npx @modelcontextprotocol/inspector --config inspector-config.json
```

### Step 2: Customize the Configuration (Optional)

If you need to customize the configuration, edit `inspector-config.json`:

```json
{
  "mcpServers": {
    "banyandb": {
      "command": "node",
      "args": [
        "/absolute/path/to/skywalking-banyandb/mcp-server/dist/index.js"
      ],
      "env": {
        "BANYANDB_ADDRESS": "localhost:17900",
        "OPENAI_API_KEY": "sk-your-openai-api-key-here"
      }
    }
  }
}
```

**Note:** The `OPENAI_API_KEY` is optional. If omitted, the server will use pattern-based query generation.

### Step 3: Get the Absolute Path

To find the absolute path to your MCP server:

**macOS/Linux:**
```bash
cd skywalking-banyandb/mcp-server
pwd
# This will output something like: /Users/fine/opensource/fork/skywalking-banyandb/mcp-server
# Use: /Users/fine/opensource/fork/skywalking-banyandb/mcp-server/dist/index.js
```

**Windows (PowerShell):**
```powershell
cd C:\path\to\skywalking-banyandb\mcp-server
pwd
# Use the full path: C:\path\to\skywalking-banyandb\mcp-server\dist\index.js
```

### Step 4: Launch the Inspector

**UI Mode (Visual Interface):**
```bash
npx @modelcontextprotocol/inspector --config inspector-config.json
```

This opens `http://localhost:6274` in your browser where you can:
- Browse available tools
- Test queries with a visual interface
- View request/response history
- Debug your MCP server interactively

**CLI Mode (Command Line):**
```bash
# List available tools (use absolute path)
npx @modelcontextprotocol/inspector --cli node dist/index.js \
  -e BANYANDB_ADDRESS=localhost:17900 \
  --method tools/list

# Call a tool (use absolute path and environment variables)
npx @modelcontextprotocol/inspector --cli node dist/index.js \
  -e BANYANDB_ADDRESS=localhost:17900 \
  --method tools/call \
  --tool-name list_resources_bydbql \
  --tool-arg "description=Show me all error logs from the last hour"
```

**Note:** Replace `dist/index.js` with your actual absolute path. You can also use shell environment variables instead of `-e` flags:

```bash
# Alternative: Using shell environment variables
cd /Users/fine/opensource/fork/skywalking-banyandb/mcp-server
BANYANDB_ADDRESS=localhost:17900 \
npx @modelcontextprotocol/inspector --cli node dist/index.js \
  --method tools/call \
  --tool-name list_resources_bydbql \
  --tool-arg "description=Show me all error logs from the last hour"
```

### Step 5: Verify the Connection

1. **UI Mode**: Open `http://localhost:6274`, click "Tools", select `list_resources_bydbql`, and test a query
2. **CLI Mode**: Run the list command above to see available tools

## Configuration Options

### Environment Variables

You can customize the BanyanDB connection and enable LLM-powered query generation:

```json
{
  "mcpServers": {
    "banyandb": {
      "command": "node",
      "args": [
        "/path/to/mcp-server/dist/index.js"
      ],
      "env": {
        "BANYANDB_ADDRESS": "192.168.1.100:17900",
        "OPENAI_API_KEY": "sk-your-openai-api-key-here"
      }
    }
  }
}
```

**BANYANDB_ADDRESS options:**
- `localhost:17900` - Default, connects to local BanyanDB
- `192.168.1.100:17900` - Connect to remote BanyanDB server
- `banyandb.example.com:17900` - Connect using hostname

**Note**: The client automatically converts gRPC port (17900) to HTTP port (17913) internally.

### LLM-Powered Query Generation (Optional)

The MCP server supports using OpenAI's API to generate more accurate BydbQL queries from natural language descriptions. This is optional - if no API key is provided, the server will use pattern-based query generation.

**To enable LLM query generation:**

1. **Get an OpenAI API Key:**
   - Sign up at [OpenAI](https://platform.openai.com/)
   - Create an API key from your [API keys page](https://platform.openai.com/api-keys)
   - Copy your API key (starts with `sk-`)

2. **Add the API key to your configuration:**
   ```json
   {
     "mcpServers": {
       "banyandb": {
         "command": "node",
         "args": ["/path/to/mcp-server/dist/index.js"],
         "env": {
           "BANYANDB_ADDRESS": "localhost:17900",
           "OPENAI_API_KEY": "sk-your-actual-api-key-here"
         }
       }
     }
   }
   ```

3. **Restart the MCP Inspector** (if running)

**Benefits of LLM-powered generation:**
- More accurate query generation for complex natural language descriptions
- Better understanding of context and intent
- Handles edge cases and variations in phrasing better

**Fallback behavior:**
- If `OPENAI_API_KEY` is not set, the server uses pattern-based query generation
- If the LLM API call fails, the server automatically falls back to pattern matching
- No API key is required for basic functionality

## Configuration for Other MCP Clients

### Using MCP CLI (Command Line)

If you're using the MCP CLI tool:

```bash
export BANYANDB_ADDRESS=localhost:17900
mcp-server-node /path/to/mcp-server/dist/index.js
```

### Using MCP SDK Directly

If you're building a custom MCP client:

```typescript
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const transport = new StdioClientTransport({
  command: "node",
  args: ["/path/to/mcp-server/dist/index.js"],
  env: {
    BANYANDB_ADDRESS: "localhost:17900"
  }
});

const client = new Client({
  name: "my-client",
  version: "1.0.0"
}, {
  capabilities: {}
});

await client.connect(transport);
```

## Troubleshooting

### Problem: MCP server not appearing

1. Check that the JSON config file is valid (use a JSON validator)
2. Ensure the path to `dist/index.js` in `inspector-config.json` is absolute and correct
3. Verify Node.js is in your PATH: `which node` (macOS/Linux) or `where node` (Windows)
4. Check that you've built the server: `npm run build` in the mcp-server directory
5. Restart the MCP Inspector

### Problem: "Command not found: node"

**Solutions:**
1. Install Node.js 18+ from [nodejs.org](https://nodejs.org/)
2. Add Node.js to your PATH
3. Use the full path to node in the config:
   ```json
   {
     "mcpServers": {
       "banyandb": {
         "command": "/usr/local/bin/node",
         "args": ["/path/to/mcp-server/dist/index.js"]
       }
     }
   }
   ```

### Problem: "fetch failed" or Connection refused errors

**Symptoms:**
- Error message: `Failed to call tool list_resources_bydbql: MCP error -32603: fetch failed`
- Error message: `Failed to connect to BanyanDB at http://...`
- Connection refused errors

**Solutions:**
1. **Verify BanyanDB is running:**
   ```bash
   # Check health endpoint (should return {"status":"SERVING"})
   curl http://localhost:17913/api/healthz
   
   # Test BydbQL endpoint with POST request
   curl -X POST http://localhost:17913/api/v1/bydbql/query \
     -H "Content-Type: application/json" \
     -d '{"query":"SELECT * FROM stream LIMIT 1"}'
   ```
   
   **Expected responses:**
   - Health endpoint: `{"status":"SERVING"}` ✅ BanyanDB is running
   - BydbQL endpoint: 
     - `404 Not Found` - BydbQL endpoint may not be available in this BanyanDB version
     - `400 Bad Request` - Endpoint exists but query is invalid ✅ This is good!
     - `Connection refused` - BanyanDB is not running

2. **Start BanyanDB (Choose one method):**
   
   **Option A: Using Docker Compose (Easiest):**
   ```bash
   cd mcp-server
   docker-compose up -d
   ```
   This uses the provided `docker-compose.yml` file.
   
   **Option B: Using Docker directly:**
   ```bash
   docker run -d \
     -p 17912:17912 \
     -p 17913:17913 \
     --name banyandb \
     apache/skywalking-banyandb:latest \
     standalone
   ```
   
   **Option C: Build from source:**
   - Follow the [BanyanDB installation guide](https://skywalking.apache.org/docs/skywalking-banyandb/)
   - Run `banyand standalone` after building
   - Ensure the HTTP API is enabled (default port 17913)

3. **Check the `BANYANDB_ADDRESS` environment variable:**
   - Make sure you're passing it correctly: `-e BANYANDB_ADDRESS=localhost:17900`
   - Verify the address format matches your BanyanDB setup
   - The client automatically converts gRPC port (17900) to HTTP port (17913)

4. **Test the connection manually:**
   ```bash
   # Test health endpoint first
   curl http://localhost:17913/api/healthz
   # Should return: {"status":"SERVING"}
   
   # Test BydbQL endpoint
   curl -X POST http://localhost:17913/api/v1/bydbql/query \
     -H "Content-Type: application/json" \
     -d '{"query":"SELECT * FROM stream LIMIT 1"}'
   ```
   
   **Understanding the responses:**
   - Health endpoint returns `{"status":"SERVING"}` → BanyanDB is running ✅
   - BydbQL endpoint returns `404 Not Found` → Endpoint may not be available in this version
   - BydbQL endpoint returns `400 Bad Request` → Endpoint exists, query syntax issue (this is actually good!)
   - Any endpoint returns `Connection refused` → BanyanDB is not running

5. **Check firewall settings** if connecting to a remote server

6. **Verify port numbers:**
   - Default gRPC port: 17900
   - Default HTTP port: 17913
   - Make sure these ports are not blocked

### Problem: ExperimentalWarning about JSON modules

**Symptoms:**
- Warning: `ExperimentalWarning: Importing JSON modules is an experimental feature`

**Solution:**
This is a harmless warning from Node.js and can be safely ignored. It doesn't affect functionality. To suppress it, you can run Node.js with:
```bash
NODE_NO_WARNINGS=1 node dist/index.js
```

### Problem: "Cannot find module" errors

**Solutions:**
1. Run `npm install` in the mcp-server directory
2. Run `npm run build` to compile TypeScript
3. Verify `dist/index.js` exists

### Viewing Logs

The MCP server logs to stderr. To see logs:

1. **MCP Inspector UI**: Check the browser console and Inspector's log panel
2. **MCP Inspector CLI**: Logs appear in the terminal output
3. **Command line**: Run directly to see output:
   ```bash
   cd mcp-server
   BANYANDB_ADDRESS=localhost:17900 node dist/index.js
   ```

## Testing the Configuration

Test your configuration manually:

```bash
# 1. Build the server
cd mcp-server
npm run build

# 2. Test running the server directly
BANYANDB_ADDRESS=localhost:17900 node dist/index.js

# You should see:
# [MCP] BanyanDB MCP server started
# [MCP] Connecting to BanyanDB at localhost:17900
```

If the server starts without errors, your configuration should work with the MCP Inspector.

## Example Usage

Once configured, you can use the MCP Inspector to query BanyanDB:

**UI Mode:**
1. Open `http://localhost:6274` in your browser
2. Click "Tools" → `list_resources_bydbql`
3. Enter descriptions like:
   - "Show me all error logs from the last hour"
   - "Get CPU metrics for service webapp"
   - "Find traces from the past 2 days"
   - "Show me warnings from service api-server"

**CLI Mode:**
```bash
# List available groups (replace with your absolute path)
npx @modelcontextprotocol/inspector --cli node /path/to/mcp-server/dist/index.js \
  -e BANYANDB_ADDRESS=localhost:17900 \
  --method tools/call \
  --tool-name list_groups_schemas \
  --tool-arg '{"resource_type":"groups"}'

# List streams in a group
npx @modelcontextprotocol/inspector --cli node /path/to/mcp-server/dist/index.js \
  -e BANYANDB_ADDRESS=localhost:17900 \
  --method tools/call \
  --tool-name list_groups_schemas \
  --tool-arg '{"resource_type":"streams","group":"default"}'

# List measures in a group
npx @modelcontextprotocol/inspector --cli node /path/to/mcp-server/dist/index.js \
  -e BANYANDB_ADDRESS=localhost:17900 \
  --method tools/call \
  --tool-name list_groups_schemas \
  --tool-arg '{"resource_type":"measures","group":"default"}'

# List traces in a group
npx @modelcontextprotocol/inspector --cli node /path/to/mcp-server/dist/index.js \
  -e BANYANDB_ADDRESS=localhost:17900 \
  --method tools/call \
  --tool-name list_groups_schemas \
  --tool-arg '{"resource_type":"traces","group":"default"}'

# List properties in a group
npx @modelcontextprotocol/inspector --cli node /path/to/mcp-server/dist/index.js \
  -e BANYANDB_ADDRESS=localhost:17900 \
  --method tools/call \
  --tool-name list_groups_schemas \
  --tool-arg '{"resource_type":"properties","group":"default"}'

# Query error logs
npx @modelcontextprotocol/inspector --cli node /path/to/mcp-server/dist/index.js \
  -e BANYANDB_ADDRESS=localhost:17900 \
  --method tools/call \
  --tool-name list_resources_bydbql \
  --tool-arg "description=Show me all error logs from the last hour"

# Query CPU metrics
npx @modelcontextprotocol/inspector --cli node /path/to/mcp-server/dist/index.js \
  -e BANYANDB_ADDRESS=localhost:17900 \
  --method tools/call \
  --tool-name list_resources_bydbql \
  --tool-arg "description=Get CPU metrics for service webapp"

# Query traces
npx @modelcontextprotocol/inspector --cli node /path/to/mcp-server/dist/index.js \
  -e BANYANDB_ADDRESS=localhost:17900 \
  --method tools/call \
  --tool-name list_resources_bydbql \
  --tool-arg "description=Find traces from the past 2 days"
```

## Additional Resources

- [MCP Documentation](https://modelcontextprotocol.io/)
- [MCP Inspector](https://github.com/modelcontextprotocol/inspector)
- [BanyanDB Documentation](https://skywalking.apache.org/docs/skywalking-banyandb/)

