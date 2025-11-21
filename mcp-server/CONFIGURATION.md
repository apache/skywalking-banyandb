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
   ```

3. **Verify BanyanDB:**
   ```bash
   curl http://localhost:17913/api/healthz  # Should return {"status":"SERVING"}
   ```

4. **Node.js 20+** installed

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
        "LLM_API_KEY": "sk-your-key-here"
      }
    }
  }
}
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
  --tool-arg "description=Show MEASURE service_cpm_minute in sw_metricsMinute from last hour
```

## Configuration Options

### Environment Variables

- `BANYANDB_ADDRESS`: BanyanDB address (default: `localhost:17900`). Auto-converts gRPC port (17900) to HTTP port (17913).
- `LLM_API_KEY`: (Optional) For LLM-powered query generation. Falls back to pattern-based if not set.
- `LLM_BASE_URL`: (Optional) Base URL for the LLM API (default: `https://api.openai.com/v1`). Only used when `LLM_API_KEY` is set.

**Address formats:**
- `localhost:17900` - Local BanyanDB
- `192.168.1.100:17900` - Remote server
- `banyandb.example.com:17900` - Hostname

## VS Code Debug Configuration

VS Code debug configurations can be set up using a `launch.json` file. **Note:** The `.vscode/` directory is excluded from version control (via `.gitignore`), so you'll need to create this file locally.

### Generating launch.json

Create the `.vscode/launch.json` file using one of these methods:

**VS Code Auto-Generate**
1. Open VS Code in the `mcp-server` directory
2. Go to Run → Add Configuration... (or press `Cmd+Shift+P` / `Ctrl+Shift+P` and type "Debug: Add Configuration")
3. Select "Node.js" as the environment
4. VS Code will create a basic `.vscode/launch.json` file
5. Replace the content with the complete configuration template below. Copy this configuration into `mcp-server/.vscode/launch.json`:

```json
{
  "version": "0.1.0",
  "configurations": [
   {
      "name": "Debug with MCP Inspector",
      "type": "node",
      "request": "launch",
      "runtimeExecutable": "npx",
      "runtimeArgs": [
        "@modelcontextprotocol/inspector",
        "--config",
        "${workspaceFolder}/inspector-config.json"
      ],
      "env": {
        "BANYANDB_ADDRESS": "localhost:17900",
        "LLM_API_KEY": "${env:LLM_API_KEY}"
      },
      "console": "integratedTerminal",
      "internalConsoleOptions": "neverOpen",
      "skipFiles": ["<node_internals>/**"],
      "sourceMaps": true,
      "outputCapture": "std"
   }
  ]
}
```

### Available Debug Configurations

1. **Debug MCP Server (tsx)** - Debug TypeScript source directly using tsx
   - Runs `src/index.ts` without compilation
   - Fast iteration for development
   - Set breakpoints in TypeScript files

2. **Debug MCP Server (compiled)** - Debug compiled JavaScript
   - Runs `dist/index.js` after building
   - Requires `npm run build` first (runs automatically via preLaunchTask)
   - Source maps enabled for TypeScript debugging

3. **Start MCP Server for Inspector (compiled)** - Start server with inspector attachment
   - Runs compiled server with `--inspect=9229`
   - Use with "Attach to MCP Server" configuration
   - Allows debugging while MCP Inspector connects

4. **Start MCP Server for Inspector (tsx)** - Start TypeScript server with inspector attachment
   - Runs TypeScript source with `--inspect=9229`
   - Use with "Attach to MCP Server" configuration
   - Faster startup for development

5. **Debug with MCP Inspector** - Launch MCP Inspector with debugging ⭐ **Recommended for testing**
   - Starts the MCP Inspector UI automatically
   - Opens http://localhost:6274 in your default browser
   - Server runs in debug mode with full breakpoint support
   - Perfect for interactive testing and debugging queries

6. **Attach to MCP Server** - Attach debugger to running server
   - Attaches to server running on port 9229
   - Use after starting server with inspector configurations
   - Allows debugging while server is running

### Usage

1. **Set environment variables** (optional):
   - Set `LLM_API_KEY` in your environment or VS Code settings
   - Default `BANYANDB_ADDRESS` is `localhost:17900`

2. **Start debugging:**
   - Open VS Code in the `mcp-server` directory
   - Go to Run → Start Debugging (F5)
   - Select a debug configuration from the dropdown
   - Set breakpoints in TypeScript files
   - Debug output appears in the integrated terminal

3. **Debug with MCP Inspector** (Recommended workflow):
   - Select "Debug with MCP Inspector" configuration from the dropdown
   - Press F5 or click Start Debugging
   - Server starts in debug mode automatically
   - Inspector UI opens at http://localhost:6274 in your browser
   - Set breakpoints in TypeScript files (e.g., `src/index.ts`, `src/query-generator.ts`)
   - Test queries through the Inspector UI - breakpoints will trigger
   - View debug output in VS Code's Debug Console and Terminal
   - This is the easiest way to debug while testing MCP tools interactively

### Environment Variables in launch.json

The launch configurations use these environment variables:
- `BANYANDB_ADDRESS`: BanyanDB server address (default: `localhost:17900`)
- `LLM_API_KEY`: API key for LLM query generation (from environment)
- `LLM_BASE_URL`: Base URL for the LLM API (optional, default: `https://api.openai.com/v1`)

To customize, edit `.vscode/launch.json` and modify the `env` section in each configuration.

## Troubleshooting

**MCP server not appearing:**
- Verify JSON config is valid
- Use absolute path to `dist/index.js`
- Check Node.js in PATH: `which node`
- Run `npm run build`

**"Command not found: node":**
- Install Node.js 20+ from [nodejs.org](https://nodejs.org/)
- Or use full path: `"command": "/usr/local/bin/node"`

**Connection refused:**
- Verify BanyanDB: `curl http://localhost:17913/api/healthz`
- Start BanyanDB: `docker-compose up -d`
- Check `BANYANDB_ADDRESS` env var
- Verify ports 17900 (gRPC) and 17913 (HTTP) are accessible

**"Cannot find module":**
- Run `npm install && npm run build`
- Verify `dist/index.js` exists

## Resources

- [MCP Documentation](https://modelcontextprotocol.io/)
- [MCP Inspector](https://github.com/modelcontextprotocol/inspector)
- [BanyanDB Documentation](https://skywalking.apache.org/docs/skywalking-banyandb/)
