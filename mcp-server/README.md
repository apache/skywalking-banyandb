# BanyanDB MCP Server

This is a Model Context Protocol (MCP) server for BanyanDB that allows querying data using natural language descriptions.

## 🚀 Quick Start

1. **Start BanyanDB**: Make sure BanyanDB is running (see [docker-compose.yml](./docker-compose.yml))
2. **Build**: `npm install && npm run build`
3. **Use**: `npx @modelcontextprotocol/inspector --config inspector-config.json`

**📖 Need detailed configuration help?** See [CONFIGURATION.md](./CONFIGURATION.md)

### Initial Setup

If your BanyanDB instance is empty (no groups, streams, measures, etc.), you need to create groups and schemas first:

```bash
# Manually create groups using curl or bydbctl
# See docs/interacting/bydbctl/schema/ for examples
```

**Note**: You need to ingest actual data using:
- Stream write API: `POST /api/v1/stream/data`
- Measure write API: `POST /api/v1/measure/data`
- Or use BydbQL INSERT statements

## Features

- Query BanyanDB streams, measures, traces, and properties using natural language
- Automatic translation from natural language to BydbQL queries
- Support for time-based queries (e.g., "last hour", "past 2 days")
- Filter support for errors, warnings, and service names

## Installation

```bash
npm install
```

## Build

```bash
npm run build
```

## Usage

### Environment Variables

- `BANYANDB_ADDRESS`: BanyanDB address (default: `localhost:17900`). The client will automatically convert gRPC port (17900) to HTTP port (17913) if needed.
- `OPENAI_API_KEY`: (Optional) OpenAI API key for LLM-powered query generation. If not set, the server will use pattern-based query generation.

### Running the Server

```bash
# Development mode
npm run dev

# Production mode
npm start
```

### Example MCP Client Configuration

**📖 For detailed step-by-step configuration instructions, see [CONFIGURATION.md](./CONFIGURATION.md)**

#### Quick Start for MCP Inspector

The [MCP Inspector](https://github.com/modelcontextprotocol/inspector) provides a visual web UI and CLI mode for testing MCP servers.

**UI Mode:**
```bash
# Recommended: Use the wrapper script with environment variable
export OPENAI_API_KEY="your-api-key-here"  # Optional, for LLM query generation
./run-inspector.sh

# Or use the config file directly (requires OPENAI_API_KEY in config)
npx @modelcontextprotocol/inspector --config inspector-config.json

# Or create your own config
npx @modelcontextprotocol/inspector --config path/to/your-config.json
```

**Note:** The `inspector-config.json` file uses `${OPENAI_API_KEY}` as a placeholder. To use your environment variable, either:
- Use the `run-inspector.sh` wrapper script (recommended), which automatically expands the environment variable
- Or manually replace `${OPENAI_API_KEY}` in the config file with your actual API key

This opens `http://localhost:6274` where you can interactively test your MCP server.

**CLI Mode:**
```bash
# List available tools
npx @modelcontextprotocol/inspector --cli node dist/index.js --method tools/list

# Call list_groups_schemas to list groups
npx @modelcontextprotocol/inspector --cli node dist/index.js --method tools/call --tool-name list_groups_schemas --tool-arg resource_type=groups

# Call list_groups_schemas to list streams in a group
npx @modelcontextprotocol/inspector --cli node dist/index.js   --method tools/call   --tool-name list_groups_schemas   --tool-arg resource_type=streams group=default

# Call list_groups_schemas to list measures in a group
npx @modelcontextprotocol/inspector --cli node dist/index.js   --method tools/call   --tool-name list_groups_schemas   --tool-arg resource_type=measures group=sw_metric

# Call list_resources_bydbql tool
npx @modelcontextprotocol/inspector --cli node dist/index.js \
  --method tools/call \
  --tool-name list_resources_bydbql \
  --tool-arg "description=Show me all error logs from the last hour"
```

**Benefits:**
- ✅ Visual interface for testing and debugging
- ✅ CLI mode for automation and scripting
- ✅ Perfect for development and CI/CD
- ✅ Request/response history and error visualization

**See [CONFIGURATION.md](./CONFIGURATION.md) for detailed instructions, troubleshooting, and examples.**

## Example Queries

You can use natural language descriptions to query BanyanDB:

- "Show me all error logs from the last hour"
- "Get CPU metrics for service webapp"
- "Find traces from the past 2 days"
- "Show me warnings from service api-server"
- "Query all streams from the past 30 minutes"
- "Get measure data for the last day"

The query generator automatically:
- Detects resource type (stream, measure, trace, property)
- Extracts time ranges from natural language
- Adds appropriate filters (errors, warnings, service names)
- Generates valid BydbQL queries

## Development

The server uses the official MCP TypeScript SDK (`@modelcontextprotocol/sdk`) and communicates with BanyanDB via HTTP API (grpc-gateway).

### Project Structure

- `src/index.ts`: Main MCP server implementation using `@modelcontextprotocol/sdk`
- `src/banyandb-client.ts`: BanyanDB HTTP client wrapper using fetch API
- `src/query-generator.ts`: Natural language to BydbQL query generator

### Requirements

- Node.js 18+ 
- BanyanDB server running and accessible via HTTP API (default port: 17913)

### Debugging

**📖 For detailed debugging instructions, see [DEBUGGING.md](./DEBUGGING.md)**

Quick start:
- **VS Code**: Press `F5` and select "Debug MCP Server (tsx)"
- **Command line**: `BANYANDB_ADDRESS=localhost:17900 npx tsx --inspect src/index.ts`
- Then attach Chrome DevTools at `chrome://inspect`

### Troubleshooting

1. **Connection refused**: Check that BanyanDB is running and accessible on HTTP port 17913 (or the port specified in `BANYANDB_ADDRESS`)
2. **Query generation errors**: Check the natural language description - it should mention resource types (stream/measure/trace) and time ranges
3. **HTTP errors**: Ensure BanyanDB HTTP API is enabled and accessible

