# BanyanDB MCP Server

MCP server for BanyanDB that translates natural language queries to BydbQL.

## Quick Start

1. **Start BanyanDB**: `docker-compose up -d` (see [docker-compose.yml](./docker-compose.yml))
2. **Build**: `npm install && npm run build`
3. **Run**: `npx @modelcontextprotocol/inspector --config inspector-config.json`

Opens `http://localhost:6274` for interactive testing.

## Features

- Natural language to BydbQL query translation
- Supports streams, measures, traces, and properties
- Time-based queries ("last hour", "past 2 days")
- Filter support (errors, warnings, service names)

## Configuration

### Environment Variables

- `BANYANDB_ADDRESS`: BanyanDB address (default: `localhost:17900`). Auto-converts gRPC port (17900) to HTTP port (17913).
- `OPENAI_API_KEY`: (Optional) For LLM-powered query generation. Falls back to pattern-based if not set.

### MCP Inspector

**UI Mode:**
```bash
export OPENAI_API_KEY="your-api-key-here"  # Optional
npx @modelcontextprotocol/inspector --config inspector-config.json
```

**CLI Mode:**
```bash
npx @modelcontextprotocol/inspector --cli node dist/index.js \
  --method tools/call \
  --tool-name list_resources_bydbql \
  --tool-arg "description=Show me all error logs from the last hour"
```

See [CONFIGURATION.md](./CONFIGURATION.md) for detailed setup.

## Example Queries

- "Show me all error logs from the last hour"
- "Get CPU metrics for service webapp"
- "Find traces from the past 2 days"
- "Show me warnings from service api-server"

## Development

**Requirements:** Node.js 18+, BanyanDB running on HTTP port 17913

**Project Structure:**
- `src/index.ts`: MCP server implementation
- `src/banyandb-client.ts`: BanyanDB HTTP client
- `src/query-generator.ts`: Natural language to BydbQL translator

**Debugging:** VS Code → Run → Start Debugging → "Debug with MCP Server"

