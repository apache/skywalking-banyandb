# BanyanDB MCP Server

MCP server for BanyanDB that translates natural language queries to BydbQL.

## Quick Start

1. **Start BanyanDB**: `docker-compose up -d` (see [docker-compose.yml](./docker-compose.yml))
2. **Build**: `npm install && npm run build`
3. **Run**: `npx @modelcontextprotocol/inspector --config inspector-config.json`

Opens `http://localhost:6274` for interactive testing.

## Configuration

### Environment Variables

- `BANYANDB_ADDRESS`: BanyanDB address (default: `localhost:17900`). Auto-converts gRPC port (17900) to HTTP port (17913).
- `LLM_API_KEY`: (Optional) For LLM-powered query generation. Falls back to pattern-based if not set.

### MCP Inspector

**UI Mode:**
```bash
export LLM_API_KEY="your-api-key-here"  # Optional
npx @modelcontextprotocol/inspector --config inspector-config.json
```

**CLI Mode:**
```bash
export LLM_API_KEY="your-api-key-here"  # Optional
npx @modelcontextprotocol/inspector --cli node dist/index.js \
  --method tools/call \
  --tool-name list_resources_bydbql \
  --tool-arg "description=show TOP3 MEASURE endpoint_2xx in metricsMinute from last 48 hours, AGGREGATE BY MAX and ORDER BY DESC"
```

See [CONFIGURATION.md](./CONFIGURATION.md) for detailed setup.

## Example Queries

- "Show STREAM log in recordsLog from last hour"
- "List TRACE zipkin_span in zipkinTrace from last 48 hour, order by timestamp_millis desc"
- "Show TOP3 MEASURE endpoint_2xx in metricsMinute from last 48 hours, AGGREGATE BY MAX and ORDER BY DESC"
- "Show MEASURE service_cpm_minute in sw_metricsMinute from last hour"
- "Show PROPERTY ui_menu IN sw_property from last hour"

## Development

**Requirements:** Node.js 20+, BanyanDB running on HTTP port 17913

**Project Structure:**
- `src/index.ts`: MCP server implementation
- `src/banyandb-client.ts`: BanyanDB HTTP client
- `src/query-generator.ts`: Natural language to BydbQL translator

**Debugging:** VS Code → Run → Start Debugging → "Debug with MCP Server"

