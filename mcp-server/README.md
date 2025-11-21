# BanyanDB MCP Server

MCP server for BanyanDB that translates natural language queries to BydbQL.

## Quick Start

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Build:**
   ```bash
   npm run build
   ```

3. **Start BanyanDB:**
   ```bash
   docker-compose up -d
   ```

4. **Run with MCP Inspector:**
   ```bash
   npx @modelcontextprotocol/inspector --config inspector-config.json
   ```

## Documentation

For detailed configuration and usage instructions, see:
- [Overview](../../docs/interacting/mcp-server/mcp.md)
- [Configuration Guide](../../docs/interacting/mcp-server/configuration.md)

## Development

- **Build:** `npm run build`
- **Dev mode:** `npm run dev`
- **Start:** `npm start`

