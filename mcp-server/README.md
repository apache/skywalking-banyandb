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

For detailed setup and usage instructions, see:
- [Setup MCP](../../docs/operation/mcp-server/setup) - How to use from binary or Docker images
- [Test via Inspector](../../docs/operation/mcp-server/inspector) - How to test using MCP Inspector
- [Build and Package](../../docs/operation/mcp-server/build) - For developers building from source

## Development

- **Build:** `npm run build`
- **Dev mode:** `npm run dev`
- **Start:** `npm start`

