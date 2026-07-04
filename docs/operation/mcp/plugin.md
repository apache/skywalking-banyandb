# BydbQL Plugin Packaging

The BanyanDB MCP server is also packaged as a Claude/Codex plugin so a host model
can generate, validate, and run read-only BydbQL. This page describes the plugin
layout, the build it requires before it can load, and how the manifests are kept
in sync.

## Layout

| Path | Purpose |
|------|---------|
| `.claude-plugin/plugin.json` | Claude plugin manifest. |
| `.codex-plugin/plugin.json` | Codex plugin manifest. |
| `.mcp.json` | MCP server definition referenced by both manifests. |
| `skills/bydbql/` | The `bydbql` skill and its `references/`. |
| `mcp/` | The MCP server source, the TypeScript build output (`dist/`), and the `bydbql-parse` validator tool. |

## Required build before loading

Both the MCP server (`mcp/dist/index.js`) and the BydbQL parse validator
(`mcp/tools/bin/bydbql-parse`) are build artifacts. `dist/` and `tools/bin/` are
git-ignored, so a fresh checkout has neither. Build both before loading the
plugin or starting the server:

```bash
cd mcp
npm install
npm run build:all   # equivalent to: npm run build && npm run build:validator
```

- `npm run build` compiles TypeScript into `mcp/dist/`.
- `npm run build:validator` compiles the Go validator into `mcp/tools/bin/bydbql-parse` (requires the Go toolchain).

`validate_bydbql` invokes the **prebuilt** `bydbql-parse` binary. It intentionally
does not fall back to `go run` at query time, so the plugin's runtime does not
depend on a Go toolchain and does not pay a cold-compile cost inside the
validation timeout. If the binary is missing, `validate_bydbql` returns an
actionable error asking you to run `npm run build:validator` rather than silently
recompiling.

## `.mcp.json` working directory

```json
{
  "mcpServers": {
    "banyandb": {
      "cwd": ".",
      "command": "node",
      "args": ["./mcp/dist/index.js"],
      "env": { "TRANSPORT": "stdio", "BANYANDB_ADDRESS": "localhost:17900" }
    }
  }
}
```

`cwd: "."` and `args: ["./mcp/dist/index.js"]` are resolved relative to the plugin
root (the repository root when developing from a checkout). Load the plugin from
the repository root, or adjust these relative paths to match your install
location if the plugin loader resolves them differently.

## Keeping the manifests in sync

`.claude-plugin/plugin.json` and `.codex-plugin/plugin.json` are intentionally
identical except for the `version` field — the Codex manifest carries a
`+codex.<timestamp>` build-metadata suffix. When you change one manifest
(description, keywords, `interface`, `skills`, `mcpServers`, etc.), mirror the
exact change into the other and keep the `version` values as the only difference.
