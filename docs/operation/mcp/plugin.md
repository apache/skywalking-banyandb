# BydbQL Plugin Packaging

The BanyanDB MCP server is also packaged as a Claude/Codex plugin so a host model
can generate, validate, and run read-only BydbQL. This page describes the plugin
layout, the build it requires before it can load, and how the manifests are kept
in sync.

## Layout

| Path | Purpose |
|------|---------|
| `.claude-plugin/plugin.json` | Claude Code plugin manifest. |
| `.claude-plugin/marketplace.json` | Claude Code plugin marketplace catalog. |
| `.codex-plugin/plugin.json` | Codex plugin manifest. |
| `.agents/plugins/marketplace.json` | Codex plugin marketplace catalog. |
| `.mcp.json` | MCP server definition referenced by both manifests. |
| `skills/bydbql/` | The `bydbql` skill and its `references/`. |
| `mcp/` | The MCP server source, the TypeScript build output (`dist/`), and the `bydbql-parse` validator tool. |

## Install from a GitHub marketplace

The repository includes both a Codex marketplace at `.agents/plugins/marketplace.json`
and a Claude Code marketplace at `.claude-plugin/marketplace.json`.
Both can install the plugin directly from GitHub; users do not need to
clone the repository just to make the plugin appear in the plugin directory.

### Claude Code

```bash
# Direct install (no marketplace needed):
/plugin install apache/skywalking-banyandb

# Or add the marketplace first, then install from it:
/plugin marketplace add apache/skywalking-banyandb
/plugin install banyandb-bydbql@banyandb
```

### Codex

```bash
codex plugin marketplace add apache/skywalking-banyandb --ref main
codex plugin add banyandb-bydbql@banyandb
```

For a fork or development branch, replace the repository and ref:

```bash
# Claude Code
/plugin marketplace add JophieQu/skywalking-banyandb --ref <branch>
/plugin install banyandb-bydbql@banyandb

# Codex
codex plugin marketplace add JophieQu/skywalking-banyandb --ref <branch>
codex plugin add banyandb-bydbql@banyandb
```

Both marketplace entries point at `./` because the plugin manifest, MCP
configuration, skills, and MCP source live at the repository root.

This remote install flow publishes the plugin to Claude Code and Codex. The
current MCP runtime is source-based, so starting the MCP tools still requires
the build artifacts described below to exist in the installed plugin copy.

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

Both Claude Code and Codex resolve the plugin root to the repository root
(whether installed via marketplace or direct GitHub install). The MCP loader
resolves `cwd: "."` relative to that installed plugin root, then resolves
`./mcp/dist/index.js` from the same directory. This means the checked-out layout
and installed layout are expected to match: `.mcp.json`, `mcp/dist/`,
`mcp/tools/bin/`, and `skills/` all live under the plugin root. If you manually
copy only part of the repository, keep that layout or update the relative paths
in `.mcp.json`.

## Keeping the manifests in sync

`.claude-plugin/plugin.json` and `.codex-plugin/plugin.json` are intentionally
identical except for the `version` field — the Codex manifest carries a
`+codex.<timestamp>` build-metadata suffix. `make -C mcp test` runs
`npm run check:plugin-manifests`, which fails if any non-version field drifts or
if the Codex version does not use the Claude version as its base. When you change
one manifest (description, keywords, `interface`, `skills`, `mcpServers`, etc.),
mirror the exact change into the other and run that check.

The marketplace catalogs (`.claude-plugin/marketplace.json` and
`.agents/plugins/marketplace.json`) serve the same plugin under their respective
platform formats and do not need to be field-identical.
