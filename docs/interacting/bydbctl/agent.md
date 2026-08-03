# BYDBQL Agent TUI

`bydbctl agent` is a two-pane terminal workspace where Codex or Claude holds a multi-turn BanyanDB conversation, discovers schemas, proposes typed query
plans, previews results, and safely runs approved queries.

Install Codex CLI 0.144.5 or newer and log in before starting the TUI:

```shell
codex login
```

Codex owns its login credentials. bydbctl neither reads nor copies them.

To use Claude instead, install Claude Code and authenticate it using either its normal login flow or `ANTHROPIC_API_KEY`:

```shell
claude auth login
bydbctl agent --provider claude
```

Claude Code owns its login credentials. bydbctl starts the CLI directly; it does not call the Anthropic Messages API or embed the TypeScript/Python Claude Agent SDK.

## Start

```shell
bydbctl agent \
  --addr http://localhost:17913 \
  --goal "top slow payment endpoints in the last 30 minutes"
```

To use a Codex binary outside `PATH`:

```shell
bydbctl agent \
  --codex-command /path/to/codex \
  --addr https://banyandb.example:17913 \
  --enable-tls \
  --cert /path/to/ca.pem
```

To select Claude Code or use a binary outside `PATH`:

```shell
bydbctl agent \
  --provider claude \
  --claude-command /path/to/claude \
  --claude-model sonnet \
  --claude-max-turns 12 \
  --addr https://banyandb.example:17913
```

`--claude-api-key` and `--claude-base-url` optionally override `ANTHROPIC_API_KEY` and `ANTHROPIC_BASE_URL` for the child process.
When omitted, Claude Code uses its normal authentication and provider configuration.

The Agent TUI uses the same `--addr`, username/password, TLS certificate, and `--insecure` semantics as the normal bydbctl HTTP commands. Codex never
receives those settings or BanyanDB credentials.

## Controlled tools and safety

Each TUI session creates a private, local MCP bridge. It exposes exactly these tools:

- `list_groups_schemas`
- `describe_schema`
- `propose_query_plan`
- `validate_bydbql`
- `probe_bydbql`
- `execute_bydbql`

The Agent starts with no selected schema. It ranks catalog candidates but resolves resources against the complete discovered catalog using exact type,
name, and group identity. It never silently substitutes a similar resource or another time granularity. Typed schemas are cached per resource and group set,
so a workflow can compile several resources independently. If the best choices remain ambiguous, it asks one focused clarification question. The Schema
panel is read-only and cannot pin a resource.

`propose_query_plan` accepts a strict JSON plan or a bounded workflow. The bridge loads the exact schema when needed, binds the compiled query to a
schema fingerprint, and returns path-based diagnostics with allowed values when compilation fails. The planner supports typed projections, tag/entity
comparison and `IN` filters with `AND`/`OR`, exact sortable index-rule ordering, numeric Measure aggregation/grouping, empty Trace projection, and
registered TopN aggregations. A normal Measure is never treated as a TopN aggregation. Failed proposals remain visible diagnostics but are not
executable candidates.

The planner rejects unknown JSON fields, implicit value coercion, field filters, tag aggregation, invalid time formats, out-of-range limits, `MATCH`,
`HAVING`, `OFFSET`, `STAGES`, `WITH QUERY_TRACE`, joins, and unknown columns rather than guessing. `validate_bydbql` remains a parse/safety and
manual-editor check; only a successful `propose_query_plan` can publish a provider candidate. The bridge rejects every other tool, shell command,
external MCP server, dynamic registration, and download.

When `propose_query_plan` returns `valid=false`, the provider receives the structured diagnostic and repairs the plan within the same agent turn.
The bridge allows at most three proposal attempts per schema-description cycle and reports the exhausted repair budget instead of looping indefinitely.

For Codex, bydbctl starts one isolated `codex app-server --stdio` process with an ephemeral in-memory thread, read-only sandboxing, and no approval requests. Built-in
shell, web, app, plugin, hook, sub-agent, goal, memory, and shell-snapshot features are disabled. Existing user MCP servers are disabled for this process.
Startup fails unless runtime inventory contains exactly the six controlled tools and no uncontrolled tools or resources.

For Claude, bydbctl starts `claude --print --output-format stream-json` directly from Go. Each TUI turn gets a supervised CLI process; later turns use
Claude's provider session ID with `--resume`. The CLI reuses the user's authentication and provider settings, including an existing DPSK configuration,
while bydbctl runs it in an isolated temporary working directory so project-level settings are not loaded. Built-in tools are empty, slash commands,
Chrome, and prompt suggestions are disabled, permission mode is `dontAsk`, and `--strict-mcp-config` injects only the private bridge. Every turn rejects
unexpected tools, extra MCP servers, failed MCP startup, or a connected bridge that does not report exactly the six qualified tools. Claude Code can
initially report the sole bridge as `pending` with an empty tool list while its handshake completes; this state is safe because no tool is then available
to the model. Claude Code persists the provider conversation in its own local session store so a later process can resume it; bydbctl does not read that
store.

The CLI does not accept arbitrary Codex arguments or external MCP configuration. The deterministic `fake` provider is test-only and is not a CLI provider.
A normal answer or clarification may complete without a candidate, but raw BYDBQL in provider text is rejected; only the controlled plan tool can publish
a query candidate.

## Execution approval

Execution policy is configurable anywhere in the workspace with `Ctrl+P`:

| Policy | Behavior |
| --- | --- |
| `ask every time` | Every probe and execution requires one-time approval |
| `auto probe` | Bounded read-only probes auto-approve; every full execution requires one-time approval |
| `trust session` | Read-only probes and executions auto-approve for the session, including manual `Ctrl+E` |

No data access runs merely because the agent generated a candidate unless the active policy allows it. `execute_bydbql`, `probe_bydbql`, manual
`Ctrl+Y` preview refreshes, and manual `Ctrl+E` full executions can create an approval card containing the exact BYDBQL statement, resource, groups,
time range, and limit. Mutation statements are rejected before approval under every policy.

- `y` approves that exact statement for one request.
- `n` rejects it.
- `e` rejects it, stops the active turn, and copies the statement into the editor for revision.

Any changed statement requires a new approval. The card also shows the effective query timeout and the fixed 50-row local preview bound. Immediately after
approval, bydbctl validates the exact statement again; failed revalidation prevents execution. A failed execution never retries automatically, but its
sanitized feedback can produce a new, separately approved plan. `Esc` or `Ctrl+C` interrupts only the active provider turn, rejects pending approvals,
and retains the agent session ID, activity, and candidate history. Exiting the TUI closes the active provider process and private MCP bridge. An
unexpected provider exit fails closed and is not silently retried.

The local semantic checks require a `TIME` clause for time-series queries and a `LIMIT` for `SELECT` queries. These checks complement BanyanDB execution
and do not grant the provider permission to access data.

## Workspace and controls

The left pane contains the conversation, candidate editor, `@` schema picker, and message composer. The right pane automatically shows Schema after a
successful `describe_schema` result; after a plan, probe, or execution it shows Data Preview. The schema picker is local: type `@`, use `↑`/`↓` to
inspect an entry, press `Enter` once to insert `@group/name`, then press `Enter` again to send the message.

When the catalog first loads, the empty conversation shows available groups and example questions based on a real local resource. This is only guidance;
it does not select or query data.

| Shortcut | Action |
| --- | --- |
| `Enter` | Send the current message to the selected agent, or insert the selected `@` reference when the schema picker is open |
| `Ctrl+G` | Ask the Agent to repair the current invalid candidate using its validation error |
| `Ctrl+Y` | Refresh the bounded preview for the current valid read-only candidate; this follows the active approval policy |
| `Ctrl+E` | Request a full execution of the current valid candidate; this is distinct from preview refresh |
| `Ctrl+P` | Cycle execution policy (`ask every time` → `auto probe` → `trust session`) |
| `Ctrl+R` | Show or hide live output emitted by the provider while a turn is in progress |
| `Ctrl+←` / `Ctrl+→` | Select a previous or next BYDBQL candidate version |
| `Ctrl+F` | Focus Data Preview; then use `←` / `→` to horizontally scroll wide cells and `↑` / `↓` to select a row |
| `Ctrl+O` | Export the visible preview or full execution result to a local file |
| `Ctrl+J` | Show or hide the full raw response after a full execution |
| `Tab` / `Shift+Tab` | Change focus between workspace controls |
| `Esc` / `Ctrl+C` | Stop active work; quit when idle |

The conversation includes a compact aggregated step line such as `✓ catalog · ✓ describe schema · ⟳ compile plan`. It shows only the controlled-tool
stages observed in the current or most recent turn, marked as completed, active, or failed. This is workflow progress, not private model reasoning.

Editing the query creates a manual candidate. The editor performs a short debounced local validation but never invokes the agent or runs a query
automatically. An invalid editor shows `[Ctrl+G let Agent fix]`; Agent and manual candidates are versioned independently. A conversational answer or
clarification can complete without changing the current QL candidate.

## Results and data sharing

Data Preview shows resource type, row count, and a bounded structured table preview. The raw HTTP response remains available only in the current process
as a detail view after a full execution and is not written to the normal session log.

When a user asks a later question, or when a workflow advances to a dependent planned query, bydbctl supplies the provider the current statement, result type, row
count, duration, column summary, sanitized error, and up to 50 preview rows. Preview values are explicitly treated as untrusted data. Stable BYDBQL rules
are installed as trusted system instructions; each turn contains only its task and structured, explicitly untrusted JSON. The same provider session retains
conversation history, so bydbctl does not inject duplicate turns. A multi-resource goal is represented as multiple independently
compiled and approved queries; BanyanDB joins are never fabricated.

## Activity log and persistence

The activity log shows user-visible plans, tool lifecycle states, approval decisions, validation, cancellation, and execution summaries. While `Ctrl+R`
is enabled, the conversation also renders the provider's live output. Tool call details include summarized arguments and outputs.

Session logs are stored in `$HOME/.bydbctl/logs` by default (override with `--log-dir`) with owner-only file permissions. They contain audit summaries:
user actions, candidate statements, tool/approval summaries, durations, row counts, and errors. Raw result rows and long provider responses stay in memory
and are not persisted. The bydbctl session ends when the TUI exits; cross-process recovery is not implemented. Claude Code may retain its own resumable
provider transcript in its normal local session store.

## Troubleshooting

- If Codex cannot start, run `codex --version` (0.144.5 or newer is required) and `codex login`. If needed, pass `--codex-command /path/to/codex`.
- If Claude cannot start, run `claude --version` and `claude auth status`. If needed, pass `--claude-command /path/to/claude`; API users can also check
  `ANTHROPIC_API_KEY` and `ANTHROPIC_BASE_URL`.
- If the BanyanDB connection fails, check the error banner, `--addr`, authentication, and TLS settings.
- If schema discovery fails, verify the normal bydbctl address, authentication, TLS, certificate, and server permissions.
- If no candidate appears, inspect the conversation and its aggregated step line. The provider may have answered a question or requested clarification.
  To publish QL, it must call `propose_query_plan`; a BYDBQL statement embedded in chat text is intentionally ignored.
- If an approval fails after `y`, review the local revalidation error, update the query, and request approval again.
