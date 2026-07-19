# BYDBQL Agent TUI

`bydbctl agent` is a three-page terminal workspace where an ACP agent holds a multi-turn BanyanDB conversation, discovers schemas, proposes typed query
plans, and safely runs approved queries.

bydbctl does not select or install an ACP provider. Pass the ACP-compatible stdio command explicitly with `--acp-command` and repeat `--acp-arg` for
each argument. Authentication is handled by that provider, so bydbctl does not require an API key.

## Start

```shell
bydbctl agent \
  --acp-command npx \
  --acp-arg -y \
  --acp-arg @agentclientprotocol/claude-agent-acp \
  --addr http://localhost:17913 \
  --goal "top slow payment endpoints in the last 30 minutes"
```

For a custom ACP provider:

```shell
bydbctl agent \
  --acp-command npx \
  --acp-arg -y \
  --acp-arg @agentclientprotocol/claude-agent-acp \
  --addr https://banyandb.example:17913 \
  --enable-tls \
  --cert /path/to/ca.pem
```

The Agent TUI uses the same `--addr`, username/password, TLS certificate, and `--insecure` semantics as the normal bydbctl HTTP commands. The external provider never receives those settings or BanyanDB credentials.

## Controlled tools and safety

Each TUI session creates a private, local MCP bridge. It exposes exactly these tools:

- `list_groups_schemas`
- `describe_schema`
- `propose_query_plan`
- `validate_bydbql`
- `probe_bydbql`
- `execute_bydbql`

The Agent starts with no selected schema. It ranks catalog candidates but resolves resources against the complete discovered catalog using exact type, name, and group identity. It never silently substitutes a similar resource or another time granularity. Typed schemas are cached per resource and group set, so a workflow can compile several resources independently. If the best choices remain ambiguous, it asks one focused clarification question. The Schema page is read-only and cannot pin a resource.

`propose_query_plan` accepts a strict JSON plan or a bounded workflow. The bridge loads the exact schema when needed, binds the compiled query to a schema fingerprint, and returns path-based diagnostics with allowed values when compilation fails. The planner supports typed projections, tag/entity comparison and `IN` filters with `AND`/`OR`, exact sortable index-rule ordering, numeric Measure aggregation/grouping, empty Trace projection, and registered TopN aggregations. A normal Measure is never treated as a TopN aggregation. Failed proposals remain visible diagnostics but are not executable candidates.

The planner rejects unknown JSON fields, implicit value coercion, field filters, tag aggregation, invalid time formats, out-of-range limits, `MATCH`, `HAVING`, `OFFSET`, `STAGES`, `WITH QUERY_TRACE`, joins, and unknown columns rather than guessing. `validate_bydbql` remains a parse/safety and manual-editor check; only a successful `propose_query_plan` can publish a provider candidate. The bridge rejects every other tool, shell command, external MCP server, dynamic registration, and download.

The legacy `--mcp-config` option is retained only to produce an explicit error; external MCP injection is not supported. The old `codex-exec` provider is removed. The deterministic `fake` provider is test-only and is not a CLI provider. A normal answer or clarification may complete without a candidate, but raw BYDBQL in provider text is rejected; only the controlled plan tool can publish a query candidate.

## Execution approval

Execution policy is configurable in the Query tab with `Ctrl+P`:

| Policy | Behavior |
| --- | --- |
| `ask every time` | Every probe and execution requires one-time approval |
| `auto probe` | Bounded read-only probes auto-approve; every full execution requires one-time approval |
| `trust session` | Read-only probes and executions auto-approve for the session, including manual `Ctrl+E` |

No data access runs merely because the agent generated a candidate unless the active policy allows it. `execute_bydbql`, `probe_bydbql`, and the manual `Ctrl+E` path can create an approval card containing the exact BYDBQL statement, resource, groups, time range, and limit. Mutation statements are rejected before approval under every policy.

- `y` approves that exact statement for one request.
- `n` rejects it.
- `e` rejects it, stops the active turn, and copies the statement into the editor for revision.

Any changed statement requires a new approval. The card also shows the effective query timeout and the fixed 50-row local preview bound. Immediately after approval, bydbctl validates the exact statement again; failed revalidation prevents execution. A failed execution never retries automatically, but its sanitized feedback can produce a new, separately approved plan. `Esc` or `Ctrl+C` stops an active agent/query operation, rejects pending approvals, and retains the activity and candidate history.

The local semantic checks require a `TIME` clause for time-series queries and a `LIMIT` for `SELECT` queries. These checks complement BanyanDB execution and do not grant the provider permission to access data.

## Pages and controls

| Page | Key | Purpose |
| --- | --- | --- |
| Schema | F1 | Review catalog candidates, inspected schemas, typed columns, and the Agent's selected evidence |
| Query / Agent | F2 | Conversation-first workspace: sent messages appear immediately; the QL editor remains a versioned artifact |
| Run / Activity | F3 | Structured result preview and live plan/tool/approval/execution activity |

Tab navigation works globally, including while typing in Query inputs:

| Shortcut | Action |
| --- | --- |
| `F1` / `F2` / `F3` | Jump directly to Schema / Query / Run |
| `[` / `]` | Previous / next tab (works even while typing) |
| `Ctrl+]` | Next tab (works even while typing) |

On the Query page:

| Shortcut | Action |
| --- | --- |
| `Ctrl+A` | Send the current message to the ACP agent |
| `Ctrl+V` | Validate the current editor content immediately |
| `Ctrl+E` | Request execution approval for the current valid content |
| `Ctrl+P` | Cycle execution policy (`ask every time` → `auto probe` → `trust session`) |
| `Ctrl+R` | Toggle visible agent reasoning stream in Activity / Conversation |
| `Ctrl+←` / `Ctrl+→` | Select a previous or next BYDBQL candidate version |
| `Tab` / `Shift+Tab` | Change focus |
| `Esc` / `Ctrl+C` | Stop active work; quit when idle |

The Query page places the conversation and multi-line composer on the left. Execution policy and reasoning visibility are compact status values above the visible start/end time controls; the old autonomous-discovery card is intentionally absent. The right side keeps the current BYDBQL candidate, version history, validation, probe summary, and approval state.

Editing the query creates a manual candidate. The editor performs a short debounced local validation but never invokes the agent or runs a query automatically. Agent and manual candidates are versioned independently; a later agent turn starts from the selected version. A conversational answer or clarification can complete without changing the current QL candidate.

## Results and data sharing

The Run page shows resource type, duration, row count, and a bounded structured table preview. The raw HTTP response remains available only in the current process as a detail view and is not written to the normal session log.

When a user asks a later question, or when a workflow advances to a dependent planned query, bydbctl supplies the provider the current statement, result type, row count, duration, column summary, sanitized error, and up to 50 preview rows. Preview values are explicitly treated as untrusted data. Persistent ACP sessions retain their own conversation history, so bydbctl does not inject duplicate turns; stateless providers receive only a bounded recent window. A multi-resource goal is represented as multiple independently compiled and approved queries; BanyanDB joins are never fabricated.

## Activity log and persistence

The activity log shows user-visible plans, tool lifecycle states, approval decisions, validation, cancellation, execution summaries, and optional agent reasoning when `Ctrl+R` is enabled. Tool call details include summarized arguments and outputs.

Session logs are stored in `$HOME/.bydbctl/logs` by default (override with `--log-dir`) with owner-only file permissions. They contain audit summaries: user actions, candidate statements, tool/approval summaries, durations, row counts, and errors. Raw result rows and long provider responses stay in memory and are not persisted. Sessions end when the TUI exits; cross-process recovery is not implemented.

## Troubleshooting

- If ACP cannot start, check the selected ACP command and its local login/runtime prerequisites.
- If the BanyanDB connection fails, check the error banner, `--addr`, authentication, and TLS settings.
- If schema discovery fails, verify the normal bydbctl address, authentication, TLS, certificate, and server permissions.
- If no candidate appears, inspect the Activity page. The provider may have answered a question or requested clarification. To publish QL, it must call `propose_query_plan`; a BYDBQL statement embedded in chat text is intentionally ignored.
- If an approval fails after `y`, review the local revalidation error, update the query, and request approval again.
