# BYDBQL Agent TUI

`bydbctl agent` is a two-pane terminal workspace where Codex or Claude holds a multi-turn BanyanDB conversation, discovers schemas, proposes typed query
plans, and safely runs read-only queries.

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
- `execute_bydbql`

A schema, capability, or usage question is answered from `list_groups_schemas` and `describe_schema` alone. Those turns are classified as
answer turns, and the provider is explicitly instructed not to compile a plan or read stored rows. Asking what fields a resource has therefore
inspects the schema instead of querying data. A request for stored data follows the full query workflow below.

When such a question names exactly one resource, bydbctl reads that schema itself and never starts a provider turn at all; see
[Direct schema lookups](#direct-schema-lookups). The reachable data is identical either way, because both paths call the same read-only schema API.

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
Startup fails unless runtime inventory contains exactly the five controlled tools and no uncontrolled tools or resources.

For Claude, bydbctl starts `claude --print --output-format stream-json` directly from Go. Each TUI turn gets a supervised CLI process; later turns use
Claude's provider session ID with `--resume`. The CLI reuses the user's authentication and provider settings, including an existing DPSK configuration,
while bydbctl runs it in an isolated temporary working directory so project-level settings are not loaded. Built-in tools are empty, slash commands,
Chrome, and prompt suggestions are disabled, permission mode is `dontAsk`, and `--strict-mcp-config` injects only the private bridge. Every turn rejects
unexpected tools, extra MCP servers, failed MCP startup, or a connected bridge that does not report exactly the five qualified tools. Claude Code can
initially report the sole bridge as `pending` with an empty tool list while its handshake completes; this state is safe because no tool is then available
to the model. Claude Code persists the provider conversation in its own local session store so a later process can resume it; bydbctl does not read that
store.

The CLI does not accept arbitrary Codex arguments or external MCP configuration. The deterministic `fake` provider is test-only and is not a CLI provider.
A normal answer or clarification may complete without a candidate, but raw BYDBQL in provider text is rejected; only the controlled plan tool can publish
a query candidate.

## Execution and safety

There is no approval policy and no execution preview step. A query runs when it is asked for: `Ctrl+E` executes the current candidate immediately,
and `execute_bydbql` runs the compiled statement as soon as the provider calls it. Compiling a plan never reads data on its own, so a candidate can
appear in the editor without touching the cluster.

Only read-only statements can execute. Exactly one `SELECT` or `SHOW TOP` is accepted, and `CREATE`, `UPDATE`, `DELETE`, `DROP`, `APPLY`, `INSERT`,
and `ALTER` are rejected before reaching BanyanDB. bydbctl validates the exact statement again immediately before running it; failed revalidation
prevents execution. A failed execution never retries automatically, but its sanitized feedback can produce a new plan.

While a run is in progress the workspace shows a `■ Stop` control. `Esc` or `Ctrl+C` stops the active provider turn and cancels the in-flight query,
retaining the agent session ID, activity, and candidate history. When nothing is running, `Esc` or `Ctrl+C` asks for confirmation before quitting:
`y` exits, and `n` or any other key keeps working. Exiting the TUI closes the active provider process and private MCP bridge. An unexpected provider
exit fails closed and is not silently retried.

The local semantic checks require a `TIME` clause for time-series queries and a `LIMIT` for `SELECT` queries. These checks complement BanyanDB execution
and do not grant the provider permission to access data.

`TRACE` queries carry one further requirement: BanyanDB plans a trace scan from either an `ORDER BY` on a sortable index rule or an equality filter on the
trace ID tag, and rejects a query with neither as an internal error, which would otherwise reach the user as an opaque HTTP 500. Both the plan compiler and
the local validator refuse an unbounded trace scan first, so the agent receives a `TRACE_SCAN_UNBOUNDED` diagnostic it can repair, and a hand-written query
is marked invalid with the two clauses that would fix it. `describe_schema` reports the tag names as `trace_id_tag` and `timestamp_tag`, and repeats the
rule under `plan_constraints.trace_scan_requirement`.

## Workspace and controls

The left pane contains the conversation, candidate editor, `@` schema picker, and message composer. The right pane automatically shows Schema after a
successful `describe_schema` result or a direct schema lookup; after an execution it shows Data Preview. It appears only once a turn has produced schema
or result rows, so a turn answered in words leaves the conversation at full width. The schema picker is local: type `@`, use `↑`/`↓` to inspect an entry, press `Enter` once to
insert `@group/name`, then press `Enter` again to send the message, or `Esc` to close the picker.

The conversation is the primary panel and keeps the largest share of the height; the candidate editor is sized to the query it holds and gives its spare
rows back to the conversation. Both evidence panels scroll independently with `pgup` / `pgdn`.

The panel that currently receives input is marked three ways: its border is highlighted, its title is prefixed with `▸` and rendered bold, and the status
line names it as `Focus: <panel>`. None of these markers changes a panel's height, so moving focus never shifts the layout. Focus moves with
`Tab` / `Shift+Tab`, by jumping directly with `Alt+1`–`Alt+4` (or a bare `1`–`4` outside a text editor), or by clicking a panel. The mouse wheel scrolls
the focused panel.

Press `?` for the full keyboard reference. The footer shows only the bindings that apply to the focused panel plus the global ones, so the keys on screen
are always the useful ones.

When the catalog first loads, the empty conversation shows available groups and example questions based on a real local resource. This is only guidance;
it does not select or query data.

| Shortcut | Action |
| --- | --- |
| `Enter` | Send the current message to the selected agent, or insert the selected `@` reference when the schema picker is open |
| `@` | Search the local schema catalog from the composer |
| `/` | Focus the composer on a fresh catalog search |
| `Ctrl+G` | Ask the Agent to repair the current invalid candidate using its validation error |
| `Ctrl+E` | Run the current valid candidate, including any local edit, immediately |
| `Ctrl+←` / `Ctrl+→` | Step back to a previous or next recorded BYDBQL query |
| `Ctrl+O` | Export the current execution result to a local file |
| `Ctrl+L` | Reload the BanyanDB schema catalog |
| `Tab` / `Shift+Tab` | Change focus between workspace panels |
| `Alt+1`–`Alt+4` | Jump to the conversation, candidate QL, composer, or Data Preview |
| `↑` / `↓`, `j` / `k` | Move the selection in the focused list |
| `↑` / `↓` in the composer | Recall an earlier sent message, past the first or last line |
| `pgup` / `pgdn` | Scroll the detail of the focused panel |
| `?` | Toggle the keyboard reference |
| Mouse click | Focus the clicked panel |
| Mouse wheel | Scroll the focused panel |
| `Esc` / `Ctrl+C` | Close an open overlay; then stop the active run; when idle, ask for confirmation before quitting |

Vim-style `j` / `k` and the bare digit shortcuts only apply when no text editor holds focus, so they never replace a literal character in the composer or
the candidate editor. `Alt`-prefixed jumps work from anywhere.

### Composer history

The composer keeps a recall list of the messages it has sent, like a shell prompt. `↑` moves the cursor within a multi-line draft first, and recalls the
previous message once the cursor is already on the first line; `↓` steps back towards the newest message and then restores the draft that was set aside.
The candidate editor has no recall list of its own: its arrow keys stay with the text, and `Ctrl+←` / `Ctrl+→` step through the queries the session
recorded.

### Answers, clarifications, and queries

Not every turn produces a query. A schema, capability, or usage question is answered in words, and a request that is too vague to compile comes back as
one clarifying question. Both are shown in the conversation with a suffix naming what to do next: `answered, no query` needs nothing, and
`needs your reply` waits on the composer. On those turns the candidate card collapses to a single row explaining why the editor is empty, the validation
field disappears from the status line, and the phase reads `conversation` or `clarifying`. Press `Alt+2` to expand the card and write a query by hand at
any point.

### Direct schema lookups

Asking for the shape of one named resource — `what fields does @sw_trace/segment have?`, `describe service_cpm`, `segment 有哪些字段` — is answered by
bydbctl itself. The question is served from the same BanyanDB schema call the agent would have made, so it needs no provider round trip and produces no
BYDBQL. Only an unambiguous target qualifies: the resource is taken from an `@group/name` reference, or from a name that matches exactly one catalog
entry. A name shared by several groups, an unnamed question, a catalog question such as `which resources can I use to inspect errors?`, and any turn that
also asks for stored rows all stay with the agent, which can rank candidates and ask which one is meant.

Because a described schema and an executed query both come back as columns, a lookup says where its columns came from. The conversation credits
`Schema ›` rather than the agent and carries the suffix `schema catalog · no query run`, the Schema panel title reads `read from the catalog · no query
run`, the candidate card explains that the turn was `schema lookup only`, and the phase reads `schema`. No candidate is published, so there is nothing to
validate or execute; ask for data in the same composer to get a query.

The answer appears in both columns: the conversation entry holds the full description as its detail, and the Schema panel shows the same resource beside
it. On a terminal too narrow for two columns the panel is off screen, and the hint under the left column names the resource waiting there — `4 open
schema segment` — so `Alt+4` opens it and `1` returns to the conversation.

The described schema stays in the panel while you read it and compose the next question. It is replaced when something else claims the slot: opening an
`@` search previews that resource instead, and running a query shows its result. Focusing the panel opens the schema rather than replacing it, since that
is how a long description is scrolled. This differs from an `@` search preview, which is retracted as soon as the search closes.

### Markdown rendering

Agent replies and schema descriptions are rendered as markdown with Glamour, in the workspace palette: headings, bullet and numbered lists, bold and
inline code, block quotes, and tables all render as themselves rather than as literal markup. A described schema uses a table of typed columns, their
kinds, and whether each is indexed. Every rendered line is measured and re-wrapped to the panel width, so CJK text, which offers no spaces to break on,
wraps instead of overflowing. Tool arguments and results stay in their existing labelled key-and-JSON layout, since they are structured data rather than
prose.

### Terminal size

Terminals at least 100 columns wide place the evidence panel beside the left column. Narrower terminals show one column at a time: the conversation owns
the screen, a footer hint points at the off-screen results, and `Alt+4` opens them full screen with `Alt+1` returning. Below 60×18 the workspace is
replaced by a message naming the required size, rather than a layout that cannot fit.

The conversation includes a compact aggregated step line such as `✓ catalog · ✓ describe schema · ⟳ compile plan`. It shows only the controlled-tool
stages observed in the current or most recent turn, marked as completed, active, or failed. This is workflow progress, not private model reasoning.

The generated QL is editable. Focus the candidate editor and type: the edit creates a manual candidate, and `Ctrl+E` runs exactly what is in the editor.
The editor performs a short debounced local validation but never invokes the agent or runs a query automatically, and it stays responsive while that
validation or an agent turn is in flight. A local edit is marked `edited locally` and is never overwritten by a background turn; it is released when the
message is sent, when `Ctrl+G` asks the Agent to repair it, or when an earlier query is loaded with `Ctrl+←` / `Ctrl+→`. An invalid editor shows
`[Ctrl+G let Agent fix]`. A conversational answer or clarification can complete without changing the current QL candidate.

## Results and data sharing

Data Preview shows resource type, row count, and a bounded structured table preview of the latest execution. Selecting a row with `↑`/`↓` shows that row
field by field below the table, including the columns the table drops to stay narrow; `←`/`→` scrolls the table horizontally. The raw HTTP response stays
in the current process and is not written to the normal session log.

Before the first execution, Data Preview names the next step instead of leaving the panel blank: it points at `Ctrl+E` once a candidate is valid, and at
the composer otherwise. An execution that matches no rows says so and suggests widening the time range.

When a user asks a later question, or when a workflow advances to a dependent planned query, bydbctl supplies the provider the current statement, result type, row
count, duration, column summary, sanitized error, and up to 50 preview rows. Preview values are explicitly treated as untrusted data. Stable BYDBQL rules
are installed as trusted system instructions; each turn contains only its task and structured, explicitly untrusted JSON. The same provider session retains
conversation history, so bydbctl does not inject duplicate turns. A multi-resource goal is represented as multiple independently
compiled queries; BanyanDB joins are never fabricated.

## Activity log and persistence

The activity log shows user-visible plans, tool lifecycle states, validation, cancellation, and execution summaries. The conversation also renders the
provider's live output as it streams. Tool call details include summarized arguments and outputs.

Session logs are stored in `$HOME/.bydbctl/logs` by default (override with `--log-dir`) with owner-only file permissions. They contain audit summaries:
user actions, candidate statements, tool summaries, durations, row counts, and errors. Raw result rows and long provider responses stay in memory
and are not persisted. The bydbctl session ends when the TUI exits; cross-process recovery is not implemented. Claude Code may retain its own resumable
provider transcript in its normal local session store.

The log path is reported twice: as a `session log:` activity entry when the TUI starts, and on standard error after the TUI exits.

These categories describe what is on screen, which is the part no screenshot of a transient panel can explain:

| Category | Contents |
|---|---|
| `action` | The routing decision for a sent message: a `describe` lookup with its resolved target, or an agent turn with its composer reference |
| `schema_snapshot`, `schema_answer` | The described resource: groups, load state, entity and indexed tags, and the typed column list |
| `chat` | One line per conversation entry, with its role, kind, and detail size |
| `view` | Which evidence panel owns the right-hand slot, plus the search, focus, and phase state that decided it |

A `view` line is written whenever the evidence slot changes owner, naming the Bubble Tea message that changed it. A panel that appears and then
disappears leaves two of these lines with different owners, which identifies the transition responsible without reproducing the run.

## Troubleshooting

- If Codex cannot start, run `codex --version` (0.144.5 or newer is required) and `codex login`. If needed, pass `--codex-command /path/to/codex`.
- If Claude cannot start, run `claude --version` and `claude auth status`. If needed, pass `--claude-command /path/to/claude`; API users can also check
  `ANTHROPIC_API_KEY` and `ANTHROPIC_BASE_URL`.
- If the BanyanDB connection fails, check the error banner, `--addr`, authentication, and TLS settings.
- If schema discovery fails, verify the normal bydbctl address, authentication, TLS, certificate, and server permissions.
- If no candidate appears, inspect the conversation and its aggregated step line. The provider may have answered a question or requested clarification.
  To publish QL, it must call `propose_query_plan`; a BYDBQL statement embedded in chat text is intentionally ignored.
- If an execution is refused, review the local revalidation error, edit the query, and run it again with `Ctrl+E`.
