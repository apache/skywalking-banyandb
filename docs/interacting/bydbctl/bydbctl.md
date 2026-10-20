# bydbctl

`bydbctl` is the command line tool for interacting with BanyanDB. It is a powerful tool that can be used to create, update, read, and delete
schemas. It can also be used to query data stored in streams, measures, traces, and properties.

`bydbctl agent` opens an interactive BYDBQL agent TUI. It helps users generate, edit, validate, approve, and execute BYDBQL queries. See
[BYDBQL Agent TUI](agent.md) for details.

These are several ways to install:

- Get binaries from [download](https://skywalking.apache.org/downloads/).
- Build from [sources](https://github.com/apache/skywalking-banyandb/tree/main/bydbctl) to get latest features.

The config file named `.bydbctl.yaml` will be created in `$HOME` folder after the first CRUD command is applied.

```shell
> more ~/.bydbctl.yaml
addr: http://127.0.0.1:17913
group: ""
```

`bydbctl` leverages HTTP endpoints to retrieve data instead of gRPC.

## HTTP client

Users could select any HTTP client to access the HTTP based endpoints. The default address is `localhost:17913/api`

## TLS

TLS is supported by `bydbctl`. `--enable-tls` and `--cert <cert_file>` are the flags to enable TLS and specify the certificate file. If you want to
ignore the certificate verification, use `--insecure=true`.

## Data export

`bydbctl data export` and `bydbctl data release-session` talk to a liaison **gRPC** endpoint, not to the HTTP `--addr`: `--nodes` takes
one or more liaison addresses and every address must include the liaison gRPC port (`host:17912` by default; no port is appended for you).
Every address is probed: a liaison that cannot be reached, or that has not discovered any data node yet, is skipped with a `WARN`, and
every usable one takes part (the first usable one carries the schema, `Plan` and session calls; the node streams of a real export are
dealt round-robin over all of them); a reachable endpoint that is not a liaison is an error, and a liaison that refuses the credentials
(`Unauthenticated`/`PermissionDenied`) stops the command with exit `2` without trying the others. A server without data export
(older than 0.12) is also exit `2`. Every setting resolves the same way: an explicit flag, else `plan.yaml`, else its `BYDBCTL_*`
environment variable, else the default (`BYDBCTL_NODES` is comma separated; selectors have no variable). The data commands neither read
nor create `$HOME/.bydbctl.yaml`: everything they need comes from flags, `plan.yaml` or the environment. The root `-a/--addr`, `-g/--group` and `--config` flags do not apply and
are rejected when given explicitly; a group set through the environment is ignored.

### Export

`bydbctl data export` exports the selected groups into a local directory, encoded in the artifact format `--format` selects (see
[Formats](#formats)): their schema, one file set per data node and a `manifest.json` written last. It pins a snapshot on every data node for the duration of the run (an export [session](#sessions)), so the
exported data is the point in time the session was created, while writes go on.

```shell
bydbctl data export --nodes liaison-1:17912 --output ./backup-20261010 --selector 'catalog=measure,groups=sw_metricsMinute'
bydbctl data export --plan plan.yaml --output ./backup-20261010 --node-rate-limit 50MiB/s --node-row-rate-limit 50000/s
bydbctl data export --plan plan.yaml --output ./schema-20261010 --schema-only
```

The command:

1. checks that `--output` does not exist or is empty (anything else is exit `1`; there is no resume yet);
2. connects to the liaisons, lists the groups and validates the selectors;
3. writes the schema of the selected groups to `schema/`; with `--schema-only` it writes `manifest.json` and stops here, without a
   session or any data. With `--include-schema=false` it writes no schema at all and skips the schema revision check below;
4. creates the export session and prints `export session <id> created` as soon as the id is known. If another session holds
   the cluster, it prints every occupant with its start time, last heartbeat, catalogs and nodes and exits `2`: check the last heartbeat
   (a running export sends one every 5 minutes) and give `--preempt` to take the session over. When stdin and stdout are both
   terminals and `--preempt` is not given, it asks `Take over the session(s) above? ... [y/N]` instead: `y` or `yes` takes them over
   as `--preempt` does, anything else (Enter included) leaves them as they are, prints the `release-session` command for each and
   exits `2`;
5. exports every node over its own stream, `--parallelism` nodes at a time, heartbeating the session every 5 minutes. Node `i`
   (nodes sorted by id) streams through liaison `i mod L`; a stream
   refused as unavailable, busy or not found moves on to the next liaison (see `--send-retry-timeout`). Each file is written as `<file>.tmp`, checked against the size, CRC32 and schema
   revision the node announces, then renamed;
6. writes `manifest.json`, releases the session and prints what was written — a `NODE CATALOG GROUP STAGE UNITS FILES ROWS SIZE TIME`
   table, `TIME` adding up the time the row's units took — followed by
   `export written to <dir>: <units> unit(s), <files> file(s), <size>, <n> warning(s) in <time>`.

The progress goes to stdout; warnings and the hints to retry or release go to stderr. Every finished file and unit, and every node
that exported all its units, gets a line with its time (`[i/n]` numbers the units of a node); a node that fails gets none, and the
error is printed when the run exits:

```text
  data-hot1:17912  measure/sw_metricsDay/seg-20261004/shard-0-00001.csv.gz  180 rows  4.3 KiB  0.3s
✓ data-hot1:17912  [3/7] measure/sw_metricsDay seg-20261004  1 file(s)  180 rows  4.3 KiB  0.4s
✓ data-hot1:17912  done: 7 unit(s), 8 file(s), 526 rows, 93.0 KiB in 3.3s
```

When stdout is a terminal, each exporting node also has a live line below them, redrawn in place as the data arrives: the unit, the
shard being written (its position among the unit's shards and the unit's part count, or `segment index` for the segment-level file
of an index-mode measure), the rows and bytes so far, the unit's average transfer rate and the unit's time. The live lines stay
below the terminal height; the nodes that do not fit are counted on one line. When stdout is not a terminal there is no live
line; the start of every unit is printed as a line of its own instead
(`  data-hot1:17912  [3/7] measure/sw_metricsDay seg-20261004 started: 1 shard(s), 2 part(s)`).

A node sends a file in chunks and the live line moves when a chunk arrives. A chunk holds up to 1 MiB of on-disk bytes, or one
second's worth of a lower `--node-rate-limit`. A node about to wait for `--node-row-rate-limit` first sends the rows it holds: right
away when the wait is 100 ms or longer, so at `1/s` the line moves by one row every second, and otherwise once they are a second old
(a gzip file then ends a deflate block early, which costs a little compression). So at any node rate the line moves at least about
every second. Every bucket starts full with a burst of one second's worth, a liaison's at most 1 MiB,
and a liaison below 1 MiB/s relays a chunk in pieces of one second's worth divided among the node streams it relays, so a low
`--liaison-rate-limit` also moves every node's line about every second; nodes that share one liaison share its rate.

Warnings (dropped rows, stored tag or field values the schema no longer declares or declares with another type, property timestamps
outside the int64 nanosecond range, tags a multi-tag index rule may hold, rows larger than
`--max-file-size`, a unit whose shards held parts but yielded no row, stages the nodes report but the schema does not declare, and the skipped schema of
`--include-schema=false`) are printed as `WARN` lines and kept in `manifest.warnings`; they do not fail the export. A normal export of a
healthy cluster has none. Once the session exists, any failure keeps it and prints
`bydbctl data release-session --id <id>`; the partial directory has no `manifest.json`, and must be emptied (or another
`--output` given) before the next run. A run refused before its session exists removes the `schema/` it wrote, so the same
command can be run again. Ctrl-C or `SIGTERM` stops the export the same
way: the node streams are canceled, the session is kept, and the same `bydbctl data release-session --id <id>` line is printed before
the run exits `3`. After a successful export, a release that fails (as a whole or on some node) is only a `WARN` (the artifact is already
complete and the run exits `0`), and the same `bydbctl data release-session --id <id>` line is printed so the session can be released
by hand instead of waiting for its lease to expire.

The artifact:

```text
<output>/
  manifest.json
  schema/{group,stream,measure,trace,property,indexRule,indexRuleBinding,topNAggregation}.jsonl
  nodes/<node-dir>/{stream,measure,trace}/<group>/seg-<suffix>/<data files>
  nodes/<node-dir>/property/<group>/<data files>
```

`<node-dir>` is derived from the node id (lowercase, runs of characters outside `[a-z0-9]` folded to `-`, leading and trailing `-`
trimmed); `manifest.source.nodes` maps it back, and an export whose nodes derive the same name is refused. Group names are escaped as path components (bytes outside `[A-Za-z0-9._-]`
become `%XX`). Every source node's copy of a replicated shard is exported into its own subtree. `manifest.json` lists every file with
its size and CRC32; the names and contents of the data files depend on the format.

### Formats

`--format` selects how the data files are encoded. Each format defines its file names, its layout and the options that only apply to
it; the session, the artifact directory, the manifest and the schema files are the same for every format.

#### CSV

`--format csv`, the default, writes one CSV file set per shard (and per segment for the index-mode measure rows):

```text
nodes/<node-dir>/stream/<group>/seg-<suffix>/shard-<N>-00001.csv.gz
nodes/<node-dir>/measure/<group>/seg-<suffix>/shard-<N>-00001.csv.gz
nodes/<node-dir>/measure/<group>/seg-<suffix>/segment-00001.csv.gz    # index-mode measure rows
nodes/<node-dir>/trace/<group>/seg-<suffix>/shard-<N>-00001.csv.gz
nodes/<node-dir>/property/<group>/shard-<N>-00001.csv.gz
```

The rows of the internal `_top_n_result` measure are not exported: a target recomputes them from its source measures. A measure
data point is exported once, in its latest version, as a query returns it. With `--compress none` the files end in `.csv`. A file rolls to `-00002`, `-00003`, ... once its on-disk size reaches `--max-file-size`,
and every file repeats the header. A row whose encoded size reaches `--max-file-size` gets a file of its own, and the unit warns once
with the number of such rows.

Each CSV file of a group shares one column layout: the model's fixed columns, then the union of the group's tag columns, then for
measures the `_field.*` columns. A tag or field that two resources declare with different types, or whose header would repeat another
column's (a fixed column, a `_field.*` column, or family `a` tag `b.c` against family `a.b` tag `c`), gets one `<resource>::<column>`
column per resource, so every header is unique.

| Model | Fixed columns | Other columns |
|-------|---------------|---------------|
| stream | `_name`, `_element_id`, `_timestamp` | `family.tag` |
| measure | `_name`, `_timestamp`, `_version` | `family.tag`, then `_field.<name>` (index-mode measures have no fields) |
| trace | `_name`, `trace_id`, `span_id`, `_span` | tag names, without the trace-id and span-id tags |
| property | `_name`, `_id`, `_mod_revision`, `_create_revision`, `_updated_at`, `_delete_time` | tag names; tombstones are exported with a non-zero `_delete_time` |

`_element_id` is the id the engine stores for an element, a uint64 derived from the id it was written with, written as the 16
hexadecimal digits a stream query returns as `element_id`.

Cell encoding: `\N` is null; a string starting with a backslash gets one extra leading backslash, and a string containing `\r` is written
as `\B` plus its base64; integers are decimal; floats use the shortest exact decimal form (`NaN`, `+Inf`, `-Inf` included); binary values
and `_span` are standard base64 with padding; string arrays are JSON arrays; int arrays are `;`-joined decimals (an empty array is an empty
cell); timestamps are Unix nanoseconds. `--csv-annotations` adds a `#datatype` line before each header naming every column's type
(`string`, `long`, `double`, `base64Binary`, `stringArray`, `longArray`, `timestamp`); it has one field more than the header.

#### Native

`--format native` is reserved for a format that carries the data nodes' own part files; it is not implemented yet (exit `1`). The
design, including the manifest fields, is in [Data export and import](../../design/0.12.0/data-export-import/README.md).

### Flags

Flags of `data export`:

| Flag | Default | Meaning |
|------|---------|---------|
| `--nodes` | `connection.nodes` in `plan.yaml`, else `BYDBCTL_NODES` | liaison gRPC addresses; every usable one shares the export streams |
| `--plan` | | `plan.yaml` with the connection, the scope and the export options |
| `--selector` | `export.selectors` in `plan.yaml` | scope, repeatable: `catalog=<stream\|measure\|trace\|property>[,groups=<g1>;<g2>]`; an empty scope means every group. Separators are `,`, `;` and `=` only |
| `--parallelism` | `export.parallelism`, else `BYDBCTL_PARALLELISM`, else `max` | number of data nodes exported concurrently: `max` or an integer >= 1, clamped by the node count |
| `--output` | `export.output`, else `BYDBCTL_OUTPUT` | export directory; required for a real export; must not exist or be empty |
| `--format` | `export.format`, else `BYDBCTL_FORMAT`, else `csv` | artifact format, see [Formats](#formats): `csv`; `native` is reserved and not implemented yet (exit `1`) |
| `--compress` | `export.compress`, else `BYDBCTL_COMPRESS`, else `gzip` | `none` or `gzip` |
| `--max-file-size` | `export.maxFileSize`, else `BYDBCTL_MAX_FILE_SIZE`, else `512MiB` | soft limit of one file's on-disk (compressed) size; a file rolls after the row that reaches it; `0` never rolls. Bare bytes or a unit (`KB`, `KiB`, `MB`, `MiB`, `GB`, `GiB`, ...) |
| `--csv-annotations` | `export.csvAnnotations`, else `BYDBCTL_CSV_ANNOTATIONS`, else `false` | CSV only: write a `#datatype` line before every CSV header |
| `--include-schema` | `export.includeSchema`, else `BYDBCTL_INCLUDE_SCHEMA`, else `true` | `false` exports no schema and skips the schema revision check; `groups[].indexMode` is then recorded as `false` with a warning. Cannot be combined with `--schema-only` |
| `--schema-only` | `export.schemaOnly`, else `BYDBCTL_SCHEMA_ONLY`, else `false` | write `schema/` and `manifest.json` only, without a session or data |
| `--preempt` | `BYDBCTL_PREEMPT`, else `false` | remove the export session the data nodes hold and take over; the export running on it fails on its next read |
| `--node-rate-limit` | `export.nodeRateLimit`, else `BYDBCTL_NODE_RATE_LIMIT`, else `0` | byte rate each data node may send (compressed bytes), enforced by the node; `0` = unlimited |
| `--node-row-rate-limit` | `export.nodeRowRateLimit`, else `BYDBCTL_NODE_ROW_RATE_LIMIT`, else `0` | CSV only: rows each data node may encode per period, enforced by the node; `0` = unlimited |
| `--liaison-rate-limit` | `connection.rateLimit`, else `BYDBCTL_LIAISON_RATE_LIMIT`, else `0` | bytes each liaison relays, enforced by the liaison before it forwards each chunk; one bucket per liaison process shared by all its export streams, the stream opened last setting the value; `0` = unlimited |
| `--node-idle-timeout` | `export.nodeIdleTimeout`, else `BYDBCTL_NODE_IDLE_TIMEOUT`, else `10m` | fail a node stream that delivers nothing for this long; `0` disables it |
| `--send-retry-timeout` | `export.sendRetryTimeout`, else `BYDBCTL_SEND_RETRY_TIMEOUT`, else `15m` | how long to keep reopening a node stream refused with `UNAVAILABLE`, `RESOURCE_EXHAUSTED` or `NOT_FOUND`, trying the next liaison each time (back-off from 1s, doubling to 30s) |
| `--dry-run` | `BYDBCTL_DRY_RUN`, else `false` | inventory only, see [Dry run](#dry-run); cannot be combined with `--output`, `--preempt` or `--schema-only` |
| `-o`, `--output-format` | `BYDBCTL_OUTPUT_FORMAT`, else `table` | dry-run rendering: `table`, `yaml` or `json` (stdout; progress and warnings go to stderr) |
| `--strict-coverage` | `BYDBCTL_STRICT_COVERAGE`, else `false` | dry run only: a data node that returned no inventory is an error (exit `2`, after the report is printed). A real export fails on any unreachable node anyway, so the flag is rejected without `--dry-run` |
| `--enable-tls`, `--insecure`, `--cert` | `connection.nodesTLS` in `plan.yaml`, else `BYDBCTL_ENABLE_TLS` / `BYDBCTL_INSECURE` / `BYDBCTL_CERT` | TLS towards the liaisons |
| `--logging-level`, `--logging-env` | `BYDBCTL_LOGGING_LEVEL` / `BYDBCTL_LOGGING_ENV`, else `info`, `prod` | client-side logging |

Rates take `<quantity>[/<period>]` with the period `s` (the default), `min` or `h`; `m` is refused as ambiguous. A byte rate takes bare
bytes or a size unit (`50MiB/s`, `3GiB/min`), a row rate a plain integer (`50000/s`, `3000000/min`). A negative rate, a byte unit on a
row rate, or a non-zero rate below one per second (or below one byte) is a usage error. Nodes enforce rates in whole units per second,
so a `/min` or `/h` row rate is rounded down to a whole number of rows per second and never runs above the configured one (`90/min` runs at 1 row per second). When any rate is set, the client prints them
once before the streams start.

The node streams of a real export have no overall deadline: their progress is bounded by `--node-idle-timeout`, the longest a node
stream may go without a single frame, however long the whole export takes. Connecting to the liaisons, listing the groups and checking the selectors run
under a 3-minute deadline, a dry run under 10 minutes and `release-session` under 3 minutes. Under any rate limit a node stream
delivers a frame about every second (see above), so only a node that stalls hits the idle timeout.

`plan.yaml` accepts only the keys below; any other key (`export.checkpoint` and the `import:` section included, which arrive with resume
and the import) is rejected so a typo never falls back to a default silently. Its selectors get the same checks as `--selector` (a group
name must not be empty or repeated), and `--selector` replaces them entirely. Explicit flags override the file, which overrides the
`BYDBCTL_*` environment. `--dry-run`, `--preempt`, `--strict-coverage` and `--output-format` are one-shot switches with no plan field.

```yaml
connection:
  nodes: ["liaison-1:17912", "liaison-2:17912"]
  nodesTLS:              # optional
    enable: true
    insecure: false
    cert: /path/to/ca.crt
  rateLimit: 800MiB/s    # --liaison-rate-limit; 0 = unlimited
export:
  output: ./backup-20261010
  format: csv            # csv; native is reserved
  compress: gzip         # none | gzip
  maxFileSize: 512MiB    # 0 = never roll
  includeSchema: true
  schemaOnly: false
  csvAnnotations: false  # CSV only
  parallelism: max       # or an integer >= 1
  nodeRateLimit: 50MiB/s # 0 = unlimited
  nodeRowRateLimit: 0    # CSV only: rows, e.g. 50000/s; 0 = unlimited
  nodeIdleTimeout: 10m
  sendRetryTimeout: 15m
  selectors:
    - catalog: stream
      groups: [sw_record]
    - catalog: trace     # no groups: every trace group
```

Exit codes of `data export` and `data release-session`:

| Code | Meaning |
|------|---------|
| `0` | success; release warnings do not change it |
| `1` | usage error: a bad flag or `plan.yaml`, an unknown selector group, a non-empty `--output`, `--format native` |
| `2` | preflight rejection, nothing was exported: no liaison reachable or one refused the credentials, a server without data export, another export session holds the cluster (without `--preempt`), or, in a dry run with `--strict-coverage`, a coverage gap (the report is still printed) |
| `3` | failure at run time: a node stream failed or stalled, a file failed its size/CRC32 check, the schema changed during the export, the session was lost (its heartbeat answered `FAILED_PRECONDITION`), or a release failed on some node (`release-session`) |

### Dry run

`bydbctl data export --dry-run` inventories what an export would carry, without creating snapshots or writing any file. It lists every
`(node, catalog, group)` with its segments, distinct shards, parts, estimated rows and sizes:

```shell
bydbctl data export --dry-run --nodes liaison-1:17912 --selector 'catalog=stream,groups=sw_record' --selector 'catalog=measure,groups=sw_metricsMinute;sw_metricsHour'
bydbctl data export --dry-run --plan plan.yaml -o json
```

It takes the connection, scope and parallelism flags above plus `-o` and `--strict-coverage`; it only reports the effective parallelism on
stderr. The other real-export options are not used, though they must still be valid; `export.output` and `export.schemaOnly: true` contradict a
dry run and are refused (exit `1`): leave them out of a plan used for dry runs.

#### Reading the report

The table output starts with summary lines, then one row per `(node, catalog, group)`, then the `SNAPSHOT` line:

| Line / column | Meaning |
|---------------|---------|
| `NODES` | the data nodes the liaison planned and whether all of them answered. A node listed as giving no inventory is a coverage gap: its data would not be exported |
| `MULTI-SRC` | units held by more than one node (replicated shards), keyed `catalog/group/stage/segment/shard-N`. Each source shows its node, rows and time range. All sources are exported and the importer decides which one to use. Property groups are not listed, nor are index-mode measure segments: they have no shard id, so the dry run cannot tell replicas from different shards |
| `NODE`, `CATALOG`, `GROUP` | the row's key |
| `STAGE` | the lifecycle stage the node resolves for the group from its labels; `hot` is the default tier (no stages declared, no node labels, or no matching stage). Property groups resolve it the same way |
| `SEGMENTS` | segments of the group on that node, including segments whose shards have not flushed anything yet |
| `SHARDS` | distinct shard ids across those segments (not a sum per segment) |
| `PARTS` | flushed parts across all segments and shards |
| `EST-ROWS` | the sum of the parts' row counts. For an index-mode measure it is the document count of the segment series index, an upper bound because it also holds the series of the group's regular measures. For property it counts every stored revision, tombstones included |
| `EST-SIZE(comp/raw)` | compressed on-disk bytes / uncompressed bytes. comp includes the per-shard and segment-level indexes; raw includes the per-shard indexes, and the segment-level index only for an index-mode measure (where it holds the rows; otherwise it is rebuilt from the parts). For trace, raw counts the span payload only (tags excluded), so it can be smaller than comp; comp is a lower bound. Property stores one form, so both are equal |
| `TIME-RANGE` | earliest ~ latest timestamp of the parts, UTC, as `MM-DD` (with the year when the bounds fall in different years); `-` when unknown |
| `SNAPSHOT` | each data node pins its own snapshot during a real export; the line names how much the largest node needs (its compressed subtotal) so you can check `df` there first |

On stderr the command also prints the effective parallelism and `WARN` lines for a node that reports a stage the group schema does not
declare (or a group the schema no longer has): that node holds data the current lifecycle configuration does not describe.

A dry run reads the live directories, so its numbers are estimates: rows still in a memtable are not counted, and only the parts a
shard's latest snapshot names are, so a flush or merge in flight can make the numbers briefly low.

A data node the liaison reported unreachable (stopped, evicted from its connection pool, silent for longer than the liaison's idle
timeout, not holding the export session, or running a version older than 0.12 without `ExportService`) is reported in the `NODES` line
and as a warning; `--strict-coverage` turns it into exit code `2`. A node removed from the registry entirely is not counted. A real
export does not tolerate such a node: creating the session fails, and the session is released again.

### Sessions

A real export pins one snapshot per data node for the duration of the run, identified by a session id announced in the first `Plan`
frame. The snapshot contains the data that was flushed to disk when the session was created; rows still in memory at that moment are
not part of it. Each data node holds at most one export session, and one session id covers the whole cluster: a data node refuses to
create a new one while **any** other session exists on it, whatever that session's lease says. A session is held until it is released, taken over with `preempt`, or expires five
days after its last heartbeat (`Sessions(ACTION_HEARTBEAT)`, every 5 minutes) and is swept by the node (the sweeper runs every minute).
On a data node the snapshots live in `<catalog>/export-snapshots/<session-id>` (`--<catalog>-export-snapshot-path`), apart from the
backup snapshots. For stream, measure and trace that path must be on the data path's filesystem, because their snapshots are made of hard
links; a property snapshot is a full copy of the property index and needs as much free space as the property data. The server refuses to
start when it equals or sits inside a catalog's backup snapshot directory (`<root-path>/<catalog>/snapshots`), equals, contains or sits
inside any catalog's data path, or overlaps another catalog's export snapshot path. A pinned snapshot keeps the parts it links alive on disk even after retention removes them from the data
directory, and the disk monitor's forced retention cleanup cannot reclaim that space: release a session you no longer need before the
disk comes under pressure.

`data export` creates the session, renders the occupant it was refused by, keeps the heartbeat and releases the session once
`manifest.json` is written; `--preempt` takes over a session the cluster already holds. A heartbeat answered with `FAILED_PRECONDITION`
(the session expired or its lease is unreadable) aborts the export with exit `3`. To release a session an aborted export left behind:

```shell
bydbctl data release-session --id <session-id> --nodes liaison-1:17912
```

`release-session` drops the session snapshot on every data node and prints `export session <id> released`. A node that does not
hold the session has nothing to delete and is listed on stderr (`export session <id> was not held on [...]`); when no data node holds
it, the command fails with `export session <id> is not held on any data node` and exits `2`. `--id` is validated locally (32 lowercase hex characters, exit `1` otherwise) and the command takes no positional
arguments. When some nodes fail to release, the command prints one line per failed node with its reason (unreachable, removal failed,
or a data node without `ExportService`), then `export session <id> released except on [...] (via <liaison>); retry later` — or
`export session <id> could not be released on [...] (via <liaison>); retry later` when no node released it — and exits `3`; retry later, the sweeper reclaims an expired session on its own after its lease
runs out. Its flags are `--id` (required), `--nodes`, `--plan`, `--enable-tls`, `--insecure`, `--cert`, `--logging-level` and
`--logging-env`, with the same precedence as `data export`.

### Permissions

With RBAC enabled the data commands need:

- `data export`: `cluster:read` (preflight: current node and cluster state), `schema:read` on the selected groups (group listing,
  selector validation and, for a real export, listing every resource of the groups for `schema/`) and `cluster:admin`
  (`ExportService.Plan`, `ExportService.Sessions` for the heartbeat and the release, and `ExportService.Export`). `--dry-run` needs
  only `ExportService.Plan`; `--schema-only` needs no `ExportService` method.
- `data release-session`: `cluster:read` (preflight) and `cluster:admin` (`ExportService.Sessions`).

Pass `-u/-p` or `BYDBCTL_USERNAME`/`BYDBCTL_PASSWORD`, and enable TLS (`--enable-tls`, or `connection.nodesTLS` in `plan.yaml`) so the
credentials are not sent in clear text. The liaison forwards these calls to the data nodes over the internal cluster port, which carries
no authentication of its own and must stay unreachable from outside the cluster. An end-to-end example with one hot and one warm data
node behind a liaison, covering the dry run, `release-session` and the CSV export, lives in `test/e2e-v2/cases/transfer/export/`.
