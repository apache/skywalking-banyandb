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
The addresses are tried in order: a liaison that cannot be reached, or that has not discovered any data node yet, is skipped and the
next one is tried; a reachable endpoint that is not a liaison is an error, and a liaison that refuses the credentials
(`Unauthenticated`/`PermissionDenied`) stops the command with exit `2` without trying the others. A server without data export
(older than 0.12) is also exit `2`. Explicit flags override `plan.yaml`, which overrides `BYDBCTL_*` environment variables
(`BYDBCTL_NODES` is comma separated). The data commands neither read nor create `$HOME/.bydbctl.yaml`: everything they need comes
from flags, `plan.yaml` or the environment. TLS settings are the exception: an explicit flag wins over `connection.nodesTLS` in
`plan.yaml`, and there is no environment level for them. The root `-a/--addr`, `-g/--group` and `--config` flags do not apply and
are rejected when given explicitly; a group set through the environment is ignored.

### Dry run

`bydbctl data export --dry-run` inventories what an export would carry, without creating snapshots or writing any file. It lists every
`(node, catalog, group)` with its segments, distinct shards, parts, estimated rows and sizes:

```shell
bydbctl data export --dry-run --nodes liaison-1:17912 --selector 'catalog=stream,groups=sw_record' --selector 'catalog=measure,groups=sw_metricsMinute;sw_metricsHour'
bydbctl data export --dry-run --plan plan.yaml -o json
```

The whole command runs under a 10-minute deadline (`release-session`: 3 minutes).

Flags of `data export`:

| Flag | Default | Meaning |
|------|---------|---------|
| `--nodes` | `connection.nodes` in `plan.yaml`, else `BYDBCTL_NODES` | liaison gRPC addresses, tried in order |
| `--plan` | | `plan.yaml` describing connection, scope and parallelism |
| `--selector` | `export.selectors` in `plan.yaml` | scope, repeatable: `catalog=<stream\|measure\|trace\|property>[,groups=<g1>;<g2>]`; an empty scope means every group. Separators are `,`, `;` and `=` only |
| `--dry-run` | `false` | inventory only; the only mode implemented so far (without it the command answers "not implemented yet", exit `1`). The real-export flags `--format`, `--include-schema`, `--output` and `--preempt` arrive with the transfer itself |
| `-o`, `--output-format` | `table` | dry-run rendering: `table`, `yaml` or `json` (stdout; progress and warnings go to stderr) |
| `--strict-coverage` | `false` | a data node that returned no inventory is an error (exit `2`, after the report is printed) instead of a warning |
| `--parallelism` | `export.parallelism` in `plan.yaml`, else `BYDBCTL_PARALLELISM`, else `max` | number of data nodes exported concurrently: `max` or an integer >= 1, clamped by the node count. A dry run only reports the effective value on stderr |
| `--enable-tls`, `--insecure`, `--cert` | `connection.nodesTLS` in `plan.yaml` | TLS towards the liaison; each flag given explicitly wins over the file |
| `--logging-level`, `--logging-env` | `info`, `prod` | client-side logging |

`plan.yaml` accepts only the keys below; any other key is rejected so a typo never falls back to a default silently. Its selectors
get the same checks as `--selector` (a group name must not be empty or repeated), and `--selector` replaces them entirely:

```yaml
connection:
  nodes: ["liaison-1:17912", "liaison-2:17912"]
  nodesTLS:          # optional
    enable: true
    insecure: false
    cert: /path/to/ca.crt
export:
  parallelism: max   # or an integer >= 1
  selectors:
    - catalog: stream
      groups: [sw_record]
    - catalog: trace  # no groups: every trace group
```

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

A dry run reads the live directories, so its numbers are estimates: rows still in a memtable are not counted, and parts that are being
merged or not yet published may be counted twice (the merge inputs and the output) until the merge completes.

A data node the liaison reported unreachable (stopped, evicted from its connection pool, silent for longer than the liaison's idle
timeout, not holding the export session, or running a version older than 0.12 without `ExportService`) is reported in the `NODES` line
and as a warning; `--strict-coverage` turns it into exit code `2`. A node removed from the registry entirely is not counted. Exit codes:
`1` usage error, `2` preflight rejection (nothing was written anywhere) or, with `--strict-coverage`, a coverage gap (the report is still
printed), `3` failure at run time.

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

The server side of this protocol is complete; the client side that creates a session, renders the occupant it was refused by and keeps
the heartbeat (`--preempt` included) ships with the real export. Until then `bydbctl` drives one session action only, the release:

```shell
bydbctl data release-session --id <session-id> --nodes liaison-1:17912
```

`release-session` drops the session snapshot on every data node and prints `export session <id> released`. A node that does not
hold the session has nothing to delete and is listed on stderr (`export session <id> was not held on [...]`); when no data node holds
it, the command fails with `export session <id> is not held on any data node` and exits `2`. `--id` is validated locally (1-64 lowercase hex characters, exit `1` otherwise) and the command takes no positional
arguments. When some nodes fail to release, the command prints one line per failed node with its reason (unreachable, removal failed,
or a data node without `ExportService`), then `export session <id> released except on [...]` — or `export session <id> could not be
released on [...]` when no node released it — and exits `3`; retry later, the sweeper reclaims an expired session on its own after its lease
runs out. Its flags are `--id` (required), `--nodes`, `--plan`, `--enable-tls`, `--insecure`, `--cert`, `--logging-level` and
`--logging-env`, with the same precedence as `data export`.

### Permissions

With RBAC enabled the data commands need:

- `data export`: `cluster:read` (preflight: current node and cluster state), `schema:read` on the selected groups (group listing and
  selector validation) and `cluster:admin` (`ExportService.Plan`).
- `data release-session`: `cluster:read` (preflight) and `cluster:admin` (`ExportService.Sessions`).

Pass `-u/-p` or `BYDBCTL_USERNAME`/`BYDBCTL_PASSWORD`, and enable TLS (`--enable-tls`, or `connection.nodesTLS` in `plan.yaml`) so the
credentials are not sent in clear text. The liaison forwards these calls to the data nodes over the internal cluster port, which carries
no authentication of its own and must stay unreachable from outside the cluster. An end-to-end example with one hot and one warm data
node behind a liaison lives in `test/e2e-v2/cases/transfer/dry-run/`.
