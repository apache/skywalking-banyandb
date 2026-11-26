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
[Formats](#formats)): their schema, one file set per data node and a `manifest.json` written last. It pins a snapshot on every data node for the duration of the run (an export [session](#sessions)), while writes go on.
Export reads stable snapshots captured on each data node during session creation. These do not represent a single globally atomic
instant; part-based snapshots (stream, measure and trace) contain flushed data only.

```shell
bydbctl data export --nodes liaison-1:17912 --output ./backup-20261010 --selector 'catalog=measure,groups=sw_metricsMinute'
bydbctl data export --plan plan.yaml --output ./backup-20261010 --node-rate-limit 50MiB/s --node-row-rate-limit 50000/s
bydbctl data export --plan plan.yaml --output ./schema-20261010 --schema-only
```

The command checks `--output` as in step 2 before it connects to anything, so a directory it would refuse is exit `1` without
contacting the cluster; then it:

1. connects to the liaisons, lists the groups and validates the selectors;
2. takes the [lock](#progress-file-lock) of the progress file (`<progress file>.lock`; another `bydbctl` holding it is exit `1`,
   `another bydbctl is using <file>`) and keeps it for the whole run; checks that `--output` does not exist or is empty (anything else is exit `1`,
   naming what the directory holds), unless it holds the progress file of an interrupted export, which is then
   [resumed](#resume), or only what an export killed before its first progress write left (`schema/` and temporary files of a
   progress file kept in the output directory, the default one or a `--checkpoint` there), which is removed first; it creates the directory of the progress file (`--checkpoint`) and checks
   that a file can be written there, so an unusable `--checkpoint` is exit `1` before anything is written;
3. writes the schema of the selected groups to `schema/`; with `--schema-only` it writes `manifest.json` and stops here, without a
   session or any data. With `--include-schema=false` it writes no schema at all and skips the schema revision check below;
4. creates the export session and prints `export session <id> created` as soon as the id is known. If another session holds
   the cluster, it prints every occupant with its start time, last heartbeat, catalogs and nodes and exits `2`: check the last heartbeat
   (a running export sends one every 5 minutes) and give `--preempt` to take the session over. When stdin is a terminal and `--preempt` is not given, it asks `Take over the session(s) above? ... [y/N]` instead (on stdout when that is a terminal too; otherwise on stderr when stderr is a terminal, with what the run printed before shown there first): `y` or `yes` takes them over
   as `--preempt` does, anything else (Enter included) leaves them as they are, prints the `release-session` command for each and
   exits `2`;
5. exports every node over its own stream, `--parallelism` nodes at a time, heartbeating the session every 5 minutes. Node `i`
   (nodes sorted by id) streams through liaison `i mod L`; a stream
   refused as unavailable, busy or not found moves on to the next liaison (see `--send-retry-timeout`). Each file is written as `<file>.tmp`, checked against the size and CRC32 the node announces
   (CSV files also against the schema revision), then renamed;
6. writes `manifest.json`, releases the session, deletes the progress file and its plan, and prints what was written — a `NODE CATALOG GROUP STAGE UNITS FILES ROWS SIZE TIME`
   table, `TIME` adding up the time the row's units took — followed by
   `export written to <dir>: <units> unit(s), <files> file(s), <size>, <n> warning(s) in <time>`.

Everything the export prints goes to stdout: the information, the progress, the warnings and the result. Only a failure goes to
stderr: its error and, before it, the hints to resume or release (`export session <id> and the progress file <file> are kept`,
`to resume: …`, `to give up: …`, which names what to delete, the progress file included). A `--dry-run` or an import rendered with `-o json|yaml` keeps stdout to that rendering:
the lines about the connection and the settings (`liaisons:`, `liaison:`, the `info:` of where the schema comes from, `parallelism:`, `rate limits:`) are left
out, and the other information (warnings, notes, the resume summary, the progress and the result) goes to stderr: the warning
about a rate limit the artifact's format ignores (`WARN --node-rate-limit … apply to CSV rows only`) and the `info:` lines the
target's state gives (`schema barrier probe skipped: …`) are among them, not dropped with the settings. Every finished file and unit, and every node
that exported all its units, gets a line with its time (`[i/n]` numbers the units of a node); a node that fails gets none, and the
error is printed when the run exits:

```text
  data-hot1:17912  measure/sw_metricsDay/seg-20261004/shard-0-00001.csv.gz  180 rows  4.3 KiB  0.3s
✓ data-hot1:17912  [3/7] measure/sw_metricsDay seg-20261004  1 file(s)  180 rows  4.3 KiB  0.4s
✓ data-hot1:17912  done: 7 unit(s), 8 file(s), 526 rows, 93.0 KiB in 3.3s
```

When stdout is a terminal, each exporting node also has a live line below them, redrawn in place as the data arrives: the unit, the
shard being written (its position among the unit's shards and the unit's part count, or `segment-level` for a segment-level file:
the rows of an index-mode measure, or a native segment's metadata and series index), the rows (CSV only) and bytes so far, the unit's average transfer rate and the unit's time. The live lines stay
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
healthy cluster has none. Most of them mean the artifact lacks rows or values the source holds, so exit `0` alone does not prove a
complete copy: inspect `manifest.warnings` before relying on the artifact for a migration. Once the session exists, any failure keeps it and prints
`bydbctl data release-session --id <id>`; the partial directory has no `manifest.json` and keeps its progress file, so the same
command [resumes](#resume) it (or empty it, or give another `--output`, to start over). A run refused before its session exists removes the `schema/` it wrote, so the same
command can be run again; Ctrl-C while the session is being created, after its id was announced, releases that session before the
run exits `3`, printing `stopping: releasing export session <id>, waiting up to 30s; press Ctrl-C again to stop now`. Ctrl-C or `SIGTERM` stops the export the same
way: the node streams are canceled, the session and the progress file are kept, and the same `bydbctl data release-session --id <id>`
line is printed before the run exits `3` with `export interrupted: <cause>` (such as `interrupt signal received`). A second Ctrl-C or `SIGTERM` (of `data export` or `data import`) is no longer caught: it
stops `bydbctl` at once, like `kill -9`, without waiting for what the first one is winding down. After a successful export, a release that fails (as a whole or on some node) is only a `WARN` (the artifact is already
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

`<node-dir>` is derived from the node id: a readable prefix (lowercase, runs of characters outside `[a-z0-9]` folded to `-`, at most 32
characters, leading and trailing `-` trimmed), then `-` and the first 8 hex digits of the SHA-256 of the id, for example
`10-0-0-5-17912-8a05db98` for `10.0.0.5:17912`; `manifest.source.nodes` maps it back. Group names are escaped as path components (bytes outside `[A-Za-z0-9._-]`
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

The internal `_top_n_result` measure is exported like any other measure, its binary `value` field base64-encoded. A measure
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

`--format native` carries the data nodes' own engine files byte for byte, packed into `.bnc` containers: per segment its `metadata`
file, `sidx-NNNNN.bnc` (the series index) and one `shard-<N>-NNNNN.bnc` per shard (its live parts, their trace secondary index parts and
the stream element index); per property shard one `shard-<N>-NNNNN.bnc`. It is much faster than CSV in both directions and reproduces
the stored data exactly, but `bydbctl data import` accepts it only into a target whose shard numbers, segment intervals, time zone,
segment file format and index rule ids match the source's. A native artifact is never compressed (`--compress` defaults to `none`, `gzip` is exit `1`);
`--max-file-size` splits a container into volumes and must be `0` or at least `64KiB`. The format, including the manifest fields, is
described in [Data export and import](../../design/0.12.0/data-export-import/README.md).

Importing a native artifact has gates and options of its own, see [Native artifacts](#native-artifacts).

### Resume

| Situation | What to do |
|---|---|
| the export was interrupted (a failure, Ctrl-C, SIGTERM, a crash, `kill -9`) | run the same command again while its session is held: it shows what is done and left and continues; add `--resume` when not at a terminal (otherwise exit `1`) |
| `another bydbctl is using <file>` (exit `1`) | another export of this directory runs: wait until it ends, or stop it |
| a lock file a killed run left behind | nothing: the lock went with the process, and the next run takes the file with `WARN taking over a stale lock ...` |
| the resume exits `6` | see [What to do when a resume exits 6](#what-to-do-when-an-export-resume-exits-6) |
| a node of the session is restarting | run the same command again once it is back, while the session's lease lasts |
| give up | `bydbctl data release-session --id <id>`, then delete what is in the output directory (*Giving up and starting over* below) |
| preview what is left | `--dry-run` with the same `--output` (or `--checkpoint`) |
| the progress file belongs outside the output directory | `--checkpoint <path>` (`export.checkpoint`, `BYDBCTL_CHECKPOINT`), given again on every run of this export; it names the file: a directory, the output directory itself, or a name that ends in `-plan.json` or `-cursor.json` (the names of the side files) is exit `1` |


While it runs, an export keeps a progress file, `<output>/.export-progress.json` (or `--checkpoint`, mode `0600`): the session id
(written as soon as the session is created), the unit each node is exporting and every finished unit with its files; the
session's plan goes to the side file `<progress file without .json>-plan.json`, written once before any node stream opens. Every write replaces
the file atomically. Finished units are recorded at most once a second, so a crash makes the resume export the units of the
last second again, never more. A run that cannot write the progress file fails with exit `3`. Once `manifest.json` is written
the session is released, then the progress file and its plan are deleted (a crash in between leaves the progress file, and the
same command finishes the export again without the session). A `--checkpoint` in a subdirectory of `--output` is refused (exit
`1`): put it directly in the output directory or outside it. The progress file has format version `3`, and a file of another version is refused ([A progress file of another bydbctl version](#a-progress-file-of-another-bydbctl-version)); a path that ends in `-plan.json`
or `-cursor.json` is refused (exit `1`), as it would be the side file of another progress file. The progress file is JSON with
two-space indentation; each finished file is listed with its size and CRC-32, and the bindings a resume is checked against (the
selectors, the options that change the bytes of a file, the source nodes) are in clear.

#### A progress file of another bydbctl version

A progress file and its side files can be resumed only by a bydbctl that reads the same progress file version (this bydbctl reads version `3` for an export,
version `2` for an import). When the version differs, because bydbctl was upgraded or downgraded between two runs of one export or import, the
command exits `6` before it reads the file any further. Nothing is written or deleted, no summary is printed, and it is the same
under `--dry-run`, without `--resume` and with `-o json|yaml`: the refusal goes to stderr and stdout stays free of it. It names
the file, the version in it and the version this bydbctl reads, and says how to go on. An import:

```text
an earlier import left the progress file /data/backup-20260914/.import-progress.json, but another version of bydbctl wrote it (progress file version 1, this bydbctl reads version 2), so it cannot be resumed; nothing was written. Resume it with the bydbctl version that wrote it; or give --restart to import from scratch (the progress file and its side files are kept as .bak files, and the stream and trace data already imported into this target is then stored twice); or give --checkpoint <another path> to run this import separately
```

An export:

```text
an earlier export left the progress file /data/backup-20260914/.export-progress.json, but another version of bydbctl wrote it (progress file version 1, this bydbctl reads version 3), so it cannot be resumed; nothing was deleted. Resume it with the bydbctl version that wrote it; or to start over: bydbctl data release-session --id 85f65ac92eafa06eec2ab5e6f53c1b67, then delete what is in /data/backup-20260914 and export again, or export into a new output directory once the session is released
```

- To go on, run the same command with the bydbctl version that wrote the file, and finish the export or import with it.
- To start an import over, give `--restart`: the progress file and its side files are set aside as `.bak` files, and the stream and trace data
  the earlier run already imported into the target is stored once more. `--checkpoint <another path>` runs the import apart from
  the file.
- To start an export over, release its session with the command the message gives, delete what is in the output directory (or give
  another `--output`) and export again. The message names the session id when the file records one, and gives
  `bydbctl data release-session --id <id>` otherwise; a session that is not released is reclaimed when its lease runs out.

#### Progress file lock

Every run of `data export` and of `data import` over a progress file holds the lock file `<progress file>.lock` for the whole
run and removes it on every way out: success, failure, Ctrl-C or SIGTERM. The lock is a file lock (`flock`, `LockFileEx` on
Windows), which the operating system releases when the process ends, however it ends. The lock file holds one line of JSON
naming its holder (pid, host, start time and the command up to its first flag), which messages render as `pid <n> on <host>
since <time> (<command>)`. A second `bydbctl` on the same progress file is exit `1`:

```text
another bydbctl is using <file> (locked by another process; lock file <file>.lock, held by pid <n> on <host> since <time> (bydbctl data export)): wait until it ends, or stop it
```

A lock file that a killed run (`kill -9`, a crash) left behind does not block the next run: it prints `WARN taking over a stale
lock of pid <n> on <host> ...: <file>.lock; the process that held it is gone` and goes on. On NFS the lock excludes other hosts
only when the mount passes locks to the server (not with `nolock` or `local_lock`); on a file system without file locks every
run is exit `1`: put the progress file on a local disk with `--checkpoint`. A lock file that cannot be locked for another reason
than a holder (a read-only file system, an I/O error, no locks available) is exit `1` with `progress file <file> cannot be locked:
<cause>`, not `another bydbctl is using`.

A lock file that is not a regular file (a symlink) is never followed: the run fails with `... <file>.lock is not a regular
file (...); remove it if no bydbctl uses <file>`.

#### Resuming an export

Running the same command again over a directory that holds a progress file — after a failure, Ctrl-C, a crash or `kill -9` —
continues that export against the same frozen session snapshot and ends with the artifact an uninterrupted export started at the
first run would have written. Only `exportedAt` tells them apart, by design: it is when the first run started (in UTC, `…Z`), the time the
session's snapshot was taken, not when the run that writes the manifest started. `--preempt` (or `BYDBCTL_PREEMPT`) does
nothing on a resume, which creates no session: a `NOTE` says it is ignored.

1. The progress file and its plan must be readable and match this invocation: the same `--output` directory, selectors and the
   options that change the bytes of a file (`--format`, `--compress`, `--csv-annotations`, `--include-schema`).
   Rate limits, parallelism, timeouts and liaisons may change. A mismatch names every difference, field by field (a value equal
   to the option's default is marked `(default)`), and exits `6`; nothing is deleted:

   ```text
     options differs (format, compress, csv-annotations, include-schema):
       compress: recorded gzip (default), now none
   ```

   `--max-file-size` may change too: it only decides where the files of a unit roll over, and an interrupted unit is exported
   again from its start. The units already exported keep their files, the units left get the new limit, and a `WARN` says so:

   ```text
   WARN --max-file-size differs from the earlier run(s) (maxFileSize: recorded 512.0 MiB (default), now 64.0 MiB): the files of the 3 unit(s) already exported keep their sizes; the 5 unit(s) left are written with the new limit
   ```
2. A summary goes to stdout:

   ```text
   RESUME   ./backup-20260914/.export-progress.json  (last updated 2026-11-12T17:14:03+09:00, 41m ago)
            session 9f2a0c… (export)
   DONE     2 group(s), 863 unit(s), 5212 file(s), 504.0 GiB
   CATALOG  GROUP      STAGE  UNITS  FILES  SIZE
   measure  sw_metric  hot    120    1212   18.3 GiB
   stream   sw_record  warm   743    4000   485.7 GiB

   LEFT     2 group(s), 577 unit(s), 625 shard(s), est. 337.6 GiB
   NODE          CATALOG  GROUP      STAGE  SEGMENTS  SHARDS  PARTS  EST-ROWS  EST-SIZE   TIME-RANGE
   data-1:17912  measure  sw_metric  hot    48        96      288    12.4M     7.1 GiB    09-01 ~ 09-07
   data-2:17912  stream   sw_record  warm   529       529     1587   31.0M     330.5 GiB  09-01 ~ 09-07
   ```

   The first table lists what is done, per catalog, group and stage. The second is the plan of **every unit not done** —
   also those that were in flight when the run stopped, which are exported again from their start — read from the plan side
   file, so the cluster is not contacted; it is the table `--dry-run` prints, without its `NODES` line unless a node is missing.
   `shard(s)` counts each shard of a group once, however many nodes hold a replica of it, and a resumed export prints no
   `SNAPSHOT` line: the snapshot is the one the first run pinned.
   Run times (when a file was written or an import finished), here and in the progress files, carry the offset of the local time zone of the machine running `bydbctl`; the data times (`TIME-RANGE`) and the `exportedAt` of `manifest.json` are UTC.

   Without a recorded plan (the first run stopped between the session create and the plan write) `LEFT` reads
   `unknown: no plan is recorded yet; it is read from the export session when the export resumes`.
3. Every file of a finished unit must still be there with the size it was written with, and neither it nor a directory on its
   way below `nodes/` (`nodes/` included) may be a symlink, which the cleanup would not follow, nor may anything else below `nodes/`
   be other than a regular file or directory; once a unit is done, the files under
   `schema/` must be those the first run wrote (the finished units were exported with that schema). Unless every unit is done, every
   data node that still has units to export must still be registered in the cluster (a node added since, or one whose units are all
   done, does not matter). Otherwise exit `6`; nothing is deleted. These checks only read, so they come before the question.
4. With `--resume` (or `BYDBCTL_RESUME=true`) the export continues. Otherwise, when stdin is a terminal, it asks
   `Resume? [y/N]` below the summary: on stdout when that is a terminal too; when stdout is redirected (`| tee log`, `> log`) and stderr is a
   terminal, on stderr, after the summary (`RESUME`, `DONE`, the tables, `LEFT`) is written there as well, while stdout keeps its usual content. Anything but `y`/`yes` exits `1` with
   `not resumed; <file> and the files it records are left in place; to start over: release the session with: bydbctl data release-session --id <id>; then delete what is in <output> and export again`
   (a progress file outside the output directory adds itself and its plan side file: `then delete <file>, <file without .json>-plan.json and what
   is in <output> and export again`); Ctrl-C at the question exits `3` and leaves everything in place too. With no terminal to ask on (stdin is not one, or neither stdout nor stderr is) the run gets
   `a previous run left <file>; add --resume to continue it (needed when not at a terminal); to start over: ...` (the same start-over
   text) and exit `1`.
5. When every unit is done (the first run stopped before or while writing `manifest.json`), the session is not needed: the leftovers
   of step 7 are deleted, `manifest.json` is written from the progress file, the session is released if it is still held (a failed
   release is only a `WARN`), and the progress file is deleted.
6. Otherwise an export with finished units (and the schema) compares the live schema with the `schema/` files by mod revision,
   before the session is read and before anything is deleted. A native export compares every group, stream, measure, trace,
   property, index rule, index rule binding and TopN aggregation of its groups, since a native file carries no schema revision of
   its own: a resource changed or removed since exits `6`, naming each; one added since does not, since the units left come from the
   session snapshot of the first run (a `NOTE` names it, it is not in the export). A CSV export compares the streams, measures,
   traces and properties of the groups that still have units left — the resources a CSV file binds its columns to; index rules,
   bindings, TopN aggregations and finished groups do not count: any of those changed, added or removed exits `6`, naming each,
   with the start-over hint, where the first unit left would otherwise fail on its revision check. Then the session is read again (`Plan` with `read{id}`, which also renews its lease). A session
   that is gone or expired, or an inventory that differs from the recorded plan (a group deleted since, say) exits `6`: an export
   against a new point in time cannot be mixed with this one; the message names the start-over command. A data node that held the
   session and does not answer (it no longer holds it, or cannot be reached) is exit `6` too, with `If a node is restarting or
   unreachable, run the same command again later; only if the session is lost, to start over: ...`: nothing is deleted at this
   step, so a node that is only restarting can be waited for while the session's lease lasts. A data node the cluster gained since,
   which never held the session, does not count, here or for the heartbeat, and neither does a node whose units are all done, even
   when its lease has expired (a long network partition, say): only the nodes with units left must still hold the session. This
   needs a liaison of the same version, which supports `required_nodes` in the session read: an older liaison fails the resume on
   any node whose lease has expired, one with all units done included. Any other failure of the read (`UNAVAILABLE`, say) is
   exit `3`, and so is a refusal of the server on the resume path (`PERMISSION_DENIED`, say), which a fresh export reports as exit
   `2`: earlier runs exported units, so the same command can run again.
7. Every file below `nodes/` that no finished unit lists — `.tmp` files and the volumes of unfinished units — is deleted, then the
   empty directories, a manifest temporary file and temporary files of the progress file a crash left behind.
8. The schema written by the first run is reused once a unit is done. While none is, `schema/` is written again from the live schema,
   for the groups the first run selected only (a group created since is not added), so the units are checked against the current
   schema. The units left are exported, each unit always whole; `manifest.json` is written from the finished units and the new
   ones, the session is released and the progress file deleted. The final summary adds `RESUMED <n> unit(s) from the previous run(s)`,
   its total line ends `(this run)`, and a last line gives the time all runs took: `cumulative time <time> over every run, the first
   started at <time>`.

Every exit `6` of a resume comes before anything in the output directory is deleted.

A run that fails once its session exists, resumed or not, keeps the session and the progress file and prints both ways on:

```text
export session <id> and the progress file <file> are kept
  to resume: run the same command again (add --resume when not at a terminal)
  to give up: release the session with: bydbctl data release-session --id <id>; then delete what is in <output>; a released session cannot be resumed
```

Two failures cannot be resumed and print only the second way, with the progress file among what to delete: a session the heartbeat
found lost, and a CSV file encoded with another schema than the exported one after some units are done (a resume checks against that
same schema, so it cannot get past the change).

`--dry-run` over such a directory (`--output`, or `--checkpoint`, given as a flag or in `plan.yaml`) prints the summary and then the
plan of what is left, and changes nothing; with `-o json` or `-o yaml` that plan has `resumed: {path, done, total}`. It makes the
checks of steps 1 and 3 that need no session (bindings, plan, the files of the finished units, `schema/`, the data nodes with units
left) and exits `6` as the resume would; it does not contact the session, which a `NOTE` line says, so whether the session is still
held and its inventory and schema unchanged is found only by the real resume. It does not take the lock, so it also reads an export
that is running. With `--checkpoint` and no `--output` it reads the output directory the progress file records, and says so under
the summary. `BYDBCTL_OUTPUT` and `BYDBCTL_CHECKPOINT` are defaults for real exports: alone they never turn a dry run into a resume
preview, but with `--output` given as a flag or in `plan.yaml` a progress file from `BYDBCTL_CHECKPOINT` is kept and previewed, since
that is where that export records its progress. A schema-only export refuses a directory with a progress file. A progress file outside the output directory
(`--checkpoint`) must be given again on every resume: without it the directory reads as `not empty and holds no export to resume`
(exit `1`). The progress file records the absolute path of `--output`, so an export directory that was moved or is mounted under
another path cannot be resumed (exit `6`).

**How long a resume stays possible, and what it costs.** An interrupted export can be resumed while its session is held: until it
is released, taken over with `--preempt`, or five days after its last heartbeat ([Sessions](#sessions)). All that time the session
pins its snapshots on every data node, disk that retention cannot reclaim, and every other export on the cluster is refused (exit
`2`).

**Giving up and starting over.** Release the session, `bydbctl data release-session --id <id>` (the id is in the failure message
and in the progress file, `session.id`), then delete what is in the output directory (and a progress file outside it, with its
plan side file) and export again, or export into an empty directory. An export into another directory
while the old session is held is refused with exit `2`.

#### What to do when an export resume exits 6

| The message names | What to do |
|---|---|
| a binding that differs (`outputDir`, `selectorsDigest`, `options`) | run the first run's command again: the same `--output` path, selectors and file options. Otherwise start over |
| data node(s) that still have units of this export to export but are not registered in the cluster now | a node that is only restarting: run the same command again once it is back. Only if it is gone for good, start over |
| an export session that is gone or expired | start over (release the session if some node still holds it) |
| data node(s) that could not be read: they no longer hold the session or cannot be reached | a node that is only restarting: run the same command again once it is back. Otherwise start over |
| an inventory that differs from the recorded plan | start over: the data the session pinned no longer matches the first run's |
| a file of a finished unit that is missing or short | start over |
| `schema/` files that no longer match the progress file, or are unusable, while units are done | restore them, or start over |
| a schema that changed after units were done (native: a resource changed or removed; CSV: a stream, measure, trace or property of a group with units left changed, added or removed) | start over: the finished units were exported with the old schema |
| a plan side file that is missing or not the one the progress file was written with | start over |
| `another version of bydbctl wrote it` | run the same command with the bydbctl that wrote the progress file, or start over ([A progress file of another bydbctl version](#a-progress-file-of-another-bydbctl-version)) |
| a progress file that cannot be read or parsed | start over; release the session whose id the first run printed, or let its lease run out |

### Flags

Flags of `data export`:

| Flag | Default | Meaning |
|------|---------|---------|
| `--nodes` | `connection.nodes` in `plan.yaml`, else `BYDBCTL_NODES` | liaison gRPC addresses; every usable one shares the export streams |
| `--plan` | | `plan.yaml` with the connection, the scope and the export options |
| `--selector` | `export.selectors` in `plan.yaml` | scope, repeatable: `catalog=<stream\|measure\|trace\|property>[,groups=<g1>;<g2>]`; an empty scope means every group. Separators are `,`, `;` and `=` only |
| `--parallelism` | `export.parallelism`, else `BYDBCTL_PARALLELISM`, else `max` | number of data nodes exported concurrently: `max` or an integer >= 1, clamped by the node count |
| `--output` | `export.output`, else `BYDBCTL_OUTPUT` | export directory; required for a real export; must not exist or be empty, or hold the progress file of an export to [resume](#resume) |
| `--checkpoint` | `export.checkpoint`, else `BYDBCTL_CHECKPOINT`, else `<output>/.export-progress.json` | the progress file of the export, see [Resume](#resume); a directory, the output directory itself, or a name that ends in `-plan.json` or `-cursor.json` is exit `1` |
| `--resume` | `BYDBCTL_RESUME`, else `false` | continue the interrupted export recorded in the progress file (`--checkpoint`, default `<output>/.export-progress.json`) without asking; without a progress file the export starts afresh |
| `--format` | `export.format`, else `BYDBCTL_FORMAT`, else `csv` | artifact format, see [Formats](#formats): `csv` or `native` |
| `--compress` | `export.compress`, else `BYDBCTL_COMPRESS`, else `gzip` (`none` for native) | `none` or `gzip`; native accepts only `none` (a `BYDBCTL_COMPRESS` value is ignored for native) |
| `--max-file-size` | `export.maxFileSize`, else `BYDBCTL_MAX_FILE_SIZE`, else `512MiB` | CSV: soft limit of one file's on-disk (compressed) size; a file rolls after the row that reaches it. Native: hard cap on every `.bnc` volume, footer and trailer included; must be `0` or at least `64KiB`, otherwise exit `1`. `0` never rolls. Bare bytes or a unit (`KB`, `KiB`, `MB`, `MiB`, `GB`, `GiB`, ...) |
| `--csv-annotations` | `export.csvAnnotations`, else `BYDBCTL_CSV_ANNOTATIONS`, else `false` | CSV only: write a `#datatype` line before every CSV header; refused with `--format native` when set as a flag or in `plan.yaml` |
| `--include-schema` | `export.includeSchema`, else `BYDBCTL_INCLUDE_SCHEMA`, else `true` | `false` exports no schema and skips the schema revision check (CSV only; a native export never runs it); `groups[].indexMode` is then recorded as `false` with a warning. Cannot be combined with `--schema-only` |
| `--schema-only` | `export.schemaOnly`, else `BYDBCTL_SCHEMA_ONLY`, else `false` | write `schema/` and `manifest.json` only, without a session or data |
| `--preempt` | `BYDBCTL_PREEMPT`, else `false` | remove the export session the data nodes hold and take over; the export running on it fails on its next read; ignored, with a `NOTE`, by a resume |
| `--node-rate-limit` | `export.nodeRateLimit`, else `BYDBCTL_NODE_RATE_LIMIT`, else `0` | byte rate each data node may send (compressed bytes), enforced by the node; `0` = unlimited |
| `--node-row-rate-limit` | `export.nodeRowRateLimit`, else `BYDBCTL_NODE_ROW_RATE_LIMIT`, else `0` | CSV only: rows each data node may encode per period, enforced by the node; `0` = unlimited; refused with `--format native` |
| `--liaison-rate-limit` | `connection.rateLimit`, else `BYDBCTL_LIAISON_RATE_LIMIT`, else `0` | bytes each liaison relays, enforced by the liaison before it forwards each chunk; one bucket per liaison process shared by all its export streams, the stream opened last setting the value; `0` = unlimited |
| `--node-idle-timeout` | `export.nodeIdleTimeout`, else `BYDBCTL_NODE_IDLE_TIMEOUT`, else `10m` | fail a node stream that delivers nothing for this long; `0` disables it |
| `--send-retry-timeout` | `export.sendRetryTimeout`, else `BYDBCTL_SEND_RETRY_TIMEOUT`, else `15m` | how long to keep reopening a node stream refused with `UNAVAILABLE`, `RESOURCE_EXHAUSTED` or `NOT_FOUND`, trying the next liaison each time (back-off from 1s, doubling to 30s) |
| `--dry-run` | `BYDBCTL_DRY_RUN`, else `false` | inventory only, see [Dry run](#dry-run); cannot be combined with `--output`, `--checkpoint`, `--preempt` or `--schema-only`, except that `--output` / `--checkpoint` naming the progress file of an interrupted export prints what is left of it ([Resume](#resume)) |
| `-o`, `--output-format` | `BYDBCTL_OUTPUT_FORMAT`, else `table` | dry-run rendering: `table`, `yaml` or `json`. With `yaml` or `json` stdout holds the rendering only: the lines about the connection and the settings (`liaison: …`, `parallelism: …`) are left out and the warnings go to stderr |
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

`plan.yaml` accepts only the keys below plus the `import.*` keys of `data import` ([Import flags](#import-flags)); any other key
is rejected so a typo never falls back to a default silently. Its selectors get the same checks as `--selector` (a group
name must not be empty or repeated), and `--selector` replaces them entirely. Explicit flags override the file, which overrides the
`BYDBCTL_*` environment. `--dry-run`, `--preempt`, `--resume`, `--strict-coverage` and `--output-format` are one-shot switches with no plan field.

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
  checkpoint: ./backup-20261010/.export-progress.json  # the default
  format: csv            # csv | native
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
| `0` | success; release warnings do not change it. It can include data-loss warnings: check `manifest.warnings` before treating the export as complete |
| `1` | usage error: a bad flag or `plan.yaml`, an unknown selector group (of a fresh export; a resume leaves a group deleted since to its own checks, exit `6`), a non-empty `--output` without a progress file, another `bydbctl` holding the lock of the progress file, a `--checkpoint` that is a directory (or the output directory itself), whose directory cannot be created or written or that is in a subdirectory of `--output`, a resume that was not confirmed (no `--resume` and no terminal, or not answered `y`), or with `--format native` any of `--compress gzip`, a non-zero `--max-file-size` below 64KiB, a `--node-row-rate-limit` above 0 or `--csv-annotations` (set as a flag or in `plan.yaml`) |
| `2` | preflight rejection, nothing was exported: no liaison reachable or one refused the credentials, a server without data export, another export session holds the cluster (without `--preempt`), or, in a dry run with `--strict-coverage`, a coverage gap (the report is still printed) |
| `3` | failure at run time: a node stream failed or stalled, a file failed its size/CRC32 check, the schema changed during the export, the session was lost (its heartbeat answered `FAILED_PRECONDITION`), or a release failed on some node (`release-session`), or the progress file could not be written; on a resume also a refusal of the server (exit `2` on a fresh export) and any failure of the session read but its loss; also Ctrl-C at the `Resume?` question |
| `6` | resume impossible: the progress file is unusable, one of its bindings differs, a data node with units left is not registered in the cluster, the export session is gone, a data node does not answer for it (it no longer holds it or cannot be reached: run the same command again later if a node is restarting), the session's inventory differs from the recorded plan, a file of a finished unit is missing or short (or it, or a directory on its way below `nodes/`, is a symlink, or anything below `nodes/` is neither a regular file nor a directory), a group the first run recorded is gone while no unit is done, or the `schema/` files of the first run changed or are unusable while units are done, or the live schema changed while units are done (see [Resume](#resume) step 6). Every such exit comes before anything is deleted: the progress file, the session and every file in the output directory are kept. See [Resume](#resume) for what to do |

### Dry run

`bydbctl data export --dry-run` inventories what an export would carry, without creating snapshots or writing any file. It lists every
`(node, catalog, group)` with its segments, distinct shards, parts, estimated rows and sizes:

```shell
bydbctl data export --dry-run --nodes liaison-1:17912 --selector 'catalog=stream,groups=sw_record' --selector 'catalog=measure,groups=sw_metricsMinute;sw_metricsHour'
bydbctl data export --dry-run --plan plan.yaml -o json
```

It takes the connection, scope and parallelism flags above plus `-o` and `--strict-coverage`; it only reports the effective parallelism on
stdout. The other real-export options are not used, though they must still be valid; `export.output` and `export.schemaOnly: true` contradict a
dry run and are refused (exit `1`), except an `export.output` (or `export.checkpoint`) that names the progress file of an interrupted
export, whose resume the dry run then previews ([Resume](#resume)): leave them out of a plan used for dry runs.

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
| `EST-SIZE` | estimated on-disk size of the parts and their indexes, what a native export transfers. `-o json` and `-o yaml` also carry the uncompressed size |
| `TIME-RANGE` | earliest ~ latest timestamp of the parts and, for an index-mode measure group, of its series index, UTC, as `MM-DD` (with the year when the bounds fall in different years); `-` when unknown |
| `SNAPSHOT` | each data node pins its own snapshot during a real export; the line names how much the largest node needs (its compressed subtotal) so you can check `df` there first |

On stdout the command also prints the effective parallelism and `WARN` lines for a node that reports a stage the group schema does not
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
(the session expired or its lease is unreadable on a node of the export; on a resume only a node that still has units to export
counts) aborts the export with exit `3`, and so does a node stream refused with it once a heartbeat to that node finds the session gone (it
was released or taken over meanwhile); the hint says the session is lost and how to start over, not to run the command again.
A node answers `FAILED_PRECONDITION` for other reasons too (a catalog CSV cannot encode, a trace group whose traces share a tag set,
which `--format native` exports): when the heartbeat finds the session still held, the stream's own error stands, with the hints to run the
command again or to release the session. To release a session an aborted export left behind:

```shell
bydbctl data release-session --id <session-id> --nodes liaison-1:17912
```

`release-session` drops the session snapshot on every data node and prints `export session <id> released`. A node that does not
hold the session has nothing to delete and is listed on stdout (`export session <id> was not held on [...]`); when no data node holds
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
node behind a liaison, covering the dry run, `release-session`, the export and the import, lives in `test/e2e-v2/cases/transfer/csv/`
(CSV) and `test/e2e-v2/cases/transfer/native/` (native).

## Data import

`bydbctl data import` writes an artifact that `bydbctl data export` wrote (CSV or native) into a cluster: the artifact's schema,
then its data, and checks afterwards that the target holds what was sent. Use it to migrate data between clusters, to restore an
export, or to copy a subset of groups. `--dry-run` prints what the import would do and writes nothing. The command talks to a liaison
**gRPC** endpoint exactly as `data export` does (see [Data export](#data-export): `--nodes`, every usable liaison takes part, settings
resolve flag → `plan.yaml` → `BYDBCTL_*` variable → default, no `$HOME/.bydbctl.yaml`). Import into a cluster that runs one version on every node, not during a rolling upgrade.

### Import

```shell
bydbctl data import --nodes liaison-1:17912 --input ./backup-20261010 --dry-run
bydbctl data import --nodes liaison-1:17912 --input ./backup-20261010
bydbctl data import --plan plan.yaml --input ./backup-20261010 --on-schema-conflict ignore --force-ttl
bydbctl data import --nodes liaison-1:17912 --input ./schema-20261010 --schema-only
```

The import:

1. connects to the liaisons and checks the artifact: `manifest.json`, and every schema and data file with its size and CRC32 (with
   `--schema-only`, data files by size only); a file that is missing or damaged refuses the import;
2. takes the [lock](#progress-file-lock) of its progress file (`<progress file>.lock`, not under `--dry-run` or `--schema-only`) and
   reads the progress file an earlier run of this import left, if there is one ([Import resume](#import-resume));
3. compares the artifact's schema with the target's, read-only, and resolves every difference with the schema conflict policy
   (`--on-schema-conflict error|ignore|overwrite`, or rules per resource in `import.schemaConflictPolicy` of `plan.yaml`);
4. asks the liaison where every (group, stage) is routed and decides the plan: one work item per shard of a segment (one per segment
   for index-mode measure rows, one per source shard for a property group), its sources (the minimal set that covers the shard, or
   every source with `--all-sources`) and its target nodes, and runs the gates: the schema, the TTL of the target stage (a unit whose
   data the target would already have expired is `expired` and left out unless `--force-ttl`), for CSV a check of every row, for
   native the [native gates](#native-artifacts);
5. prints the plan, the same bytes for `--dry-run` and the real run: one row per (catalog, group, stage) with
   `CATALOG GROUP STAGE UNITS SOURCES ROWS SCHEMA CHANNEL TARGET-NODES STATUS TIME-RANGE(UTC)`, then `SCHEMA` (per group, the resources
   to create, overwrite, keep or ignore), `PROBLEMS` (only when there are some: conflicts with the `match:` rule that would accept
   them, refused work items, warnings) and `RESULT`, the conclusion in one line (`import <n> work item(s), <rows> rows`,
   `refused, nothing written (exit 2)`, ...). A group the import creates has no route yet, so its `TARGET-NODES` reads `after-schema`.
   A dry run ends here: `0` when the import would proceed, `2` when it would be refused;
6. writes the schema (`schema: <n> resource(s) written and applied on every node`), waits until every node applied it and decides
   the plan again: a plan that changed is printed and the import goes on, or exits `5` with `--abort-on-plan-change`;
7. creates its progress file and prints `progress: <file> (kept after the import; the same command resumes an interrupted one)`, then
   counts the target's rows before anything is pushed (the baseline of [Verify](#verify));
8. pushes the work items, `--parallelism` at a time: CSV rows in batches through `ImportRows`, native parts and indexes as they are
   through `ImportParts`, relayed by the liaison to every target node of their shard. A line is printed for every finished work item
   and every finished (catalog, group, stage); on a terminal, live lines show the groups in flight. The push ends with
   `IMPORTED  <n> rows of <m> work item(s) in <time>` (native: `IMPORTED  <m> work item(s) of native files, <n> rows in the imported
   parts, in <time>`);
9. verifies, unless `--verify=false` ([Verify](#verify)): `VERIFY    ok in <time>`, or `VERIFY    FAILED in <time> (exit 4)` followed by
   one line per problem.

Nothing is written before step 6: a refusal up to there (exit `1` or `2`) leaves the target as it was. Importing the schema only
(`--schema-only`) stops after step 6, keeps no progress file and pushes no data; `--include-schema=false` neither compares nor writes
the schema, so every resource the artifact needs must exist on the target already.

**Schema conflicts.** A resource that differs from the target's is a `CONFLICT` and refuses the import (exit `2`) under the default
policy `error`; every conflict prints a ready-to-paste `match:` line. `ignore` keeps the target's resource (columns only one side has
are dropped or sent as null), `overwrite` replaces it, and an `overwrite` of a group that holds data needs `--force`. Some differences
(an identity, an entity, a measure's index mode or interval) are always conflicts. Rules are matched in order, `match:
<kind>/<group>[/<name>]`, and win over the flag:

```yaml
import:
  schemaConflictPolicy:
    default: error            # error | ignore | overwrite
    rules:
    - match: stream/sw_record
      policy: overwrite
```

The internal `_top_n_result` measure is imported verbatim, never recomputed; its schema is created by the target with every measure
group and must match the artifact's columns.

**Several sources.** An artifact holds every source node's copy of a replicated shard. The import pushes the minimal set of sources
that covers each shard; `--all-sources` pushes every one (stream and trace rows are then stored once per source), and
`--strict-coverage` refuses (exit `2`) a shard work item, or a CSV stream or trace segment-level item, that would still need more
than one source.

#### Native artifacts

Importing a native artifact refuses the whole import (exit `2`, the gate and its reason listed under `PROBLEMS`) when one of these
differs: `TOPOLOGY` (the shard number or segment interval of the target stage or group), `TZ` (a target data node's time zone),
`VERSION` (a segment file format a target data node cannot read) or `INDEX_RULE` (an index rule that exists on the target with another
id; index rules the import creates keep the artifact's ids, so let the import create the schema rather than creating it beforehand).
A payload a target node failed is pushed again to that node only, up to three attempts. A stream or trace part that failed on a node
after its completion reached it may be stored there already: the import reads the node's counts and takes the part as landed when they
grew by its rows; otherwise the import fails (exit `3`) and the [resume](#import-resume) decides it, see
[Reconciliation](#reconciliation). `--part-rate-limit` and `--part-chunk-size` apply to the native import only; the row and byte rate
limits of the CSV import do not. A native import copies whole segments into a cluster that usually serves live traffic, and the
liaison writes every byte to each replica; set `--part-rate-limit` so the copy does not saturate the target's network and disks. Raise
`--part-chunk-size` over a high-latency link to cut acknowledgement round trips, or lower it to reduce the memory the liaison and
receivers hold per in-flight frame.

### Verify

`--verify` is on by default (`--verify=false` turns it off) and polls the target for up to `--verify-timeout` (`2m`) after the push,
every 5 s, since only flushed data is counted; a failure exits `4`.

The target's row counts per (catalog, group, stage, node, shard), taken with a session-less `Plan` before the
first push (the baseline) and after the last, must have grown as the rows sent say: a stream or trace shard on exactly its target
nodes, by the same rows on each, the shards together by the rows pushed; a measure on every target node of a shard, by at most the
rows pushed × copies (a merge may fold versions). An index-mode measure is only warned about when a node gained nothing; properties are
not counted. Rows a resume pushed again under `warn` or `push` are a known tolerance, a warning rather than a failure
([Duplication policy](#duplication-policy)). A segment retention removed meanwhile is left out with a warning; a stream or trace
(group, stage) that then grew by fewer rows than were sent is not checked (`... a segment of it disappeared meanwhile (retention?)
and may have held the rest`), which fails verify. The check assumes
that nothing else writes into the imported groups meanwhile.

### Import resume

| Situation | What to do |
|---|---|
| the import was interrupted (a failure, Ctrl-C, SIGTERM, a crash, `kill -9`) | run the same command again: it shows what is done, left and ambiguous and continues; add `--resume` when not at a terminal (otherwise exit `1`) |
| the import finished | nothing to do: the same command prints `already imported into this cluster ...`, pushes nothing and exits `0` ([A finished import](#a-finished-import)) |
| the same artifact goes into **another cluster** | give that import its own `--checkpoint <another path>`: the default progress file belongs to the first target, and another cluster exits `6` |
| the target was wiped or redeployed, so the import exits `6` with `the target looks reset or is another cluster` | `--restart` imports the artifact again into this cluster; another cluster needs another `--checkpoint` |
| `another bydbctl is using <file>` (exit `1`) | another import of this artifact runs: wait until it ends, or stop it ([Progress file lock](#progress-file-lock)) |
| a lock file a killed run left behind | nothing: the lock went with the process, and the next run takes the file with `WARN taking over a stale lock ...` |
| the import exits `3` saying a node did not answer the check of what it imported | run the same command again once the node is reachable; nothing was written |
| the import exits `6` for another reason | see [What to do when a resume exits 6](#what-to-do-when-a-resume-exits-6) |
| the import exits `7` | the duplication policy is `block` for payloads that may have landed already: give `--resume-duplication-policy warn`, `push` or `skip` (skip stays refused where a payload line says so), and run the same command again ([Duplication policy](#duplication-policy)) |
| start over on purpose | `--restart` ([Starting over](#starting-over)) |
| the artifact directory is read-only, or the progress file belongs elsewhere | `--checkpoint <path>` (`import.checkpoint`, `BYDBCTL_IMPORT_CHECKPOINT`), given again on every run of this import |

#### The progress file

`bydbctl data import` records what it puts into the target in a progress file, `<input>/.import-progress.json`, or the file
`--checkpoint` names (mode `0600`; a directory, the `--input` directory itself, a file of the artifact (`manifest.json` or a file it lists, which `--restart` would set aside) or a name that ends in `-plan.json` or `-cursor.json`, the names of the side files, is exit `1`). It records the bindings of the
run (the artifact's `manifest.json`, the target's liaisons and data nodes, `--all-sources` and `--include-schema`), the plan the
first run decided, the baseline (the target's counts before the first push), and per work item which native parts and indexes
landed on which target node, or how far a CSV item is acknowledged and sent; also the rows pushed so far, the payloads `skip`
left out, when a run pushed the last work item, and whether the verify of that run has not passed yet. The plan and the baseline
are in one side file next to it (`<file without .json>-plan.json`), and while a CSV import runs, its positions in another (`-cursor.json`).

- **When it is written.** Before the schema is written (also under `--dry-run`) the import checks that a file can be created
  next to the progress file: a read-only artifact directory, or a `--checkpoint` whose directory does not exist, is exit `1`
  with nothing written. A fresh import creates the progress file once the schema is written and the plan re-checked, and prints
  `progress: <file> (kept after the import; the same command resumes an interrupted one)`; a resumed import prints the same line
  before it writes the schema. A run that cannot write it exits `3`, and every failure after it exists names it: `the progress
  file <file> is kept: run the same command again to resume (add --resume when not at a terminal)`.
- **It is kept after a successful import**: it records what this artifact put into this cluster, so that a rerun knows what not
  to store again and that the import is finished. Its name does not depend on the target: importing the same artifact into
  another cluster needs its own `--checkpoint`.
- **The lock.** For the whole run (fresh, resumed or `--restart`; not `--dry-run` or `--schema-only`) the import holds the
  [lock](#progress-file-lock) of the progress file; a refusal adds `; to import this artifact into another cluster give
  --checkpoint <another path>`, and the `WARN` of a lock file a killed run left behind goes to the import's log.
- **`--schema-only`** uses no progress file at all: it reads none, takes no lock and writes none, and with `--resume` or
  `--restart` it prints `NOTE a schema-only import uses no progress file: --resume and --restart are ignored, <file> is left as
  it is`.
- **What a crash can cost.** A native import records a payload as in flight, synced, before it sends it, so at most one payload
  per worker is in doubt after a crash; the resume reconciles or pushes it again. A CSV import records how far it sent an item
  before each batch and the position of every acknowledged batch, unsynced, and folds them into the progress file, synced, every
  `--checkpoint-every-batches` acknowledged batches across the import (default `64`) and when a work item completes. After a
  `kill -9` the resume knows every position; after a power loss or an OS crash it may know only those of the last fold, and the
  rows acknowledged or sent since are pushed again as rows that were never sent, without the duplication policy and without being
  counted as possible duplicates (stream and trace rows among them are then stored twice). A lower value narrows that window at
  the cost of one more synced write each time; the default suits most imports.
- **Stopping cleanly.** When a worker fails, or on Ctrl-C or SIGTERM, the CSV workers stop sending and wait up to 30 s for the
  acknowledgements of the batches they have in flight (`waiting up to 30s for the acknowledgements of <n> batch(es) in flight, so
  that they are recorded; press Ctrl-C again to stop now`). A native worker cancels a payload still being sent at once, but waits
  up to 30 s for the answer of one whose completion was sent (`waiting up to 30s for the answer of the <KIND> <group>/<stage> shard
  <n> in flight, so that it is recorded; press Ctrl-C again to stop now`). So what landed is recorded rather than left
  ambiguous; a payload whose answer does not come within the 30 s is recorded as possibly still being committed on its nodes
  (`no_answer`), which a resume does not read as not landed. When a worker's failure, not a Ctrl-C, started the wait, the line
  ends with `press Ctrl-C twice to stop now`. A second Ctrl-C or SIGTERM stops `bydbctl` at once instead; the batches and
  payloads in flight are then ambiguous, as after a `kill -9`.

**The baseline.** The target's counts before the first push, for `--verify` and the reconciliation, are taken once, by the first
run that pushes anything, and kept for every later run. Counting is retried for up to a minute (first after 1 s, then doubling)
while it fails or misses a target node. When `--verify` or a reconciliation (native stream or trace parts) needs the baseline,
one that still did not count a target node is exit `3` before anything is pushed, with `no data was sent: run the same command
again once the target answers`. When neither needs it, the import goes on with `WARN <reason>; neither --verify nor a
reconciliation needs it, so the import goes on, and a verify forced later has no baseline to compare against`; a verify a later
run needs (forced by `skip`, or `--verify` on a resumed or finished import) is then exit `4`, saying that the first run of this
import could not count the target before its push. A resume first counts what the baseline lacks, before anything of it is
pushed, and records it with the baseline: a target data node added since (`baseline: target node(s) <nodes>, which the baseline
did not count, were counted before the push and recorded with it`; only a node the progress file records nothing pushed to, since
another may already hold rows of the import), and the groups of work items appended to the plan that the first run imported
nothing into (`baseline: <n> group(s) the appended items add were counted before the push and recorded with the baseline:
<groups>`).

#### Target check

Every run over a progress file (unfinished or finished, `--dry-run` too) checks that the target is the cluster the file was
written for, in two layers:

- **Who the target is.** Some liaison the file records must still be a liaison of the target, and some recorded data node a data
  node of it: liaisons and data nodes may be added, removed or restarted, and an import may go through any liaison of the
  cluster, but a cluster that shares no liaison or no data node with the recorded one is another cluster. Over an unfinished
  import every target node of a work item not complete yet must also still be a data node of the target. A progress file
  without the liaison list (written before it was recorded) cannot be checked and is refused.
- **What the target holds.** Node names alone do not tell a cluster apart from one deployed from the same Helm values,
  StatefulSet or compose file, or from the recorded cluster wiped and deployed again. So when the progress file records data as
  imported, before a resumed push and before saying `already imported`, the schema comparison must not have to create any group
  or resource the import wrote rows into (`the progress file <file> records data this import wrote, but the target lacks
  <resources>; the target looks reset or is another cluster`, naming the first five resources and counting the rest), and one count of those groups must find rows wherever the
  progress file records rows as landed (`the progress file <file> records data this import wrote that the target does not hold
  (<n> place(s)):` with up to five places). Where the check can, it looks per data node, shard and segment (a native stream or
  trace item), or per shard and segment (any other shard item whose target stage has the source's shard number); otherwise per
  group and stage, with `NOTE the data check of <catalog> <group> stage <stage> is per (group, stage), not per shard: ...`. A
  property group must hold documents. Segments past the TTL of their target stage are not checked, and a trace group that
  samples spans in merges, which may have dropped them, only gets a `NOTE`. A target segment whose suffix is not a time of the
  stage's segment interval is exit `6`: `the target reports segment "<suffix>" of <catalog> group <group> stage <stage> on node
  <node>, whose suffix is not a time of the stage's <interval> segment interval`.

A refusal of either layer exits `6`, writes nothing and ends with `to import this artifact into another cluster give
--checkpoint <another path>; to import it again into this cluster give --restart`. When rows are missing where a node that
may hold them did not answer the count, the check cannot tell: it exits `3` (`... did not answer the count, and no node that
answered holds it ...; nothing was written; run again once the node(s) are reachable`).

#### Resuming an interrupted import

Running the same command again over an input that holds a progress file — after a failure, Ctrl-C, a crash or `kill -9` —
continues that import. The `--resume` flag given while there is no progress file at the path is exit `1` with `--resume given
but no progress file at <path>; without --resume a fresh import starts`, never a fresh import (not checked under `--dry-run` or
`--schema-only`); `BYDBCTL_IMPORT_RESUME=true` without one starts a fresh import with `NOTE BYDBCTL_IMPORT_RESUME is set, but
there is no progress file at <path>: a fresh import starts`. Without a terminal and without `--resume`, an unfinished progress
file is refused after the resume summary (`RESUME`, `DONE`, `LEFT`, the tables and the `AMBIGUOUS` section, the same text a terminal
shows before `Resume? [y/N]`) is printed, which reads the target (not the rows of the artifact) but writes nothing and changes no file: exit `1` with `a
previous run left <file>; add --resume to continue it (needed when not at a terminal); give --restart to import from scratch
(<file> is kept as a .bak file); stream and trace data already imported is then stored twice`. A dry run is not refused.

1. The checks, the schema comparison and the plan run as usual (the TTL gate against now).
2. The progress file must match this invocation: the same `manifest.json` bytes, the same target ([Target check](#target-check),
   first layer) and the same `--all-sources` and `--include-schema` (a `schemaOnly` option an earlier version recorded is
   ignored). A mismatch names every difference and exits `6`; nothing is written:

   ```text
     manifestDigest differs: recorded sha256:…, now sha256:… (the artifact's manifest.json)
     liaisons differs (the liaisons of the target cluster):
       recorded [liaison-0 liaison-1], now [liaison-9]: none of the recorded liaisons is a liaison of the target
     targetNodes differs (the data nodes of the target cluster):
       target node data-a of the recorded import is no longer a data node of the target
     options differs (--all-sources, --include-schema):
       allSources: recorded false, now true
   ```

   A `manifestDigest` or `options` difference ends with `give --checkpoint <another path> to run this import separately, or
   --restart to start this one over; the stream and trace data an earlier run already imported into this target is then stored
   twice`; a target difference with `the target is not the cluster this progress file was written for: to import this artifact
   into another cluster give --checkpoint <another path>; to import it again into this cluster give --restart`. Then the second
   layer of the target check runs.
3. A summary goes to stdout, with the payloads whose landing is not known ([Ambiguous payloads](#ambiguous-payloads)) and what
   the resume does with each. Each payload line reads `  <payload>: <action> [(<source>)]`, where the action is `reconcile`
   (followed by `, else <policy> (<source>)`, the policy of what reconciliation cannot decide), `re-push` or the policy, and the
   source says where the policy comes from: `--resume-duplication-policy` when it is set (by the flag,
   `import.resumeDuplicationPolicy` or `BYDBCTL_IMPORT_RESUME_DUPLICATION_POLICY`), else `default`:

   ```text
   RESUME   ./backup-20260914/.import-progress.json  (last updated 2026-11-12T18:02:41+09:00, 7m ago)
   DONE     2 group(s), 118 work item(s), 236 file(s), 81.2 GiB
   CATALOG  GROUP      STAGE  ITEMS  FILES  SIZE
   measure  sw_metric  hot    40     80     18.3 GiB
   stream   sw_record  hot    78     156    62.9 GiB

   LEFT     2 group(s), 46 work item(s), 61.3M rows, est. 30.4 GiB
   AMBIGUOUS 3 payload(s) may have landed already:
     stream/sw_record/hot/20260907/shard-0 part 7 of data-1:17912 on data-b: reconcile, else warn (default)
     stream/sw_record/hot/20260907/shard-1 ELEMENT_INDEX of data-1:17912 on data-a,data-b: warn (default)
     trace/sw_trace/hot/20260907/shard-0 rows after nodes/…/shard-0-00003.csv.gz record 192000 up to record 196000 (4000 row(s) sent): warn (default)
   ```

   The `rows` of `LEFT` are the rows left: of a CSV item a cursor has already acknowledged part of, only the rows after it.
   The table lists what is done; what is left follows it (step 4) as one table, not the whole plan of a fresh import:

   ```text
   CATALOG  GROUP      STAGE  ITEMS  ROWS   EST-SIZE  TARGET-NODES   TIME-RANGE(UTC)
   measure  sw_metric  hot    12     11.2M  5.8 GiB   data-a         09-07 ~ 09-07
   stream   sw_record  hot    34     50.1M  24.6 GiB  data-a,data-b  09-07 ~ 09-07
   ```

   A `STATUS` column (`expired`, `refused`) comes only when a row is not `ok`, the `SCHEMA` table only with the groups whose
   schema is not `ok`, and `PROBLEMS` and the `after-schema` note only when there are; a finished import prints no table.
   A `--dry-run` over the progress file prints the same; a dry run of a fresh import (no progress file, or `--restart`) prints
   the whole plan. `-o json|yaml` prints the plan with the work items left in `entries` and `resumed: {path, done, total,
   finishedAt}` over a progress file, and the whole plan without one.

4. The plan must select the same sources for every recorded work item, and an item with progress must keep its target stage and
   nodes; otherwise exit `6`, no data sent (under `--dry-run` too). This is checked again after the schema is written, so a plan
   that changes in between exits `6` with the schema written. An item the TTL gate now refuses is listed as refused and keeps its
   progress. An item that is not in the recorded plan because a gate refused it then and accepts now (say with `--force-ttl`) has
   no progress, so nothing of it can be stored twice: the summary lists it under `NEW <n> work item(s) a gate refused when the plan
   was recorded are accepted now; they are added to the recorded plan and imported:`, and the real run appends it to the plan
   before pushing (`plan: <n> work item(s) a gate refused when the plan was recorded are added to it`). Only an item not in the
   recorded plan whose progress the file records exits `6`. What is left is printed next.
5. With `--resume` (or `BYDBCTL_IMPORT_RESUME=true`) the import continues, printing `resume: <file> is continued without asking
   (--resume|BYDBCTL_IMPORT_RESUME)`; otherwise, when stdin is a terminal, it asks `Resume? [y/N]` on stdout, or on stderr when stdout is redirected and stderr is a terminal, after the summary is written there as well (with `-o json|yaml` it asks on stderr, which must then be a terminal, below the summary that went there). An answer other than
   `y`/`yes` exits `1` with `not resumed; <file> and the files it records are left in place` and the same way to start over as
   above; Ctrl-C at the question exits `3`. A finished import (including one with newly accepted items, which are imported
   without asking), a refused plan and a dry run stop before the question.
6. The schema is not written again when it compares `OK`; the plan is still re-checked.
7. The ambiguous payloads are resolved once, before anything is pushed, and `RESUMED   <n> of <m> work item(s) done by the
   previous run(s); <k> left` is printed; then what is left is pushed: finished work items, native parts and indexes that landed
   everywhere and CSV rows up to the acknowledged position are skipped, a native payload that missed some nodes is pushed to
   those nodes only, and CSV rows after the sent position are pushed as usual.
8. `--verify` compares against the baseline and the rows of every run together ([Verify](#verify)).

#### A finished import

An import counts as finished once a run pushed its last work item and no work item of the plan is left. Started again, it asks no
`Resume?` question and prints the `FINISHED` summary — the `DONE` line and the table of what is done, with no `LEFT` or
`AMBIGUOUS` lines and, as nothing is left, no plan table:

```text
FINISHED ./backup-20260914/.import-progress.json  (finished at 2026-11-12T19:41:07+09:00, 2h3m ago)
DONE     2 group(s), 164 work item(s), 328 file(s), 111.6 GiB
CATALOG  GROUP      STAGE  ITEMS  FILES  SIZE
measure  sw_metric  hot    48     96     25.4 GiB
stream   sw_record  hot    116    232    86.2 GiB
```

Once both layers of the [target check](#target-check) passed, it prints `already imported into this cluster at <RFC3339 time>
(liaisons <names>, data nodes <names>); nothing is pushed (--restart imports it again)` and exits `0` without pushing, unless it
verifies. One with work items a gate refused then and accepts now, say with `--force-ttl`, is not `already imported`: it prints
`the import was finished at <time>; the <n> work item(s) a gate refused then and accepts now are imported`, appends them to the
plan (the progress file no longer records the import as finished until they are pushed) and pushes only them, without asking,
like a resume. When a work item of the recorded plan is left instead (a gate refused it on the run that finished the import), it
may have payloads in flight: the run is a resume like any other, which asks first, or needs `--resume` when not at a terminal.

A finished import verifies again only:

- when the `--verify` flag is given on the command line (`import.verify` and `BYDBCTL_IMPORT_VERIFY` set a default for every
  run, not a request to verify a finished import again);
- when the run that finished the push was to verify and no verify has passed since (it failed with exit `4` or was
  interrupted): it prints `verify: on, since no verify of this import has passed yet`; `--verify=false` turns that off;
- when payloads that `skip` left out have not been verified yet, `--verify=false` notwithstanding.

Such a verify prints `verify: against the baseline the first run recorded (the baseline was taken at <time>); rows written into
these groups since by anything else count as mismatches` and re-checks the per-node counts; when they pass it clears the pending
verify and the skipped payloads. A failure exits `4` again. Otherwise, with `--verify` on only by default, it prints `NOTE verify skipped: the
baseline was taken at <time>, and whatever was written into these groups since would count as a mismatch; give --verify
explicitly to check against it anyway`, querying nothing. A first run that recorded no baseline makes such a verify exit `4`.

#### Dry run over a progress file

`--dry-run` with a progress file prints the summary, checks the target and compares the plan (exit `6` when either refuses),
prints the plan of what is left and writes nothing; over a finished import it prints the `already imported` line and exits `0`.
With `-o json` or `-o yaml` the plan of a dry run or of a real run over a progress file lists the work items left in `entries`
and has `resumed: {path, done, total, finishedAt}` (`finishedAt` only when the progress file records one). The table output is
only the table of what is left, after the summary: it has no `TARGET`, `SOURCE` or `RESULT` line, and no table when nothing
is left. `--resume-duplication-policy` is used by a dry run: the summary names the policy of every ambiguous
payload. A dry run ignores `--resume`, so `--dry-run --resume --restart` is accepted.

#### Starting over

`--restart` (the flag only: no variable, like `--force`, and no plan field) imports from scratch although a progress file
exists, which it does not read. The `--restart` flag with the `--resume` flag is exit `1` (they exclude each other), unless
`--dry-run` or `--schema-only` ignores `--resume`; a resume from `BYDBCTL_IMPORT_RESUME` yields to `--restart`.

The old progress file is set aside only when the new one is about to be created, after the schema write and the re-check of the
plan, so a run refused or failing before keeps it in place. Its side files are renamed first, to
their own names plus `.<stamp>.bak` (`<file without .json>-cursor.json.<stamp>.bak` and `-plan.json.<stamp>.bak`; the side files of a version 1 file, `<file>.plan`,
`.cursor`, `.baseline` and `<file without .json>-baseline.json`, with them), then the progress file to `<file>.<stamp>.bak`, with one stamp `<YYYYMMDDTHHMMSSZ>`
(UTC), `-<n>` appended when a `.bak` of that stamp exists already; the progress file stays in place until its side files are kept. The fresh import runs after `WARN --restart: <file> is kept as
<bak> and the import starts over; <what>`, where `<what>` describes the old progress file, read best-effort: `the stream and trace
rows an earlier run already imported are stored twice, in stream sw_stream, trace sw_trace` for the stream and trace groups it
records rows or payloads of (a native artifact says `parts and element indexes` instead of `rows`; none of them says `nothing of
this plan is stored twice: measures and properties overwrite what is there`); `it records nothing pushed yet, so nothing is stored
twice`; `it records the import of another artifact, target or options, so nothing it records is stored twice by this one (it
differs in: ...)`; or, for a file that cannot be read, `it cannot be read (...), so what it records is unknown: if an earlier run
imported this artifact into this cluster, ... may be stored twice ...`. Emptying or recreating the target first avoids storing
anything twice; measures and properties converge anyway.

`--dry-run --restart` reads no progress file: it previews the plan of a fresh import, with `NOTE --restart: the plan below is that
of a fresh import, <file> is not read; the real run keeps it as <file>.<timestamp>.bak` when one exists (whatever import it
records), and writes nothing.

Another `--checkpoint` path also starts from scratch, keeping the old progress file as it is; it stores the stream and trace data
already imported twice the same way. Use it to import the artifact into another cluster, or with other `--all-sources` /
`--include-schema` (which exit `6` against the old file).

#### Ambiguous payloads

What happens to a payload whose answer was lost (it was in flight, or a target failed it after its completion was sent):

| Payload | At resume |
|---|---|
| native stream / trace part | reconciled from the target's counts: landed → done; not landed → pushed to that node; undecidable → `resumeDuplicationPolicy` |
| native measure part, series index, property shard | pushed again: they converge |
| native element index | `resumeDuplicationPolicy`, except that `skip` becomes `block`: `--verify` does not count element index documents |
| native trace part of a group sampling spans in merges | `resumeDuplicationPolicy`, except that `skip` becomes `block`: `--verify` cannot count what it would leave out |
| CSV measure, index-mode measure, property rows after the position | pushed again |
| CSV stream / trace rows after the position up to the sent one | `resumeDuplicationPolicy` (`skip` becomes `block` for a trace group sampling spans in merges) |
| CSV rows after the sent position | pushed: they were never sent; an item with nothing sent after its position is not ambiguous |
| CSV stream / trace rows after the position up to the item's recorded send limit, when the cursor file (`-cursor.json`) was lost or left older by a power loss or an OS crash | `resumeDuplicationPolicy`, as for the sent ones; the summary adds `NOTE the cursor side file is missing, empty or older than the progress file; the rows up to the recorded send limit are treated as possibly landed` |

When the first run recorded no baseline, `skip` becomes `block` for every payload: `--verify` has nothing to compare against. A
payload whose `skip` was refused names why after its source (`<source>; skip is refused: <why>`), and the exit `7` then asks for
`--resume-duplication-policy warn|push (skip stays refused where a line says so)`.

#### Reconciliation

A native stream or trace part whose answer was lost is decided from the target's counts: the rows the node counts in the part's
(segment, shard), compared with the baseline plus the parts recorded as landed there. No growth means not landed (pushed to that
node), growth by the part's rows means landed (done), anything else is undecidable and follows the
[duplication policy](#duplication-policy). The counts are read up to five times, 5 s apart, until they stop moving. A node the
baseline did not count, a (segment, shard, node) where an earlier part was pushed again or left out, and a node the liaison
stopped waiting for (`failure_cause` `NO_ANSWER`, recorded in the progress file's `causes` as `no_answer`) whose count did not grow are undecidable too (a cause this client does not know, such as one a newer client wrote in the file, is taken as none: the counts decide); so is every part of a trace group whose sampling
pipeline may drop spans in merges (an enabled sampler with the `MERGE` or `FINALIZE` event). Each decision is logged as
`reconcile <item> part <part id> of <source node> on <node>: landed|not landed|undecidable (<expected>→<actual>, part <n> rows)`,
an undecidable one followed by `: <reason>`.

**Keep the target quiet.** Reconciliation compares row counts, so from the first count until the import ends, resumes included, nothing else may write to or delete from the imported groups on the target: no other import, no application writes, no data removed by hand. Most violations are noticed: the counts grow by neither 0 nor the part's rows, or a segment the count expected is gone, so the part is undecidable and follows the duplication policy. Two cases are not noticed, and `--resume-duplication-policy block` does not stop them either, since the part counts as decided:

- Others' net change at the part's (segment, shard, node) equals the part's rows: a part that did not land is taken as landed and not pushed, so its rows are missing. A net change of zero, such as others' writes and deletes cancelling, takes a part that landed as not landed: it is pushed again and stored twice.
- Rows disappear from a segment that is still there (retention or lifecycle migration) between the first count and a later one. A segment that vanished entirely is undecidable, but fewer rows in a surviving segment is judged like any other change: normally undecidable, yet when the loss equals the rows of a part that landed, the part is taken as not landed and pushed again.

When the data lies close to the retention boundary of the target stage, avoid leaving the import interrupted for long, and resume with `--resume-duplication-policy block`, so that the parts the counts cannot decide stop the import (exit `7`) for you to decide instead of being pushed again.

The import also reconciles during a run: a stream or trace part that a target node failed after its completion reached it is done
when the node's count grew by its rows; otherwise the import fails (exit `3`) and a resume decides it, since the node may still be
committing the part. An element index that failed that way fails the import at once. A run that stops in the middle of such a
reconciliation leaves the part in flight, and the next resume lists it under `AMBIGUOUS`.

#### Duplication policy

`resumeDuplicationPolicy` decides what resume does with a **stream or trace** payload that reconciliation cannot decide (and with
the CSV stream and trace rows after the position up to the sent one, and native element indexes, which are never reconciled). Measure and property
payloads are always pushed again, whatever the policy says: a second copy converges to the first. `--resume-duplication-policy`
(or `import.resumeDuplicationPolicy` in `plan.yaml`) sets one policy for every stream and trace payload.

| Value | At resume |
|---|---|
| `block` | push nothing, list the payloads, exit `7`; what was imported before is valid |
| `warn` | push again with a `WARN` line naming what it may store twice |
| `push` | push again silently |
| `skip` | leave the payload out (for CSV, the rows after the position up to the sent one; the push goes on after them), count it as landed and turn `--verify` on: a gap fails verify (exit `4`) and the error lists the skipped payloads. The progress file records them, and every later run turns `--verify` on until one passes |

The default is `warn`. `warn` and `push` push a CSV item again from its position; the `WARN` line and the verify tolerance use the
rows between the position and the sent one. `--verify` treats the rows pushed again by `warn` or `push`, and the measure points a
resume pushes again (`re-push`, counted twice until a merge), as a known tolerance: a (group, stage) that grew by up to those rows
more is a verify warning naming the payloads, not exit `4`. A trace group that samples spans in merges is checked partially
(`VERIFY    ok (N group(s) not fully checked)`), since it cannot be compared row for row.

Choosing a policy:

- `warn` (the default) or `push`: nothing is lost; what may have landed is stored twice. Stream and trace keep both copies (a query
  returns the element or span twice), so pick these when an occasional duplicate is acceptable.
- `block`: when no duplicate is acceptable. Resume stops before pushing anything (exit `7`) and lists the payloads with the source of
  their policy. To continue, give `--resume-duplication-policy warn|push|skip` and run the same command again.
- `skip`: when you know the payloads landed. For CSV it leaves out only the rows that were sent after the position (recorded before
  each batch is sent), counts them as pushed and pushes the rest of the item. Only `--verify` (turned on by skip) can tell: a
  gap fails it with exit `4` and lists the skipped payloads. The progress file keeps them, so running the same command again shows
  the `FINISHED` summary and verifies again, but cannot push them; the way back is [starting over](#starting-over). Since a
  skipped payload turns `--verify` on, also in a later run whose policy is no longer `skip`, `--verify-timeout` is accepted with
  `--verify=false` (only `--schema-only` refuses it, `has no effect`) and must be positive whenever data is written.

#### What to do when a resume exits 6

| The message names | What to do |
|---|---|
| `manifestDigest` | the artifact's `manifest.json` changed: import the original artifact, or start over |
| `liaisons` or `targetNodes` | the target shares no liaison or no data node with the recorded one, or a target node of an unfinished work item is no longer a data node of it: import into the first target, give another `--checkpoint` for another cluster, or `--restart` |
| `the target lacks …` or `… that the target does not hold` | the target was reset, or is another cluster with the same node names: give another `--checkpoint` for another cluster, or `--restart` to import into this one again |
| `records no liaisons` | the progress file was written before the liaisons were recorded: give another `--checkpoint`, or `--restart` |
| `options` | run with the first run's `--all-sources` and `--include-schema`, or start over |
| a plan that differs from the recorded one | restore what changed it (the first run's options, the target's stages and nodes), or start over |
| `another version of bydbctl wrote it` | run the same command with the bydbctl that wrote the progress file, or give `--restart` ([A progress file of another bydbctl version](#a-progress-file-of-another-bydbctl-version)) |
| a progress file or a cursor side file that cannot be read or parsed, or a plan side file that is missing or has another digest, or a progress file that records a baseline or pushed rows but names no plan side file | start over |

### Import flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--nodes` | `connection.nodes` in `plan.yaml`, else `BYDBCTL_NODES` | liaison gRPC addresses; every usable one shares the import streams |
| `--plan` | | `plan.yaml` with the connection and the import options |
| `--input` | `import.input`, else `BYDBCTL_IMPORT_INPUT` | the artifact directory `data export` wrote; required |
| `--dry-run` | `BYDBCTL_DRY_RUN`, else `false` | print the plan and write nothing: `0` = it would proceed (or the progress file records it as already imported), `2` = it would be refused, `1` = the progress file cannot be written where it would go, `6` = the progress file records another import, is unusable, or the target or the plan no longer matches what it records ([What to do when a resume exits 6](#what-to-do-when-a-resume-exits-6)) |
| `-o`, `--output-format` | `BYDBCTL_OUTPUT_FORMAT`, else `table` | rendering of the plan: `table`, `yaml` or `json` |
| `--schema-only` | `import.schemaOnly`, else `BYDBCTL_IMPORT_SCHEMA_ONLY`, else `false` | write the schema only, without data and without a progress file |
| `--include-schema` | `import.includeSchema`, else `BYDBCTL_IMPORT_INCLUDE_SCHEMA`, else `true` | `false` neither compares nor writes the artifact's schema |
| `--on-schema-conflict` | `import.schemaConflictPolicy.default`, else `BYDBCTL_IMPORT_ON_SCHEMA_CONFLICT`, else `error` | default handling of a schema that differs from the target: `error`, `ignore` or `overwrite`; rules in `plan.yaml` win over it |
| `--force` | `false` | confirm an `overwrite` of a group that holds data |
| `--force-ttl` | `false` | import units whose data the target's TTL has already expired |
| `--all-sources` | `false` | push every source of a shard instead of the minimal covering set |
| `--strict-coverage` | `BYDBCTL_IMPORT_STRICT_COVERAGE`, else `false` | refuse (exit `2`) a work item that would be imported from more than one source |
| `--abort-on-plan-change` | `false` | exit `5` when the plan re-checked after the schema write differs (the schema is written, no data sent) |
| `--verify` | `import.verify`, else `BYDBCTL_IMPORT_VERIFY`, else `true` | check the target after the push ([Verify](#verify)); over a finished import only the flag on the command line verifies again |
| `--verify-timeout` | `import.verifyTimeout`, else `BYDBCTL_IMPORT_VERIFY_TIMEOUT`, else `2m` | how long `--verify` polls for the written rows to show up; must be positive whenever data is written |
| `--parallelism` | `import.parallelism`, else `BYDBCTL_IMPORT_PARALLELISM`, else `max` | work items imported concurrently: `max` (the target data nodes) or an integer >= 1 |
| `--send-retry-timeout` | `import.sendRetryTimeout`, else `BYDBCTL_IMPORT_SEND_RETRY_TIMEOUT`, else `15m` | how long to keep reopening a stream the liaison refused as unavailable or busy (next liaison) or for a schema not live yet (same liaison) |
| `--write-batch-size` | `import.writeBatchSize`, else `BYDBCTL_IMPORT_WRITE_BATCH_SIZE`, else `1000` | CSV rows sent in one batch, `1` to `10000` |
| `--write-batch-bytes` | `import.writeBatchBytes`, else `BYDBCTL_IMPORT_WRITE_BATCH_BYTES`, else `4MiB` | bytes of rows sent in one batch, `64KiB` to `64MiB` |
| `--write-inflight-batches` | `import.writeInflightBatches`, else `BYDBCTL_IMPORT_WRITE_INFLIGHT_BATCHES`, else `2` | batches a worker keeps sent and not yet acknowledged per stream, `1` to `8` |
| `--write-rate-limit` | `import.writeRateLimit`, else `BYDBCTL_IMPORT_WRITE_RATE_LIMIT`, else `0` | CSV only: rows per period across the import, e.g. `50000/s`; `0` = unlimited |
| `--liaison-rate-limit` | `connection.rateLimit`, else `BYDBCTL_LIAISON_RATE_LIMIT`, else `0` | CSV only: bytes per period each liaison receives from this import |
| `--node-rate-limit` | `import.nodeRateLimit`, else `BYDBCTL_IMPORT_NODE_RATE_LIMIT`, else `0` | CSV only: bytes per period each target data node admits from the imports, however many liaisons write to it |
| `--node-row-rate-limit` | `import.nodeRowRateLimit`, else `BYDBCTL_IMPORT_NODE_ROW_RATE_LIMIT`, else `0` | CSV only: rows per period each target data node admits from the imports |
| `--part-rate-limit` | `import.partRateLimit`, else `BYDBCTL_IMPORT_PART_RATE_LIMIT`, else `0` | native only: total byte rate of the part files the import sends, e.g. `50MiB/s`; `0` = unlimited |
| `--part-chunk-size` | `import.partChunkSize`, else `BYDBCTL_IMPORT_PART_CHUNK_SIZE`, else `1MiB` | native only: bytes of part files sent in one frame, `64KiB` to `4MiB`; out of range is exit `1` |
| `--checkpoint` | `import.checkpoint`, else `BYDBCTL_IMPORT_CHECKPOINT`, else `<input>/.import-progress.json` | the progress file ([Import resume](#import-resume)); one outside the input directory must be given again on every run; a directory, the `--input` directory itself, a file of the artifact (`manifest.json` or a file it lists) or a name that ends in `-plan.json` or `-cursor.json` is exit `1` |
| `--checkpoint-every-batches` | `import.checkpointEveryBatches`, else `BYDBCTL_IMPORT_CHECKPOINT_EVERY_BATCHES`, else `64` | CSV only: acknowledged batches between two synced progress-file writes, `1` to `10000` |
| `--resume` | `BYDBCTL_IMPORT_RESUME`, else `false` | continue an interrupted import without asking; the flag without a progress file at the path exits `1`, the variable without one starts a fresh import; ignored by `--dry-run` and `--schema-only`, which therefore accept it together with `--restart` |
| `--restart` | `false` (no variable, no plan field) | keep the progress file and its side files as `.bak` files and import from scratch ([Starting over](#starting-over)); excludes the `--resume` flag except under `--dry-run` or `--schema-only` (a resume from the variable yields to it); with `--dry-run`, preview the fresh import without reading the progress file; ignored by `--schema-only` |
| `--resume-duplication-policy` | `import.resumeDuplicationPolicy`, else `BYDBCTL_IMPORT_RESUME_DUPLICATION_POLICY`, else `warn` (the help says `(default "warn")`) | the policy of the stream and trace payloads: `block`, `warn`, `push` or `skip` ([Duplication policy](#duplication-policy)) |
| `--enable-tls`, `--insecure`, `--cert` | `connection.nodesTLS` in `plan.yaml`, else `BYDBCTL_ENABLE_TLS` / `BYDBCTL_INSECURE` / `BYDBCTL_CERT` | TLS towards the liaisons |
| `--logging-level`, `--logging-env` | `BYDBCTL_LOGGING_LEVEL` / `BYDBCTL_LOGGING_ENV`, else `info`, `prod` | client-side logging |

Rates take the same `<quantity>[/<period>]` forms as the export's. `--dry-run` ignores the settings that only shape the writes and
names those given explicitly in a `NOTE` line. `plan.yaml` accepts the `connection` keys and the `import.*` keys above (`import.workDir`
is rejected), with the export keys in the same file:

```yaml
connection:
  nodes: ["liaison-1:17912"]
import:
  input: ./backup-20261010
  checkpoint: /var/lib/bydbctl/import-progress.json  # default <input>/.import-progress.json
  checkpointEveryBatches: 64                         # CSV, 1 to 10000
  verify: true
  verifyTimeout: 2m
  parallelism: max
  schemaConflictPolicy:
    default: error
  resumeDuplicationPolicy: warn                      # block | warn | push | skip
```

### Import exit codes

| Code | Meaning |
|------|---------|
| `0` | success, `--verify` included when on; warnings, and work items refused by the TTL gate, do not change it. A finished import run again exits `0` too (`already imported`) |
| `1` | usage error (also `--resume` with `--restart`, or `--resume` without a progress file), a progress file that cannot be written (checked before anything is written), another `bydbctl` holding its lock, or a resume that was not confirmed (no `--resume` and no terminal, or not answered `y`) |
| `2` | refused before anything was written: the preflight, the schema gate, a gate listed under `PROBLEMS` (the row check, a corrupt artifact, the native gates), every work item refused, or `--strict-coverage` |
| `3` | failure after writing started (the schema write, the push, Ctrl-C or SIGTERM, a progress file that cannot be written, a baseline needed by `--verify` or a reconciliation that did not count every target node); the progress file is kept, so the same command resumes; also Ctrl-C at the `Resume?` question, and a [target check](#target-check) that a node did not answer (nothing written) |
| `4` | `--verify` failed: the per-node counts do not match the rows sent, the check could not finish, or there is no baseline because the first run recorded none. A verify that failed stays pending: the next run of the same command, also over a finished import, verifies again until one passes (`--verify=false` turns that off, except while payloads that `skip` left out are unverified) |
| `5` | `--abort-on-plan-change`: the plan re-checked after the schema write differed; the schema is written, no data was sent |
| `6` | resume impossible ([What to do when a resume exits 6](#what-to-do-when-a-resume-exits-6)): the progress file belongs to another artifact, target or options, the target looks reset, the plan differs from the recorded one, or a progress or side file is unusable. No data was sent and nothing was deleted; the schema may be written only when the plan changed after the schema write |
| `7` | halted by `resumeDuplicationPolicy: block` before anything was pushed; what was imported before is valid. The message asks for `--resume-duplication-policy warn\|push\|skip`, or `warn\|push (skip stays refused where a line says so)` |

### Import permissions

With RBAC enabled `data import` needs `cluster:read` (preflight), schema read and write on the imported groups, `cluster:admin` for the
liaison's `ImportService` and for the session-less `ExportService.Plan` that the baseline, the reconciliation and `--verify` count
with. `--dry-run` writes nothing. See
[Permissions](#permissions) for the credentials and TLS.
