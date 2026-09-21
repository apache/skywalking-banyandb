# Logging

BanyanDB uses the [zerolog](https://github.com/rs/zerolog) library for logging. The log level can be set using the `logging-level` flag. The supported log levels are `debug`, `info`, `warn`, `error`, and `fatal`. The default log level is `info`.

Every shipped BanyanDB binary accepts the same four logging flags: the server (`standalone`, `data`, `liaison`), `backup`, `restore`, `lifecycle`, `migration`, the FODC proxy and the FODC agent. Each flag is also bound to an environment variable named after it, prefixed with `BYDB_` and upper-cased with dashes replaced by underscores, which is the usual way to configure them in a container:

| Flag | Environment variable |
|---|---|
| `--logging-level` | `BYDB_LOGGING_LEVEL` |
| `--logging-env` | `BYDB_LOGGING_ENV` |
| `--logging-modules` | `BYDB_LOGGING_MODULES` |
| `--logging-levels` | `BYDB_LOGGING_LEVELS` |

A process logs a little before the configured logger exists: the command tree registers its
flags, and `GOMAXPROCS` is resolved. Those early lines honor `--logging-level` / `--logging-env`
and their environment variables all the same -- the level is read straight from the command line
and the environment at that point, with the flag winning over the environment, as everywhere else.
Only `--logging-modules` and `--logging-levels` are not applied that early; they take effect once
the logger is initialized. Without any of them the early lines stay at `debug`, which is the
historical behavior. The ASCII banner is printed directly to the console and is not a log line,
so no level silences it.

`logging-env` is used to set the logging environment. The default value is `prod`. The logging environment can be set to `dev` for development or `prod` for production. The logging environment affects the log format and output. In the `dev` environment, logs are output in a human-readable format, while in the `prod` environment, logs are output in JSON format.

`logging-modules` and `logging-levels` are used to set the log level for specific modules. The `logging-modules` flag is a comma-separated list of module names, and the `logging-levels` flag is a comma-separated list of log levels corresponding to the module names. The log level for a specific module can be set using these flags. Modules on the server include `storage`, `distributed-query`, `liaison-grpc`, `liaison-http`, `measure`, `stream`, `trace`, `metadata`, `property-schema-registry`, `metrics`, `pprof-service`, `query`, `server-queue-sub`, `server-queue-pub`. The other binaries name theirs differently: `migration` in `banyand-migration`, `fodc-proxy` in the FODC proxy, and `fodc`, `server` and `watchdog` in the FODC agent.

Those lists are not exhaustive. A module name is whatever scope the code passes to `logger.GetLogger`, and every log line carries it in its `module` field, so reading one off a log line is the reliable way to find the name you need. Matching is case-insensitive. For example, to set the log level for the `storage` module to `debug`, you can use the following flags:

```sh
--logging-modules=storage --logging-levels=debug
```

## Native Self-Storage

BanyanDB can store its own log events in BanyanDB, alongside the measures native observability already writes into `_monitoring`. It is off by default and adds a second destination rather than replacing the normal one, which stays enabled and is never degraded by it.

```sh
banyand standalone --logging-native-enabled --logging-native-level=info
```

Events land in the `_monitoring_log` group as elements of a stream named `log`, queryable through the ordinary stream API:

```sh
bydbctl stream query -f - <<EOF
name: "log"
groups: ["_monitoring_log"]
projection:
  tagFamilies:
    - name: "searchable"
      tags: ["node_id", "module", "level", "message"]
EOF
```

### What is stored where

A log line is split across two tag families, and nothing is stored twice.

| Tag family | Tags | Holds |
|---|---|---|
| `searchable` | `node_id`, `node_type`, `module`, `level`, `grpc_address`, `http_address`, `message`, `log_id` | the keys with a tag of their own |
| `data` | `fields` (binary) | **everything else on the line**, as a JSON object; absent when the line has no extra keys |

The split matters when you go looking for a value. A call site that writes

```go
l.Warn().Str("group", "sw_metric").Dur("took", d).Msg("flush took longer than expected")
```

stores `message` in `searchable` and `{"group":"sw_metric","took":1200}` in `fields`. The query above would not show `group` at all — project the `data` family to get it:

```sh
bydbctl stream query -f - <<EOF
name: "log"
groups: ["_monitoring_log"]
projection:
  tagFamilies:
    - name: "searchable"
      tags: ["node_id", "module", "level", "message"]
    - name: "data"
      tags: ["fields"]
EOF
```

`fields` is a binary tag holding JSON, so it is returned as an opaque blob and is not filterable. A call site that adds a new key needs no schema change for it to be kept.

### Filtering

`node_id` and `level` are the entity, so filtering on them selects series directly. Entity tags accept only `=` and `IN`, which is why several levels are selected with `IN` rather than by negating one.

Every other tag is filterable but **unindexed** in this version: there are no index rules on the stream, so a condition on `module` or `message` is evaluated after the scan rather than through a posting list. Narrow the time range and the entity first.

`MATCH` on `message` does **not** do what it does elsewhere. An analyzer comes from an index rule, and this stream has none, so the value is compared whole instead of being tokenized: searching for `timeout` will not find `write timeout exceeded`. The condition does not error — it just fails to match — so an empty result may be this rather than an absence of logs. Use `=` on the full message, or project `message` and filter outside the database.

The flags live in their own namespace, `--logging-native-*` with `BYDB_LOGGING_NATIVE_*`, which inherits nothing from `--logging-*`. An explicit flag beats its environment variable, as everywhere else.

| Flag | Default | Meaning |
|---|---|---|
| `--logging-native-enabled` | `false` | store this process's own logs |
| `--logging-native-level` | `info` | minimum level reaching storage, independent of `--logging-level` |
| `--logging-native-exclude-modules` | built-in set | module prefixes never stored; replaces the built-in set rather than adding to it |
| `--logging-native-flush-interval` | `1s` | longest a buffered event waits |
| `--logging-native-flush-size` | `100` | buffered events that trigger a write ahead of the interval |
| `--logging-native-max-bytes` | `32mb` | cap on the buffer |
| `--logging-native-max-event-bytes` | `64kb` | larger events are dropped whole rather than truncated |
| `--logging-native-shard-num` | `2` | shards of `_monitoring_log`; raisable later through the group schema |
| `--logging-native-ttl-days` | `7` | retention |

Because the two destinations have independent thresholds, native can be the more verbose of the two. Running normal logging at `error` and native at `info` keeps stderr quiet while the database retains the `info` and `warn` events that describe what a node was doing beforehand:

| Event | Normal logging | Native storage |
|---|---|---|
| `debug` | – | – |
| `info`, `warn` | dropped | stored |
| `error` | printed | stored |

Note the consequence: an `info` event dropped by the buffer has no copy on stderr. Losses are counted rather than silent, under `banyandb_logging_native_log_dropped_total{reason}`, with `banyandb_logging_native_log_written_total` and the buffer gauges alongside. Those counters travel the same transport as the events they count, so keeping `--observability-modes=prometheus` enabled is what makes a loss visible during an outage.

The `reason` label takes one of eight values, so a loss is always attributable to a stage:

| `reason` | Meaning | What to do |
|---|---|---|
| `buffer_full` | the buffer was at its cap | raise `--logging-native-max-bytes`, or raise `--logging-native-level` to admit less |
| `memory_pressure` | the adaptive budget was exhausted | the node is short of memory; this is the sink yielding, as intended |
| `oversize_event` | one event exceeded `--logging-native-max-event-bytes` | raise it, or shorten the log line |
| `encode_failed` | the line was not the JSON the sink expects | a bug; report it with the module name |
| `publish_failed` | the batch did not reach storage | look at the write path — the whole batch is lost, never re-queued |
| `schema_unavailable` | the group or stream could not be created yet | usually transient at startup; retried every 10s |
| `schema_incompatible` | a `log` stream exists with a shape this version cannot write | drop and recreate the `_monitoring_log` group; retrying will not fix it |
| `shutdown_deadline` | still buffered when the drain deadline passed | expected on a busy node during shutdown |

The modules on the write path the sink publishes through are never stored: admitting them would let one stored line produce the next. The buffer is bounded and in-memory only -- no queue files, no write-ahead log, no disk fallback -- and it never blocks the caller: over budget the newest event is dropped and counted, so a burst keeps the head that explains it.

`restore` and `migration` do not offer these flags at all. Both run when the data tier is unavailable, and a tool that runs while the database is down cannot log into it.

## Slow Query Logging

BanyanDB supports slow query logging. The `slow-query` flag is used to set the slow query threshold. If a query takes longer than the threshold, it will be logged as a slow query. The default value is `0`, which means no slow query logging. This flag is only used for the data and standalone servers.

The `dst-slow-query` flag is used to set the distributed slow query threshold. This flag is only used for the liaison server. The default value is `5s`; set it to `0` to disable distributed slow query logging.

When query tracing is enabled, the slow query log won't be generated.

> **What these two logs contain.** Both `slow-query` and `dst-slow-query` write the entire query request, including the tag filter values it was executed with. Those values come from the client and may be user data. There is no redaction option on these two flags — if that matters for your deployment, control it by raising the threshold or by routing the log somewhere with appropriate access.

### BydbQL Slow Queries

BydbQL queries are tracked separately, because they are parameterized: the query text is a reusable template and the values arrive alongside it as `?` parameters. Instead of logging every slow occurrence, the liaison keeps a bounded top-K of the slowest templates and dumps it periodically, so a hot bad query is reported once per interval rather than once per request. `--bydbql-slow-query-threshold` sets what counts as slow and `--bydbql-topk-log-interval` sets how often the list is dumped.

Because the template and its values are separate here, how much of the values reaches the log is a choice — `--bydbql-topk-param-mode`:

| Mode | `last_params` field | Reveals |
|---|---|---|
| `none` | absent | nothing |
| `fingerprint` (default) | `str(len=12):fp=1a2b3c4d` | the length, and *whether two slow queries used the same value* — not the value |
| `raw` | `"checkout-svc"` | the value itself |

Numeric, timestamp and null parameters render verbatim under both `fingerprint` and `raw`: time-window width, `LIMIT` and thresholds are usually what explains why a query is slow, and they carry no user-identifying content. Only `str` and `str_array` are subject to the mode; binary is always digested and never rendered verbatim, not even under `raw`.

The reported sample is the **most recent** slow occurrence of that template, which is why the field is named `last_params` and not `params` — it is not necessarily the occurrence that produced `max_latency`.

For the complete flag list and the caveats on the fingerprint (it is unsalted, so it is not a cryptographic guarantee), see [Configuration](../configuration.md). If you need the full, unredacted parameters of a query, enable the query access log (`--enable-query-access-log`) instead: it records every request in full, in its own file, with its own retention and permissions.
