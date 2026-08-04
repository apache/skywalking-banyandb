# Logging

BanyanDB uses the [zerolog](https://github.com/rs/zerolog) library for logging. The log level can be set using the `log-level` flag. The supported log levels are `debug`, `info`, `warn`, `error`, and `fatal`. The default log level is `info`.

`logging-env` is used to set the logging environment. The default value is `prod`. The logging environment can be set to `dev` for development or `prod` for production. The logging environment affects the log format and output. In the `dev` environment, logs are output in a human-readable format, while in the `prod` environment, logs are output in JSON format.

`logging-modules` and `logging-levels` are used to set the log level for specific modules. The `logging-modules` flag is a comma-separated list of module names, and the `logging-levels` flag is a comma-separated list of log levels corresponding to the module names. The log level for a specific module can be set using these flags. Available modules are `storage`, `distributed-query`, `liaison-grpc`, `liaison-http`, `measure`, `stream`, `trace`, `metadata`, `property-schema-registry`, `metrics`, `pprof-service`, `query`, `server-queue-sub`, `server-queue-pub`. For example, to set the log level for the `storage` module to `debug`, you can use the following flags:

```sh
--logging-modules=storage --logging-levels=debug
```

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

Numeric, timestamp and null parameters render verbatim under both `fingerprint` and `raw`: time-window width, `LIMIT` and thresholds are usually what explains why a query is slow, and they carry no user-identifying content. Only strings and binary are subject to the mode, and binary is never rendered verbatim.

The reported sample is the **most recent** slow occurrence of that template, which is why the field is named `last_params` and not `params` — it is not necessarily the occurrence that produced `max_latency`.

For the complete flag list and the caveats on the fingerprint (it is unsalted, so it is not a cryptographic guarantee), see [Configuration](../configuration.md). If you need the full, unredacted parameters of a query, enable the query access log (`--enable-query-access-log`) instead: it records every request in full, in its own file, with its own retention and permissions.
