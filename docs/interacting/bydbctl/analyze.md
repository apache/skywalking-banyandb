# Analyze the data

`bydbctl` provides a powerful subcommand `analyze` to analyze the data stored in BanyanDB. The `analyze` subcommand supports the following operations:

## Analyze the series

`bydbctl analyze series` to analyze the series cardinality and distribution, which is useful for understanding the performance impact of the series.

Flags:

* `-s` or `--subject`: The name of stream or measure in the series index. If it's absent, the command will analyze all series in the series index.

Arguments:

* `path`: The path of the series index. It's mandatory.

Example:

```shell
bydbctl analyze series -s endpoint_sla_day_topn /tmp/measure/measure-default/seg-xxxxxx/sidx
```

This command will analyze the series `endpoint_sla_day_topn` in the series index `/tmp/measure/measure-default/seg-xxxxxx/sidx`.
