# Analyze the data

`bydbctl` provides a powerful subcommand `analyze` to analyze the data stored in BanyanDB. The `analyze` subcommand supports the following operations:

## Analyze the series

`bydbctl analyze series` to analyze the series cardinality and distribution, which is useful for understanding the performance impact of the series.

Flags:

* `-s` or `--subject`: The name of stream or measure in the series index. If it's absent, the command will analyze all series in the series index.

Arguments:

* `path`: The path of the series index. It's mandatory.

### All series distribution in the series index

```shell
bydbctl analyze series /tmp/measure/measure-default/seg-xxxxxx/sidx
```

The expected result is a csv file with the following content:

```csv
browser_app_page_ajax_error_sum_day, 1
browser_app_page_ajax_error_sum_hour, 1
browser_app_page_dns_avg_day_topn, 1644
....
service_sidecar_internal_resp_latency_nanos_hour, 10
service_sla_day, 7
service_sla_hour, 10
service_traffic_minute, 48
tag_autocomplete_minute, 8
zipkin_service_span_traffic_minute, 31
zipkin_service_traffic_minute, 4
total, 1266040
```

The first column is the series name, and the second column is the cardinality of the series.

The `total` line shows the total number of series in the series index.

### A specific series detail in the series index

```shell
bydbctl analyze series -s endpoint_sla_day /tmp/measure/measure-default/seg-xxxxxx/sidx
```

The expected result is a csv file with the following content:

```csv
...
"bWVzaC1zdnI6OnJhdGluZy5zYW1wbGUtc2VydmljZXM=.1_R0VUOi9zb25ncy8zNi9yZXZpZXdzLzM3",
"bWVzaC1zdnI6OnJhdGluZy5zYW1wbGUtc2VydmljZXM=.1_R0VUOi9zb25ncy8zNy9yZXZpZXdzLzM4",
"bWVzaC1zdnI6OnJhdGluZy5zYW1wbGUtc2VydmljZXM=.1_R0VUOi9zb25ncy8zOC9yZXZpZXdzLzM5",
"bWVzaC1zdnI6OnJhdGluZy5zYW1wbGUtc2VydmljZXM=.1_R0VUOi9zb25ncy8zOS9yZXZpZXdzLzQw",
"bWVzaC1zdnI6OnJlY29tbWVuZGF0aW9uLnNhbXBsZS1zZXJ2aWNlcw==.1_R0VUOi9yY21k",
"bWVzaC1zdnI6OnNvbmdzLnNhbXBsZS1zZXJ2aWNlcw==.1_R0VUOi9zb25ncy90b3A=",
...
```

Each row represents a group of tags which compose a series. They are defined in the `entity` field on `Stream` or `Measure`.

In this example, `endpoint_sla_day`'s `entity` is `entity_id` tag which comes from OAP's internal `endpoint_id`.
