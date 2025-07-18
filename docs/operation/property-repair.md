# Property Background Repair Observability

Based on the [Property Background Repair Strategy documentation](../concept/property-repair.md), 
this article explains how to visualize and monitor each synchronization cycle to enhance observability and debugging.

This feature is enabled by default. You can configure whether to record the data through the `--property-repair-obs-enabled` option.

## Tracing

Tracing is used to record the operation flow at each node during gossip-based Property repair. 
This allows for fast and accurate diagnosis in the event of issues or inconsistencies during the repair process.

All trace data is written to the `_property_repair_spans` stream group and can be queried for inspection and analysis.

| Field Name        | Type     | Description                                                                          |
|-------------------|----------|--------------------------------------------------------------------------------------|
| `tracing_id`      | `string` | Unique trace ID for the entire repair task, typically `sender_node_id + start_time`. |
| `span_id`         | `int`    | Unique span ID within the trace.                                                     |
| `parent_span_id`  | `int`    | ID of the parent span (0 if root span).                                              |
| `current_node`    | `string` | ID of the node where the span was executed.                                          |
| `start_time`      | `int64`  | Unix timestamp of span start (in milliseconds).                                      |
| `end_time`        | `int64`  | Unix timestamp of span end (in milliseconds).                                        |
| `duration`        | `int64`  | Total duration of the span, in milliseconds.                                         |
| `message`         | `string` | Descriptive log of the action performed in this span.                                |
| `tags_json`       | `string` | JSON-formatted key-value tags attached to the span (e.g. group, shard\_id).          |
| `is_error`        | `bool`   | Whether this span encountered an error.                                              |
| `error_reason`    | `string` | Error message or failure reason if `is_error` is true.                               |
| `sequence_number` | `int`    | Which round number of gossip propagation.                                            |

All Property-related metadata is encapsulated within the `tags_json` field of each trace span. 
This allows for flexible and structured logging of contextual information. The following are **key fields** commonly included in `tags_json`:

| Field Name     | Description                                                                                                |
|----------------|------------------------------------------------------------------------------------------------------------|
| `target_node`  | The ID of the target node involved in the current operation.                                               |
| `group_name`   | The name of the Property group being synchronized.                                                         |
| `shard_id`     | The shard identifier being processed.                                                                      |
| `operate_type` | The type of operation being performed, such as `"send_summary"`, `"compare_leaf"`, or `"update_property"`. |
| `property_id`  | The identifier of the specific Property being updated or compared.                                         |


### Data TTL

By default, the system automatically retains all background repair records for **three days**.
This retention period can be configured via the `--property-repair-history-days` option.

## Metrics

All metrics are reported through the internal self-observation system and can be queried using standard tools as described in 
the [Observability documentation](./observability.md).

In the context of Property background repair, the following key metrics are exposed:

| Metric Name                             | Type                | Description                                                                                                           |
|-----------------------------------------|---------------------|-----------------------------------------------------------------------------------------------------------------------|
| `property_repair_success_count`         | Counter             | Total number of Properties successfully repaired across all nodes.                                                    |
| `property_repair_failure_count`         | Counter             | Total number of Properties that failed to repair due to validation, write errors, or version conflicts.               |
| `property_repair_gossip_abort_count`    | Counter             | Total number of gossip repair sessions that were forcefully aborted due to unrecoverable errors or unavailable peers. |
| `property_repair_trigger_count`         | Counter             | Total Number of times the repair process was triggered (either scheduled or event-based).                             |

