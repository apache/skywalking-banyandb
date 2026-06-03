# Vectorized Measure Query Tracing

Vectorized distributed measure queries use the existing `pkg/query.Tracer` and `common.v1.Trace` tree. Tracing is opt-in through `QueryRequest.trace`; trace-off raw wire responses remain raw frame bytes.

## Span shape

Aggregation path:

```text
distributed-{nodeID}
├── broadcast-agg
│   └── data-{nodeID}
│       ├── scan
│       ├── groupby-agg-map
│       └── frame-encode
├── reduce-raw-frames
├── apply-top-to-reduce optional
└── build-iterator
```

Rows path:

```text
distributed-{nodeID}
├── broadcast-rows
│   └── data-{nodeID}
│       ├── scan
│       ├── groupby-first optional
│       ├── top optional
│       ├── limit optional
│       └── frame-encode
├── merge-distributed-rows
├── apply-groupby-first-to-rows optional
├── apply-batch-top-to-rows optional
└── build-iterator
```

Multi-group rows path:

```text
distributed-{nodeID}
├── build-multi-group-schema
├── broadcast-per-group-{group}
├── merge-distributed-rows-multi
└── build-iterator
```

## Vocabulary

Trace tag keys are exported from `pkg/query/tracelabels` and should be used instead of string literals.

| Constant | Key |
| --- | --- |
| `TagRowsIn` | `rows_in` |
| `TagRowsOut` | `rows_out` |
| `TagBatchesIn` | `batches_in` |
| `TagBatchesOut` | `batches_out` |
| `TagGroupsIn` | `groups_in` |
| `TagGroupsOut` | `groups_out` |
| `TagBytesIn` | `bytes_in` |
| `TagBytesOut` | `bytes_out` |
| `TagDroppedRows` | `dropped_rows` |
| `TagDropReason` | `drop_reason` |
| `TagMode` | `mode` |
| `TagSchemaCols` | `schema_cols` |
| `TagSchemaDegraded` | `schema_degraded` |
| `TagSchemaDegradedTags` | `schema_degraded_tags` |
| `TagSchemaDegradedFields` | `schema_degraded_fields` |
| `TagTypeDivergences` | `type_divergences` |
| `TagNodeCount` | `node_count` |
| `TagNodeErrors` | `node_errors` |
| `TagRespCount` | `resp_count` |
| `TagRounds` | `rounds` |
| `TagSize` | `size` |
| `TagResponseCount` | `response_count` |
| `TagResponseDataPointCount` | `response_data_point_count` |
| `TagLimitN` | `limit_n` |
| `TagLimitOffset` | `limit_offset` |
| `TagTopN` | `top_n` |
| `TagTopAsc` | `top_asc` |
| `TagCalibratedPerNodeLimit` | `calibrated_per_node_limit` |
| `TagMemoryChargedBytes` | `memory_charged_bytes` |
| `TagDedupKeysSeen` | `dedup_keys_seen` |
| `TagDedupCollisions` | `dedup_collisions` |
| `TagBlocksSkipped` | `blocks_skipped` |
| `TagTimeFilterReason` | `time_filter_reason` |
| `TagDecodeNS` | `decode_ns` |
| `TagDecodeNSTotal` | `decode_ns_total` |
| `TagDecodeNSP50` | `decode_ns_p50` |
| `TagDecodeNSP99` | `decode_ns_p99` |
| `TagDecodeNSMax` | `decode_ns_max` |
| `TagFramesIn` | `frames_in` |
| `TagSourcesIn` | `sources_in` |
| `TagGroupName` | `group_name` |
| `TagFramesTotal` | `frames_total` |
| `TagFramesEmittedIndividually` | `frames_emitted_individually` |
| `TagFramesSkipped` | `frames_skipped` |
| `TagMergeHeapPops` | `merge_heap_pops` |
| `TagCoercedColumns` | `coerced_columns` |
| `TagHiddenTagsStripped` | `hidden_tags_stripped` |
| `TagHiddenFieldStripped` | `hidden_field_stripped` |
| `TagOrderByColIdx` | `orderby_col_idx` |
| `TagOrderByFamily` | `orderby_family` |
| `TagOrderByTag` | `orderby_tag` |
| `TagDesc` | `desc` |
| `TagIndexMode` | `index_mode` |
| `TagAggFunc` | `agg_func` |
| `TagAggPartialKind` | `agg_partial_kind` |
| `TagAggValuePath` | `agg_value_path` |
| `TagFrameDecoderVersion` | `frame_decoder_version` |
| `TagBroadcastTimeoutMS` | `broadcast_timeout_ms` |
| `TagAggregatedDataNodeSpans` | `aggregated_data_node_spans` |
| `TagNodesWithErrors` | `nodes_with_errors` |
| `TagNodesWithZeroRows` | `nodes_with_zero_rows` |
| `TagTotalRowsAcrossNodes` | `total_rows_across_nodes` |
| `TagTotalBytesAcrossNodes` | `total_bytes_across_nodes` |
| `TagNodeLatencyNSP50` | `node_latency_ns_p50` |
| `TagNodeLatencyNSP95` | `node_latency_ns_p95` |
| `TagNodeLatencyNSP99` | `node_latency_ns_p99` |
| `TagNodeLatencyNSMin` | `node_latency_ns_min` |
| `TagNodeLatencyNSMax` | `node_latency_ns_max` |
| `TagPlan` | `plan` |
| `TagRequest` | `request` |
| `TagNodeSelectors` | `node_selectors` |
| `TagTimeRange` | `time_range` |
| `TagRespKind` | `resp_kind` |
| `TagFrameBytesTotal` | `frame_bytes_total` |
| `TagErrorMsg` | `error_msg` |
| `TagIgnoredChildSpans` | `ignored_child_spans` |

## Empty-result diagnosis runbook

1. Check `response_data_point_count` on the user-visible response trace/root context.
2. Walk from `broadcast-*` into data-node children and find the first span whose `rows_out` is unexpectedly low.
3. Use `drop_reason` to distinguish `top`, `limit`, `dedup`, `groupby-first`, `time-filter`, and `index-skip`.
4. For aggregation value issues, inspect `agg_value_path` on `groupby-agg-map` or `reduce-raw-frames`.
5. For fanout issues, compare `node_count`, `response_count`, `node_errors`, and summary latency tags.

## Examples

Each example below shows the trace tree returned in `QueryResponse.trace` for a representative query. `(123ms)` is the span `Duration` (rounded). Tag values follow `key=value`. Only the tags relevant to the walk are shown — the actual response contains the full vocabulary.

### Enabling trace

Set `trace=true` on the request:

```bash
bydbctl measure query -f - <<'EOF'
groups:  ["sw_metric"]
name:    "service_cpm_minute"
tagProjection:
  tagFamilies:
    - name: default
      tags: [entity_id]
fieldProjection:
  names: [value]
groupBy:
  tagProjection:
    tagFamilies:
      - name: default
        tags: [entity_id]
  fieldName: value
agg:
  function: AGGREGATION_FUNCTION_SUM
  fieldName: value
timeRange:
  begin: 2026-05-24T00:00:00Z
  end:   2026-05-24T00:05:00Z
trace: true
EOF
```

The response carries `trace: { spans: [ ... ] }` alongside `dataPoints`.

### Example 1 — Healthy aggregation across 3 data nodes

Query: `SUM(value) GROUP BY entity_id` over 5 minutes, 3 data nodes.

```text
distributed-liaison-0 (47ms)  plan="… AggSum on value" node_selectors="{sw_metric:[n1 n2 n3]}"
├── broadcast-agg (38ms)  node_count=3 response_count=3 broadcast_timeout_ms=15000 frame_bytes_total=12480
│   ├── data-n1 (28ms)  resp_kind=raw-frame bytes_out=4160
│   │   ├── scan (18ms)  rows_out=8400 batches_out=66 schema_cols=4
│   │   ├── groupby-agg-map (7ms)  mode=map rows_in=8400 rows_out=42 groups_out=42 agg_value_path=typed
│   │   └── frame-encode (2ms)  rows_out=42 bytes_out=4160 schema_cols=4
│   ├── data-n2 (31ms)  …
│   └── data-n3 (26ms)  …
├── reduce-raw-frames (5ms)  frames_in=3 rows_out=51 groups_out=51 agg_value_path=typed
├── apply-top-to-reduce  (skipped — no Top in request)
└── build-iterator (1ms)
```

**Reading it:** the bottom-up `rows_in/rows_out` chain shows where rows were consumed and where groups were folded; `agg_value_path=typed` on both the data-node `groupby-agg-map` and liaison `reduce-raw-frames` confirms the native numeric value column was used (no FieldValue fallback).

### Example 2 — Empty result via Top.N truncation (Q1 case 2)

Same query plus `top { number: 1 }` and a tight time range that produced very few rows.

```text
distributed-liaison-0 (12ms)  response_data_point_count=0   ← unexpected
└── broadcast-rows (8ms)  node_count=3 response_count=3
    ├── data-n1 (4ms)
    │   ├── scan (3ms)  rows_out=2 batches_out=1
    │   ├── top   (0ms)  top_n=1 top_asc=false rows_in=2 rows_out=1 dropped_rows=1 drop_reason=top
    │   └── frame-encode  rows_out=1
    ├── data-n2 (3ms)   scan rows_out=0  (time range filter)
    └── data-n3 (3ms)   scan rows_out=0
└── merge-distributed-rows  rows_out=1
└── apply-batch-top-to-rows  top_n=1 rows_in=1 rows_out=1
└── build-iterator (1ms)
```

**Diagnosis:** `response_data_point_count=0` would be the user-visible surprise, but in this example the liaison merge returned 1 row. The empty case is when `top_n` plus filtering combine to drop everything — look for any `drop_reason=top` or `drop_reason=limit` where `dropped_rows` is large.

### Example 3 — Aggregation passthrough fallback (Q1 case 4)

Same `SUM` query, but one data node's measure schema still emits the field as a `*modelv1.FieldValue` passthrough wrapper (mid-rollout).

```text
distributed-liaison-0
├── broadcast-agg
│   ├── data-n1
│   │   └── groupby-agg-map  agg_value_path=fieldvalue-fallback   ← look here
│   ├── data-n2
│   │   └── groupby-agg-map  agg_value_path=typed
│   └── data-n3
│       └── groupby-agg-map  agg_value_path=typed
└── reduce-raw-frames  agg_value_path=fieldvalue-fallback   ← worst-of mode
```

**Action:** `agg_value_path=fieldvalue-fallback` means the operator resolved the agg value via the FieldValue passthrough column instead of a native numeric column. This is correct but slower; it usually points to a tag-type migration in flight on node `n1`. If the result also looks numerically off, query the suspect node directly via `bydbctl measure query --node n1 ...` to confirm.

### Example 4 — Schema divergence in multi-group (Q1 case 5)

Query joins two groups whose `service_name` tag has different types across nodes mid-migration.

```text
distributed-liaison-0  response_data_point_count=0
├── build-multi-group-schema (3ms)  groups_in=2 schema_cols=7 schema_degraded=true
│      schema_degraded_tags=["service_name"]
│      type_divergences="service_name:Int@sw_metric→TagValue@traffic"
├── broadcast-per-group-sw_metric
├── broadcast-per-group-traffic
└── merge-distributed-rows-multi  sources_in=6 rows_out=0   ← drop here
```

**Action:** `schema_degraded=true` on `build-multi-group-schema` is the early-warning. `type_divergences` lists the exact (column → type@group) tuples. The merge then sees rows it can't unify and produces 0. Resolution: complete the tag-type migration so both groups carry the same type for `service_name`.

### Example 5 — Fanout asymmetry with > 20 data nodes (Q1 case 7)

Same query over a 25-node fanout. 3 nodes returned errors; 5 returned zero rows.

```text
distributed-liaison-0
└── broadcast-agg  node_count=25 response_count=22 broadcast_timeout_ms=15000
    ├── data-n3   error_msg="dial tcp: …"
    ├── data-n7   error_msg="context deadline exceeded"
    ├── data-n12  error_msg="dial tcp: …"
    ├── data-n4   rows_out=0
    ├── data-n5   rows_out=0
    ├── … (14 more individual children, ordered by descending latency)
    └── data-summary
           aggregated_data_node_spans=6
           nodes_with_errors=0  nodes_with_zero_rows=0
           total_rows_across_nodes=18420  total_bytes_across_nodes=921600
           node_latency_ns_p50=42_000_000  p95=58_000_000  p99=61_000_000
           node_latency_ns_min=11_000_000  node_latency_ns_max=63_000_000
```

**Reading it:** `node_count - response_count = 3` indicates three nodes never responded (the three with `error_msg`). The fanout summary span captures the lower-latency 6 healthy nodes that didn't get individual slots — their aggregate is in `total_rows_across_nodes` and the percentiles. If `nodes_with_zero_rows` on `data-summary` is non-zero, a healthy-looking node was actually empty — a likely indicator of stage / segment misrouting.

### Example 6 — Performance bottleneck identification (Q2)

Same query, all nodes healthy, but the user reports slow query.

```text
distributed-liaison-0 (812ms)   ← total
├── broadcast-agg          (47ms)
│   └── data-n1 / n2 / n3  (peak 31ms)
├── reduce-raw-frames     (740ms)  ← dominates 16× over peers
│      frames_in=3 rows_out=87432  groups_out=87432
│      agg_value_path=typed
│      memory_charged_bytes=210000000
├── apply-top-to-reduce    (12ms)
└── build-iterator          (1ms)
```

**Diagnosis:** the bottleneck is the span with the largest `Duration` — here `reduce-raw-frames` at 740ms. Tags reveal high `groups_out=87432` and `memory_charged_bytes=210MB`, pointing at a high-cardinality `GROUP BY`. Fix by tightening the time range, adding a more selective filter, or capping with `Top.N`.

### Example 7 — Decode-frame summary at high frame count

When the liaison processes > 19 frames (e.g. a wide fanout returning many partials), per-frame decode spans are summarized:

```text
reduce-raw-frames (95ms)  frames_in=100
└── decode-frame-summary
       frames_total=100  frames_emitted_individually=0  frames_skipped=100
       decode_ns_total=84_000_000  decode_ns_p50=820_000  decode_ns_p99=1_280_000  decode_ns_max=1_910_000
```

The summary fully represents every frame's decode duration. `decode_ns_p99 ≫ p50` would point at a single slow decode worth investigating; here the distribution is tight, so decode is not the bottleneck.

## Wire contract

Under raw wire mode, trace-off responses are still opaque raw frame bytes beginning with `RawFrameMagicLeadingByte`. Trace-on responses use the existing `measure.v1.InternalQueryResponse` envelope with `raw_frame_body` and `trace` populated. Proto `data_points` responses under raw mode are rejected loudly by the vectorized collector.
