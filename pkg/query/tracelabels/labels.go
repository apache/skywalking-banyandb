// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Package tracelabels defines stable query trace tag keys.
package tracelabels

const (
	// TagRowsIn records rows consumed by an operator.
	TagRowsIn = "rows_in"
	// TagRowsOut records rows emitted by an operator.
	TagRowsOut = "rows_out"
	// TagBatchesIn records batches consumed by an operator.
	TagBatchesIn = "batches_in"
	// TagBatchesOut records batches emitted by an operator.
	TagBatchesOut = "batches_out"
	// TagGroupsIn records groups consumed by an operator.
	TagGroupsIn = "groups_in"
	// TagGroupsOut records groups emitted by an operator.
	TagGroupsOut = "groups_out"
	// TagBytesIn records bytes consumed by an operator.
	TagBytesIn = "bytes_in"
	// TagBytesOut records bytes emitted by an operator.
	TagBytesOut = "bytes_out"
	// TagDroppedRows records rows dropped by an operator.
	TagDroppedRows = "dropped_rows"
	// TagDropReason records the reason rows were dropped.
	TagDropReason = "drop_reason"
	// TagMode records an operator mode.
	TagMode = "mode"
	// TagSchemaCols records schema column count.
	TagSchemaCols = "schema_cols"
	// TagSchemaDegraded records whether schema degradation occurred.
	TagSchemaDegraded = "schema_degraded"
	// TagSchemaDegradedTags records degraded tag names.
	TagSchemaDegradedTags = "schema_degraded_tags"
	// TagSchemaDegradedFields records degraded field names.
	TagSchemaDegradedFields = "schema_degraded_fields"
	// TagTypeDivergences records schema type divergences.
	TagTypeDivergences = "type_divergences"
	// TagNodeCount records broadcast node count.
	TagNodeCount = "node_count"
	// TagNodeErrors records broadcast node error count.
	TagNodeErrors = "node_errors"
	// TagRespCount records legacy response count.
	TagRespCount = "resp_count"
	// TagRounds records iterator rounds.
	TagRounds = "rounds"
	// TagSize records iterator output size.
	TagSize = "size"
	// TagResponseCount records response count.
	TagResponseCount = "response_count"
	// TagResponseDataPointCount records user-visible response point count.
	TagResponseDataPointCount = "response_data_point_count"
	// TagLimitN records limit value.
	TagLimitN = "limit_n"
	// TagLimitOffset records limit offset value.
	TagLimitOffset = "limit_offset"
	// TagTopN records top N value.
	TagTopN = "top_n"
	// TagTopAsc records top sort direction.
	TagTopAsc = "top_asc"
	// TagCalibratedPerNodeLimit records calibrated per-node limit.
	TagCalibratedPerNodeLimit = "calibrated_per_node_limit"
	// TagMemoryChargedBytes records memory charged to an operator.
	TagMemoryChargedBytes = "memory_charged_bytes"
	// TagDedupKeysSeen records dedup keys observed by merge.
	TagDedupKeysSeen = "dedup_keys_seen"
	// TagDedupCollisions records dedup collisions observed by merge.
	TagDedupCollisions = "dedup_collisions"
	// TagBlocksSkipped records skipped storage blocks.
	TagBlocksSkipped = "blocks_skipped"
	// TagTimeFilterReason records time-filter outcome.
	TagTimeFilterReason = "time_filter_reason"
	// TagDecodeNS records decode duration in nanoseconds.
	TagDecodeNS = "decode_ns"
	// TagDecodeNSTotal records total decode duration in nanoseconds.
	TagDecodeNSTotal = "decode_ns_total"
	// TagDecodeNSP50 records p50 decode duration in nanoseconds.
	TagDecodeNSP50 = "decode_ns_p50"
	// TagDecodeNSP99 records p99 decode duration in nanoseconds.
	TagDecodeNSP99 = "decode_ns_p99"
	// TagDecodeNSMax records max decode duration in nanoseconds.
	TagDecodeNSMax = "decode_ns_max"
	// TagFramesIn records frames consumed by an operator.
	TagFramesIn = "frames_in"
	// TagSourcesIn records source count consumed by a merge operator.
	TagSourcesIn = "sources_in"
	// TagGroupName records a distributed group name.
	TagGroupName = "group_name"
	// TagFramesTotal records total frame count.
	TagFramesTotal = "frames_total"
	// TagFramesEmittedIndividually records individually emitted frame spans.
	TagFramesEmittedIndividually = "frames_emitted_individually"
	// TagFramesSkipped records summarized frame count.
	TagFramesSkipped = "frames_skipped"
	// TagMergeHeapPops records merge heap pop count.
	TagMergeHeapPops = "merge_heap_pops"
	// TagCoercedColumns records coerced columns.
	TagCoercedColumns = "coerced_columns"
	// TagHiddenTagsStripped records hidden tags stripped at egress.
	TagHiddenTagsStripped = "hidden_tags_stripped"
	// TagHiddenFieldStripped records hidden fields stripped at egress.
	TagHiddenFieldStripped = "hidden_field_stripped"
	// TagOrderByColIdx records resolved order-by column index.
	TagOrderByColIdx = "orderby_col_idx"
	// TagOrderByFamily records resolved order-by tag family.
	TagOrderByFamily = "orderby_family"
	// TagOrderByTag records resolved order-by tag.
	TagOrderByTag = "orderby_tag"
	// TagDesc records descending sort flag.
	TagDesc = "desc"
	// TagIndexMode records whether index mode is active.
	TagIndexMode = "index_mode"
	// TagAggFunc records aggregation function.
	TagAggFunc = "agg_func"
	// TagAggPartialKind records aggregation partial kind.
	TagAggPartialKind = "agg_partial_kind"
	// TagAggValuePath records aggregation value resolution path.
	TagAggValuePath = "agg_value_path"
	// TagFrameDecoderVersion records frame decoder version.
	TagFrameDecoderVersion = "frame_decoder_version"
	// TagBroadcastTimeoutMS records broadcast timeout in milliseconds.
	TagBroadcastTimeoutMS = "broadcast_timeout_ms"
	// TagAggregatedDataNodeSpans records summarized data-node spans.
	TagAggregatedDataNodeSpans = "aggregated_data_node_spans"
	// TagNodesWithErrors records summarized nodes with errors.
	TagNodesWithErrors = "nodes_with_errors"
	// TagNodesWithZeroRows records summarized nodes with zero rows.
	TagNodesWithZeroRows = "nodes_with_zero_rows"
	// TagTotalRowsAcrossNodes records summarized row count across nodes.
	TagTotalRowsAcrossNodes = "total_rows_across_nodes"
	// TagTotalBytesAcrossNodes records summarized byte count across nodes.
	TagTotalBytesAcrossNodes = "total_bytes_across_nodes"
	// TagNodeLatencyNSP50 records p50 node latency in nanoseconds.
	TagNodeLatencyNSP50 = "node_latency_ns_p50"
	// TagNodeLatencyNSP95 records p95 node latency in nanoseconds.
	TagNodeLatencyNSP95 = "node_latency_ns_p95"
	// TagNodeLatencyNSP99 records p99 node latency in nanoseconds.
	TagNodeLatencyNSP99 = "node_latency_ns_p99"
	// TagNodeLatencyNSMin records min node latency in nanoseconds.
	TagNodeLatencyNSMin = "node_latency_ns_min"
	// TagNodeLatencyNSMax records max node latency in nanoseconds.
	TagNodeLatencyNSMax = "node_latency_ns_max"
	// TagPlan records query plan string.
	TagPlan = "plan"
	// TagRequest records request contents.
	TagRequest = "request"
	// TagNodeSelectors records distributed node selectors.
	TagNodeSelectors = "node_selectors"
	// TagTimeRange records query time range.
	TagTimeRange = "time_range"
	// TagRespKind records internal response kind.
	TagRespKind = "resp_kind"
	// TagFrameBytesTotal records total raw-frame bytes.
	TagFrameBytesTotal = "frame_bytes_total"
	// TagErrorMsg records span error message.
	TagErrorMsg = "error_msg"
	// TagIgnoredChildSpans records children omitted by tracer fanout cap.
	TagIgnoredChildSpans = "ignored_child_spans"
)
