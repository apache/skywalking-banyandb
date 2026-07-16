/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import type {
  Group, IntervalRule, LifecycleStage,
  StreamSchema, MeasureSchema, TraceSchema, PropertySchema,
  IndexRuleSchema, IndexRuleBindingSchema,
  TagFamilySpec, FieldSpec, TagType, FieldType, CompressMethod, EncodingMethod, IndexType,
} from './schema.js';

export interface ApiResponse<T> {
  readonly data?: T;
  readonly error?: string;
}

export interface GroupListResponse {
  readonly groups: Group[];
}

export interface ResourceListResponse<T> {
  readonly items: T[];
}

// Group CRUD

export interface CreateGroupRequest {
  readonly group: {
    readonly metadata: { readonly name: string };
    readonly catalog: Group['catalog'];
    readonly resourceOpts: {
      readonly shardNum: number;
      readonly replicas?: number;
      readonly segmentInterval?: IntervalRule;
      readonly ttl?: IntervalRule;
      readonly stages?: readonly LifecycleStage[];
      readonly defaultStages?: readonly string[];
    };
  };
}

export interface UpdateGroupRequest {
  readonly group: {
    readonly metadata: { readonly name: string };
    readonly catalog?: Group['catalog'];
    readonly resourceOpts: {
      readonly shardNum: number;
      readonly replicas?: number;
      readonly segmentInterval?: IntervalRule;
      readonly ttl?: IntervalRule;
      readonly stages?: readonly LifecycleStage[];
      readonly defaultStages?: readonly string[];
    };
  };
}

// Stream CRUD

export interface CreateStreamRequest {
  readonly stream: Omit<StreamSchema, 'metadata'> & {
    readonly metadata: { readonly name: string; readonly group: string };
  };
}

export interface UpdateStreamRequest {
  readonly stream: StreamSchema;
}

// Measure CRUD

export interface CreateMeasureRequest {
  readonly measure: Omit<MeasureSchema, 'metadata'> & {
    readonly metadata: { readonly name: string; readonly group: string };
  };
}

export interface UpdateMeasureRequest {
  readonly measure: MeasureSchema;
}

// Trace CRUD

export interface CreateTraceRequest {
  readonly trace: Omit<TraceSchema, 'metadata'> & {
    readonly metadata: { readonly name: string; readonly group: string };
  };
}

export interface UpdateTraceRequest {
  readonly trace: TraceSchema;
}

// IndexRule CRUD

export interface CreateIndexRuleRequest {
  readonly indexRule: Omit<IndexRuleSchema, 'metadata'> & {
    readonly metadata: { readonly name: string; readonly group: string };
  };
}

export interface UpdateIndexRuleRequest {
  readonly indexRule: IndexRuleSchema;
}

// IndexRuleBinding CRUD

export interface CreateIndexRuleBindingRequest {
  readonly indexRuleBinding: Omit<IndexRuleBindingSchema, 'metadata'> & {
    readonly metadata: { readonly name: string; readonly group: string };
  };
}

export interface UpdateIndexRuleBindingRequest {
  readonly indexRuleBinding: IndexRuleBindingSchema;
}

// Query
//
// Wire shape mirrors the BanyanDB proto definitions under
// api/proto/banyandb/{bydbql,measure,stream,trace,v1}/* in this monorepo:
//   - bydbql.v1.QueryRequest = { string query }   (the gateway parses BydbQL)
//   - bydbql.v1.QueryResponse = oneof { stream_result, measure_result,
//                                      trace_result, topn_result,
//                                      property_result }
//   - Each per-catalog response has its own { elements/data_points/lists/items }
//     shape with nested tag_families → tags (key + value).
// runQuery dispatches by (1) calling POST /api/v1/bydbql/query with the
// BydbQL string, or (2) for TopN, POST /v1/measure/topn with the structured
// TopNRequest. The result views consume a flattened VIEW-MODEL that
// api.ts produces from these wire shapes.

export interface Tag {
  readonly key: string;
  readonly value: string | number | boolean | null;
}

export interface TagFamily {
  readonly name?: string;
  readonly tags: readonly Tag[];
}

export interface FieldValue {
  // BanyanDB FieldValue is a oneof; the wire encodes only the populated branch.
  readonly str?: string;
  readonly int?: number;
  readonly float?: number;
  readonly binary?: Uint8Array | null;
}

export interface MeasureField {
  readonly name: string;
  readonly value: FieldValue;
}

export interface StreamElement {
  readonly element_id: string;
  readonly timestamp?: string; // RFC3339
  readonly tag_families: readonly TagFamily[];
}

export interface TraceSpan {
  readonly span_id?: string;
  readonly trace_id?: string;
  readonly name?: string;
  readonly timestamp?: string;
  readonly duration?: number; // ms
  readonly tag_families?: readonly TagFamily[];
}

export interface MeasureDataPoint {
  readonly timestamp?: string;
  readonly tag_families: readonly TagFamily[];
  readonly fields: readonly MeasureField[];
  readonly sid?: string;
  readonly version?: number;
}

export interface TopNItem {
  readonly entity: readonly Tag[];
  readonly value: FieldValue;
  readonly version?: number;
  readonly timestamp?: string;
}

export interface TopNList {
  readonly timestamp?: string;
  readonly items: readonly TopNItem[];
}

export interface QueryTraceSpan {
  readonly startTime?: string;
  readonly endTime?: string;
  readonly error?: boolean;
  readonly tags?: ReadonlyArray<{ readonly key: string; readonly value: string }>;
  readonly message?: string;
  readonly children?: ReadonlyArray<QueryTraceSpan>;
  readonly duration?: string;
}

export interface QueryTrace {
  readonly traceId?: string;
  readonly spans?: ReadonlyArray<QueryTraceSpan>;
  readonly error?: boolean;
}

// Wire responses — bydbql returns ONE of these inside its `result` oneof.
// We type the union rather than carry the oneof around.

export interface StreamQueryResponse {
  readonly elements?: readonly StreamElement[];
  readonly trace?: QueryTrace;
  readonly group_statuses?: Readonly<Record<string, string>>;
}

export interface MeasureQueryResponse {
  readonly data_points?: readonly MeasureDataPoint[];
  readonly trace?: QueryTrace;
  readonly group_statuses?: Readonly<Record<string, string>>;
}

export interface TraceQueryResponse {
  readonly elements?: readonly TraceSpan[];
  readonly trace?: QueryTrace;
}

export interface TopNResponse {
  readonly lists?: readonly TopNList[];
  readonly trace?: QueryTrace;
}

export interface QueryRequest {
  // BydbQL QueryRequest is just a single `query` string — BanyanDB parses it.
  readonly query: string;
}

export interface QueryResponse {
  // The bydbql response carries one of these per the `result` oneof. We
  // surface whichever branch the gateway populated, plus a flattened
  // view-model that the result views consume uniformly.
  readonly stream_result?: StreamQueryResponse;
  readonly measure_result?: MeasureQueryResponse;
  readonly trace_result?: TraceQueryResponse;
  readonly topn_result?: TopNResponse;
  readonly property_result?: unknown;
  // Flattened view-model for the result views: each element has been
  // re-shaped into a flat key→value map with reserved keys for timestamp,
  // element_id, span_id and trace_id. The N-row cap status (totalRowCount +
  // truncated) is set by api.ts after applying MAX_QUERY_ROWS.
  readonly elements: readonly Record<string, unknown>[];
  readonly totalRowCount?: number;
  readonly truncated?: boolean;
}

// Direct TopN endpoint payload (separate from bydbql — the TopN liaison uses
// /v1/measure/topn with the structured TopNRequest below).
export interface TopNQueryRequest {
  readonly groups: readonly string[];
  readonly name: string;
  readonly top_n: number;
  readonly agg?: { readonly function: string; readonly field_name: string };
  readonly conditions?: ReadonlyArray<{ readonly name: string; readonly op: string; readonly value: string | number }>;
  readonly field_value_sort?: 'SORT_ASC' | 'SORT_DESC';
  readonly time_range?: { readonly begin: string; readonly end: string };
  readonly trace?: boolean;
}

export interface TopNQueryResponse {
  readonly lists?: readonly TopNList[];
}

// TopNAggregation schema — BanyanDB's precomputed leaderboard definition.
// Wire shape (per the v0.x liaison) is camelCase protojson:
//   { metadata: {group, name}, sourceMeasure: {group, name}, fieldName,
//     fieldValueSort: 'SORT_ASC'|'SORT_DESC', groupByTagNames: string[],
//     criteria, countersNumber, lruSize, updatedAt, createdAt }
// Sourced from GET /api/v1/topn-agg/schema/lists/{group}. The list response
// repeats under the `topNAggregation` key.
//
// Unlike the per-resource schemas (Measure/Stream/Trace), TopNAggregation does
// not carry tagFamilies directly — only the names of the entity tags it
// groups by. For richer WHERE-clause support the consumer can fall back to
// the source measure's tagFamilies via `sourceMeasure.{group,name}`.
export interface TopNAggregationSchema {
  readonly metadata: { readonly name: string; readonly group: string };
  readonly sourceMeasure?: { readonly name: string; readonly group: string };
  readonly fieldName?: string;
  readonly fieldValueSort?: 'SORT_ASC' | 'SORT_DESC';
  readonly groupByTagNames?: readonly string[];
  readonly countersNumber?: number;
  readonly lruSize?: number;
  readonly createdAt?: string;
  readonly updatedAt?: string;
}

export interface TopNAggregationListResponse {
  readonly topNAggregation: readonly TopNAggregationSchema[];
}

// Re-export schema types for convenience
export type {
  Group, LifecycleStage, StreamSchema, MeasureSchema, TraceSchema, PropertySchema,
  IndexRuleSchema, IndexRuleBindingSchema,
  TagFamilySpec, FieldSpec, TagType, FieldType, CompressMethod, EncodingMethod, IndexType,
};
