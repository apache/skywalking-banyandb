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
  GroupListResponse, Group,
  CreateGroupRequest, UpdateGroupRequest,
  CreateStreamRequest, UpdateStreamRequest, StreamSchema,
  CreateMeasureRequest, UpdateMeasureRequest, MeasureSchema,
  CreateTraceRequest, UpdateTraceRequest, TraceSchema,
  CreateIndexRuleRequest, UpdateIndexRuleRequest, IndexRuleSchema,
  CreateIndexRuleBindingRequest, UpdateIndexRuleBindingRequest, IndexRuleBindingSchema,
  PropertySchema, TopNAggregationSchema,
  QueryRequest, QueryResponse, TopNQueryRequest, TopNQueryResponse,
} from 'canopy-shared';

import type { DataSource } from './DataSource.js';

async function apiFetch<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    let msg = `${res.status} ${res.statusText}`;
    try {
      const body = await res.json() as { message?: string };
      if (body.message) msg = body.message;
    } catch { /* ignore */ }
    throw new Error(msg);
  }
  if (res.status === 204) return undefined as unknown as T;
  return res.json() as Promise<T>;
}

const JSON_HEADERS = { 'content-type': 'application/json' };

// BanyanDB liaison speaks grpc-gateway, which serializes `google.protobuf.Timestamp`
// as RFC3339 strings (`2026-01-01T00:00:00Z`). Internally the web app works with
// epoch ms (smaller, comparable, easy to feed into `<input type="datetime-local">`),
// so we convert at the boundary instead of carrying strings through the UI.
type RawBinding = Omit<IndexRuleBindingSchema, 'beginAt' | 'expireAt'> & {
  readonly beginAt?: string | number;
  readonly expireAt?: string | number;
};

function decodeBinding(raw: RawBinding): IndexRuleBindingSchema {
  return {
    ...raw,
    beginAt: toMs(raw.beginAt),
    expireAt: toMs(raw.expireAt),
  };
}

function toMs(v: string | number | undefined | null): number {
  if (v == null) return 0;
  if (typeof v === 'number') return v;
  const t = Date.parse(v);
  return Number.isNaN(t) ? 0 : t;
}

function encodeBinding(
  b: IndexRuleBindingSchema,
): Omit<RawBinding, 'beginAt' | 'expireAt'> & { beginAt: string; expireAt: string } {
  // BanyanDB liaison rejects epoch-ms for `google.protobuf.Timestamp` (returns
  // 400 "type mismatch"). Convert to RFC3339 strings here. Guard against a
  // missing or malformed payload so the failure surfaces as a readable error
  // instead of the cryptic "undefined is not an object (evaluating 'e.beginAt')"
  // we'd otherwise hit inside `new Date(...)`.
  if (b == null) {
    throw new Error('encodeBinding: payload is missing (got ' + String(b) + ')');
  }
  if (typeof b.beginAt !== 'number' || !Number.isFinite(b.beginAt)) {
    throw new Error('encodeBinding: beginAt must be a finite number (got ' + String(b.beginAt) + ')');
  }
  if (typeof b.expireAt !== 'number' || !Number.isFinite(b.expireAt)) {
    throw new Error('encodeBinding: expireAt must be a finite number (got ' + String(b.expireAt) + ')');
  }
  return {
    ...b,
    beginAt: new Date(b.beginAt).toISOString(),
    expireAt: new Date(b.expireAt).toISOString(),
  };
}

// BanyanDB encodes a group with `metadata.name` (no top-level `name`) on both
// list and write responses. RawGroup mirrors that wire shape; normalizeGroupResponse
// lifts `metadata.name` to the top-level `name` the UI relies on. Write responses may
// omit the group entirely (the liaison returns only a modRevision) — in that case
// there is nothing to lift, so the (undefined) value passes through and callers that
// guard on a truthy result (e.g. post-create navigation) keep their behavior.
type RawGroup = Omit<Group, 'name'> & { metadata: { name: string } };

function normalizeGroupResponse(raw: RawGroup | undefined): Group {
  if (!raw) return raw as unknown as Group;
  return { ...raw, name: raw.metadata.name };
}

// BanyanDB REST API uses singular resource type names in paths; the app routes use plural.
const TYPE_SINGULAR: Record<string, string> = {
  measures: 'measure',
  streams: 'stream',
  traces: 'trace',
  properties: 'property',
};

export class ApiDataSource implements DataSource {
  // ── Groups ──────────────────────────────────────────────────────────────

  async listGroups(): Promise<GroupListResponse> {
    const data = await apiFetch<{ group?: RawGroup[] }>('/api/v1/group/schema/lists');
    // BanyanDB liaison encodes the proto `repeated Group` field as a map keyed
    // by index ("1": {...}, "2": {...}), so the resulting JS object's
    // iteration order is whatever the server emitted — non-deterministic
    // across requests. Sort by name on the client for a stable list order.
    //
    // BanyanDB also exposes internal objects prefixed with `_` (e.g.
    // `_deletion_task`, backing the property schema registry). Strip them at
    // the data layer so no consumer sees them as user data.
    const groups = (data.group ?? [])
      .filter((g) => !g.metadata.name.startsWith('_'))
      .map(normalizeGroupResponse)
      .sort((a, b) => a.name.localeCompare(b.name));
    return { groups };
  }

  async createGroup(req: CreateGroupRequest): Promise<Group> {
    const data = await apiFetch<{ group?: RawGroup }>('/api/v1/group/schema', {
      method: 'POST', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return normalizeGroupResponse(data.group);
  }

  async updateGroup(name: string, req: UpdateGroupRequest): Promise<Group> {
    const data = await apiFetch<{ group?: RawGroup }>(`/api/v1/group/schema/${name}`, {
      method: 'PUT', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return normalizeGroupResponse(data.group);
  }

  async deleteGroup(name: string): Promise<void> {
    await apiFetch<void>(`/api/v1/group/schema/${name}`, { method: 'DELETE' });
  }

  // ── Resources (read) ────────────────────────────────────────────────────

  async listResourcesInGroup(type: string, group: string): Promise<(StreamSchema | MeasureSchema | TraceSchema | PropertySchema)[]> {
    const singularType = TYPE_SINGULAR[type] ?? type;
    // BanyanDB uses singular key for stream/measure/trace but plural "properties" for property list responses.
    type ListResp = { stream?: StreamSchema[]; measure?: MeasureSchema[]; trace?: TraceSchema[]; property?: PropertySchema[]; properties?: PropertySchema[] };
    const data = await apiFetch<ListResp>(`/api/v1/${singularType}/schema/lists/${group}`);
    // Same map-encoded-repeated-field ordering issue as listGroups — sort by
    // resource name so the GroupPage table renders in a stable order.
    const arr = (data.stream ?? data.measure ?? data.trace ?? data.properties ?? data.property ?? []) as (StreamSchema | MeasureSchema | TraceSchema | PropertySchema)[];
    return arr.slice().sort((a, b) => a.metadata.name.localeCompare(b.metadata.name));
  }

  async getResource(type: string, group: string, name: string): Promise<StreamSchema | MeasureSchema | TraceSchema | PropertySchema> {
    const singularType = TYPE_SINGULAR[type] ?? type;
    type GetResp = { stream?: StreamSchema; measure?: MeasureSchema; trace?: TraceSchema; property?: PropertySchema };
    const data = await apiFetch<GetResp>(`/api/v1/${singularType}/schema/${group}/${name}`);
    const resource = data.stream ?? data.measure ?? data.trace ?? data.property;
    if (!resource) throw new Error(`Resource not found: ${type}/${group}/${name}`);
    return resource;
  }

  // ── TopNAggregation (Top-N schema) ──────────────────────────────────────
  //
  // The Top-N SCHEMA endpoint differs from the per-resource schema endpoints:
  // BanyanDB's grpc-gateway maps TopNAggregationRegistryService.List to
  //   GET /api/v1/topn-agg/schema/lists/{group}
  // (note the `/api/v1/` prefix and the `topNAggregation` response key — NOT
  // the singular `topnAggregation` that the rpc.proto would suggest).
  // Top-N queries dispatch through /v1/measure/topn (runQuery below) using
  // the aggregated name from this list, so the From-row dropdown in the
  // query builder must surface topn-aggregation names rather than the
  // underlying measure names.

  async listTopNAggregations(group: string): Promise<TopNAggregationSchema[]> {
    if (!group) return [];
    const data = await apiFetch<{ topNAggregation?: TopNAggregationSchema[] }>(
      `/api/v1/topn-agg/schema/lists/${encodeURIComponent(group)}`,
    );
    // Sort by name so the FROM-row dropdown and the fuzzy-search index render
    // in a stable order (mirrors listResourcesInGroup).
    return (data.topNAggregation ?? [])
      .slice()
      .sort((a, b) => a.metadata.name.localeCompare(b.metadata.name));
  }

  // ── Stream CRUD ──────────────────────────────────────────────────────────

  async createStream(req: CreateStreamRequest): Promise<StreamSchema> {
    const data = await apiFetch<{ stream: StreamSchema }>('/api/v1/stream/schema', {
      method: 'POST', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return data.stream;
  }

  async updateStream(group: string, name: string, req: UpdateStreamRequest): Promise<StreamSchema> {
    const data = await apiFetch<{ stream: StreamSchema }>(`/api/v1/stream/schema/${group}/${name}`, {
      method: 'PUT', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return data.stream;
  }

  // ── Measure CRUD ─────────────────────────────────────────────────────────

  async createMeasure(req: CreateMeasureRequest): Promise<MeasureSchema> {
    const data = await apiFetch<{ measure: MeasureSchema }>('/api/v1/measure/schema', {
      method: 'POST', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return data.measure;
  }

  async updateMeasure(group: string, name: string, req: UpdateMeasureRequest): Promise<MeasureSchema> {
    const data = await apiFetch<{ measure: MeasureSchema }>(`/api/v1/measure/schema/${group}/${name}`, {
      method: 'PUT', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return data.measure;
  }

  // ── Trace CRUD ──────────────────────────────────────────────────────────────

  async createTrace(req: CreateTraceRequest): Promise<TraceSchema> {
    const data = await apiFetch<{ trace: TraceSchema }>('/api/v1/trace/schema', {
      method: 'POST', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return data.trace;
  }

  async updateTrace(group: string, name: string, req: UpdateTraceRequest): Promise<TraceSchema> {
    const data = await apiFetch<{ trace: TraceSchema }>(`/api/v1/trace/schema/${group}/${name}`, {
      method: 'PUT', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return data.trace;
  }

  // ── IndexRule CRUD ───────────────────────────────────────────────────────

  async listIndexRules(group: string): Promise<IndexRuleSchema[]> {
    const data = await apiFetch<{ indexRule?: IndexRuleSchema[] }>(`/api/v1/index-rule/schema/lists/${group}`);
    // Sort by name so the IndexPage table is stable across requests.
    return (data.indexRule ?? []).slice().sort((a, b) => a.metadata.name.localeCompare(b.metadata.name));
  }

  async getIndexRule(group: string, name: string): Promise<IndexRuleSchema> {
    const data = await apiFetch<{ indexRule: IndexRuleSchema }>(`/api/v1/index-rule/schema/${group}/${name}`);
    return data.indexRule;
  }

  async createIndexRule(req: CreateIndexRuleRequest): Promise<IndexRuleSchema> {
    const data = await apiFetch<{ indexRule: IndexRuleSchema }>('/api/v1/index-rule/schema', {
      method: 'POST', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return data.indexRule;
  }

  async updateIndexRule(group: string, name: string, req: UpdateIndexRuleRequest): Promise<IndexRuleSchema> {
    const data = await apiFetch<{ indexRule: IndexRuleSchema }>(`/api/v1/index-rule/schema/${group}/${name}`, {
      method: 'PUT', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return data.indexRule;
  }

  async deleteIndexRule(group: string, name: string): Promise<void> {
    await apiFetch<void>(`/api/v1/index-rule/schema/${group}/${name}`, { method: 'DELETE' });
  }

  // ── IndexRuleBinding CRUD ────────────────────────────────────────────────

  async listIndexRuleBindings(group: string): Promise<IndexRuleBindingSchema[]> {
    const data = await apiFetch<{ indexRuleBinding?: RawBinding[] }>(`/api/v1/index-rule-binding/schema/lists/${group}`);
    // Sort by name so the IndexPage bindings list is stable across requests.
    return (data.indexRuleBinding ?? []).map(decodeBinding).slice().sort((a, b) => a.metadata.name.localeCompare(b.metadata.name));
  }

  async getIndexRuleBinding(group: string, name: string): Promise<IndexRuleBindingSchema> {
    const data = await apiFetch<{ indexRuleBinding: RawBinding }>(`/api/v1/index-rule-binding/schema/${group}/${name}`);
    return decodeBinding(data.indexRuleBinding);
  }

  async createIndexRuleBinding(req: CreateIndexRuleBindingRequest): Promise<IndexRuleBindingSchema> {
    // CreateIndexRuleBindingRequest.indexRuleBinding is structurally identical
    // to IndexRuleBindingSchema; cast for the encoder. Surface a clear error
    // if the caller passes a malformed request instead of letting the encoder
    // crash on `e.beginAt` mid-flight.
    if (req?.indexRuleBinding == null) {
      throw new Error('createIndexRuleBinding: request is missing indexRuleBinding');
    }
    const body = { indexRuleBinding: encodeBinding(req.indexRuleBinding as unknown as IndexRuleBindingSchema) };
    // BanyanDB liaison write operations return only `{modRevision}` — NOT the
    // full binding. Decoding `data.indexRuleBinding` (undefined) here would
    // throw `undefined.beginAt`, which (after minification) surfaced in the
    // form's error banner as "undefined is not an object (evaluating
    // 'e.beginAt')". Echo the request payload instead — the form only uses
    // the result for `onClose(binding)` and the parent's onClose ignores it.
    await apiFetch<{ modRevision: string }>('/api/v1/index-rule-binding/schema', {
      method: 'POST', headers: JSON_HEADERS, body: JSON.stringify(body),
    });
    return req.indexRuleBinding as unknown as IndexRuleBindingSchema;
  }

  async updateIndexRuleBinding(group: string, name: string, req: UpdateIndexRuleBindingRequest): Promise<IndexRuleBindingSchema> {
    if (req?.indexRuleBinding == null) {
      throw new Error('updateIndexRuleBinding: request is missing indexRuleBinding');
    }
    const body = { indexRuleBinding: encodeBinding(req.indexRuleBinding) };
    // Same write-response shape as create: BanyanDB returns only the
    // modRevision. Echo the payload rather than crashing on a missing body.
    await apiFetch<{ modRevision: string }>(`/api/v1/index-rule-binding/schema/${group}/${name}`, {
      method: 'PUT', headers: JSON_HEADERS, body: JSON.stringify(body),
    });
    return req.indexRuleBinding;
  }

  async deleteIndexRuleBinding(group: string, name: string): Promise<void> {
    await apiFetch<void>(`/api/v1/index-rule-binding/schema/${group}/${name}`, { method: 'DELETE' });
  }

  // ── Generic delete ────────────────────────────────────────────────────────

  async deleteResource(type: string, group: string, name: string): Promise<void> {
    const singularType = TYPE_SINGULAR[type] ?? type;
    await apiFetch<void>(`/api/v1/${singularType}/schema/${group}/${name}`, { method: 'DELETE' });
  }

  // ── Query ─────────────────────────────────────────────────────────────────

  // Wire shape (see implement-m4-note.md decision #24): BanyanDB's BydbQL
  // gateway accepts a single { query: string } and returns a `oneof result`
  // carrying one of stream_result / measure_result / trace_result /
  // topn_result / property_result. We dispatch by the consumer-provided
  // request shape: topN → POST /v1/measure/topn with the structured TopN
  // request; everything else → POST /v1/bydbql/query with the BydbQL string.
  //
  // The result views consume a uniform VIEW-MODEL (flat key→value map per
  // element). This method flattens the wire shape into that view-model so
  // the views don't need to know about tag_families / data_points / lists.

  async runQuery(request: QueryRequest): Promise<QueryResponse> {
    // TopN → /v1/measure/topn (separate endpoint per plan SF5)
    if ((request as unknown as { topN?: TopNQueryRequest }).topN) {
      const topN = (request as unknown as { topN: TopNQueryRequest }).topN;
      const data = await apiFetch<TopNQueryResponse>('/api/v1/measure/topn', {
        method: 'POST', headers: JSON_HEADERS,
        body: JSON.stringify(topN),
      });
      const flat = flattenTopNResponse(data);
      const truncated = flat.length > MAX_QUERY_ROWS;
      return {
        // Preserve the wire-shape `topn_result.lists` so the result view can
        // group rows by per-list timestamp for the time-bucket picker. Without
        // this, the view would have to re-group the flat elements by
        // `timestamp`, which loses the bucket boundaries.
        topn_result: data,
        elements: truncated ? flat.slice(0, MAX_QUERY_ROWS) : flat,
        totalRowCount: flat.length,
        truncated,
      };
    }
    // measure / stream / trace → /v1/bydbql/query with the BydbQL string.
    const data = await apiFetch<QueryResponse>(
      '/api/v1/bydbql/query',
      { method: 'POST', headers: JSON_HEADERS, body: JSON.stringify(request) },
    );
    const flat = flattenQueryResponse(data);
    const truncated = flat.length > MAX_QUERY_ROWS;
    return {
      ...data,
      elements: truncated ? flat.slice(0, MAX_QUERY_ROWS) : flat,
      totalRowCount: flat.length,
      truncated,
    };
  }
}

// ── Wire-shape → view-model flatteners ─────────────────────────────────────

// Wire-shape note: BanyanDB's protojson serializer emits camelCase, NOT the
// snake_case declared in api-dto.ts. The flattener therefore reads camelCase
// keys. See implement-m4-note.md #33 (the DTOs are aspirational; the wire
// is what we actually receive). The DTO types still describe snake_case
// because that's what the .proto files use, but runtime data is camelCase.

function readFieldValue(v: unknown): number | string | undefined {
  if (!v || typeof v !== 'object') return undefined;
  const o = v as { float?: unknown; int?: unknown; str?: unknown; timestamp?: unknown };
  if (typeof o.float === 'number') return o.float;
  // protojson renders int64/str as {"int": {"value": "2600"}} / {"str": {"value": "x"}}
  // to preserve 64-bit precision. Some BanyanDB versions also flatten to just
  // {"int": <number>} / {"str": <string>}.
  const innerInt = (o.int as { value?: unknown } | number | undefined);
  if (innerInt !== undefined) {
    const raw = typeof innerInt === 'object' && innerInt !== null ? (innerInt as { value?: unknown }).value : innerInt;
    if (typeof raw === 'string') return Number(raw);
    if (typeof raw === 'number') return raw;
  }
  const innerStr = (o.str as { value?: unknown } | string | undefined);
  if (innerStr !== undefined) {
    const raw = typeof innerStr === 'object' && innerStr !== null ? (innerStr as { value?: unknown }).value : innerStr;
    if (typeof raw === 'string') return raw;
  }
  // TIMESTAMP tags are transmitted as {"timestamp": "2026-07-13T...Z"}.
  const ts = (o.timestamp as { value?: unknown } | string | undefined);
  if (ts !== undefined) {
    const raw = typeof ts === 'object' && ts !== null ? (ts as { value?: unknown }).value : ts;
    if (typeof raw === 'string') {
      const ms = Date.parse(raw);
      if (Number.isFinite(ms)) return ms;
    }
  }
  return undefined;
}

export function flattenQueryResponse(data: QueryResponse): Record<string, unknown>[] {
  const d = data as unknown as Record<string, unknown>;
  // Runtime protojson uses camelCase, but test fixtures and some static mocks
  // still use snake_case. Accept both so downstream views always get elements.
  const streamResult = (d.streamResult ?? d.stream_result) as { elements?: unknown[] } | undefined;
  const measureResult = (d.measureResult ?? d.measure_result) as { dataPoints?: unknown[]; data_points?: unknown[] } | undefined;
  const traceResult = (d.traceResult ?? d.trace_result) as { traces?: unknown[] } | undefined;
  if (streamResult?.elements) return streamResult.elements.map((e) => flattenStreamElement(e as never));
  const measurePoints = measureResult?.dataPoints ?? measureResult?.data_points;
  if (measurePoints) return measurePoints.map((e) => flattenMeasureDataPoint(e as never));
  if (traceResult?.traces) {
    const flat: Record<string, unknown>[] = [];
    for (const t of traceResult.traces) {
      const trace = t as { trace_id?: string; spans?: unknown[] };
      for (const s of trace.spans ?? []) {
        flat.push(flattenTraceSpan(s as never, trace.trace_id));
      }
    }
    return flat;
  }
  return [];
}

function flattenStreamElement(e: { element_id?: string; timestamp?: string; tagFamilies?: readonly { tags?: readonly { key: string; value: unknown }[] }[]; tag_families?: readonly { tags?: readonly { key: string; value: unknown }[] }[] }): Record<string, unknown> {
  const flat: Record<string, unknown> = { element_id: e.element_id, timestamp: e.timestamp };
  const families = e.tagFamilies ?? e.tag_families ?? [];
  for (const fam of families) {
    for (const t of fam.tags ?? []) {
      flat[t.key] = readFieldValue(t.value) ?? t.value;
    }
  }
  return flat;
}

function flattenMeasureDataPoint(dp: { timestamp?: string; sid?: string; version?: number; tagFamilies?: readonly { tags?: readonly { key: string; value: unknown }[] }[]; tag_families?: readonly { tags?: readonly { key: string; value: unknown }[] }[]; fields?: readonly { name: string; value: unknown }[] }): Record<string, unknown> {
  const flat: Record<string, unknown> = { timestamp: dp.timestamp };
  if (dp.sid !== undefined) flat.sid = dp.sid;
  if (dp.version !== undefined) flat.version = dp.version;
  const families = dp.tagFamilies ?? dp.tag_families ?? [];
  for (const fam of families) {
    for (const t of fam.tags ?? []) {
      flat[t.key] = readFieldValue(t.value) ?? t.value;
    }
  }
  for (const f of dp.fields ?? []) {
    const v = readFieldValue(f.value);
    if (v !== undefined) flat[f.name] = v;
  }
  return flat;
}

function flattenTraceSpan(
  s: {
    spanId?: string;
    span_id?: string;
    traceId?: string;
    trace_id?: string;
    tags?: readonly { key: string; value: unknown }[];
    tag_families?: readonly { tags?: readonly { key: string; value: unknown }[] }[];
    span?: unknown;
  },
  parentTraceId?: string,
): Record<string, unknown> {
  // BanyanDB's protojson gateway emits camelCase keys at runtime; the hand-
  // authored fixtures still use snake_case. Accept both so live traces and
  // tests flatten correctly.
  const traceId = s.traceId ?? s.trace_id ?? parentTraceId;
  const spanId = s.spanId ?? s.span_id;
  const flat: Record<string, unknown> = { trace_id: traceId, span_id: spanId };
  if (s.span !== undefined) flat.span = s.span;
  const tagList = s.tags ?? s.tag_families?.flatMap((f) => f.tags ?? []) ?? [];
  for (const t of tagList) {
    flat[t.key] = readFieldValue(t.value) ?? t.value;
  }
  return flat;
}

export function flattenTopNResponse(data: TopNQueryResponse): Record<string, unknown>[] {
  const flat: Record<string, unknown>[] = [];
  for (const list of data.lists ?? []) {
    for (const item of list.items ?? []) {
      const row: Record<string, unknown> = { timestamp: list.timestamp };
      for (const t of item.entity ?? []) {
        row[t.key] = readFieldValue(t.value) ?? t.value;
      }
      row.value = item.value?.float ?? item.value?.int ?? item.value?.str;
      flat.push(row);
    }
  }
  return flat;
}

// Bound applied to result rendering per plan SF2. Configurable; lifted to a
// module-level constant so future config plumbing can rebind it.
export const MAX_QUERY_ROWS = 1000;

export const apiDataSource = new ApiDataSource();
