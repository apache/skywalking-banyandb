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
  CreateTopNAggregationRequest, UpdateTopNAggregationRequest,
  QueryRequest, QueryResponse, TopNQueryResponse,
  CreatePropertySchemaRequest, PropertyApplyRequest, PropertyApplyResponse,
  PropertyQueryRequest, PropertyQueryResponse, PropertyDocument, PropertyDocTag,
  PropertyWireDocument, PropertyWireTag, PropertyTagValue,
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

  async getTopNAggregation(group: string, name: string): Promise<TopNAggregationSchema> {
    const data = await apiFetch<{ topNAggregation: TopNAggregationSchema }>(
      `/api/v1/topn-agg/schema/${encodeURIComponent(group)}/${encodeURIComponent(name)}`,
    );
    return data.topNAggregation;
  }

  async createTopNAggregation(req: CreateTopNAggregationRequest): Promise<TopNAggregationSchema> {
    // TopNAggregationRegistryService.Create returns only {modRevision} (see
    // rpc.proto) — same write-response shape as IndexRuleBinding/Property.
    // Echo the request payload back rather than decoding a missing body.
    await apiFetch<{ modRevision?: string }>('/api/v1/topn-agg/schema', {
      method: 'POST', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return req.topNAggregation as unknown as TopNAggregationSchema;
  }

  async updateTopNAggregation(group: string, name: string, req: UpdateTopNAggregationRequest): Promise<TopNAggregationSchema> {
    await apiFetch<{ modRevision?: string }>(
      `/api/v1/topn-agg/schema/${encodeURIComponent(group)}/${encodeURIComponent(name)}`,
      { method: 'PUT', headers: JSON_HEADERS, body: JSON.stringify(req) },
    );
    return req.topNAggregation;
  }

  async deleteTopNAggregation(group: string, name: string): Promise<void> {
    await apiFetch<void>(`/api/v1/topn-agg/schema/${encodeURIComponent(group)}/${encodeURIComponent(name)}`, { method: 'DELETE' });
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
    if (request.topN) {
      const topN = request.topN;
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

  // ── Property schema (collection) CRUD — database/v1 PropertyRegistryService ──
  // List/Get reuse listResourcesInGroup/getResource (TYPE_SINGULAR maps
  // 'properties' -> 'property', matching PropertyRegistryService's REST paths).

  async createPropertySchema(req: CreatePropertySchemaRequest): Promise<PropertySchema> {
    // database/v1.Property.tags has `min_items = 1` (validate.rules) even
    // though the collection itself is schema-free — documents aren't
    // validated against these declared tags at write time (confirmed live:
    // pkg/index/inverted's BuildPropertyQuery schema stub treats every tag
    // name as indexed regardless of what's declared here). The UI doesn't
    // ask the user for a tag just to satisfy this proto formality, so a
    // single placeholder TagSpec is synthesized here to pass validation.
    const body = {
      property: {
        metadata: req.property.metadata,
        tags: [{ name: '_placeholder', type: 'TAG_TYPE_STRING' }],
      },
    };
    // The registry Create RPC returns only { modRevision } (see
    // PropertyRegistryServiceCreateResponse) — echo the request's property
    // shape back as the created schema.
    await apiFetch<{ modRevision?: string }>('/api/v1/property/schema', {
      method: 'POST', headers: JSON_HEADERS, body: JSON.stringify(body),
    });
    return { metadata: req.property.metadata, tags: [] };
  }

  async deletePropertySchema(group: string, name: string): Promise<void> {
    await apiFetch<void>(`/api/v1/property/schema/${encodeURIComponent(group)}/${encodeURIComponent(name)}`, { method: 'DELETE' });
  }

  // ── Property documents — property/v1 PropertyService ────────────────────────

  async applyPropertyDocument(group: string, name: string, id: string, req: PropertyApplyRequest): Promise<PropertyApplyResponse> {
    // Despite being "schema-free" from the UI's perspective, BanyanDB's
    // property module IS schema-checked server-side (confirmed live):
    // banyand/liaison/grpc/property.go's validatePropertyTags rejects any
    // Apply whose tag keys aren't already declared on the collection (or
    // whose declared type conflicts), and Create requires >=1 TagSpec
    // up front (createPropertySchema above seeds a "_placeholder" one so
    // the New-property modal doesn't have to ask for a schema). Auto-grow
    // the declared tag set here so the tag editor really can add any key —
    // the user never sees a schema step.
    await this.ensurePropertyTagsDeclared(group, name, req.property.tags);
    const data = await apiFetch<PropertyApplyResponse>(
      `/api/v1/property/data/${encodeURIComponent(group)}/${encodeURIComponent(name)}/${encodeURIComponent(id)}`,
      { method: 'PUT', headers: JSON_HEADERS, body: JSON.stringify(req) },
    );
    return data;
  }

  private async ensurePropertyTagsDeclared(group: string, name: string, tags: readonly PropertyWireTag[]): Promise<void> {
    let schema: PropertySchema;
    try {
      schema = await this.getResource('properties', group, name) as PropertySchema;
    } catch {
      // Collection missing/unreachable — let Apply's own error surface.
      return;
    }
    const declared = schema.tags ?? [];
    const declaredNames = new Set(declared.map((t) => t.name));
    const missing = tags.filter((t) => !declaredNames.has(t.key));
    if (!missing.length) return;
    const nextTags: Array<{ name: string; type: string }> = [
      ...declared.map((t) => ({ name: t.name, type: t.type as string })),
      ...missing.map((t) => ({ name: t.key, type: inferPropertyTagType(t.value) })),
    ];
    await apiFetch<{ modRevision?: string }>(
      `/api/v1/property/schema/${encodeURIComponent(group)}/${encodeURIComponent(name)}`,
      { method: 'PUT', headers: JSON_HEADERS, body: JSON.stringify({ property: { metadata: { group, name }, tags: nextTags } }) },
    );
  }

  async deletePropertyDocument(group: string, name: string, id: string): Promise<void> {
    await apiFetch<void>(
      `/api/v1/property/data/${encodeURIComponent(group)}/${encodeURIComponent(name)}/${encodeURIComponent(id)}`,
      { method: 'DELETE' },
    );
  }

  async queryPropertyDocuments(req: PropertyQueryRequest): Promise<{ readonly documents: readonly PropertyDocument[] }> {
    const data = await apiFetch<PropertyQueryResponse>('/api/v1/property/data/query', {
      method: 'POST', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return { documents: (data.properties ?? []).map(flattenPropertyDocument) };
  }
}

// ── Property tag value encode/decode ───────────────────────────────────────
//
// model.v1.TagValue is a oneof: null, str, str_array, int, int_array,
// binary_data, timestamp — NO float (unlike model.v1.FieldValue, used for
// measure Fields, which does carry one). bytes fields (binary_data)
// serialize as a bare base64 string in protojson; Timestamp serializes as an
// RFC3339 string; Str/Int/StrArray/IntArray are one-field wrapper messages.

/** Encode a tag editor row (valueType + raw text) into a wire TagValue. */
// Emits the database/v1.TagType string values the live liaison's grpc-gateway
// accepts (verified against a running BanyanDB instance). These now match the
// corrected `TagType` enum in schema.ts (TAG_TYPE_INT / TAG_TYPE_INT_ARRAY /
// TAG_TYPE_TIMESTAMP — there is no float tag type).
function inferPropertyTagType(v: PropertyTagValue): string {
  if (v.int !== undefined) return 'TAG_TYPE_INT';
  if (v.strArray !== undefined) return 'TAG_TYPE_STRING_ARRAY';
  if (v.intArray !== undefined) return 'TAG_TYPE_INT_ARRAY';
  if (v.binaryData !== undefined) return 'TAG_TYPE_DATA_BINARY';
  if (v.timestamp !== undefined) return 'TAG_TYPE_TIMESTAMP';
  return 'TAG_TYPE_STRING';
}

export function encodePropertyTagValue(valueType: PropertyDocTag['valueType'], raw: string): PropertyTagValue {
  switch (valueType) {
    case 'int':
      return { int: { value: raw.trim() } };
    case 'int_array':
      return { intArray: { value: raw.split(',').map((v) => v.trim()).filter(Boolean) } };
    case 'str_array':
      return { strArray: { value: raw.split(',').map((v) => v.trim()).filter(Boolean) } };
    case 'binary':
      return { binaryData: raw };
    case 'timestamp': {
      const ms = Date.parse(raw);
      return { timestamp: Number.isFinite(ms) ? new Date(ms).toISOString() : raw };
    }
    case 'null':
      return { null: null };
    case 'str':
    default:
      return { str: { value: raw } };
  }
}

/** Decode a wire TagValue into the flat {valueType, value} shape the UI renders/edits. */
export function decodePropertyTagValue(v: PropertyTagValue | undefined): { valueType: PropertyDocTag['valueType']; value: string } {
  if (!v) return { valueType: 'str', value: '' };
  if (v.str !== undefined) return { valueType: 'str', value: unwrapScalar(v.str) };
  if (v.int !== undefined) return { valueType: 'int', value: String(unwrapScalar(v.int)) };
  if (v.strArray !== undefined) return { valueType: 'str_array', value: (unwrapArray(v.strArray)).join(', ') };
  if (v.intArray !== undefined) return { valueType: 'int_array', value: (unwrapArray(v.intArray)).join(', ') };
  if (v.binaryData !== undefined) return { valueType: 'binary', value: String(v.binaryData) };
  if (v.timestamp !== undefined) return { valueType: 'timestamp', value: String(v.timestamp) };
  return { valueType: 'null', value: '' };
}

// protojson may render a wrapper message ({"value": "x"}) or (for some
// gateway versions) flatten it to the bare scalar — accept both.
function unwrapScalar(w: { value?: unknown } | string | number | undefined): string {
  if (w == null) return '';
  if (typeof w === 'object') return String((w as { value?: unknown }).value ?? '');
  return String(w);
}
function unwrapArray(w: { value?: readonly unknown[] } | readonly unknown[] | undefined): string[] {
  if (w == null) return [];
  const arr: readonly unknown[] = Array.isArray(w) ? w : ((w as { value?: readonly unknown[] }).value ?? []);
  return arr.map((x: unknown) => String(x));
}

function flattenPropertyTag(t: PropertyWireTag): PropertyDocTag {
  const { valueType, value } = decodePropertyTagValue(t.value);
  return { key: t.key, valueType, value };
}

function flattenPropertyDocument(p: PropertyWireDocument): PropertyDocument {
  return {
    id: p.id,
    tags: (p.tags ?? []).map(flattenPropertyTag),
    updatedAt: p.updatedAt,
  };
}

// ── Wire-shape → view-model flatteners ─────────────────────────────────────

// Wire-shape note: BanyanDB's protojson serializer emits camelCase, NOT the
// snake_case declared in api-dto.ts. The flattener therefore reads camelCase
// keys. See implement-m4-note.md #33 (the DTOs are aspirational; the wire
// is what we actually receive). The DTO types still describe snake_case
// because that's what the .proto files use, but runtime data is camelCase.

// model/v1 TagValue and FieldValue are different messages with different
// oneofs (api/proto/banyandb/model/v1/common.proto):
//   TagValue:   null / str / strArray / int / intArray / binaryData / timestamp
//   FieldValue: null / str / int / binaryData / float
// so each gets its own reader. Each reader must be total over its oneof —
// a missed variant would put the raw object into the flattened row, and the
// result tables would render it as "[object Object]" (seen live: SkyWalking
// leaves service_cpm's attr1-5 unset, which protojson emits as {"null": null}).

/** Read a wire TagValue (7-variant oneof) into a scalar. Null reads as ''. */
export function readTagValue(v: unknown): number | string | undefined {
  // Test fixtures and some gateway versions flatten values to bare scalars.
  if (typeof v === 'string' || typeof v === 'number') return v;
  if (!v || typeof v !== 'object') return undefined;
  const o = v as { null?: unknown; str?: unknown; strArray?: unknown; int?: unknown; intArray?: unknown; binaryData?: unknown; timestamp?: unknown };
  // google.protobuf.NullValue serializes as {"null": null}.
  if ('null' in o) return '';
  // protojson renders int64/str as {"int": {"value": "2600"}} / {"str": {"value": "x"}}
  // to preserve 64-bit precision; unwrapScalar also accepts the bare-scalar form.
  if (o.str !== undefined) return unwrapScalar(o.str as never);
  if (o.strArray !== undefined) return unwrapArray(o.strArray as never).join(', ');
  if (o.int !== undefined) return Number(unwrapScalar(o.int as never));
  if (o.intArray !== undefined) return unwrapArray(o.intArray as never).join(', ');
  if (o.binaryData !== undefined) return String(o.binaryData);
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

/** Read a wire FieldValue (5-variant oneof) into a scalar. Null/unreadable → undefined. */
function readFieldValue(v: unknown): number | string | undefined {
  if (typeof v === 'string' || typeof v === 'number') return v;
  if (!v || typeof v !== 'object') return undefined;
  const o = v as { null?: unknown; float?: unknown; int?: unknown; str?: unknown; binaryData?: unknown };
  if ('null' in o) return undefined;
  if (typeof o.float === 'number') return o.float;
  if (o.int !== undefined) return Number(unwrapScalar(o.int as never));
  if (o.str !== undefined) return unwrapScalar(o.str as never);
  if (o.binaryData !== undefined) return String(o.binaryData);
  return undefined;
}

export function flattenQueryResponse(data: QueryResponse): Record<string, unknown>[] {
  const d = data as unknown as Record<string, unknown>;
  // Runtime protojson uses camelCase, but test fixtures and some static mocks
  // still use snake_case. Accept both so downstream views always get elements.
  const streamResult = (d.streamResult ?? d.stream_result) as { elements?: unknown[] } | undefined;
  const measureResult = (d.measureResult ?? d.measure_result) as { dataPoints?: unknown[]; data_points?: unknown[] } | undefined;
  const traceResult = (d.traceResult ?? d.trace_result) as {
    elements?: unknown[];
    traces?: readonly { traceId?: string; trace_id?: string; spans?: readonly unknown[] }[];
  } | undefined;
  if (streamResult?.elements) return streamResult.elements.map((e) => flattenStreamElement(e as never));
  const measurePoints = measureResult?.dataPoints ?? measureResult?.data_points;
  if (measurePoints) return measurePoints.map((e) => flattenMeasureDataPoint(e as never));
  // Older gateways and the hand-authored fixtures use a flat span list
  // ({ elements: [Span] }); the live v0.10 gateway groups spans per trace
  // ({ traces: [{ traceId, spans: [Span] }] }, trace/v1 query.proto). Accept
  // both, carrying the parent's traceId down to each span.
  if (traceResult?.elements) return traceResult.elements.map((s) => flattenTraceSpan(s as never));
  if (traceResult?.traces) {
    return traceResult.traces.flatMap((g) => (g.spans ?? []).map((s) => flattenTraceSpan(s as never, g.traceId ?? g.trace_id)));
  }
  return [];
}

function flattenStreamElement(e: { elementId?: string; element_id?: string; timestamp?: string; tagFamilies?: readonly { tags?: readonly { key: string; value: unknown }[] }[]; tag_families?: readonly { tags?: readonly { key: string; value: unknown }[] }[] }): Record<string, unknown> {
  const flat: Record<string, unknown> = { element_id: e.elementId ?? e.element_id, timestamp: e.timestamp };
  const families = e.tagFamilies ?? e.tag_families ?? [];
  for (const fam of families) {
    for (const t of fam.tags ?? []) {
      const tagVal = readTagValue(t.value);
      if (tagVal !== undefined) flat[t.key] = tagVal;
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
      const tagVal = readTagValue(t.value);
      if (tagVal !== undefined) flat[t.key] = tagVal;
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
    name?: string;
    timestamp?: string;
    duration?: number;
    tags?: readonly { key: string; value: unknown }[];
    tagFamilies?: readonly { tags?: readonly { key: string; value: unknown }[] }[];
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
  // Spine fields the result view renders (timestamp column, name, duration).
  if (s.name !== undefined) flat.name = s.name;
  if (s.timestamp !== undefined) flat.timestamp = s.timestamp;
  if (s.duration !== undefined) flat.duration = s.duration;
  if (s.span !== undefined) flat.span = s.span;
  const tagList = s.tags ?? (s.tagFamilies ?? s.tag_families)?.flatMap((f) => f.tags ?? []) ?? [];
  for (const t of tagList) {
    const tagVal = readTagValue(t.value);
    if (tagVal !== undefined) flat[t.key] = tagVal;
  }
  return flat;
}

export function flattenTopNResponse(data: TopNQueryResponse): Record<string, unknown>[] {
  const flat: Record<string, unknown>[] = [];
  for (const list of data.lists ?? []) {
    for (const item of list.items ?? []) {
      const row: Record<string, unknown> = { timestamp: list.timestamp };
      for (const t of item.entity ?? []) {
        const entityVal = readTagValue(t.value);
        if (entityVal !== undefined) row[t.key] = entityVal;
      }
      // protojson wraps int64/str values as {"int":{"value":"2600"}} etc.;
      // readFieldValue unwraps them just like the other flatteners do.
      row.value = readFieldValue(item.value);
      flat.push(row);
    }
  }
  return flat;
}

// Bound applied to result rendering per plan SF2. Configurable; lifted to a
// module-level constant so future config plumbing can rebind it.
export const MAX_QUERY_ROWS = 1000;

export const apiDataSource = new ApiDataSource();
