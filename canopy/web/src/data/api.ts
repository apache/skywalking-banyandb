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
  PropertySchema,
  QueryRequest, QueryResponse,
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
    type RawGroup = Omit<Group, 'name'> & { metadata: { name: string } };
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
      .map((g) => ({ ...g, name: g.metadata.name }))
      .sort((a, b) => a.name.localeCompare(b.name));
    return { groups };
  }

  async createGroup(req: CreateGroupRequest): Promise<Group> {
    const data = await apiFetch<{ group: Group }>('/api/v1/group/schema', {
      method: 'POST', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return data.group;
  }

  async updateGroup(name: string, req: UpdateGroupRequest): Promise<Group> {
    const data = await apiFetch<{ group: Group }>(`/api/v1/group/schema/${name}`, {
      method: 'PUT', headers: JSON_HEADERS, body: JSON.stringify(req),
    });
    return data.group;
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

  async runQuery(_request: QueryRequest): Promise<QueryResponse> {
    throw new Error('runQuery not implemented until M4');
  }
}

export const apiDataSource = new ApiDataSource();
