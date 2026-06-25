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
  TraceSchema, PropertySchema,
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

export class ApiDataSource implements DataSource {
  // ── Groups ──────────────────────────────────────────────────────────────

  async listGroups(): Promise<GroupListResponse> {
    const data = await apiFetch<{ groups?: Group[] }>('/api/v1/group/schema/lists');
    return { groups: data.groups ?? [] };
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
    type ListResp = { stream?: StreamSchema[]; measure?: MeasureSchema[]; trace?: TraceSchema[]; property?: PropertySchema[] };
    const data = await apiFetch<ListResp>(`/api/v1/${type}/schema/lists/${group}`);
    return (data.stream ?? data.measure ?? data.trace ?? data.property ?? []) as (StreamSchema | MeasureSchema | TraceSchema | PropertySchema)[];
  }

  async getResource(type: string, group: string, name: string): Promise<StreamSchema | MeasureSchema | TraceSchema | PropertySchema> {
    type GetResp = { stream?: StreamSchema; measure?: MeasureSchema; trace?: TraceSchema; property?: PropertySchema };
    const data = await apiFetch<GetResp>(`/api/v1/${type}/schema/${group}/${name}`);
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

  // ── Generic delete ────────────────────────────────────────────────────────

  async deleteResource(type: string, group: string, name: string): Promise<void> {
    await apiFetch<void>(`/api/v1/${type}/schema/${group}/${name}`, { method: 'DELETE' });
  }

  // ── Query ─────────────────────────────────────────────────────────────────

  async runQuery(_request: QueryRequest): Promise<QueryResponse> {
    throw new Error('runQuery not implemented until M4');
  }
}

export const apiDataSource = new ApiDataSource();
