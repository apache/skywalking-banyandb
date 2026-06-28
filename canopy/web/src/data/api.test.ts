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

import { describe, it, expect, vi, beforeEach } from 'vitest';

import type {
  IndexRuleBindingSchema, CreateIndexRuleBindingRequest, UpdateIndexRuleBindingRequest,
} from 'canopy-shared';

// Mock the global fetch the ApiDataSource uses so we can exercise the encoder
// and decoder without hitting the network.
const fetchMock = vi.fn();
vi.stubGlobal('fetch', fetchMock);

const okJson = (body: unknown): Response =>
  new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } });

const BINDING: IndexRuleBindingSchema = {
  metadata: { name: 'b-1', group: 'demo_logs' },
  rules: ['by_entity'],
  subject: { name: 'demo_logs-pg-00', catalog: 'CATALOG_STREAM' },
  beginAt: Date.parse('2026-06-27T14:00:00Z'),
  expireAt: Date.parse('2099-12-31T00:00:00Z'),
};

async function loadApi() {
  // Import after the fetch stub so the module picks it up.
  const mod = await import('./api.js');
  return mod.apiDataSource;
}

describe('apiDataSource — IndexRuleBinding encoder/decoder', () => {
  beforeEach(() => {
    fetchMock.mockReset();
  });

  it('encode → POST body uses RFC3339 strings for beginAt and expireAt', async () => {
    let capturedBody: unknown;
    // BanyanDB liaison write operations return ONLY `{modRevision}` — not the
    // full binding. The encoder still has to convert epoch ms to RFC3339.
    fetchMock.mockImplementation(async (_url: string, init?: RequestInit) => {
      capturedBody = JSON.parse(init?.body as string);
      return okJson({ modRevision: '123' });
    });
    const api = await loadApi();
    const req: CreateIndexRuleBindingRequest = { indexRuleBinding: BINDING };
    const got = await api.createIndexRuleBinding(req);
    const body = capturedBody as { indexRuleBinding: Record<string, unknown> };
    expect(typeof body.indexRuleBinding.beginAt).toBe('string');
    expect((body.indexRuleBinding.beginAt as string)).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
    expect(typeof body.indexRuleBinding.expireAt).toBe('string');
    expect((body.indexRuleBinding.expireAt as string)).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
    // Round-trip: parse the emitted ISO back into a Date and compare to the input.
    expect(Date.parse(body.indexRuleBinding.beginAt as string)).toBe(BINDING.beginAt);
    expect(Date.parse(body.indexRuleBinding.expireAt as string)).toBe(BINDING.expireAt);
    // The create call echoes the request payload back as the result so callers
    // (e.g. the form's onSuccess) have something to hand to onClose.
    expect(got).toBe(BINDING);
  });

  it('update → echoes the request payload even though the liaison only returns modRevision', async () => {
    fetchMock.mockResolvedValue(okJson({ modRevision: '456' }));
    const api = await loadApi();
    const got = await api.updateIndexRuleBinding('demo_logs', 'b-1', { indexRuleBinding: BINDING });
    expect(got).toBe(BINDING);
  });

  it('create → no longer crashes with "e.beginAt" when the liaison returns only modRevision', async () => {
    // Regression: the previous code did `decodeBinding(data.indexRuleBinding)`
    // even when the response had no binding field, which crashed inside the
    // decoder with `undefined.beginAt`. The fix returns the request payload
    // instead. This test would have thrown before the fix.
    fetchMock.mockResolvedValue(okJson({ modRevision: '789' }));
    const api = await loadApi();
    const got = await api.createIndexRuleBinding({ indexRuleBinding: BINDING });
    expect(got.metadata.name).toBe('b-1');
  });

  it('decode → reads RFC3339 strings from the API response into epoch ms', async () => {
    fetchMock.mockResolvedValue(okJson({
      indexRuleBinding: {
        metadata: BINDING.metadata,
        rules: BINDING.rules,
        subject: BINDING.subject,
        beginAt: '2026-06-27T14:00:00Z',
        expireAt: '2099-12-31T00:00:00Z',
      },
    }));
    const api = await loadApi();
    const got = await api.getIndexRuleBinding('demo_logs', 'b-1');
    expect(got.beginAt).toBe(BINDING.beginAt);
    expect(got.expireAt).toBe(BINDING.expireAt);
  });

  it('decode → tolerates missing beginAt/expireAt by defaulting to 0', async () => {
    fetchMock.mockResolvedValue(okJson({
      indexRuleBinding: {
        metadata: BINDING.metadata,
        rules: BINDING.rules,
        subject: BINDING.subject,
        // Both fields omitted — should default to 0 instead of throwing.
      },
    }));
    const api = await loadApi();
    const got = await api.getIndexRuleBinding('demo_logs', 'b-1');
    expect(got.beginAt).toBe(0);
    expect(got.expireAt).toBe(0);
  });

  it('encode → throws a clear error when the payload is undefined', async () => {
    const api = await loadApi();
    // Cast to `any` to simulate the kind of broken input the encoder is meant to
    // protect against (e.g. an upstream caller passing a malformed request).
    await expect(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      api.createIndexRuleBinding(undefined as any),
    ).rejects.toThrow(/missing indexRuleBinding/);
  });

  it('encode → throws a clear error when beginAt is not a number', async () => {
    const api = await loadApi();
    const broken: CreateIndexRuleBindingRequest = {
      indexRuleBinding: {
        ...BINDING,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        beginAt: undefined as any,
      },
    };
    await expect(api.createIndexRuleBinding(broken)).rejects.toThrow(/beginAt/);
  });

  it('encode → throws a clear error when expireAt is not a number', async () => {
    const api = await loadApi();
    const broken: CreateIndexRuleBindingRequest = {
      indexRuleBinding: {
        ...BINDING,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        expireAt: null as unknown as number,
      },
    };
    await expect(api.createIndexRuleBinding(broken)).rejects.toThrow(/expireAt/);
  });

  it('update → encode path uses the same guards', async () => {
    const api = await loadApi();
    const broken: UpdateIndexRuleBindingRequest = {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      indexRuleBinding: undefined as any,
    };
    await expect(api.updateIndexRuleBinding('demo_logs', 'b-1', broken)).rejects.toThrow(/missing indexRuleBinding/);
  });
});
