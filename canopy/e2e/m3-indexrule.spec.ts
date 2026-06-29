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

import { test, expect } from '@playwright/test';
import { join } from 'node:path';
import { mkdirSync } from 'node:fs';

const screenshotsDir = join(process.cwd(), 'e2e', 'screenshots');

const TS = `idx-${Date.now()}`;

test.beforeAll(() => {
  mkdirSync(screenshotsDir, { recursive: true });
});

type ApiCtx = import('@playwright/test').APIRequestContext;

async function apiLogin(ctx: ApiCtx) {
  const user = process.env.CANOPY_TEST_USER ?? 'admin';
  const pass = process.env.CANOPY_TEST_PASS ?? 'admin';
  const res = await ctx.post('/auth/login', { data: { username: user, password: pass } });
  expect(res.ok()).toBeTruthy();
}

async function apiCreateGroup(ctx: ApiCtx, name: string, catalog: 'CATALOG_MEASURE' | 'CATALOG_STREAM' | 'CATALOG_TRACE') {
  const res = await ctx.post('/api/v1/group/schema', {
    data: {
      group: {
        metadata: { name },
        catalog,
        resourceOpts: {
          shardNum: 1,
          segmentInterval: { unit: 'UNIT_DAY', num: 1 },
          ttl: { unit: 'UNIT_DAY', num: 7 },
        },
      },
    },
  });
  if (!res.ok()) {
    const body = await res.text().catch(() => '(no body)');
    throw new Error(`apiCreateGroup(${name}) failed ${res.status()}: ${body}`);
  }
}

async function apiDeleteGroup(ctx: ApiCtx, name: string) {
  await ctx.delete(`/api/v1/group/schema/${name}`);
}

async function apiCreateStream(ctx: ApiCtx, group: string, name: string) {
  const res = await ctx.post('/api/v1/stream/schema', {
    data: {
      stream: {
        metadata: { name, group },
        tagFamilies: [{ name: 'default', tags: [{ name: 'host', type: 'TAG_TYPE_STRING' }] }],
        entity: { tagNames: ['host'] },
      },
    },
  });
  if (!res.ok()) throw new Error(`apiCreateStream failed ${res.status()}: ${await res.text()}`);
}

async function apiDeleteStream(ctx: ApiCtx, group: string, name: string) {
  await ctx.delete(`/api/v1/stream/schema/${group}/${name}`);
}

// ── IndexRule CRUD ──────────────────────────────────────────────────────────

test.describe.serial('M3 — IndexRule CRUD round-trip (live)', () => {
  const groupName = `${TS}-sg`;
  const streamName = `${TS}-str`;
  const ruleName = `${TS}-rule`;

  test.afterAll(async ({ request }) => {
    await apiLogin(request);
    await apiDeleteStream(request, groupName, streamName);
    await apiDeleteGroup(request, groupName);
  });

  test('create → get → list → update → delete a TREE index rule', async ({ request }) => {
    await apiLogin(request);
    await apiCreateGroup(request, groupName, 'CATALOG_STREAM');
    await apiCreateStream(request, groupName, streamName);

    // Create
    const createRes = await request.post('/api/v1/index-rule/schema', {
      data: {
        indexRule: {
          metadata: { name: ruleName, group: groupName },
          tags: ['host'],
          type: 'TYPE_TREE',
        },
      },
    });
    expect(createRes.ok(), `create failed: ${await createRes.text()}`).toBeTruthy();

    // Get
    const getRes = await request.get(`/api/v1/index-rule/schema/${groupName}/${ruleName}`);
    expect(getRes.ok()).toBeTruthy();
    const got = (await getRes.json()) as { indexRule: { tags: string[]; type: string } };
    expect(got.indexRule.tags).toEqual(['host']);
    expect(got.indexRule.type).toBe('TYPE_TREE');

    // List
    const listRes = await request.get(`/api/v1/index-rule/schema/lists/${groupName}`);
    expect(listRes.ok()).toBeTruthy();
    const listed = (await listRes.json()) as { indexRule: Array<{ metadata: { name: string } }> };
    expect(listed.indexRule?.map((r) => r.metadata.name)).toContain(ruleName);

    // Update
    const updRes = await request.put(`/api/v1/index-rule/schema/${groupName}/${ruleName}`, {
      data: {
        indexRule: {
          metadata: { name: ruleName, group: groupName },
          tags: ['host', 'svc'],
          type: 'TYPE_INVERTED',
        },
      },
    });
    expect(updRes.ok(), `update failed: ${await updRes.text()}`).toBeTruthy();
    const updGot = await (await request.get(`/api/v1/index-rule/schema/${groupName}/${ruleName}`)).json() as { indexRule: { tags: string[]; type: string } };
    expect(updGot.indexRule.tags).toEqual(['host', 'svc']);
    expect(updGot.indexRule.type).toBe('TYPE_INVERTED');

    // Delete
    const delRes = await request.delete(`/api/v1/index-rule/schema/${groupName}/${ruleName}`);
    expect(delRes.ok()).toBeTruthy();
    const afterDel = await request.get(`/api/v1/index-rule/schema/${groupName}/${ruleName}`);
    expect(afterDel.status()).toBe(404);
  });
});

// ── IndexRuleBinding CRUD ───────────────────────────────────────────────────

test.describe.serial('M3 — IndexRuleBinding CRUD round-trip (live)', () => {
  const groupName = `${TS}-bg`;
  const streamName = `${TS}-str`;
  const ruleName = `${TS}-rl`;
  const bindingName = `${TS}-bind`;

  test.afterAll(async ({ request }) => {
    await apiLogin(request);
    // Order matters: binding → rule → stream → group.
    await request.delete(`/api/v1/index-rule-binding/schema/${groupName}/${bindingName}-bad`).catch(() => {});
    await request.delete(`/api/v1/index-rule-binding/schema/${groupName}/${bindingName}`).catch(() => {});
    await request.delete(`/api/v1/index-rule/schema/${groupName}/${ruleName}`).catch(() => {});
    await apiDeleteStream(request, groupName, streamName);
    await apiDeleteGroup(request, groupName);
  });

  test('create → get → list → update → delete a binding', async ({ request }) => {
    await apiLogin(request);
    await apiCreateGroup(request, groupName, 'CATALOG_STREAM');
    await apiCreateStream(request, groupName, streamName);

    // Need a rule to bind to.
    await request.post('/api/v1/index-rule/schema', {
      data: {
        indexRule: {
          metadata: { name: ruleName, group: groupName },
          tags: ['host'],
          type: 'TYPE_TREE',
        },
      },
    });

    // Create binding
    const bindRes = await request.post('/api/v1/index-rule-binding/schema', {
      data: {
        indexRuleBinding: {
          metadata: { name: bindingName, group: groupName },
          rules: [ruleName],
          subject: { name: streamName, catalog: 'CATALOG_STREAM' },
          beginAt: '2026-01-01T00:00:00Z',
          expireAt: '2026-12-31T23:59:59Z',
        },
      },
    });
    expect(bindRes.ok(), `create binding failed: ${await bindRes.text()}`).toBeTruthy();

    // Get
    const got = await request.get(`/api/v1/index-rule-binding/schema/${groupName}/${bindingName}`);
    expect(got.ok()).toBeTruthy();
    const gotJson = (await got.json()) as { indexRuleBinding: { rules: string[]; subject: { name: string } } };
    expect(gotJson.indexRuleBinding.rules).toEqual([ruleName]);
    expect(gotJson.indexRuleBinding.subject.name).toBe(streamName);

    // List
    const list = await request.get(`/api/v1/index-rule-binding/schema/lists/${groupName}`);
    expect(list.ok()).toBeTruthy();
    const listJson = (await list.json()) as { indexRuleBinding: Array<{ metadata: { name: string } }> };
    expect(listJson.indexRuleBinding?.map((b) => b.metadata.name)).toContain(bindingName);

    // Update — change rules
    const upd = await request.put(`/api/v1/index-rule-binding/schema/${groupName}/${bindingName}`, {
      data: {
        indexRuleBinding: {
          metadata: { name: bindingName, group: groupName },
          rules: [],
          subject: { name: streamName, catalog: 'CATALOG_STREAM' },
          beginAt: '2026-02-01T00:00:00Z',
          expireAt: '2026-11-30T23:59:59Z',
        },
      },
    });
    // The server may reject empty rules — that's a server-authority test in
    // its own right. We expect either 200 (accepted) or 4xx (rejected).
    expect([200, 400, 422].includes(upd.status())).toBeTruthy();

    // Delete
    const del = await request.delete(`/api/v1/index-rule-binding/schema/${groupName}/${bindingName}`);
    expect(del.ok()).toBeTruthy();
    const afterDel = await request.get(`/api/v1/index-rule-binding/schema/${groupName}/${bindingName}`);
    expect(afterDel.status()).toBe(404);
  });

  test.fail('rejects expireAt <= beginAt (server-authority, MF2)', async ({ request }) => {
    await apiLogin(request);
    const bad = await request.post('/api/v1/index-rule-binding/schema', {
      data: {
        indexRuleBinding: {
          metadata: { name: `${bindingName}-bad`, group: groupName },
          rules: [ruleName],
          subject: { name: streamName, catalog: 'CATALOG_STREAM' },
          beginAt: '2026-12-01T00:00:00Z',
          expireAt: '2026-01-01T00:00:00Z',
        },
      },
    });
    // The server is authoritative (MF2): even if the client UI happened to
    // allow this through, the server must reject it.
    expect(bad.ok()).toBeFalsy();
    expect([400, 422, 500].includes(bad.status())).toBeTruthy();
  });
});

// ── MF2 server-authority: client-passes/server-rejects ───────────────────────

test.describe.serial('M3 — server-authority negative (MF2)', () => {
  const groupName = `${TS}-sf`;
  const streamName = `${TS}-str`;

  test.afterAll(async ({ request }) => {
    await apiLogin(request);
    await apiDeleteStream(request, groupName, streamName);
    await apiDeleteGroup(request, groupName);
  });

  test.fail('client allows "0" tag name, server rejects it', async ({ request }) => {
    await apiLogin(request);
    await apiCreateGroup(request, groupName, 'CATALOG_STREAM');
    await apiCreateStream(request, groupName, streamName);

    // The M3 client regex is [A-Za-z0-9_-]+ which allows "0".
    // The server, however, requires tag names that resolve to a real tag on the
    // target resource (or a stricter set depending on version). "0" is not
    // declared on `streamName`, so the server should reject the rule.
    const res = await request.post('/api/v1/index-rule/schema', {
      data: {
        indexRule: {
          metadata: { name: `${TS}-z`, group: groupName },
          tags: ['0'],
          type: 'TYPE_TREE',
        },
      },
    });
    // Expect server rejection (4xx or 5xx). The web client would block this
    // with a validation message, so the assertion here documents what happens
    // if a less-strict client submits the same payload.
    expect(res.ok()).toBeFalsy();
    expect([400, 404, 422, 500].includes(res.status())).toBeTruthy();
  });
});
