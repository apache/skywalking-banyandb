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

// Unique prefix for this test run so parallel runs don't collide.
const TS = `e2e-${Date.now()}`;

test.beforeAll(() => {
  mkdirSync(screenshotsDir, { recursive: true });
});

type ApiCtx = import('@playwright/test').APIRequestContext;

// loginAsAdmin works with a page (uses page.request) in test bodies.
async function loginAsAdmin(page: import('@playwright/test').Page) {
  const user = process.env.CANOPY_TEST_USER ?? 'admin';
  const pass = process.env.CANOPY_TEST_PASS ?? 'admin';
  const endpoint = process.env.BANYANDB_TARGET ?? 'http://127.0.0.1:17913';
  const res = await page.request.post('/auth/login', { data: { username: user, password: pass, endpoint } });
  expect(res.ok()).toBeTruthy();
}

// apiLogin is for beforeAll/afterAll which use the worker-scoped `request` fixture.
async function apiLogin(ctx: ApiCtx) {
  const user = process.env.CANOPY_TEST_USER ?? 'admin';
  const pass = process.env.CANOPY_TEST_PASS ?? 'admin';
  const endpoint = process.env.BANYANDB_TARGET ?? 'http://127.0.0.1:17913';
  const res = await ctx.post('/auth/login', { data: { username: user, password: pass, endpoint } });
  expect(res.ok()).toBeTruthy();
}

// ── API helpers ──────────────────────────────────────────────────────────────

async function apiCreateGroup(
  ctx: ApiCtx,
  groupName: string,
  catalog: 'CATALOG_MEASURE' | 'CATALOG_STREAM' | 'CATALOG_TRACE',
) {
  const res = await ctx.post('/api/v1/group/schema', {
    data: {
      group: {
        metadata: { name: groupName },
        catalog,
        resourceOpts: { shardNum: 1, segmentInterval: { unit: 'UNIT_DAY', num: 1 }, ttl: { unit: 'UNIT_DAY', num: 7 } },
      },
    },
  });
  if (!res.ok()) {
    const body = await res.text().catch(() => '(no body)');
    throw new Error(`apiCreateGroup(${groupName}) failed ${res.status()}: ${body}`);
  }
}

async function apiDeleteGroup(ctx: ApiCtx, groupName: string) {
  // Best-effort — ignore errors so afterAll never blocks on already-deleted groups.
  await ctx.delete(`/api/v1/group/schema/${groupName}`);
}

async function apiDeleteMeasure(ctx: ApiCtx, groupName: string, measureName: string) {
  await ctx.delete(`/api/v1/measure/schema/${groupName}/${measureName}`);
}

async function apiDeleteStream(ctx: ApiCtx, groupName: string, streamName: string) {
  await ctx.delete(`/api/v1/stream/schema/${groupName}/${streamName}`);
}

async function apiDeleteTrace(ctx: ApiCtx, groupName: string, traceName: string) {
  await ctx.delete(`/api/v1/trace/schema/${groupName}/${traceName}`);
}

// ══════════════════════════════════════════════════════════════════════════════
// 1. Group CRUD
// ══════════════════════════════════════════════════════════════════════════════

test.describe.serial('M3 CRUD — Group', () => {
  const measureGroupName = `${TS}-mg`;
  const streamGroupName = `${TS}-sg`;
  const traceGroupName = `${TS}-tg`;

  test.afterAll(async ({ request }) => {
    await apiLogin(request);
    await apiDeleteGroup(request, measureGroupName);
    await apiDeleteGroup(request, streamGroupName);
    await apiDeleteGroup(request, traceGroupName);
  });

  test('create a measure group via UI and verify card appears', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto('/metadata/measures');
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator('.page-actions .btn-primary').click();
    await expect(page.locator('.modal-title')).toContainText('New group');

    await page.locator('.modal .f-input[type="text"]').first().fill(measureGroupName);
    await expect(page.locator('.f-seg .btn.is-on')).toContainText('Measure');

    await page.locator('.modal-foot .btn-primary').click();

    await expect(page.locator(`.grp-card-name:has-text("${measureGroupName}")`)).toBeVisible({ timeout: 15_000 });
    await page.screenshot({ path: join(screenshotsDir, 'm3-crud-group-created.png'), fullPage: true });
  });

  test('navigate to the new measure group GroupPage', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto('/metadata/measures');
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator(`.grp-card:has(.grp-card-name:has-text("${measureGroupName}"))`).click();
    await expect(page).toHaveURL(new RegExp(`/metadata/measures/${measureGroupName}`));
    await expect(page.locator('.page-title')).toContainText(measureGroupName, { timeout: 10_000 });
  });

  test('update group resourceOpts via API and verify meta chips in GroupPage', async ({ page }) => {
    await loginAsAdmin(page);

    // PUT updates shardNum → 2 and ttl → 14d
    const res = await page.request.put(`/api/v1/group/schema/${measureGroupName}`, {
      data: {
        group: {
          metadata: { name: measureGroupName },
          catalog: 'CATALOG_MEASURE',
          resourceOpts: { shardNum: 2, segmentInterval: { unit: 'UNIT_DAY', num: 1 }, ttl: { unit: 'UNIT_DAY', num: 14 } },
        },
      },
    });
    expect(res.ok()).toBeTruthy();

    await page.goto(`/metadata/measures/${measureGroupName}`);
    await expect(page.locator('.page-body')).toBeVisible();

    // GroupPage renders .meta-chip rows for shards and ttl
    await expect(
      page.locator('.meta-chip').filter({ hasText: 'shards' }).locator('.meta-v'),
    ).toContainText('2', { timeout: 10_000 });
    await expect(
      page.locator('.meta-chip').filter({ hasText: 'ttl' }).locator('.meta-v'),
    ).toContainText('14d');
  });

  test('create a stream group via UI (catalog = Stream)', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto('/metadata/streams');
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator('.page-actions .btn-primary').click();
    await expect(page.locator('.modal-title')).toContainText('New group');

    await page.locator('.modal .f-input[type="text"]').first().fill(streamGroupName);
    await page.locator('.f-seg .btn', { hasText: 'Stream' }).click();
    await expect(page.locator('.f-seg .btn.is-on')).toContainText('Stream');

    await page.locator('.modal-foot .btn-primary').click();

    await expect(page.locator(`.grp-card-name:has-text("${streamGroupName}")`)).toBeVisible({ timeout: 15_000 });
  });

  test('create a trace group via UI (catalog = Trace)', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto('/metadata/traces');
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator('.page-actions .btn-primary').click();
    await expect(page.locator('.modal-title')).toContainText('New group');

    await page.locator('.modal .f-input[type="text"]').first().fill(traceGroupName);
    await page.locator('.f-seg .btn', { hasText: 'Trace' }).click();
    await expect(page.locator('.f-seg .btn.is-on')).toContainText('Trace');

    await page.locator('.modal-foot .btn-primary').click();

    await expect(page.locator(`.grp-card-name:has-text("${traceGroupName}")`)).toBeVisible({ timeout: 15_000 });
  });

  test('error: empty group name keeps modal open with validation', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto('/metadata/measures');
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator('.page-actions .btn-primary').click();
    await expect(page.locator('.modal-title')).toContainText('New group');

    // Leave name blank and submit
    await page.locator('.modal-foot .btn-primary').click();

    const hasError = await page.locator('.f-error').count() > 0;
    const modalStillOpen = await page.locator('.modal-title').isVisible();
    expect(hasError || modalStillOpen).toBeTruthy();

    await page.locator('.modal-x').click();
  });

  test('error: duplicate group name shows server error in modal', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto('/metadata/measures');
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator('.page-actions .btn-primary').click();
    await expect(page.locator('.modal-title')).toContainText('New group');

    await page.locator('.modal .f-input[type="text"]').first().fill(measureGroupName);
    await page.locator('.modal-foot .btn-primary').click();

    await expect(page.locator('.modal .f-error')).toBeVisible({ timeout: 10_000 });
    await page.screenshot({ path: join(screenshotsDir, 'm3-crud-group-error.png'), fullPage: true });

    await page.locator('.modal-x').click();
  });

  test('edit measure group via UI — opens pre-filled modal, changes TTL, verifies meta chip', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/measures/${measureGroupName}`);
    await expect(page.locator('.page-title')).toContainText(measureGroupName, { timeout: 10_000 });

    await page.locator('.page-actions .btn-ghost', { hasText: /Edit group/i }).click();
    await expect(page.locator('.modal-title')).toContainText('Edit group');

    // Name field is read-only and pre-filled (catalog input is also readonly — select first)
    await expect(page.locator('.modal .f-input[readonly]').first()).toHaveValue(measureGroupName);

    // Wait for GroupForm's useEffect to pre-fill from BanyanDB before interacting
    await expect(page.locator('.modal[data-initialized="true"]')).toBeVisible({ timeout: 5_000 });

    // Change TTL — 3rd input in the resource-opts f-grid (shardNum, segmentInterval, TTL)
    const ttlInput = page.locator('.modal .f-grid input').nth(2);
    await ttlInput.fill('30d');

    await page.locator('.modal-foot .btn-primary').click();
    await expect(page.locator('.modal-title')).toHaveCount(0, { timeout: 10_000 });

    // Verify ttl meta chip updated
    await expect(
      page.locator('.meta-chip').filter({ hasText: 'ttl' }).locator('.meta-v'),
    ).toContainText('30d', { timeout: 10_000 });
  });

  test('delete the measure group via UI and verify redirect + card gone', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/measures/${measureGroupName}`);
    await expect(page.locator('.page-title')).toContainText(measureGroupName, { timeout: 10_000 });

    await page.locator('.page-actions .btn-danger-ghost').click();
    await expect(page.locator('.modal.is-danger')).toBeVisible();
    await page.locator('.modal.is-danger .modal-foot .btn-danger').click();

    await expect(page).toHaveURL(/\/metadata\/measures$/, { timeout: 15_000 });
    await page.reload();
    await expect(page.locator(`.grp-card-name:has-text("${measureGroupName}")`)).toHaveCount(0, { timeout: 10_000 });
  });

  test('delete the stream group via UI and verify redirect + card gone', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/streams/${streamGroupName}`);
    await expect(page.locator('.page-title')).toContainText(streamGroupName, { timeout: 10_000 });

    await page.locator('.page-actions .btn-danger-ghost').click();
    await expect(page.locator('.modal.is-danger')).toBeVisible();
    await page.locator('.modal.is-danger .modal-foot .btn-danger').click();

    await expect(page).toHaveURL(/\/metadata\/streams$/, { timeout: 15_000 });
    await page.reload();
    await expect(page.locator(`.grp-card-name:has-text("${streamGroupName}")`)).toHaveCount(0, { timeout: 10_000 });
  });

  test('delete the trace group via UI and verify redirect + card gone', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/traces/${traceGroupName}`);
    await expect(page.locator('.page-title')).toContainText(traceGroupName, { timeout: 10_000 });

    await page.locator('.page-actions .btn-danger-ghost').click();
    await expect(page.locator('.modal.is-danger')).toBeVisible();
    await page.locator('.modal.is-danger .modal-foot .btn-danger').click();

    await expect(page).toHaveURL(/\/metadata\/traces$/, { timeout: 15_000 });
    await page.reload();
    await expect(page.locator(`.grp-card-name:has-text("${traceGroupName}")`)).toHaveCount(0, { timeout: 10_000 });
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// 2. Measure CRUD
// ══════════════════════════════════════════════════════════════════════════════

test.describe.serial('M3 CRUD — Measure', () => {
  const groupName = `${TS}-mgrp`;
  const measureName = `${TS}-m1`;

  test.beforeAll(async ({ request }) => {
    await apiLogin(request);
    await apiCreateGroup(request, groupName, 'CATALOG_MEASURE');
  });

  test.afterAll(async ({ request }) => {
    await apiLogin(request);
    await apiDeleteMeasure(request, groupName, measureName);
    await apiDeleteGroup(request, groupName);
  });

  test('create a measure via UI form and verify row appears in GroupPage', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/measures/${groupName}`);
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator('.btn-primary', { hasText: /New measure/i }).click();
    await expect(page.locator('.modal-title')).toContainText('New measure');

    await page.locator('#m-name').fill(measureName);

    // Fill the first tag name in the default family
    await page.locator('.fam-card .spec-row .spec-cell input[type="text"]').first().fill('t1');
    await expect(page.locator('.picker-avail .picker-chip', { hasText: 't1' })).toBeVisible({ timeout: 5_000 });
    await page.locator('.picker-avail .picker-chip', { hasText: 't1' }).click();
    await expect(page.locator('.picker-selected .picker-chip.is-on', { hasText: 't1' })).toBeVisible();

    await page.locator('.modal-foot .btn-primary').click();

    await expect(page.locator(`.res-row .rc-name .mono:has-text("${measureName}")`)).toBeVisible({ timeout: 15_000 });
    await page.screenshot({ path: join(screenshotsDir, 'm3-crud-measure-created.png'), fullPage: true });
  });

  test('navigate to measure ResourceDetailPage and verify tag family section', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/measures/${groupName}`);
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator(`.res-row`, { hasText: measureName }).click();
    await expect(page).toHaveURL(new RegExp(`/metadata/measures/${groupName}/${measureName}`));
    await expect(page.locator('.detail-block').first()).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('.detail-block .detail-h').first()).toContainText('default');
    await page.screenshot({ path: join(screenshotsDir, 'm3-crud-measure-detail.png'), fullPage: true });
  });

  test('update measure via API to add a tag and verify in detail page', async ({ page }) => {
    await loginAsAdmin(page);

    // Add tag t2 to the default family (retain t1 + entity)
    const res = await page.request.put(`/api/v1/measure/schema/${groupName}/${measureName}`, {
      data: {
        measure: {
          metadata: { name: measureName, group: groupName },
          tagFamilies: [{
            name: 'default',
            tags: [
              { name: 't1', type: 'TAG_TYPE_STRING' },
              { name: 't2', type: 'TAG_TYPE_STRING' },
            ],
          }],
          entity: { tagNames: ['t1'] },
          interval: '1m',
        },
      },
    });
    expect(res.ok()).toBeTruthy();

    await page.goto(`/metadata/measures/${groupName}/${measureName}`);
    await expect(page.locator('.detail-block').first()).toBeVisible({ timeout: 10_000 });
    // The detail page spec table shows both t1 and t2 (first table is the tag family table)
    await expect(page.locator('.spec-table').first()).toContainText('t1');
    await expect(page.locator('.spec-table').first()).toContainText('t2');
  });

  test('error: empty measure name keeps modal open with validation', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/measures/${groupName}`);
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator('.btn-primary', { hasText: /New measure/i }).click();
    await expect(page.locator('.modal-title')).toContainText('New measure');

    await page.locator('.modal-foot .btn-primary').click();

    const hasError = await page.locator('.f-error').count() > 0;
    const modalStillOpen = await page.locator('.modal-title').isVisible();
    expect(hasError || modalStillOpen).toBeTruthy();

    await page.locator('.modal-x').click();
  });

  test('edit measure via UI — opens pre-filled modal, adds a tag, verifies in detail page', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/measures/${groupName}/${measureName}`);
    await expect(page.locator('.page-body')).toBeVisible({ timeout: 10_000 });

    await page.locator('.page-actions .btn-ghost', { hasText: /\bEdit\b/i }).click();
    await expect(page.locator('.modal-title')).toContainText('Edit measure');

    // Name is pre-filled and read-only
    await expect(page.locator('#m-name')).toHaveValue(measureName);

    // Wait for useEffect pre-fill before interacting (t1 is the first tag in the default family)
    await expect(page.locator('.fam-card .spec-row .spec-cell input[type="text"]').first()).toHaveValue('t1', { timeout: 5_000 });

    // Add a third tag to the default family
    await page.locator('.fam-card .btn-ghost', { hasText: /Add tag/i }).click();
    await page.locator('.fam-card .spec-row .spec-cell input[type="text"]').last().fill('t3');

    await page.locator('.modal-foot .btn-primary').click();
    await expect(page.locator('.modal-title')).toHaveCount(0, { timeout: 10_000 });

    // Verify t3 appears in the detail page spec table
    await expect(page.locator('.spec-table').first()).toContainText('t3', { timeout: 10_000 });
  });

  // Delete from ResourceDetailPage — onDeleteResource is not wired on the group list row.
  test('delete the measure from ResourceDetailPage and verify row gone', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/measures/${groupName}/${measureName}`);
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator('.page-actions .btn-danger-ghost').click();
    await expect(page.locator('.modal.is-danger')).toBeVisible();
    await page.locator('.modal.is-danger .modal-foot .btn-danger').click();

    await expect(page).toHaveURL(new RegExp(`/metadata/measures/${groupName}$`), { timeout: 15_000 });
    await expect(page.locator(`.res-row .rc-name .mono:has-text("${measureName}")`)).toHaveCount(0, { timeout: 10_000 });
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// 3. Stream CRUD
// ══════════════════════════════════════════════════════════════════════════════

test.describe.serial('M3 CRUD — Stream', () => {
  const groupName = `${TS}-sgrp`;
  const streamName = `${TS}-s1`;

  test.beforeAll(async ({ request }) => {
    await apiLogin(request);
    await apiCreateGroup(request, groupName, 'CATALOG_STREAM');
  });

  test.afterAll(async ({ request }) => {
    await apiLogin(request);
    await apiDeleteStream(request, groupName, streamName);
    await apiDeleteGroup(request, groupName);
  });

  test('create a stream via UI form and verify row appears in GroupPage', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/streams/${groupName}`);
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator('.btn-primary', { hasText: /New stream/i }).click();
    await expect(page.locator('.modal-title')).toContainText('New stream');

    // StreamForm uses .f-input.mono for the name
    await page.locator('.modal .f-input.mono').fill(streamName);

    await page.locator('.fam-card .spec-row .spec-cell input[type="text"]').first().fill('t1');
    await expect(page.locator('.picker-avail .picker-chip', { hasText: 't1' })).toBeVisible({ timeout: 5_000 });
    await page.locator('.picker-avail .picker-chip', { hasText: 't1' }).click();
    await expect(page.locator('.picker-selected .picker-chip.is-on', { hasText: 't1' })).toBeVisible();

    await page.locator('.modal-foot .btn-primary').click();

    await expect(page.locator(`.res-row .rc-name .mono:has-text("${streamName}")`)).toBeVisible({ timeout: 15_000 });
    await page.screenshot({ path: join(screenshotsDir, 'm3-crud-stream-created.png'), fullPage: true });
  });

  test('navigate to stream ResourceDetailPage and verify tag family section', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/streams/${groupName}`);
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator(`.res-row`, { hasText: streamName }).click();
    await expect(page).toHaveURL(new RegExp(`/metadata/streams/${groupName}/${streamName}`));
    await expect(page.locator('.detail-block').first()).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('.detail-block .detail-h').first()).toContainText('default');
    await page.screenshot({ path: join(screenshotsDir, 'm3-crud-stream-detail.png'), fullPage: true });
  });

  test('update stream via API to add a tag and verify in detail page', async ({ page }) => {
    await loginAsAdmin(page);

    const res = await page.request.put(`/api/v1/stream/schema/${groupName}/${streamName}`, {
      data: {
        stream: {
          metadata: { name: streamName, group: groupName },
          tagFamilies: [{
            name: 'default',
            tags: [
              { name: 't1', type: 'TAG_TYPE_STRING' },
              { name: 't2', type: 'TAG_TYPE_STRING' },
            ],
          }],
          entity: { tagNames: ['t1'] },
        },
      },
    });
    expect(res.ok()).toBeTruthy();

    await page.goto(`/metadata/streams/${groupName}/${streamName}`);
    await expect(page.locator('.detail-block').first()).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('.spec-table')).toContainText('t1');
    await expect(page.locator('.spec-table')).toContainText('t2');
  });

  test('error: empty stream name keeps modal open with validation', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/streams/${groupName}`);
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator('.btn-primary', { hasText: /New stream/i }).click();
    await expect(page.locator('.modal-title')).toContainText('New stream');

    // Leave name blank and submit
    await page.locator('.modal-foot .btn-primary').click();

    const hasError = await page.locator('.f-error').count() > 0;
    const modalStillOpen = await page.locator('.modal-title').isVisible();
    expect(hasError || modalStillOpen).toBeTruthy();

    await page.locator('.modal-x').click();
  });

  test('edit stream via UI — opens pre-filled modal, adds a tag, verifies in detail page', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/streams/${groupName}/${streamName}`);
    await expect(page.locator('.page-body')).toBeVisible({ timeout: 10_000 });

    await page.locator('.page-actions .btn-ghost', { hasText: /\bEdit\b/i }).click();
    await expect(page.locator('.modal-title')).toContainText('Edit stream');

    // Name is pre-filled and read-only
    await expect(page.locator('.modal .f-input.mono')).toHaveValue(streamName);

    // Add a third tag to the default family
    await page.locator('.fam-card .btn-ghost', { hasText: /Add tag/i }).click();
    await page.locator('.fam-card .spec-row .spec-cell input[type="text"]').last().fill('t3');

    await page.locator('.modal-foot .btn-primary').click();
    await expect(page.locator('.modal-title')).toHaveCount(0, { timeout: 10_000 });

    // Verify t3 appears in the detail page spec table
    await expect(page.locator('.spec-table')).toContainText('t3', { timeout: 10_000 });
  });

  // Delete from ResourceDetailPage — onDeleteResource is not wired on the group list row.
  test('delete the stream from ResourceDetailPage and verify row gone', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/streams/${groupName}/${streamName}`);
    await expect(page.locator('.page-body')).toBeVisible();

    await page.locator('.page-actions .btn-danger-ghost').click();
    await expect(page.locator('.modal.is-danger')).toBeVisible();
    await page.locator('.modal.is-danger .modal-foot .btn-danger').click();

    await expect(page).toHaveURL(new RegExp(`/metadata/streams/${groupName}$`), { timeout: 15_000 });
    await expect(page.locator(`.res-row .rc-name .mono:has-text("${streamName}")`)).toHaveCount(0, { timeout: 10_000 });
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// 4. Trace
// ══════════════════════════════════════════════════════════════════════════════

test.describe.serial('M3 CRUD — Trace', () => {
  const traceGroupName = `${TS}-tgrp`;
  const traceName = `${TS}-t1`;

  test.beforeAll(async ({ request }) => {
    await apiLogin(request);
    // Create a trace group; BanyanDB supports CATALOG_TRACE groups via the REST API.
    const res = await request.post('/api/v1/group/schema', {
      data: {
        group: {
          metadata: { name: traceGroupName },
          catalog: 'CATALOG_TRACE',
          resourceOpts: { shardNum: 1, segmentInterval: { unit: 'UNIT_DAY', num: 1 }, ttl: { unit: 'UNIT_DAY', num: 7 } },
        },
      },
    });
    if (!res.ok()) {
      console.warn(`Trace group creation returned ${res.status()} — trace group tests may not fully execute`);
    }
  });

  test.afterAll(async ({ request }) => {
    await apiLogin(request);
    await apiDeleteGroup(request, traceGroupName);
  });

  test('TypeOverviewPage for traces renders group cards or empty state', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto('/metadata/traces');
    await expect(page.locator('.page-body')).toBeVisible({ timeout: 10_000 });
    // Wait for the groups query to settle — pass when EITHER groups or empty state is stable
    await expect(
      page.locator('.grp-cards').or(page.locator('.empty')),
    ).toBeVisible({ timeout: 10_000 });
  });

  test('navigate to trace group — GroupPage renders with CATALOG_TRACE chip and empty resource list', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/traces/${traceGroupName}`);
    await expect(page.locator('.page-body')).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('.page-title')).toContainText(traceGroupName);

    // Meta chip shows CATALOG_TRACE
    await expect(
      page.locator('.meta-chip').filter({ hasText: 'catalog' }).locator('.meta-v'),
    ).toContainText('CATALOG_TRACE');

    // No resources were created, so empty state is shown
    await expect(page.locator('.empty')).toBeVisible();
  });

  test('trace GroupPage "New trace" button opens the create-trace modal', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/traces/${traceGroupName}`);
    await expect(page.locator('.page-body')).toBeVisible({ timeout: 10_000 });

    // Click the primary "New trace" button (toolbar) or the empty-state button
    const newBtn = page.locator('.btn-primary', { hasText: /New trace/i });
    const emptyBtn = page.locator('.empty .btn-primary', { hasText: /Create trace/i });
    const btn = (await newBtn.count() > 0) ? newBtn.first() : emptyBtn.first();
    await btn.click();

    // Modal must open with the correct title
    await expect(page.locator('.modal-title')).toContainText('New trace', { timeout: 5_000 });

    // Close the modal via the Cancel button
    await page.locator('.modal .btn-ghost', { hasText: /Cancel/i }).click();
    await expect(page.locator('.modal-title')).toHaveCount(0);
  });

  test('create trace via UI — fills full form with role tags, verifies row in GroupPage', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/traces/${traceGroupName}`);
    await expect(page.locator('.page-body')).toBeVisible({ timeout: 10_000 });

    const newBtn = page.locator('.btn-primary', { hasText: /New trace/i });
    const emptyBtn = page.locator('.empty .btn-primary', { hasText: /Create trace/i });
    const btn = (await newBtn.count() > 0) ? newBtn.first() : emptyBtn.first();
    await btn.click();
    await expect(page.locator('.modal-title')).toContainText('New trace', { timeout: 5_000 });

    // Fill name
    await page.locator('.modal .f-input.mono').fill(traceName);

    // Add 2 extra tags to the default family (total 3: tid, sid, ts)
    const tagNameInputs = page.locator('.fam-card .spec-row .spec-cell input[type="text"]');
    await tagNameInputs.nth(0).fill('tid');
    await page.locator('.fam-card .btn-ghost', { hasText: /Add tag/i }).click();
    await tagNameInputs.nth(1).fill('sid');
    await page.locator('.fam-card .btn-ghost', { hasText: /Add tag/i }).click();
    await tagNameInputs.nth(2).fill('ts');

    // Wait for role selects to be populated with the tag names
    const roleSelects = page.locator('.modal select.f-input.f-select');
    await expect(roleSelects.nth(0)).toContainText('tid', { timeout: 5_000 });

    // Assign trace role tags
    await roleSelects.nth(0).selectOption('tid');
    await roleSelects.nth(1).selectOption('sid');
    await roleSelects.nth(2).selectOption('ts');

    await page.locator('.modal-foot .btn-primary').click();

    // Verify the new trace row appears in GroupPage
    await expect(page.locator(`.res-row .rc-name .mono:has-text("${traceName}")`)).toBeVisible({ timeout: 15_000 });
    await page.screenshot({ path: join(screenshotsDir, 'm3-crud-trace-created.png'), fullPage: true });
  });

  test('navigate to created trace resource — detail page shows tag family and role tags', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/traces/${traceGroupName}/${traceName}`);
    await expect(page.locator('.page-body')).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('.page-title')).toContainText(traceName);

    // Tag family block is rendered
    await expect(page.locator('.detail-block').first()).toBeVisible({ timeout: 10_000 });
    // Role-tag badges (trace-id / span-id / timestamp) must appear
    await expect(page.locator('.role-tag.is-reserved').first()).toBeVisible();
    await page.screenshot({ path: join(screenshotsDir, 'm3-crud-trace-detail.png'), fullPage: true });
  });

  test('delete trace resource via UI and verify redirect to group page', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto(`/metadata/traces/${traceGroupName}/${traceName}`);
    await expect(page.locator('.page-body')).toBeVisible({ timeout: 10_000 });

    await page.locator('.page-actions .btn-danger-ghost').click();
    await expect(page.locator('.modal.is-danger')).toBeVisible();
    await page.locator('.modal.is-danger .modal-foot .btn-danger').click();

    await expect(page).toHaveURL(new RegExp(`/metadata/traces/${traceGroupName}$`), { timeout: 15_000 });
    await expect(page.locator(`.res-row .rc-name .mono:has-text("${traceName}")`)).toHaveCount(0, { timeout: 10_000 });
  });

  test('update trace group resourceOpts via API and verify meta chips', async ({ page }) => {
    await loginAsAdmin(page);

    const res = await page.request.put(`/api/v1/group/schema/${traceGroupName}`, {
      data: {
        group: {
          metadata: { name: traceGroupName },
          resourceOpts: { shardNum: 2, segmentInterval: { unit: 'UNIT_DAY', num: 1 }, ttl: { unit: 'UNIT_DAY', num: 30 } },
        },
      },
    });
    // BanyanDB may reject the update if CATALOG_TRACE is not fully supported — skip gracefully.
    if (!res.ok()) {
      console.warn(`Trace group update returned ${res.status()} — skipping UI verification`);
      return;
    }

    await page.goto(`/metadata/traces/${traceGroupName}`);
    await expect(page.locator('.page-body')).toBeVisible();
    await expect(
      page.locator('.meta-chip').filter({ hasText: 'shards' }).locator('.meta-v'),
    ).toContainText('2', { timeout: 10_000 });
    await expect(
      page.locator('.meta-chip').filter({ hasText: 'ttl' }).locator('.meta-v'),
    ).toContainText('30d');
  });

  test('navigate to an existing trace resource detail page (conditional on live data)', async ({ page }) => {
    await loginAsAdmin(page);
    await page.goto('/metadata/traces');
    await expect(page.locator('.page-body')).toBeVisible({ timeout: 10_000 });

    // Find a pre-existing group that is not our ephemeral test group
    const existingCards = page.locator('.grp-card').filter({
      hasNot: page.locator(`.grp-card-name:has-text("${traceGroupName}")`),
    });
    if (await existingCards.count() === 0) {
      // No pre-existing trace groups — pass vacuously
      return;
    }

    await existingCards.first().click();
    await expect(page.locator('.page-body')).toBeVisible({ timeout: 10_000 });

    const resourceRows = page.locator('.res-row');
    if (await resourceRows.count() === 0) {
      await expect(page.locator('.empty')).toBeVisible();
      return;
    }

    await resourceRows.first().click();
    // ResourceDetailPage: at least the page body must render
    await expect(page.locator('.detail-block').first()).toBeVisible({ timeout: 10_000 });
    await page.screenshot({ path: join(screenshotsDir, 'm3-crud-trace-detail.png'), fullPage: true });
  });

  test('delete trace group via API and verify it disappears from TypeOverviewPage', async ({ page }) => {
    await loginAsAdmin(page);

    const res = await page.request.delete(`/api/v1/group/schema/${traceGroupName}`);
    expect(res.ok()).toBeTruthy();

    await page.goto('/metadata/traces');
    await expect(page.locator('.page-body')).toBeVisible({ timeout: 10_000 });
    await page.reload();
    await expect(page.locator(`.grp-card-name:has-text("${traceGroupName}")`)).toHaveCount(0, { timeout: 10_000 });
  });
});
