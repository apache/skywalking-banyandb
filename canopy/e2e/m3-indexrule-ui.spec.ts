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

/**
 * M3 IndexRule + IndexRuleBinding UI CRUD round-trip.
 *
 * Covers the two create-binding entry points surfaced by the IndexPage:
 *   1. Binding tab → "New binding" button
 *   2. Rule tab → "unbound" CTA on a rule row (presets the rule in the form)
 *
 * Each describe.serial block owns its own group/stream so the suites are
 * independent. Resource creation uses the API for setup (faster, no
 * nondeterministic typing) and the UI is reserved for the CRUD operations
 * under test, exactly the pattern used by m3-lifecycle.spec.ts.
 */
import { test, expect, type Page } from '@playwright/test';
import { join } from 'node:path';
import { mkdirSync } from 'node:fs';

const screenshotsDir = join(process.cwd(), 'e2e', 'screenshots');
// Unique prefix per run so parallel invocations do not collide on the
// BanyanDB state shared by the dev stack.
const TS = `uir-${Date.now()}`;

test.beforeAll(() => {
  mkdirSync(screenshotsDir, { recursive: true });
});

type ApiCtx = import('@playwright/test').APIRequestContext;

async function loginAsAdmin(page: Page) {
  const user = process.env.CANOPY_TEST_USER ?? 'admin';
  const pass = process.env.CANOPY_TEST_PASS ?? 'admin';
  const endpoint = process.env.BANYANDB_TARGET ?? 'http://127.0.0.1:17913';
  const res = await page.request.post('/auth/login', { data: { username: user, password: pass, endpoint } });
  expect(res.ok(), `login failed: ${await res.text()}`).toBeTruthy();
}

async function apiLogin(ctx: ApiCtx) {
  const user = process.env.CANOPY_TEST_USER ?? 'admin';
  const pass = process.env.CANOPY_TEST_PASS ?? 'admin';
  const endpoint = process.env.BANYANDB_TARGET ?? 'http://127.0.0.1:17913';
  const res = await ctx.post('/auth/login', { data: { username: user, password: pass, endpoint } });
  expect(res.ok()).toBeTruthy();
}

async function apiCreateStreamGroup(ctx: ApiCtx, name: string) {
  const res = await ctx.post('/api/v1/group/schema', {
    data: {
      group: {
        metadata: { name },
        catalog: 'CATALOG_STREAM',
        resourceOpts: { shardNum: 1, segmentInterval: { unit: 'UNIT_DAY', num: 1 }, ttl: { unit: 'UNIT_DAY', num: 7 } },
      },
    },
  });
  if (!res.ok()) throw new Error(`apiCreateStreamGroup(${name}) failed ${res.status()}: ${await res.text()}`);
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
  if (!res.ok()) throw new Error(`apiCreateStream(${name}) failed ${res.status()}: ${await res.text()}`);
}

async function apiCreateIndexRule(ctx: ApiCtx, group: string, name: string, tags: string[], type: string) {
  const res = await ctx.post('/api/v1/index-rule/schema', {
    data: { indexRule: { metadata: { name, group }, tags, type } },
  });
  if (!res.ok()) throw new Error(`apiCreateIndexRule(${name}) failed ${res.status()}: ${await res.text()}`);
}

async function apiCleanup(ctx: ApiCtx, urls: string[]) {
  // best-effort — ignore 404s so a partially-initialized test still cleans up
  for (const u of urls) await ctx.delete(u).catch(() => { /* noop */ });
}

// ── Open the Binding tab on the IndexPage and wait for the toolbar ───────────
async function openBindingTab(page: Page, type: string, group: string) {
  await page.goto(`/metadata/${type}/${group}/Index`);
  await expect(page.locator('.page-body')).toBeVisible();
  await page.locator('.idx-tab', { hasText: 'Binding' }).click();
  // The binding tab content renders either an empty state (zero bindings)
  // or a table whose header row is `.idx-bind-head`. Wait for the tab
  // itself to be active plus either the head row or the empty state, so
  // this helper works regardless of how many bindings exist.
  await expect(page.locator('.idx-tab.is-active', { hasText: 'Binding' })).toBeVisible();
  await expect(page.locator('.idx-bind-head, .empty').first()).toBeVisible();
}

async function openRuleTab(page: Page, type: string, group: string) {
  await page.goto(`/metadata/${type}/${group}/Index`);
  await expect(page.locator('.page-body')).toBeVisible();
  // The rule tab content renders either an empty state (zero rules) or a
  // table whose header row is `.idx-rule-head`. Wait for the tab itself
  // to be active plus either the head row or the empty state.
  await expect(page.locator('.idx-tab.is-active', { hasText: 'Rule' })).toBeVisible();
  await expect(page.locator('.idx-rule-head, .empty').first()).toBeVisible();
}

// Fill the name, subject, rules, and validity window of the Create/Edit
// binding modal. Always leaves "Never expires" checked — the default
// happy path, which produces a binding that renders as "active" for years.
async function fillBindingForm(
  page: Page,
  opts: { name: string; subject: string; rules: string[] },
) {
  await page.locator('.modal input[placeholder="binding_name"]').fill(opts.name);
  const subject = page.getByRole('combobox', { name: 'Resource name' });
  await subject.fill(opts.subject);
  const subjOpt = page.getByRole('option', { name: opts.subject });
  await expect(subjOpt).toBeVisible();
  await subjOpt.click();
  for (const r of opts.rules) {
    const rules = page.getByRole('combobox', { name: 'Index rules' });
    await rules.fill(r);
    const opt = page.getByRole('option', { name: r });
    await expect(opt).toBeVisible();
    await opt.click();
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// 1. IndexRule CRUD — full UI round-trip
// ══════════════════════════════════════════════════════════════════════════════

test.describe.serial('M3 UI — IndexRule CRUD', () => {
  const groupName = `${TS}-g1`;
  const streamName = `${TS}-s1`;
  const ruleName = `${TS}-r1`;

  test.beforeAll(async ({ request }) => {
    await apiLogin(request);
    await apiCreateStreamGroup(request, groupName);
    await apiCreateStream(request, groupName, streamName);
  });

  test.afterAll(async ({ request }) => {
    await apiLogin(request);
    await apiCleanup(request, [
      `/api/v1/index-rule/schema/${groupName}/${ruleName}`,
      `/api/v1/stream/schema/${groupName}/${streamName}`,
      `/api/v1/group/schema/${groupName}`,
    ]);
  });

  test('create a TREE index rule via the IndexPage Rule tab', async ({ page }) => {
    await loginAsAdmin(page);
    await openRuleTab(page, 'streams', groupName);

    await page.locator('.res-toolbar .btn-primary', { hasText: /New index rule/i }).click();
    await expect(page.locator('.modal-title')).toContainText('Create index rule');

    // Name
    await page.locator('.modal input[placeholder="by_service"]').fill(ruleName);

    // Add a custom tag — the recommended-tags picker was removed in favour
    // of the custom-tag flow.
    await page.locator('.modal input[placeholder="tag_name"]').fill('host');
    await page.locator('.modal input[placeholder="tag_name"]').press('Enter');
    await expect(page.locator('.modal .picker-chip.is-on', { hasText: 'host' })).toBeVisible();

    // Pick the "tree" type card.
    await page.locator('.modal .idx-type-card', { hasText: 'tree' }).click();
    await expect(page.locator('.modal .idx-type-card.is-on', { hasText: 'tree' })).toBeVisible();

    await page.locator('.modal-foot .btn-primary').click();
    await expect(page.locator('.modal-title')).toHaveCount(0, { timeout: 10_000 });

    // Row appears in the IndexPage rule table.
    await expect(
      page.locator(`.idx-rule-row .idx-name:has-text("${ruleName}")`),
    ).toBeVisible({ timeout: 10_000 });
    // The "tree" badge should be visible somewhere in that row.
    await expect(
      page.locator(`.idx-rule-row:has(.idx-name:has-text("${ruleName}")) .idx-type-badge`),
    ).toContainText(/tree/i);
    await page.screenshot({ path: join(screenshotsDir, 'm3-indexrule-ui-created.png'), fullPage: true });
  });

  test('edit the rule — change tags and switch to inverted type', async ({ page }) => {
    await loginAsAdmin(page);
    await openRuleTab(page, 'streams', groupName);

    // Find the row, click the Edit action (pencil icon, second .idx-act).
    const row = page.locator('.idx-rule-row', { hasText: ruleName });
    await expect(row).toBeVisible();
    await row.locator('.idx-act[title="Edit"]').click();
    await expect(page.locator('.modal-title')).toContainText('Edit index rule');

    // Name input is read-only.
    await expect(page.locator('.modal input[placeholder="by_service"]')).toHaveValue(ruleName);
    await expect(page.locator('.modal input[placeholder="by_service"]')).toHaveAttribute('readonly', '');

    // Existing tag is shown.
    await expect(page.locator('.modal .picker-chip.is-on', { hasText: 'host' })).toBeVisible();

    // Add svc and switch to inverted.
    await page.locator('.modal input[placeholder="tag_name"]').fill('svc');
    await page.locator('.modal input[placeholder="tag_name"]').press('Enter');
    await expect(page.locator('.modal .picker-chip.is-on', { hasText: 'svc' })).toBeVisible();

    await page.locator('.modal .idx-type-card', { hasText: 'inverted' }).click();
    await expect(page.locator('.modal .idx-type-card.is-on', { hasText: 'inverted' })).toBeVisible();

    await page.locator('.modal-foot .btn-primary').click();
    await expect(page.locator('.modal-title')).toHaveCount(0, { timeout: 10_000 });

    // Row now shows two chips and the inverted badge.
    const updatedRow = page.locator('.idx-rule-row', { hasText: ruleName });
    await expect(updatedRow.locator('.idx-tag', { hasText: 'host' })).toBeVisible();
    await expect(updatedRow.locator('.idx-tag', { hasText: 'svc' })).toBeVisible();
    await expect(updatedRow.locator('.idx-type-badge')).toContainText(/inv/i);
  });

  test('delete the rule and confirm it disappears from the table', async ({ page }) => {
    await loginAsAdmin(page);
    await openRuleTab(page, 'streams', groupName);

    const rowCountBefore = await page.locator('.idx-rule-row').count();
    const row = page.locator('.idx-rule-row', { hasText: ruleName });
    await expect(row).toBeVisible();
    await row.locator('.idx-act.is-danger[title="Delete"]').click();
    await expect(page.locator('.modal-title')).toContainText('Delete index rule');
    await page.locator('.modal-foot .btn-danger').click();
    await expect(page.locator('.modal-title')).toHaveCount(0, { timeout: 10_000 });

    await expect(page.locator('.idx-rule-row', { hasText: ruleName })).toHaveCount(0, { timeout: 10_000 });
    const rowCountAfter = await page.locator('.idx-rule-row').count();
    expect(rowCountAfter).toBe(rowCountBefore - 1);

    // Round-trip via API: GET on the deleted rule returns 404.
    const apiCheck = await page.request.get(`/api/v1/index-rule/schema/${groupName}/${ruleName}`);
    expect(apiCheck.status()).toBe(404);
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// 2. IndexRuleBinding CRUD — entry point 1 (Binding tab "New binding")
// ══════════════════════════════════════════════════════════════════════════════

test.describe.serial('M3 UI — IndexRuleBinding CRUD (entry: Binding tab)', () => {
  const groupName = `${TS}-g2`;
  const streamName = `${TS}-s2`;
  const rule1 = `${TS}-r2a`;
  const rule2 = `${TS}-r2b`;
  const bindingName = `${TS}-b2`;

  test.beforeAll(async ({ request }) => {
    await apiLogin(request);
    await apiCreateStreamGroup(request, groupName);
    await apiCreateStream(request, groupName, streamName);
    await apiCreateIndexRule(request, groupName, rule1, ['host'], 'TYPE_TREE');
    await apiCreateIndexRule(request, groupName, rule2, ['host'], 'TYPE_TREE');
  });

  test.afterAll(async ({ request }) => {
    await apiLogin(request);
    await apiCleanup(request, [
      `/api/v1/index-rule-binding/schema/${groupName}/${bindingName}`,
      `/api/v1/index-rule/schema/${groupName}/${rule1}`,
      `/api/v1/index-rule/schema/${groupName}/${rule2}`,
      `/api/v1/stream/schema/${groupName}/${streamName}`,
      `/api/v1/group/schema/${groupName}`,
    ]);
  });

  test('create a binding via the Binding tab "New binding" button', async ({ page }) => {
    await loginAsAdmin(page);
    await openBindingTab(page, 'streams', groupName);

    await page.locator('.res-toolbar .btn-primary', { hasText: /New binding/i }).click();
    await expect(page.locator('.modal-title')).toContainText('Create binding');

    await fillBindingForm(page, { name: bindingName, subject: streamName, rules: [rule1, rule2] });

    await page.locator('.modal-foot .btn-primary').click();
    await expect(page.locator('.modal-title')).toHaveCount(0, { timeout: 10_000 });

    // Default scope is "Needs attention", which hides the just-created
    // binding (it is active, not orphan/expired). Switch to "All" so the
    // new row is visible. The filter button label is e.g. "All1" (no space).
    await page.locator('.idx-filter-btn', { hasText: /^All/ }).click();

    // Row appears in the Binding tab table with both rules and the right subject.
    // Give BanyanDB plenty of time to make the new binding visible — it has
    // eventual consistency, and the refetch via query invalidation can lag.
    const row = page.locator('.idx-bind-row', { hasText: bindingName });
    await expect(row).toBeVisible({ timeout: 20_000 });
    await expect(row.locator('.subj-chip', { hasText: streamName })).toBeVisible();
    await expect(row.locator('.idx-tag', { hasText: rule1 })).toBeVisible();
    await expect(row.locator('.idx-tag', { hasText: rule2 })).toBeVisible();
    // Default is "active" because never-expires puts expireAt at 2099-12-31.
    await expect(row.locator('.idx-status')).toContainText(/active/i);
    await page.screenshot({ path: join(screenshotsDir, 'm3-indexrule-ui-binding-created.png'), fullPage: true });
  });

  test('edit the binding — remove one rule, leave the other', async ({ page }) => {
    await loginAsAdmin(page);
    await openBindingTab(page, 'streams', groupName);

    // Default scope is "Needs attention"; the active binding is hidden
    // there. Switch to "All" so the row is visible.
    await page.locator('.idx-filter-btn', { hasText: /^All/ }).click();

    const row = page.locator('.idx-bind-row', { hasText: bindingName });
    await expect(row).toBeVisible();
    await row.locator('.idx-act[title="Edit"]').click();
    await expect(page.locator('.modal-title')).toContainText('Edit binding');

    // Name is read-only and pre-filled.
    await expect(page.locator('.modal input[placeholder="binding_name"]')).toHaveValue(bindingName);
    await expect(page.locator('.modal input[placeholder="binding_name"]')).toHaveAttribute('readonly', '');

    // Subject combobox carries the existing subject.
    await expect(page.getByRole('combobox', { name: 'Resource name' })).toHaveValue(streamName);

    // Both rules are pre-selected as chips. Clicking the chip itself
    // removes it (the onClick lives on the picker-chip button, not on the
    // nested × span).
    await page.locator('.modal .picker-chip.is-on', { hasText: rule1 }).click();
    await expect(page.locator('.modal .picker-chip.is-on', { hasText: rule1 })).toHaveCount(0);
    await expect(page.locator('.modal .picker-chip.is-on', { hasText: rule2 })).toBeVisible();

    await page.locator('.modal-foot .btn-primary').click();
    await expect(page.locator('.modal-title')).toHaveCount(0, { timeout: 10_000 });

    // Row now shows only rule2.
    const updatedRow = page.locator('.idx-bind-row', { hasText: bindingName });
    await expect(updatedRow.locator('.idx-tag', { hasText: rule2 })).toBeVisible();
    await expect(updatedRow.locator('.idx-tag', { hasText: rule1 })).toHaveCount(0);
  });

  test('delete the binding and confirm it disappears from the table', async ({ page }) => {
    await loginAsAdmin(page);
    await openBindingTab(page, 'streams', groupName);

    // Default scope is "Needs attention"; switch to "All" so the row is visible.
    await page.locator('.idx-filter-btn', { hasText: /^All/ }).click();

    const rowCountBefore = await page.locator('.idx-bind-row').count();
    const row = page.locator('.idx-bind-row', { hasText: bindingName });
    await expect(row).toBeVisible();
    await row.locator('.idx-act.is-danger[title="Delete"]').click();
    await expect(page.locator('.modal-title')).toContainText('Delete binding');
    await page.locator('.modal-foot .btn-danger').click();
    await expect(page.locator('.modal-title')).toHaveCount(0, { timeout: 10_000 });

    await expect(page.locator('.idx-bind-row', { hasText: bindingName })).toHaveCount(0, { timeout: 10_000 });
    const rowCountAfter = await page.locator('.idx-bind-row').count();
    expect(rowCountAfter).toBe(rowCountBefore - 1);

    const apiCheck = await page.request.get(`/api/v1/index-rule-binding/schema/${groupName}/${bindingName}`);
    expect(apiCheck.status()).toBe(404);
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// 3. IndexRuleBinding CRUD — entry point 2 (rule row "unbound" CTA)
// ══════════════════════════════════════════════════════════════════════════════

test.describe.serial('M3 UI — IndexRuleBinding CRUD (entry: rule row Bind CTA)', () => {
  const groupName = `${TS}-g3`;
  const streamName = `${TS}-s3`;
  const ruleName = `${TS}-r3`;
  const bindingName = `${TS}-b3`;

  test.beforeAll(async ({ request }) => {
    await apiLogin(request);
    await apiCreateStreamGroup(request, groupName);
    await apiCreateStream(request, groupName, streamName);
    await apiCreateIndexRule(request, groupName, ruleName, ['host'], 'TYPE_TREE');
  });

  test.afterAll(async ({ request }) => {
    await apiLogin(request);
    await apiCleanup(request, [
      `/api/v1/index-rule-binding/schema/${groupName}/${bindingName}`,
      `/api/v1/index-rule/schema/${groupName}/${ruleName}`,
      `/api/v1/stream/schema/${groupName}/${streamName}`,
      `/api/v1/group/schema/${groupName}`,
    ]);
  });

  test('click "unbound" on a rule row → modal opens with preset rule', async ({ page }) => {
    await loginAsAdmin(page);
    await openRuleTab(page, 'streams', groupName);

    const row = page.locator('.idx-rule-row', { hasText: ruleName });
    await expect(row).toBeVisible();

    // The "unbound" CTA appears as a link-styled button in the Bound subjects
    // cell — the IconLink + 'unbound' text. Clicking it should open the
    // Create binding modal with the rule pre-selected.
    await row.locator('.idx-link-cta', { hasText: /unbound/i }).click();
    await expect(page.locator('.modal-title')).toContainText('Create binding');

    // Preset notice banner: "Created from rule <ruleName> — it has been pre-selected …"
    await expect(page.locator('.modal .idx-inline-warn', { hasText: ruleName })).toBeVisible();

    // Rules picker already contains the preset rule as a selected chip.
    await expect(page.locator('.modal .picker-chip.is-on', { hasText: ruleName })).toBeVisible();

    // Fill the rest of the form (subject + window).
    await page.locator('.modal input[placeholder="binding_name"]').fill(bindingName);
    const subject = page.getByRole('combobox', { name: 'Resource name' });
    await subject.fill(streamName);
    const subjOpt = page.getByRole('option', { name: streamName });
    await expect(subjOpt).toBeVisible();
    await subjOpt.click();

    await page.locator('.modal-foot .btn-primary').click();
    await expect(page.locator('.modal-title')).toHaveCount(0, { timeout: 10_000 });

    // The row no longer offers the "unbound" CTA — it now shows a real
    // subject chip pointing at the stream.
    const updatedRow = page.locator('.idx-rule-row', { hasText: ruleName });
    await expect(updatedRow.locator('.idx-link-cta', { hasText: /unbound/i })).toHaveCount(0);

    // Switch to the Binding tab and confirm the new binding is listed.
    await page.locator('.idx-tab', { hasText: 'Binding' }).click();
    // Either the table head or the empty state appears once the tab is active;
    // once we know which, give BanyanDB extra time to surface the binding.
    await expect(page.locator('.idx-tab.is-active', { hasText: 'Binding' })).toBeVisible();
    // Default scope is "Needs attention" — switch to "All" so the active binding is visible.
    await page.locator('.idx-filter-btn', { hasText: /^All/ }).click();
    const bindRow = page.locator('.idx-bind-row', { hasText: bindingName });
    await expect(bindRow).toBeVisible({ timeout: 20_000 });
    await expect(bindRow.locator('.idx-tag', { hasText: ruleName })).toBeVisible();
  });
});
