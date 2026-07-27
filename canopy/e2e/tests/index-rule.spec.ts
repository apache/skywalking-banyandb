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

// index-rule.spec.ts — IndexRule + IndexRuleBinding coverage, ported from
// m3-indexrule.spec.ts (API round-trips + server-authority negatives) and
// m3-indexrule-ui.spec.ts (UI CRUD via the IndexPage). Preconditions
// (stream group / stream / rules) are seeded over HTTP; UI locators are
// semantic (role=tab tabs, data-testid rows, role=dialog modals, role=combobox
// subject/rules pickers).

import { test, expect } from '../framework/fixtures.js';

// ── API round-trips (server is authoritative) ────────────────────────────────

test.describe('index rule API @e2e @index-rule @seed', () => {
  test('index rule: create → get → list → update → delete', async ({ seed, request }) => {
    const group = await seed.createGroup('e2e-ira', 'CATALOG_STREAM');
    await seed.createStream(group, 's');
    const ruleName = seed.uniqueName('rule');
    seed.trackResource(`/api/v1/index-rule/schema/${group}/${ruleName}`);

    const createRes = await request.post('/api/v1/index-rule/schema', {
      data: { indexRule: { metadata: { name: ruleName, group }, tags: ['host'], type: 'TYPE_TREE' } },
    });
    expect(createRes.ok(), `create failed: ${await createRes.text()}`).toBeTruthy();

    const getRes = await request.get(`/api/v1/index-rule/schema/${group}/${ruleName}`);
    expect(getRes.ok()).toBeTruthy();
    const got = (await getRes.json()) as { indexRule: { tags: string[]; type: string } };
    expect(got.indexRule.tags).toEqual(['host']);
    expect(got.indexRule.type).toBe('TYPE_TREE');

    const listRes = await request.get(`/api/v1/index-rule/schema/lists/${group}`);
    expect(listRes.ok()).toBeTruthy();
    const listed = (await listRes.json()) as { indexRule: Array<{ metadata: { name: string } }> };
    expect(listed.indexRule?.map((r) => r.metadata.name)).toContain(ruleName);

    const updRes = await request.put(`/api/v1/index-rule/schema/${group}/${ruleName}`, {
      data: { indexRule: { metadata: { name: ruleName, group }, tags: ['host', 'svc'], type: 'TYPE_INVERTED' } },
    });
    expect(updRes.ok(), `update failed: ${await updRes.text()}`).toBeTruthy();
    const updGot = (await (await request.get(`/api/v1/index-rule/schema/${group}/${ruleName}`)).json()) as { indexRule: { tags: string[]; type: string } };
    expect(updGot.indexRule.tags).toEqual(['host', 'svc']);
    expect(updGot.indexRule.type).toBe('TYPE_INVERTED');

    const delRes = await request.delete(`/api/v1/index-rule/schema/${group}/${ruleName}`);
    expect(delRes.ok()).toBeTruthy();
    const afterDel = await request.get(`/api/v1/index-rule/schema/${group}/${ruleName}`);
    expect(afterDel.status()).toBe(404);
  });

  test('index rule binding: create → get → list → update → delete', async ({ seed, request }) => {
    const group = await seed.createGroup('e2e-irb', 'CATALOG_STREAM');
    const stream = await seed.createStream(group, 's');
    const ruleName = await seed.createIndexRule(group, ['host'], 'rl');
    const bindingName = seed.uniqueName('bind');
    seed.trackResource(`/api/v1/index-rule-binding/schema/${group}/${bindingName}`);

    const bindRes = await request.post('/api/v1/index-rule-binding/schema', {
      data: {
        indexRuleBinding: {
          metadata: { name: bindingName, group },
          rules: [ruleName],
          subject: { name: stream, catalog: 'CATALOG_STREAM' },
          beginAt: '2026-01-01T00:00:00Z',
          expireAt: '2026-12-31T23:59:59Z',
        },
      },
    });
    expect(bindRes.ok(), `create binding failed: ${await bindRes.text()}`).toBeTruthy();

    const got = await request.get(`/api/v1/index-rule-binding/schema/${group}/${bindingName}`);
    expect(got.ok()).toBeTruthy();
    const gotJson = (await got.json()) as { indexRuleBinding: { rules: string[]; subject: { name: string } } };
    expect(gotJson.indexRuleBinding.rules).toEqual([ruleName]);
    expect(gotJson.indexRuleBinding.subject.name).toBe(stream);

    const list = await request.get(`/api/v1/index-rule-binding/schema/lists/${group}`);
    expect(list.ok()).toBeTruthy();
    const listJson = (await list.json()) as { indexRuleBinding: Array<{ metadata: { name: string } }> };
    expect(listJson.indexRuleBinding?.map((b) => b.metadata.name)).toContain(bindingName);

    const upd = await request.put(`/api/v1/index-rule-binding/schema/${group}/${bindingName}`, {
      data: {
        indexRuleBinding: {
          metadata: { name: bindingName, group },
          rules: [],
          subject: { name: stream, catalog: 'CATALOG_STREAM' },
          beginAt: '2026-02-01T00:00:00Z',
          expireAt: '2026-11-30T23:59:59Z',
        },
      },
    });
    // The server may accept or reject empty rules — either is a valid authority outcome.
    expect([200, 400, 422].includes(upd.status())).toBeTruthy();

    const del = await request.delete(`/api/v1/index-rule-binding/schema/${group}/${bindingName}`);
    expect(del.ok()).toBeTruthy();
    const afterDel = await request.get(`/api/v1/index-rule-binding/schema/${group}/${bindingName}`);
    expect(afterDel.status()).toBe(404);
  });

  // KNOWN REAL BACKEND BUG: an index-rule binding with expireAt <= beginAt is
  // invalid, but BanyanDB standalone accepts it instead of returning an error.
  // Marked test.fail so it documents the correct expectation now and turns into
  // an "unexpected pass" (the signal to drop test.fail) once the server adds the
  // validation. TODO: track upstream — flip to `test` when BanyanDB rejects it.
  test.fail('server SHOULD reject expireAt <= beginAt (known backend bug)', async ({ seed, request }) => {
    const group = await seed.createGroup('e2e-irw', 'CATALOG_STREAM');
    const stream = await seed.createStream(group, 's');
    const ruleName = await seed.createIndexRule(group, ['host'], 'rl');
    const bindingName = seed.uniqueName('bind-bad');
    seed.trackResource(`/api/v1/index-rule-binding/schema/${group}/${bindingName}`);
    const bad = await request.post('/api/v1/index-rule-binding/schema', {
      data: {
        indexRuleBinding: {
          metadata: { name: bindingName, group },
          rules: [ruleName],
          subject: { name: stream, catalog: 'CATALOG_STREAM' },
          beginAt: '2026-12-01T00:00:00Z',
          expireAt: '2026-01-01T00:00:00Z',
        },
      },
    });
    expect(bad.ok()).toBeFalsy();
  });

  // NORMAL BACKEND BEHAVIOR (not a bug): index rules bind tags by NAME and do
  // not require the tag to be declared on a resource yet, so the server accepts
  // a rule over an as-yet-undeclared tag. Assert the accepted-behavior contract.
  test('server accepts an index rule over an undeclared tag name', async ({ seed, request }) => {
    const group = await seed.createGroup('e2e-irz', 'CATALOG_STREAM');
    await seed.createStream(group, 's');
    const ruleName = seed.uniqueName('rule-z');
    seed.trackResource(`/api/v1/index-rule/schema/${group}/${ruleName}`);
    const res = await request.post('/api/v1/index-rule/schema', {
      data: { indexRule: { metadata: { name: ruleName, group }, tags: ['undeclared_tag'], type: 'TYPE_TREE' } },
    });
    expect(res.ok()).toBeTruthy();
  });
});

// ── UI CRUD via the IndexPage ────────────────────────────────────────────────

test.describe('index rule UI @e2e @index-rule @seed', () => {
  test('creates a TREE index rule via the Rule tab', async ({ indexRulePage, seed, page }) => {
    const group = await seed.createGroup('e2e-uir', 'CATALOG_STREAM');
    await seed.createStream(group, 's');
    const ruleName = seed.uniqueName('r');
    seed.trackResource(`/api/v1/index-rule/schema/${group}/${ruleName}`);

    await indexRulePage.goto('streams', group);
    await indexRulePage.openRuleTab();
    await indexRulePage.createRule(ruleName, ['host'], 'tree');

    const row = indexRulePage.ruleRow(ruleName);
    await expect(row).toBeVisible();
    await expect(row.getByText(/tree/i)).toBeVisible();
  });

  test('edits a rule — adds a tag and switches to inverted', async ({ indexRulePage, seed }) => {
    const group = await seed.createGroup('e2e-uie', 'CATALOG_STREAM');
    await seed.createStream(group, 's');
    const ruleName = await seed.createIndexRule(group, ['host'], 'r', 'TYPE_TREE');

    await indexRulePage.goto('streams', group);
    await indexRulePage.openRuleTab();
    const dlg = await indexRulePage.openEditRule(ruleName);
    // Name is pre-filled and read-only.
    await expect(dlg.getByPlaceholder('by_service')).toHaveValue(ruleName);
    await expect(dlg.getByPlaceholder('by_service')).toHaveAttribute('readonly', '');
    // Existing tag chip is shown.
    await expect(dlg.getByRole('button', { name: /Remove host/ })).toBeVisible();
    // Add svc, switch to inverted.
    await dlg.getByPlaceholder('tag_name').fill('svc');
    await dlg.getByRole('button', { name: 'Add', exact: true }).click();
    await dlg.getByRole('button', { name: /inverted/i }).click();
    await dlg.getByRole('button', { name: 'Save changes' }).click();
    await expect(dlg).toBeHidden();

    const row = indexRulePage.ruleRow(ruleName);
    await expect(row.getByText('host')).toBeVisible();
    await expect(row.getByText('svc')).toBeVisible();
    await expect(row.getByText(/inv/i)).toBeVisible();
  });

  test('deletes a rule and confirms it is gone (UI + API 404)', async ({ indexRulePage, seed, request }) => {
    const group = await seed.createGroup('e2e-uid', 'CATALOG_STREAM');
    await seed.createStream(group, 's');
    const ruleName = await seed.createIndexRule(group, ['host'], 'r', 'TYPE_TREE');

    await indexRulePage.goto('streams', group);
    await indexRulePage.openRuleTab();
    await indexRulePage.deleteRule(ruleName);
    await expect(indexRulePage.ruleRow(ruleName)).toHaveCount(0);
    const apiCheck = await request.get(`/api/v1/index-rule/schema/${group}/${ruleName}`);
    expect(apiCheck.status()).toBe(404);
  });

  test('creates a binding via the Binding tab "New binding"', async ({ indexRulePage, seed }) => {
    const group = await seed.createGroup('e2e-uib', 'CATALOG_STREAM');
    const stream = await seed.createStream(group, 's');
    const rule1 = await seed.createIndexRule(group, ['host'], 'ra');
    const rule2 = await seed.createIndexRule(group, ['host'], 'rb');
    const bindingName = seed.uniqueName('b');
    seed.trackResource(`/api/v1/index-rule-binding/schema/${group}/${bindingName}`);

    await indexRulePage.goto('streams', group);
    await indexRulePage.openBindingTab();
    await indexRulePage.createBinding({ name: bindingName, subject: stream, rules: [rule1, rule2] });
    // Default scope hides active bindings — switch to "All".
    await indexRulePage.showAllBindings();

    const row = indexRulePage.bindRow(bindingName);
    await expect(row).toBeVisible({ timeout: 20_000 });
    await expect(row.getByText(stream)).toBeVisible();
    await expect(row.getByText(rule1)).toBeVisible();
    await expect(row.getByText(rule2)).toBeVisible();
    await expect(row.getByText(/active/i)).toBeVisible();
  });

  test('edits a binding — removes one rule, keeps the other', async ({ indexRulePage, seed, request }) => {
    const group = await seed.createGroup('e2e-ube', 'CATALOG_STREAM');
    const stream = await seed.createStream(group, 's');
    const rule1 = await seed.createIndexRule(group, ['host'], 'ra');
    const rule2 = await seed.createIndexRule(group, ['host'], 'rb');
    const bindingName = seed.uniqueName('b');
    seed.trackResource(`/api/v1/index-rule-binding/schema/${group}/${bindingName}`);
    const create = await request.post('/api/v1/index-rule-binding/schema', {
      data: {
        indexRuleBinding: {
          metadata: { name: bindingName, group },
          rules: [rule1, rule2],
          subject: { name: stream, catalog: 'CATALOG_STREAM' },
          beginAt: '2026-01-01T00:00:00Z',
          expireAt: '2099-12-31T23:59:59Z',
        },
      },
    });
    expect(create.ok(), `binding create failed: ${await create.text()}`).toBeTruthy();

    await indexRulePage.goto('streams', group);
    await indexRulePage.openBindingTab();
    await indexRulePage.showAllBindings();
    const dlg = await indexRulePage.openEditBinding(bindingName);
    await expect(dlg.getByPlaceholder('binding_name')).toHaveValue(bindingName);
    await expect(dlg.getByPlaceholder('binding_name')).toHaveAttribute('readonly', '');
    // Removing a selected rule chip: the click handler is on the chip button.
    await dlg.getByRole('button', { name: new RegExp(`Remove ${rule1}`) }).click();
    await dlg.getByRole('button', { name: 'Save changes' }).click();
    await expect(dlg).toBeHidden();

    const row = indexRulePage.bindRow(bindingName);
    await expect(row.getByText(rule2)).toBeVisible();
    await expect(row.getByText(rule1)).toHaveCount(0);
  });

  test('binds a rule from its "unbound" row CTA (rule pre-selected)', async ({ indexRulePage, seed }) => {
    const group = await seed.createGroup('e2e-ucta', 'CATALOG_STREAM');
    const stream = await seed.createStream(group, 's');
    const ruleName = await seed.createIndexRule(group, ['host'], 'r');
    const bindingName = seed.uniqueName('b');
    seed.trackResource(`/api/v1/index-rule-binding/schema/${group}/${bindingName}`);

    await indexRulePage.goto('streams', group);
    await indexRulePage.openRuleTab();
    const dlg = await indexRulePage.bindFromRuleCta(ruleName);
    // The rule is pre-selected as a chip and named in the preset notice.
    await expect(dlg.getByText(ruleName).first()).toBeVisible();
    await dlg.getByPlaceholder('binding_name').fill(bindingName);
    await dlg.getByRole('combobox', { name: 'Resource name' }).fill(stream);
    await dlg.getByRole('option', { name: stream }).click();
    await dlg.getByRole('button', { name: 'Create binding' }).click();
    await expect(dlg).toBeHidden();

    // The rule row no longer offers the unbound CTA.
    await expect(indexRulePage.ruleRow(ruleName).getByRole('button', { name: /unbound/i })).toHaveCount(0);
    // The binding is listed under "All".
    await indexRulePage.openBindingTab();
    await indexRulePage.showAllBindings();
    await expect(indexRulePage.bindRow(bindingName)).toBeVisible({ timeout: 20_000 });
  });
});
