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

// lifecycle.spec.ts — group lifecycle-stage editor, ported from
// m3-lifecycle.spec.ts. The stage editor lives inside the Group create/edit
// modal. Stages are addressed via role="group" landmarks ("Stage N"),
// aria-labelled inputs, and aria-pressed unit toggles (added by the redesign),
// so no positional nth()/CSS selectors remain. Preconditions are seeded over
// HTTP; UI-created groups are registered for teardown.

import { test, expect } from '../framework/fixtures.js';
import type { APIRequestContext } from '@playwright/test';

async function seedGroupWithStage(request: APIRequestContext, name: string): Promise<void> {
  const res = await request.post('/api/v1/group/schema', {
    data: {
      group: {
        metadata: { name },
        catalog: 'CATALOG_MEASURE',
        resourceOpts: {
          shardNum: 1,
          segmentInterval: { unit: 'UNIT_DAY', num: 1 },
          ttl: { unit: 'UNIT_DAY', num: 7 },
          stages: [
            { name: 'warm', shardNum: 2, segmentInterval: { unit: 'UNIT_DAY', num: 1 }, ttl: { unit: 'UNIT_DAY', num: 7 }, nodeSelector: 'tier=warm', close: true, replicas: 0 },
          ],
        },
      },
    },
  });
  if (!res.ok()) throw new Error(`seedGroupWithStage failed: ${res.status()} ${await res.text()}`);
}

test.describe('lifecycle stages — create modal @e2e @lifecycle @seed', () => {
  test('the stages section is visible for a non-PROPERTY catalog', async ({ schemaPage, lifecyclePage }) => {
    await schemaPage.gotoOverview('measures');
    await schemaPage.newGroupButton().click();
    await expect(schemaPage.dialog('New group')).toBeVisible();
    await expect(lifecyclePage.sectionTitle()).toBeVisible();
    await expect(lifecyclePage.addStageButton()).toBeVisible();
  });

  test('the stages section is hidden when PROPERTY catalog is chosen', async ({ schemaPage, lifecyclePage }) => {
    await schemaPage.gotoOverview('measures');
    await schemaPage.newGroupButton().click();
    const dlg = schemaPage.dialog('New group');
    await expect(lifecyclePage.sectionTitle()).toBeVisible();
    await dlg.getByRole('button', { name: 'PROPERTY' }).click();
    await expect(lifecyclePage.sectionTitle()).toHaveCount(0);
    // Switching back restores it.
    await dlg.getByRole('button', { name: 'MEASURE' }).click();
    await expect(lifecyclePage.sectionTitle()).toBeVisible();
  });

  test('adding and removing stage cards', async ({ schemaPage, lifecyclePage }) => {
    await schemaPage.gotoOverview('measures');
    await schemaPage.newGroupButton().click();
    await expect(lifecyclePage.stageCards()).toHaveCount(0);
    await lifecyclePage.addStage();
    await expect(lifecyclePage.stageCards()).toHaveCount(1);
    await expect(lifecyclePage.stage(0)).toBeVisible();
    await lifecyclePage.addStage();
    await expect(lifecyclePage.stageCards()).toHaveCount(2);
    await expect(lifecyclePage.stage(1)).toBeVisible();
    await lifecyclePage.removeStage(0);
    await expect(lifecyclePage.stageCards()).toHaveCount(1);
  });

  test('a new stage card is pre-filled with defaults', async ({ schemaPage, lifecyclePage }) => {
    await schemaPage.gotoOverview('measures');
    await schemaPage.newGroupButton().click();
    await lifecyclePage.addStage();
    await expect(lifecyclePage.stage(0).getByLabel('Stage shards')).toHaveValue('2');
    await expect(lifecyclePage.stage(0).getByLabel('Stage segment interval value')).toHaveValue('1');
    await lifecyclePage.expectUnitSelected(0, 'Stage segment interval', 'Day');
    await expect(lifecyclePage.stage(0).getByLabel('Stage TTL value')).toHaveValue('7');
    await lifecyclePage.expectUnitSelected(0, 'Stage TTL', 'Day');
    await expect(lifecyclePage.closeCheckbox(0)).toBeChecked();
  });

  test('the segment-interval unit toggles between Hour and Day', async ({ schemaPage, lifecyclePage }) => {
    await schemaPage.gotoOverview('measures');
    await schemaPage.newGroupButton().click();
    await lifecyclePage.addStage();
    await lifecyclePage.expectUnitSelected(0, 'Stage segment interval', 'Day');
    await lifecyclePage.setUnit(0, 'Stage segment interval', 'Hour');
    await lifecyclePage.expectUnitSelected(0, 'Stage segment interval', 'Hour');
    await lifecyclePage.setUnit(0, 'Stage segment interval', 'Day');
    await lifecyclePage.expectUnitSelected(0, 'Stage segment interval', 'Day');
  });

  test('submitting a stage with an empty name shows a validation error', async ({ schemaPage, lifecyclePage }) => {
    await schemaPage.gotoOverview('measures');
    await schemaPage.newGroupButton().click();
    const dlg = schemaPage.dialog('New group');
    await dlg.getByPlaceholder('sw_metric').fill('val-err-group');
    await lifecyclePage.addStage();
    await dlg.getByRole('button', { name: 'Create group' }).click();
    await expect(lifecyclePage.stage(0).getByText('Stage name is required')).toBeVisible();
    await expect(dlg).toBeVisible();
  });

  test('submitting a stage with empty shards shows a validation error', async ({ schemaPage, lifecyclePage }) => {
    await schemaPage.gotoOverview('measures');
    await schemaPage.newGroupButton().click();
    const dlg = schemaPage.dialog('New group');
    await dlg.getByPlaceholder('sw_metric').fill('val-err-shards');
    await lifecyclePage.addStage();
    await lifecyclePage.fillStage(0, { name: 'warm' });
    await lifecyclePage.stage(0).getByLabel('Stage shards').fill('');
    await dlg.getByRole('button', { name: 'Create group' }).click();
    await expect(lifecyclePage.stage(0).getByText('Must be > 0')).toBeVisible();
    await expect(dlg).toBeVisible();
  });

  test('creates a measure group with a "warm" lifecycle stage', async ({ schemaPage, lifecyclePage, seed }) => {
    const name = seed.uniqueName('e2e-lc');
    await schemaPage.gotoOverview('measures');
    await schemaPage.newGroupButton().click();
    const dlg = schemaPage.dialog('New group');
    await dlg.getByPlaceholder('sw_metric').fill(name);
    await lifecyclePage.addStage();
    await lifecyclePage.fillStage(0, { name: 'warm', shards: 2, segNum: 1, ttlNum: 7, nodeSelector: 'tier=hot' });
    await dlg.getByRole('button', { name: 'Create group' }).click();
    seed.trackGroup(name);
    await expect(schemaPage.groupCard(name)).toBeVisible();
    await schemaPage.gotoGroup('measures', name);
    await expect(schemaPage.metaChip('stages')).toContainText('warm');
  });
});

test.describe('lifecycle stages — edit modal @e2e @lifecycle @seed', () => {
  test('adds a second "cold" stage to a group that already has "warm"', async ({ schemaPage, lifecyclePage, seed, request }) => {
    const name = seed.uniqueName('e2e-lce');
    await seedGroupWithStage(request, name);
    seed.trackGroup(name);
    await schemaPage.gotoGroup('measures', name);
    const dlg = await schemaPage.openEditGroup();
    const before = await lifecyclePage.stageCards().count();
    await lifecyclePage.addStage();
    await lifecyclePage.fillStage(before, { name: 'cold', shards: 1, segNum: 7, ttlNum: 30, nodeSelector: 'tier=cold' });
    await dlg.getByRole('button', { name: 'Save changes' }).click();
    await expect(dlg).toBeHidden();
  });

  test('re-opening the edit modal loads stages from the server', async ({ schemaPage, lifecyclePage, seed, request }) => {
    const name = seed.uniqueName('e2e-lcr');
    await seedGroupWithStage(request, name);
    seed.trackGroup(name);
    await schemaPage.gotoGroup('measures', name);
    await schemaPage.openEditGroup();
    await expect(lifecyclePage.stageCards().first()).toBeVisible();
    await expect(lifecyclePage.stage(0).getByLabel('Stage name')).not.toHaveValue('');
  });

  test('an empty-stage group edit shows the section with an Add button and no cards', async ({ schemaPage, lifecyclePage, seed }) => {
    const name = await seed.createGroup('e2e-lcempty', 'CATALOG_MEASURE');
    await schemaPage.gotoGroup('measures', name);
    await schemaPage.openEditGroup();
    await expect(lifecyclePage.sectionTitle()).toBeVisible();
    await expect(lifecyclePage.addStageButton()).toBeVisible();
    await expect(lifecyclePage.stageCards()).toHaveCount(0);
  });

  test('adding a stage then cancelling does not persist it', async ({ schemaPage, lifecyclePage, seed }) => {
    const name = await seed.createGroup('e2e-lccancel', 'CATALOG_MEASURE');
    await schemaPage.gotoGroup('measures', name);
    const dlg = await schemaPage.openEditGroup();
    await lifecyclePage.addStage();
    await expect(lifecyclePage.stageCards()).toHaveCount(1);
    await dlg.getByRole('button', { name: 'Cancel' }).click();
    await expect(dlg).toBeHidden();
    // Re-open — the unsaved stage is gone.
    await schemaPage.openEditGroup();
    await expect(lifecyclePage.stageCards()).toHaveCount(0);
  });
});
