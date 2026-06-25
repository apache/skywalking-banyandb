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

test.beforeAll(() => {
  mkdirSync(screenshotsDir, { recursive: true });
});

async function loginAsAdmin(page: import('@playwright/test').Page) {
  const user = process.env.CANOPY_TEST_USER ?? 'admin';
  const pass = process.env.CANOPY_TEST_PASS ?? 'admin';
  const endpoint = process.env.BANYANDB_TARGET ?? 'http://127.0.0.1:17913';

  const res = await page.request.post('/auth/login', {
    data: { username: user, password: pass, endpoint },
  });
  expect(res.ok()).toBeTruthy();
}

test('M3: TypeOverview page renders for measures', async ({ page }) => {
  await loginAsAdmin(page);
  await page.goto('/metadata/measures');

  // Page body always renders
  await expect(page.locator('.page-body')).toBeVisible();

  // Either group cards or empty state is shown
  const hasGroups = await page.locator('.grp-card').count() > 0;
  if (hasGroups) {
    await expect(page.locator('.grp-cards')).toBeVisible();
  } else {
    // Empty state is visible when no measure groups exist
    await expect(page.locator('.empty')).toBeVisible();
  }

  await page.screenshot({ path: join(screenshotsDir, 'm3-measures-overview.png'), fullPage: true });
});

test('M3: clicking a group card navigates to GroupPage', async ({ page }) => {
  await loginAsAdmin(page);
  await page.goto('/metadata/measures');
  await expect(page.locator('.page-body')).toBeVisible();

  const groupCard = page.locator('.grp-card').first();
  const hasGroup = await groupCard.count() > 0;

  if (!hasGroup) {
    // Nothing to click — verify empty state and skip navigation check
    await expect(page.locator('.empty')).toBeVisible();
    return;
  }

  const groupName = await groupCard.locator('.grp-card-name').textContent();
  await groupCard.click();

  // GroupPage renders
  await expect(page.locator('.page-body')).toBeVisible();
  // URL changed to /metadata/measures/{groupName}
  await expect(page).toHaveURL(/\/metadata\/measures\//);

  // Resource table or empty state is shown
  const hasResources = await page.locator('.res-row').count() > 0;
  if (hasResources) {
    await expect(page.locator('.res-table')).toBeVisible();
  } else {
    await expect(page.locator('.empty')).toBeVisible();
  }

  await page.screenshot({ path: join(screenshotsDir, `m3-group-${groupName ?? 'unknown'}.png`), fullPage: true });
});

test('M3: clicking a resource row navigates to ResourceDetailPage', async ({ page }) => {
  await loginAsAdmin(page);
  await page.goto('/metadata/measures');
  await expect(page.locator('.page-body')).toBeVisible();

  const groupCard = page.locator('.grp-card').first();
  const hasGroup = await groupCard.count() > 0;

  if (!hasGroup) {
    // No groups — nothing to navigate to
    await expect(page.locator('.empty')).toBeVisible();
    return;
  }

  await groupCard.click();
  await expect(page).toHaveURL(/\/metadata\/measures\//);

  const resourceRow = page.locator('.res-row').first();
  const hasResource = await resourceRow.count() > 0;

  if (!hasResource) {
    // No resources in this group
    await expect(page.locator('.empty')).toBeVisible();
    return;
  }

  const resourceName = await resourceRow.locator('.rc-name').textContent();
  await resourceRow.click();

  // ResourceDetailPage renders
  await expect(page.locator('.page-body')).toBeVisible();
  await expect(page).toHaveURL(/\/metadata\/measures\/[^/]+\//);

  // Detail sections render
  await expect(page.locator('.detail-block').first()).toBeVisible();

  await page.screenshot({ path: join(screenshotsDir, `m3-resource-${(resourceName ?? 'unknown').trim()}.png`), fullPage: true });
});
