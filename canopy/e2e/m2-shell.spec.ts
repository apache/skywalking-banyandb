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

test('M2: authenticated shell renders sidebar', async ({ page }) => {
  await loginAsAdmin(page);
  await page.goto('/');

  // Shell layout
  await expect(page.locator('.shell')).toBeVisible();
  await expect(page.locator('.sidebar')).toBeVisible();

  // Brand
  await expect(page.locator('.brand-name')).toContainText('Canopy');

  // Nav items — use exact to avoid matching "Collapse Metadata", "Toggle sidebar", etc.
  await expect(page.locator('button[title="Home"]')).toBeVisible();
  await expect(page.locator('button[title="Metadata"]')).toBeVisible();
  await expect(page.locator('button[title="Properties"]')).toBeVisible();
  await expect(page.locator('button[title="Query"]')).toBeVisible();

  // Connection indicator
  await expect(page.locator('.conn-dot')).toBeVisible();

  // Sign out button
  await expect(page.getByRole('button', { name: /Sign out/i })).toBeVisible();

  await page.screenshot({ path: join(screenshotsDir, 'm2-shell-sidebar.png'), fullPage: true });
});

test('M2: home page shows welcome and quick-nav', async ({ page }) => {
  await loginAsAdmin(page);
  await page.goto('/');

  // Home page title
  await expect(page.locator('.page-title')).toContainText('Home', { timeout: 15_000 });

  // Welcome section renders
  await expect(page.locator('.welcome')).toBeVisible();

  // Quick-grid cards render
  await expect(page.locator('.quick-grid')).toBeVisible();
  await expect(page.locator('.quick-card')).toHaveCount(6);

  await page.screenshot({ path: join(screenshotsDir, 'm2-home-groups.png'), fullPage: true });
});

test('M2: sidebar collapse toggle works', async ({ page }) => {
  await loginAsAdmin(page);
  await page.goto('/');

  await expect(page.locator('.sidebar')).toBeVisible();
  expect(await page.locator('.sidebar.is-collapsed').count()).toBe(0);

  await page.getByRole('button', { name: /Toggle sidebar/i }).click();
  await expect(page.locator('.sidebar.is-collapsed')).toBeVisible();

  await page.getByRole('button', { name: /Toggle sidebar/i }).click();
  expect(await page.locator('.sidebar.is-collapsed').count()).toBe(0);
});
