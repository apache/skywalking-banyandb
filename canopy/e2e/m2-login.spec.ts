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

test('M2: login page renders design elements', async ({ page }) => {
  await page.goto('/');

  // Verify the login card is visible
  await expect(page.locator('.lf-card')).toBeVisible();

  // Brand panel with wordmark
  await expect(page.locator('.lf-wordmark')).toContainText('Canopy');
  await expect(page.locator('.lf-eyebrow')).toContainText('BanyanDB Console');

  // Form fields
  await expect(page.getByLabel(/Endpoint/i)).toBeVisible();
  await expect(page.getByLabel(/Username/i)).toBeVisible();
  await expect(page.getByLabel(/Password/i)).toBeVisible();

  // Role picker
  await expect(page.getByRole('radio', { name: /Administrator/i })).toBeVisible();
  await expect(page.getByRole('radio', { name: /Read-only/i })).toBeVisible();

  // Connect button
  await expect(page.getByRole('button', { name: /Connect/i })).toBeVisible();

  await page.screenshot({ path: join(screenshotsDir, 'm2-login.png'), fullPage: true });
});

test('M2: bad credentials show error banner', async ({ page }) => {
  await page.goto('/');
  await expect(page.locator('.lf-card')).toBeVisible();

  await page.getByLabel(/Password/i).fill('wrongpassword');
  await page.getByRole('button', { name: /Connect/i }).click();

  // Wait for the error banner to appear
  await expect(page.locator('.lf-banner.err')).toBeVisible({ timeout: 10_000 });

  await page.screenshot({ path: join(screenshotsDir, 'm2-login-error.png'), fullPage: true });
});

test('M2: login succeeds and navigates to shell', async ({ page }) => {
  await page.goto('/');
  await expect(page.locator('.lf-card')).toBeVisible();

  const user = process.env.CANOPY_TEST_USER ?? 'admin';
  const pass = process.env.CANOPY_TEST_PASS ?? 'admin';

  await page.getByLabel(/Username/i).fill(user);
  await page.getByLabel(/Password/i).fill(pass);
  await page.getByRole('button', { name: /Connect/i }).click();

  // Wait for shell to appear after login
  await expect(page.locator('.shell')).toBeVisible({ timeout: 15_000 });

  await page.screenshot({ path: join(screenshotsDir, 'm2-shell.png'), fullPage: true });
});
