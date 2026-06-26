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

test('M1: login screen renders', async ({ page }) => {
  await page.goto('/');

  // M2 will wire up the real login form; for now verify the page loads
  await expect(page.locator('body')).toBeVisible();
  await page.screenshot({ path: join(screenshotsDir, 'm1-login.png'), fullPage: true });
});

test('M1: /healthz returns ok via BFF', async ({ request }) => {
  const res = await request.get('/healthz');
  expect(res.ok()).toBeTruthy();
  const body = await res.json();
  expect(body.status).toBe('ok');
});

test('M1: authenticated empty state renders', async ({ page }) => {
  // Use page.request so the session cookie is shared with the browser context
  const loginRes = await page.request.post('/auth/login', {
    data: {
      username: process.env.CANOPY_TEST_USER ?? 'admin',
      password: process.env.CANOPY_TEST_PASS ?? 'admin',
      endpoint: process.env.BANYANDB_TARGET ?? 'http://127.0.0.1:17913',
    },
  });
  expect(loginRes.ok()).toBeTruthy();

  await page.goto('/');
  await expect(page.locator('body')).toBeVisible();
  await page.screenshot({ path: join(screenshotsDir, 'm1-authed-empty.png'), fullPage: true });
});
