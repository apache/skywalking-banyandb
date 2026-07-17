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

// boot.spec.ts — smoke coverage ported from m0-boot.spec.ts + m1-bff.spec.ts.
// Verifies the BFF is healthy, the SPA boots with the dark theme, and (via the
// storageState/NOAUTH fast path) the authenticated shell renders on load.

import { test, expect } from '../framework/fixtures.js';

test.describe('boot @e2e @boot @smoke', () => {
  test('BFF /healthz returns ok', async ({ request }) => {
    const res = await request.get('/healthz');
    expect(res.ok()).toBeTruthy();
    const body = (await res.json()) as { status: string };
    expect(body.status).toBe('ok');
  });

  test('SPA boots with the dark theme applied', async ({ page }) => {
    await page.goto('/');
    // The theme sets the body background to #0d120e = rgb(13, 18, 14).
    const bgColor = await page.evaluate(() => window.getComputedStyle(document.body).backgroundColor);
    expect(bgColor).toMatch(/rgb\(13,\s*18,\s*14\)/);
  });

  test('authenticated shell renders on boot', async ({ shellPage }) => {
    // Under the NOAUTH/storageState fast path the app is already authenticated,
    // so the shell (not the login form) is the empty-state landing surface.
    await shellPage.goto();
    await shellPage.expectShell();
  });
});
