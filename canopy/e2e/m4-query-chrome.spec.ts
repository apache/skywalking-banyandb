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

// m4-query-chrome.spec.ts — pixel-regression gate for the *chrome* of the M4
// Query console: sidebar + page header + empty-result state. The builder
// rail is intentionally excluded — its content is data-bound (selections
// reflect the live BanyanDB group / resource list, which has no fixed
// canonical seed), so a pixel test against it would be flaky.
//
// On first run Playwright writes the baselines under
// `canopy/e2e/m4-query-chrome.spec.ts-snapshots/`. Subsequent runs
// compare against the committed baselines with maxDiffPixelRatio: 0.01
// (1% of the region). Anything inside the data-bound rail changing does
// not fail this test.

import { test, expect } from '@playwright/test';

test.describe('M4 query console chrome pixel-regression', () => {
  // The regions are pixel-matched independently. Each `.toHaveScreenshot`
  // call is treated as an independent assertion.
  const REGIONS = [
    { name: 'sidebar',        x: 0,   y: 0,   w: 256,  h: 720 },
    { name: 'page-header',    x: 256, y: 0,   w: 1184, h: 110 },
    { name: 'empty-result',   x: 624, y: 110, w: 816,  h: 200 },
  ] as const;

  test('chrome regions match committed baselines', async ({ page }) => {
    // Login as admin so the Session cookie is present
    await page.goto('/login', { waitUntil: 'load', timeout: 15_000 });
    await page.locator('input[type="text"]').first().fill('admin');
    await page.locator('input[type="password"]').first().fill('admin');
    await page.locator('button[type="submit"]').first().click();
    // Wait for the post-login navigation away from /login
    await page.waitForTimeout(2000);

    // Clear localStorage so defaultState() runs fresh (no user-persisted
    // builder state) — this is the canonical "fresh boot" capture.
    await page.evaluate(() => localStorage.clear()).catch(() => {});

    await page.goto('/query', { waitUntil: 'networkidle', timeout: 15_000 });
    // Give the QueryConsole time to load groups + auto-pick a default
    // group/resource so the rail has settled into its initial state.
    await page.waitForTimeout(4000);

    for (const r of REGIONS) {
      const clip = { x: r.x, y: r.y, width: r.w, height: r.h };
      await expect(page).toHaveScreenshot(`m4-chrome/${r.name}.png`, {
        clip,
        maxDiffPixelRatio: 0.01,
      });
    }
  });
});