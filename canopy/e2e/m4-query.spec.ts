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

// E2E spec for M4 — QueryConsole + 4 result views (measure/stream/trace/TopN).
// Always runs against a live BanyanDB through the BFF (see plan §"BanyanDB
// provisioning & seeding for E2E"). PIXEL-REGRESSION GATE: every state
// captured here is asserted with `toHaveScreenshot()` against a committed
// baseline (see canopy/e2e/screenshots/m4-baseline/). Any pixel diff above
// the configured maxDiffPixelRatio fails the test — there is NO skip path.
//
// First-time setup: delete the baseline png(s) under e2e/screenshots/m4-baseline/
// and re-run; Playwright writes fresh baselines on the next run.

import { test, expect, type Page } from '@playwright/test';

async function login(page: Page) {
  await page.goto('/');
  await page.locator('input[name=endpoint]').fill(process.env.BANYANDB_TARGET ?? 'http://127.0.0.1:17913');
  await page.locator('input[name=username]').fill('admin');
  await page.locator('input[name=password]').fill('admin');
  await page.locator('button[type=submit]').click();
  await page.waitForURL((u) => !u.pathname.startsWith('/login'), { timeout: 30_000 });
}

async function runQueryAndAssert(page: Page, baselineName: string) {
  await page.goto('/query');
  await page.waitForSelector('.qb-card', { timeout: 30_000 });
  await page.locator('button.qb-btn-primary', { hasText: /run/i }).click();
  await page.waitForSelector('.rv-root', { timeout: 60_000 });
  // Pixel-regression assertion: compare to the committed baseline.
  // maxDiffPixelRatio: 0.01 = up to 1% of pixels may differ (anti-aliasing,
  // font hinting). Larger deviations fail the test.
  await expect(page).toHaveScreenshot(`m4-baseline/${baselineName}.png`, {
    maxDiffPixelRatio: 0.01,
  });
}

test.describe('M4 query console + result views (pixel-regression gated)', () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test('measure query', async ({ page }) => {
    await page.goto('/query');
    await page.waitForSelector('.qb-card', { timeout: 30_000 });
    await page.locator('button.qb-cat-btn', { hasText: /measure/i }).first().click();
    await runQueryAndAssert(page, 'impl-result-measure');
  });

  test('stream query', async ({ page }) => {
    await page.goto('/query');
    await page.waitForSelector('.qb-card', { timeout: 30_000 });
    await page.locator('button.qb-cat-btn', { hasText: /^stream$/i }).first().click();
    await runQueryAndAssert(page, 'impl-result-stream');
  });

  test('trace query', async ({ page }) => {
    await page.goto('/query');
    await page.waitForSelector('.qb-card', { timeout: 30_000 });
    await page.locator('button.qb-cat-btn', { hasText: /^trace$/i }).first().click();
    await runQueryAndAssert(page, 'impl-result-trace');
  });

  test('TopN query', async ({ page }) => {
    await page.goto('/query');
    await page.waitForSelector('.qb-card', { timeout: 30_000 });
    await page.locator('button.qb-cat-btn', { hasText: /top-n/i }).first().click();
    await runQueryAndAssert(page, 'impl-result-topn');
  });

  test('builder panel', async ({ page }) => {
    await page.goto('/query');
    await page.waitForSelector('.qb-card', { timeout: 30_000 });
    await expect(page).toHaveScreenshot('m4-baseline/impl-query-builder.png', {
      maxDiffPixelRatio: 0.01,
    });
  });
});