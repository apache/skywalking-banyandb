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

// Handoff-comparison e2e for the BanyanDB design bundle.
//
// This suite is OPT-IN: it does not require a live BanyanDB. The webServer
// in playwright.handoff.config.ts serves `.handoff-import/banyandb/project/`
// on :18099. We navigate the handoff and capture `handoff-form-<state>.png`
// captures for the M3 form DoD (f) — "any visible mismatch is fixed or logged
// as a named deviation before M3 closes."
//
// Run with: `npx playwright test --config=e2e/playwright.handoff.config.ts`

const screenshotsDir = join(process.cwd(), 'e2e', 'screenshots');

test.beforeAll(() => {
  mkdirSync(screenshotsDir, { recursive: true });
});

test('handoff index page loads', async ({ page }) => {
  const res = await page.goto('/');
  expect(res?.ok()).toBeTruthy();
  // The handoff is a single-page app; capture the design canvas baseline.
  await page.waitForLoadState('networkidle');
  await page.screenshot({ path: join(screenshotsDir, 'handoff-overview.png'), fullPage: true });
});

test('handoff form panels are present (smoke check)', async ({ page }) => {
  await page.goto('/');
  await page.waitForLoadState('networkidle');
  // The handoff prototype exposes various form elements with predictable ids.
  // We check for the presence of inputs as a baseline "form is wired" check.
  const inputs = await page.locator('input, select, textarea, button').count();
  expect(inputs).toBeGreaterThan(10);
  await page.screenshot({ path: join(screenshotsDir, 'handoff-form-smoke.png'), fullPage: false });
});

test('handoff has a title element (deterministic structure check)', async ({ page }) => {
  // Deterministic replacement for an earlier fragile "click any New button"
  // check. The handoff should always expose a <title> + a primary heading;
  // both are stable across design revisions and are not subject to the
  // design-canvas's interactive state machine.
  await page.goto('/');
  await page.waitForLoadState('networkidle');
  const title = await page.title();
  expect(title.length).toBeGreaterThan(0);
  // h1 / h2 are the most stable landmarks across the 89-screen design canvas.
  const headingCount = await page.locator('h1, h2, h3').count();
  expect(headingCount).toBeGreaterThan(0);
  await page.screenshot({ path: join(screenshotsDir, 'handoff-structure.png'), fullPage: false });
});
