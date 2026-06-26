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

test('SPA boot: dark theme applied', async ({ page }) => {
  await page.goto('/');

  // The CSS theme sets body background to #0d120e — verify dark theme
  const bgColor = await page.evaluate(() =>
    window.getComputedStyle(document.body).backgroundColor
  );
  // rgb(13, 18, 14) = #0d120e
  expect(bgColor).toMatch(/rgb\(13,\s*18,\s*14\)/);

  const screenshotsDir = join(process.cwd(), 'e2e', 'screenshots');
  mkdirSync(screenshotsDir, { recursive: true });
  await page.screenshot({ path: join(screenshotsDir, 'm0-boot.png'), fullPage: true });
});
