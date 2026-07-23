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

// shell.spec.ts — app shell + Home page, ported from m2-shell.spec.ts. All
// locators are semantic landmarks (navigation / complementary / main / status)
// and accessible-named controls; the collapse state is read from the toggle's
// aria-expanded rather than a CSS `is-collapsed` class.

import { test, expect } from '../framework/fixtures.js';

test.describe('app shell @e2e @shell', () => {
  test.beforeEach(async ({ shellPage }) => {
    await shellPage.goto();
  });

  test('renders sidebar, brand, nav, connection status, and sign out', async ({ shellPage }) => {
    await shellPage.expectShell();
    await shellPage.expectNavItems(['Home', 'Metadata', 'Properties', 'Pipelines', 'Query']);
  });

  test('home page shows the title, welcome copy, and quick-nav cards', async ({ shellPage, page }) => {
    await expect(shellPage.homeTitle()).toBeVisible();
    await expect(page.getByText(/Data is organized into/)).toBeVisible();
    await expect(shellPage.quickNav()).toBeVisible();
    // Six quick-nav shortcuts: Measures, Streams, Traces, Properties, Pipelines, Run a query.
    await expect(shellPage.quickCards()).toHaveCount(6);
  });

  test('sidebar collapse toggle works', async ({ shellPage }) => {
    await shellPage.expectCollapsed(false);
    await shellPage.toggleSidebar();
    await shellPage.expectCollapsed(true);
    await shellPage.toggleSidebar();
    await shellPage.expectCollapsed(false);
  });
});
