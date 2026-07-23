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

// Test fixtures — the single entry point every spec imports instead of
// '@playwright/test'. This is the fixture-driven pattern from the design doc:
// page objects and the seed factory are created and injected on demand via
// dependency injection, and the seed factory is torn down automatically after
// each test. Specs consume `{ consolePage, loginPage, seed }` and never
// instantiate anything themselves.
//
// Usage:  import { test, expect } from '../framework/fixtures.js';

import { test as base, expect } from '@playwright/test';
import { LoginPage } from './pages/LoginPage.js';
import { QueryConsolePage } from './pages/QueryConsolePage.js';
import { ShellPage } from './pages/ShellPage.js';
import { SchemaPage } from './pages/SchemaPage.js';
import { IndexRulePage } from './pages/IndexRulePage.js';
import { LifecyclePage } from './pages/LifecyclePage.js';
import { SeedFactory } from './seed/factory.js';

type AppFixtures = {
  loginPage: LoginPage;
  consolePage: QueryConsolePage;
  shellPage: ShellPage;
  schemaPage: SchemaPage;
  indexRulePage: IndexRulePage;
  lifecyclePage: LifecyclePage;
  seed: SeedFactory;
};

export const test = base.extend<AppFixtures>({
  loginPage: async ({ page }, use) => {
    await use(new LoginPage(page));
  },
  consolePage: async ({ page }, use) => {
    await use(new QueryConsolePage(page));
  },
  shellPage: async ({ page }, use) => {
    await use(new ShellPage(page));
  },
  schemaPage: async ({ page }, use) => {
    await use(new SchemaPage(page));
  },
  indexRulePage: async ({ page }, use) => {
    await use(new IndexRulePage(page));
  },
  lifecyclePage: async ({ page }, use) => {
    await use(new LifecyclePage(page));
  },
  seed: async ({ request }, use) => {
    const factory = new SeedFactory(request);
    await use(factory);
    // Absolute isolation: remove everything this test seeded, pass or fail.
    await factory.cleanup();
  },
});

export { expect };
