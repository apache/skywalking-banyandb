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

import { defineConfig, devices } from '@playwright/test';
import { join } from 'node:path';

export default defineConfig({
  testDir: '.',
  testMatch: '**/*.spec.ts',
  // m3-handoff runs against a different webServer (the handoff design bundle
  // served by playwright.handoff.config.ts) — exclude it from the main suite.
  testIgnore: '**/m3-handoff.spec.ts',
  outputDir: './test-results',
  timeout: 60_000,
  expect: { timeout: 10_000 },
  globalSetup: './setup/global-setup.ts',
  globalTeardown: './setup/global-teardown.ts',
  webServer: {
    command: 'node dist/src/index.js',
    cwd: join(__dirname, '..', 'server'),
    port: 4000,
    // reuseExistingServer so the harness can be run against a dev BFF
    // (npm run dev in canopy/server) without port-collision failures.
    reuseExistingServer: true,
    timeout: 30_000,
    env: {
      SESSION_SECRET: 'canopy-e2e-test-secret-32chars!!',
      CANOPY_DEV_NOAUTH: 'true',
      BANYANDB_TARGET: process.env.BANYANDB_TARGET || 'http://127.0.0.1:17913',
      PORT: '4000',
      LOG_LEVEL: 'warn',
    },
  },
  use: {
    baseURL: process.env.CANOPY_URL || 'http://localhost:4000',
    trace: 'on-first-retry',
    // 'on' so every test captures a screenshot — combined with toHaveScreenshot()
    // assertions in m4-query.spec.ts and m4-handoff-capture.spec.ts this is
    // the pixel-regression gate: a UI change that diverges from the committed
    // baseline fails the test (NOT a soft warning).
    screenshot: 'on',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
