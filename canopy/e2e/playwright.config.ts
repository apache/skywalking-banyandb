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

// The single, canonical Playwright config for the canopy E2E suite (see
// e2e/TESTING.md). The milestone-numbered specs, the .mjs probes, and the
// legacy/handoff/reuse configs were all removed at migration cutover — this is
// the end state. Scoped to the redesigned framework (auth/ setup + tests/).
//
// Highlights:
//   • a `setup` project logs in once → storageState, injected into every test
//   • VRT discipline enforced globally: animations disabled, caret hidden,
//     CSS-scaled snapshots (stable across DPI)
//   • screenshots only-on-failure (the pixel gate is explicit toHaveScreenshot)
//   • local dev reuses a running BFF (reuseExistingServer: !isCI)
//   • CI: retries + sharding-ready, list+html reporters

import { defineConfig, devices } from '@playwright/test';
import { join } from 'node:path';
import { STORAGE_STATE } from './framework/paths.js';

const isCI = !!process.env.CI;

export default defineConfig({
  testDir: '.',
  outputDir: './test-results',
  fullyParallel: true,
  forbidOnly: isCI,
  retries: isCI ? 2 : 0,
  reporter: isCI ? [['list'], ['html', { open: 'never' }]] : [['list']],
  timeout: 60_000,
  expect: {
    timeout: 10_000,
    // Visual-regression defaults applied to every toHaveScreenshot() call.
    toHaveScreenshot: {
      maxDiffPixelRatio: 0.01,
      animations: 'disabled',
      caret: 'hide',
      scale: 'css',
    },
  },
  globalSetup: './setup/global-setup.ts',
  globalTeardown: './setup/global-teardown.ts',
  webServer: {
    command: 'node dist/src/index.js',
    cwd: join(__dirname, '..', 'server'),
    port: 4000,
    reuseExistingServer: !isCI,
    timeout: 30_000,
    env: {
      SESSION_SECRET: 'canopy-e2e-test-secret-32chars!!',
      // CI fast path: bypass auth for the main suite. The login form itself is
      // validated by the dedicated auth suite against an auth-enabled server.
      CANOPY_DEV_NOAUTH: 'true',
      BANYANDB_TARGET: process.env.BANYANDB_TARGET || 'http://127.0.0.1:17913',
      PORT: '4000',
      LOG_LEVEL: 'warn',
    },
  },
  use: {
    baseURL: process.env.CANOPY_URL || 'http://localhost:4000',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },
  projects: [
    {
      name: 'setup',
      testMatch: /auth\/.*\.setup\.ts$/,
    },
    {
      name: 'chromium',
      testMatch: /tests\/.*\.spec\.ts$/,
      dependencies: ['setup'],
      use: {
        ...devices['Desktop Chrome'],
        storageState: STORAGE_STATE,
      },
    },
  ],
});
