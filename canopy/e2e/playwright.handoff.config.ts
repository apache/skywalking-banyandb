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
  testMatch: 'm3-handoff.spec.ts',
  outputDir: './test-results-handoff',
  timeout: 30_000,
  expect: { timeout: 10_000 },
  webServer: {
    command: 'node handoff-server.cjs',
    cwd: __dirname,
    port: 18099,
    reuseExistingServer: true,
    timeout: 10_000,
    env: {
      PORT: '18099',
      ROOT: join(__dirname, '..', '..', '..', '.handoff-import', 'banyandb', 'project'),
    },
  },
  use: {
    baseURL: 'http://127.0.0.1:18099',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
  ],
});
