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

// auth.setup.ts — the "login once" setup project. It drives the real UI login
// a SINGLE time and persists the resulting cookies + localStorage to
// STORAGE_STATE. Every other project loads that file via `storageState`,
// skipping the login form entirely (seconds → milliseconds per test). This is
// the design doc's storageState optimization.
//
// The main suite still runs with CANOPY_DEV_NOAUTH=true (the CI fast path), so
// this setup is a no-op-friendly: if the app is already past /login it just
// captures whatever session state exists. The dedicated login suite
// (tests/auth.spec.ts, to be added during migration) is where the login form
// itself is validated against an auth-enabled server.

import { test as setup, expect } from '@playwright/test';
import { STORAGE_STATE } from '../framework/paths.js';

setup('authenticate once', async ({ page }) => {
  // Authenticate through the context-bound request client so the resulting
  // cookie is guaranteed to be present before storageState is serialized.
  // The dedicated auth specs cover the login form itself.
  const response = await page.request.post('./auth/login', {
    data: { username: 'admin', password: 'admin' },
  });
  expect(response.ok()).toBeTruthy();
  await page.context().storageState({ path: STORAGE_STATE });
});
