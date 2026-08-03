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

// auth.spec.ts — the dedicated login-form suite, ported from m2-login.spec.ts.
//
// The rest of the suite skips login via a cached storageState; this suite is
// the one place the REAL login form + POST /auth/login flow is exercised. It
// therefore resets to an EMPTY session (so it lands on /login instead of the
// authenticated app) and drives the form by hand. It does NOT rely on the
// CANOPY_DEV_NOAUTH bypass: the BFF still performs real credential checking
// against its configured user, so wrong credentials are genuinely rejected and
// only valid ones reach the shell. All locators are semantic (getByLabel /
// getByRole / getByText).

import { test, expect } from '../framework/fixtures.js';

// Start every test here unauthenticated so we actually reach the login form.
test.use({ storageState: { cookies: [], origins: [] } });

test.describe('login form @e2e @auth', () => {
  test('renders brand, role picker, fields, and the Connect button', async ({ loginPage }) => {
    await loginPage.goto();
    await loginPage.expectLoaded();

    // Brand
    await expect(loginPage.brandHeading()).toBeVisible();
    await expect(loginPage.eyebrow()).toContainText('BanyanDB Console');

    // Role picker — labels are "Administrator" and "Read-only" (Endpoint was
    // removed once the BanyanDB target became a BFF-only setting).
    await expect(loginPage.roleRadio('Administrator')).toBeVisible();
    await expect(loginPage.roleRadio('Read-only')).toBeVisible();

    // Fields + submit
    await expect(loginPage.username()).toBeVisible();
    await expect(loginPage.password()).toBeVisible();
    await expect(loginPage.submit()).toBeVisible();
  });

  test('rejects bad credentials with an error banner', async ({ loginPage, page }) => {
    await loginPage.submitExpectingError('admin', 'wrongpassword');
    // Web-first: the failure banner is role="alert"; we stay on /login.
    await expect(loginPage.errorBanner()).toBeVisible();
    await expect(page).toHaveURL(/\/login/);
  });

  test('logs in with valid credentials and reaches the shell', async ({ loginPage, shellPage }) => {
    await loginPage.loginAs('admin', 'admin', 'Administrator');
    // loginAs waits for the URL to leave /login; confirm the shell rendered.
    await shellPage.expectShell();
  });
});
