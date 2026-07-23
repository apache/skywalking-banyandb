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

// LoginPage — the ONLY place that drives the login form via the UI. Every
// other spec skips it by loading a cached storageState (see auth/auth.setup.ts
// and e2e/TESTING.md §"Authentication"). Locators are semantic, matching the
// prioritized hierarchy from the design doc: getByRole > getByLabel.

import { expect, type Locator, type Page } from '@playwright/test';
import { BasePage } from './BasePage.js';

// The role radios are labelled "Administrator" / "Read-only" in the UI.
export type LoginRole = 'Administrator' | 'Read-only';

export class LoginPage extends BasePage {
  constructor(page: Page) {
    super(page);
  }

  // getByLabel resolves the <label htmlFor> pairing — stable across styling.
  username(): Locator {
    return this.page.getByLabel('Username');
  }

  password(): Locator {
    return this.page.getByLabel('Password');
  }

  // The submit button's accessible name is its text ("Connect" → "Connecting…"
  // → "Connected"); anchor on the stable "Connect" prefix.
  submit(): Locator {
    return this.page.getByRole('button', { name: /^connect/i });
  }

  // Semantic brand landmarks: the wordmark is an <h1>, the eyebrow is body text.
  brandHeading(): Locator {
    return this.page.getByRole('heading', { name: 'Canopy' });
  }

  eyebrow(): Locator {
    return this.page.getByText('BanyanDB Console');
  }

  roleRadio(role: LoginRole): Locator {
    return this.page.getByRole('radio', { name: new RegExp(role, 'i') });
  }

  // Failure + upstream-down banners both render as role="alert".
  errorBanner(): Locator {
    return this.page.getByRole('alert');
  }

  async goto(): Promise<void> {
    await this.page.goto('/login');
  }

  // Fill the login form (role + credentials) without submitting.
  async fill(username = 'admin', password = 'admin', role: LoginRole = 'Administrator'): Promise<void> {
    const roleRadio = this.roleRadio(role);
    if (await roleRadio.count()) await roleRadio.click();
    await this.username().fill(username);
    await this.password().fill(password);
  }

  // Perform a full UI login and wait — web-first — for the app to render. On
  // success the app sets the session in place (no navigation), so the reliable
  // signal is the login form unmounting, not a URL change.
  async loginAs(username = 'admin', password = 'admin', role: LoginRole = 'Administrator'): Promise<void> {
    await this.goto();
    await this.fill(username, password, role);
    await this.submit().click();
    await expect(this.username()).toBeHidden({ timeout: 30_000 });
  }

  // Attempt a login expected to fail — submits and returns without waiting for a
  // navigation (there is none on failure).
  async submitExpectingError(username: string, password: string): Promise<void> {
    await this.goto();
    await this.fill(username, password);
    await this.submit().click();
  }

  async expectLoaded(): Promise<void> {
    await expect(this.username()).toBeVisible();
    await expect(this.password()).toBeVisible();
  }
}
