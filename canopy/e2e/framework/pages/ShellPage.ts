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

// ShellPage — page object for the authenticated app shell (sidebar + content)
// and the Home page it lands on. Locators are semantic: the sidebar exposes a
// named navigation landmark, a complementary landmark, and accessible-named
// buttons (Sign out, Toggle sidebar). The redesign added `aria-expanded` to the
// collapse toggle and an `aria-label`ed connection status so the collapse and
// connection state are assertable without CSS classes.

import { expect, type Locator, type Page } from '@playwright/test';
import { BasePage } from './BasePage.js';

export type NavItem = 'Home' | 'Metadata' | 'Properties' | 'Pipelines' | 'Query';

export class ShellPage extends BasePage {
  constructor(page: Page) {
    super(page);
  }

  // ── Landmarks ──────────────────────────────────────────────────────────────
  readonly nav = (): Locator => this.page.getByRole('navigation', { name: 'Main navigation' });
  readonly sidebar = (): Locator => this.page.getByRole('complementary', { name: 'Sidebar' });
  readonly main = (): Locator => this.page.getByRole('main');
  readonly connection = (): Locator => this.page.getByRole('status', { name: 'BanyanDB connection' });

  // ── Controls ───────────────────────────────────────────────────────────────
  navItem(name: NavItem): Locator {
    // Nav rows carry the item label as their accessible name (title + text).
    return this.nav().getByRole('button', { name, exact: true });
  }

  brandName(): Locator {
    return this.sidebar().getByText('Canopy', { exact: true });
  }

  signOut(): Locator {
    return this.page.getByRole('button', { name: 'Sign out' });
  }

  collapseToggle(): Locator {
    return this.page.getByRole('button', { name: 'Toggle sidebar' });
  }

  // ── Home page ──────────────────────────────────────────────────────────────
  homeTitle(): Locator {
    return this.page.getByRole('heading', { name: 'Home' });
  }

  quickNav(): Locator {
    return this.page.getByRole('region', { name: 'Quick navigation' });
  }

  quickCards(): Locator {
    return this.quickNav().getByRole('button');
  }

  // ── Actions ────────────────────────────────────────────────────────────────
  async goto(): Promise<void> {
    await this.page.goto('/');
    await expect(this.sidebar()).toBeVisible();
  }

  async expectShell(): Promise<void> {
    await expect(this.sidebar()).toBeVisible();
    await expect(this.nav()).toBeVisible();
    await expect(this.brandName()).toBeVisible();
    await expect(this.connection()).toBeVisible();
    await expect(this.signOut()).toBeVisible();
  }

  async expectNavItems(items: readonly NavItem[]): Promise<void> {
    for (const item of items) {
      await expect(this.navItem(item)).toBeVisible();
    }
  }

  // Toggle the sidebar and return the new collapsed state, read web-first from
  // the toggle's aria-expanded (expanded=false ⇒ collapsed).
  async toggleSidebar(): Promise<void> {
    await this.collapseToggle().click();
  }

  async expectCollapsed(collapsed: boolean): Promise<void> {
    await expect(this.collapseToggle()).toHaveAttribute('aria-expanded', String(!collapsed));
  }
}
