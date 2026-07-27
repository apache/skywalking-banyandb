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

// QueryConsolePage — page object for /query (the M4 console + result views).
// All locators are semantic (getByRole / getByLabel) per the design-doc
// hierarchy. The two region landmarks it depends on ("Query builder" and
// "Query results") were added to the app markup precisely so tests have a
// stable, user-facing handle instead of a brittle CSS class.

import { expect, type Locator, type Page } from '@playwright/test';
import { BasePage } from './BasePage.js';

export type Catalog = 'Measure' | 'Stream' | 'Trace' | 'Top-N';

export class QueryConsolePage extends BasePage {
  constructor(page: Page) {
    super(page);
  }

  // ── Landmarks ─────────────────────────────────────────────────────────────
  readonly builder = (): Locator => this.page.getByRole('region', { name: 'Query builder' });
  readonly results = (): Locator => this.page.getByRole('region', { name: 'Query results' });

  // ── Controls ──────────────────────────────────────────────────────────────
  private categoryTab(cat: Catalog): Locator {
    // The "Target type" tablist exposes each catalog as an ARIA tab. Anchor the
    // name so "Stream" doesn't also match a hypothetical "Streaming".
    const name = cat === 'Top-N' ? /top-n/i : new RegExp(`^${cat}$`, 'i');
    return this.page.getByRole('tab', { name });
  }

  private runButton(): Locator {
    return this.page.getByRole('button', { name: /^run/i });
  }

  resourceSelect(): Locator {
    return this.page.getByRole('combobox', { name: 'Resource' });
  }

  groupSelect(): Locator {
    return this.page.getByRole('combobox', { name: 'Group' });
  }

  // ── Builder clauses (semantic; no CSS accordion/condition selectors) ─────────
  private modeTab(name: 'Builder' | 'Code'): Locator {
    return this.page.getByRole('tab', { name });
  }

  private whereAccordion(): Locator {
    return this.page.getByRole('button', { name: /WHERE/ });
  }

  private addConditionButton(): Locator {
    return this.page.getByRole('button', { name: 'Add condition' });
  }

  conditionValue(): Locator {
    return this.page.getByRole('textbox', { name: 'Value' }).first();
  }

  orderField(): Locator {
    return this.page.getByRole('combobox', { name: 'Order field' });
  }

  // Add one WHERE condition row. Before a query has run the WHERE section is a
  // full editor (no accordion); after a run it collapses to an accordion that
  // must be expanded first. Handle both. Note: "Add condition" is disabled
  // until a resource with tags is selected, so callers must pick one first.
  async addWhereCondition(): Promise<void> {
    const head = this.whereAccordion();
    if ((await head.count()) > 0 && (await head.getAttribute('aria-expanded')) === 'false') {
      await head.click();
    }
    await this.addConditionButton().first().click();
  }

  async setConditionValue(value: string): Promise<void> {
    await this.conditionValue().fill(value);
  }

  async switchToCode(): Promise<void> {
    await this.modeTab('Code').click();
    await expect(this.modeTab('Code')).toHaveAttribute('aria-selected', 'true');
  }

  async switchToBuilder(): Promise<void> {
    await this.modeTab('Builder').click();
    await expect(this.modeTab('Builder')).toHaveAttribute('aria-selected', 'true');
  }

  // ── Actions (app-action style: intent, not clicks) ─────────────────────────
  async goto(): Promise<void> {
    await this.page.goto('/query');
    await expect(this.builder()).toBeVisible();
  }

  async selectCatalog(cat: Catalog): Promise<void> {
    await this.categoryTab(cat).click();
    await expect(this.categoryTab(cat)).toHaveAttribute('aria-selected', 'true');
  }

  async selectGroup(name: string): Promise<void> {
    await this.groupSelect().selectOption(name);
  }

  // selectOption auto-waits for the option to exist, so this is web-first: it
  // blocks until the seeded resource has been fetched into the dropdown.
  async selectResource(name: string): Promise<void> {
    await this.resourceSelect().selectOption(name);
  }

  // Run the current query and wait — web-first — for the run to settle. The Run
  // button is disabled while a query is in flight, so toBeEnabled() is the
  // correct signal (no fixed sleeps, no isVisible() single-tick checks).
  async run(): Promise<void> {
    await this.runButton().click();
    await expect(this.runButton()).toBeEnabled({ timeout: 60_000 });
  }

  async expectResultsVisible(): Promise<void> {
    await expect(this.results()).toBeVisible();
  }
}
