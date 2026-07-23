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

// IndexRulePage — page object for the IndexPage (Rule/Binding tabs) and the
// IndexRule / IndexRuleBinding modals. The redesign made the tabs a real
// role="tablist" with aria-selected, tagged each table row with a data-testid,
// and the binding form's subject/rules pickers already expose role="combobox"
// with aria-labels — so rows and controls are addressable semantically. Text
// inputs are reached by placeholder.

import { expect, type Locator, type Page } from '@playwright/test';
import { BasePage } from './BasePage.js';
import type { MetaType } from './SchemaPage.js';

export type RuleTypeCard = 'tree' | 'inverted' | 'skipping';

export class IndexRulePage extends BasePage {
  constructor(page: Page) {
    super(page);
  }

  async goto(type: MetaType, group: string): Promise<void> {
    await this.page.goto(`/metadata/${type}/${group}/Index`);
    await expect(this.ruleTab()).toBeVisible();
  }

  // ── Tabs ───────────────────────────────────────────────────────────────────
  ruleTab(): Locator {
    return this.page.getByRole('tab', { name: /Rule/ });
  }

  bindingTab(): Locator {
    return this.page.getByRole('tab', { name: /Binding/ });
  }

  async openRuleTab(): Promise<void> {
    await this.ruleTab().click();
    await expect(this.ruleTab()).toHaveAttribute('aria-selected', 'true');
  }

  async openBindingTab(): Promise<void> {
    await this.bindingTab().click();
    await expect(this.bindingTab()).toHaveAttribute('aria-selected', 'true');
  }

  // The "All" binding-scope filter (default scope hides active bindings).
  async showAllBindings(): Promise<void> {
    await this.page.getByRole('button', { name: /^All/ }).click();
  }

  // ── Rows ───────────────────────────────────────────────────────────────────
  ruleRow(name: string): Locator {
    return this.page.getByTestId('idx-rule-row').filter({ hasText: name });
  }

  bindRow(name: string): Locator {
    return this.page.getByTestId('idx-bind-row').filter({ hasText: name });
  }

  // ── Buttons ────────────────────────────────────────────────────────────────
  newRuleButton(): Locator {
    return this.page.getByRole('button', { name: 'New index rule' });
  }

  newBindingButton(): Locator {
    return this.page.getByRole('button', { name: 'New binding' });
  }

  dialog(name: string | RegExp): Locator {
    return this.page.getByRole('dialog', { name });
  }

  // ── Rule flow ──────────────────────────────────────────────────────────────
  async createRule(name: string, tags: readonly string[], type: RuleTypeCard): Promise<void> {
    await this.newRuleButton().click();
    const dlg = this.dialog('Create index rule');
    await expect(dlg).toBeVisible();
    await dlg.getByPlaceholder('by_service').fill(name);
    for (const tag of tags) {
      await dlg.getByPlaceholder('tag_name').fill(tag);
      await dlg.getByRole('button', { name: 'Add', exact: true }).click();
      await expect(dlg.getByRole('button', { name: new RegExp(`Remove ${tag}`) })).toBeVisible();
    }
    await dlg.getByRole('button', { name: new RegExp(type, 'i') }).click();
    await dlg.getByRole('button', { name: 'Create index rule' }).click();
  }

  async openEditRule(name: string): Promise<Locator> {
    await this.ruleRow(name).getByRole('button', { name: 'Edit' }).click();
    const dlg = this.dialog('Edit index rule');
    await expect(dlg).toBeVisible();
    return dlg;
  }

  async deleteRule(name: string): Promise<void> {
    await this.ruleRow(name).getByRole('button', { name: 'Delete' }).click();
    const dlg = this.dialog('Delete index rule');
    await expect(dlg).toBeVisible();
    await dlg.getByRole('button', { name: /^Delete/ }).click();
  }

  // ── Binding flow ───────────────────────────────────────────────────────────
  // Fill an open binding modal's name, subject, and rules (leaves "Never
  // expires" checked — the default happy path).
  async fillBinding(dlg: Locator, opts: { name: string; subject: string; rules: readonly string[] }): Promise<void> {
    await dlg.getByPlaceholder('binding_name').fill(opts.name);
    await dlg.getByRole('combobox', { name: 'Resource name' }).fill(opts.subject);
    await dlg.getByRole('option', { name: opts.subject }).click();
    for (const rule of opts.rules) {
      await dlg.getByRole('combobox', { name: 'Index rules' }).fill(rule);
      await dlg.getByRole('option', { name: rule }).click();
    }
  }

  async createBinding(opts: { name: string; subject: string; rules: readonly string[] }): Promise<void> {
    await this.newBindingButton().click();
    const dlg = this.dialog('Create binding');
    await expect(dlg).toBeVisible();
    await this.fillBinding(dlg, opts);
    await dlg.getByRole('button', { name: 'Create binding' }).click();
  }

  async openEditBinding(name: string): Promise<Locator> {
    await this.bindRow(name).getByRole('button', { name: 'Edit' }).click();
    const dlg = this.dialog('Edit binding');
    await expect(dlg).toBeVisible();
    return dlg;
  }

  async deleteBinding(name: string): Promise<void> {
    await this.bindRow(name).getByRole('button', { name: 'Delete' }).click();
    const dlg = this.dialog('Delete binding');
    await expect(dlg).toBeVisible();
    await dlg.getByRole('button', { name: /^Delete/ }).click();
  }

  // Click the "unbound" CTA on a rule row → opens the Create-binding modal
  // pre-selected with that rule.
  async bindFromRuleCta(name: string): Promise<Locator> {
    await this.ruleRow(name).getByRole('button', { name: /unbound/i }).click();
    const dlg = this.dialog('Create binding');
    await expect(dlg).toBeVisible();
    return dlg;
  }
}
