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

// PropertyPage — page object for the Property surfaces (MP milestone):
// the overview (/properties), group page (/properties/:group, reusing
// GroupPage/TypeOverviewPage), and the NEW collection detail page
// (/properties/:group/:name — PropertyDetailPage): New property/document
// modals, the embedded PropertyQuery builder + Run, and DocList.
//
// Locators are semantic, matching SchemaPage.ts's conventions: modals are
// role="dialog" named by their title; the query builder is
// role="region" aria-label="Query builder" style landmarks; DocList carries
// role="region" aria-label="Query results".

import { expect, type Locator, type Page } from '@playwright/test';
import { BasePage } from './BasePage.js';

export class PropertyPage extends BasePage {
  constructor(page: Page) {
    super(page);
  }

  // ── Navigation ─────────────────────────────────────────────────────────────
  async gotoOverview(): Promise<void> {
    await this.page.goto('./properties');
  }

  async gotoGroup(group: string): Promise<void> {
    await this.page.goto(`./properties/${group}`);
    await expect(this.pageTitle(group)).toBeVisible();
  }

  async gotoDetail(group: string, name: string): Promise<void> {
    await this.page.goto(`./properties/${group}/${name}`);
    await expect(this.pageTitle(name)).toBeVisible();
  }

  // ── Landmarks ──────────────────────────────────────────────────────────────
  pageTitle(name: string): Locator {
    return this.page.getByRole('heading', { name: new RegExp(name) });
  }

  dialog(name: string | RegExp): Locator {
    return this.page.getByRole('dialog', { name });
  }

  newPropertyButton(): Locator {
    return this.page
      .getByRole('button', { name: /New property/i })
      .or(this.page.getByRole('button', { name: /Create property/i }))
      .first();
  }

  // ── Collection (schema) CRUD ─────────────────────────────────────────────────
  async createCollection(group: string, name: string): Promise<void> {
    await this.newPropertyButton().click();
    const dlg = this.dialog('Create property');
    await expect(dlg).toBeVisible();
    await dlg.getByPlaceholder('temp_data').fill(name);
    await dlg.getByRole('button', { name: 'Create property' }).click();
    await expect(dlg).not.toBeVisible();
  }

  async deleteCollection(name: string): Promise<void> {
    await this.page.getByRole('button', { name: /Delete property/i }).click();
    const dlg = this.dialog('Delete property');
    await expect(dlg).toBeVisible();
    await dlg.getByRole('textbox').fill(name);
    await dlg.getByRole('button', { name: 'Delete property' }).click();
    await expect(dlg).not.toBeVisible();
  }

  // ── Document CRUD ────────────────────────────────────────────────────────────
  newDocumentButton(): Locator {
    return this.page.getByRole('button', { name: 'New document' });
  }

  async applyDocument(id: string, tagKey: string, tagValue: string): Promise<void> {
    await this.newDocumentButton().click();
    const dlg = this.dialog('Apply property document');
    await expect(dlg).toBeVisible();
    await dlg.getByPlaceholder('General-Service').fill(id);
    await dlg.getByPlaceholder('key').first().fill(tagKey);
    await dlg.getByPlaceholder('value').first().fill(tagValue);
    await dlg.getByRole('button', { name: 'Apply document' }).click();
    await expect(dlg).not.toBeVisible();
  }

  editButton(docId: string): Locator {
    return this.docCard(docId).getByTitle('Edit document');
  }

  async editDocument(docId: string, tagValue: string): Promise<void> {
    await this.editButton(docId).click();
    const dlg = this.dialog('Edit property document');
    await expect(dlg).toBeVisible();
    await dlg.getByPlaceholder('value').first().fill(tagValue);
    await dlg.getByRole('button', { name: 'Save changes' }).click();
    await expect(dlg).not.toBeVisible();
  }

  deleteButton(docId: string): Locator {
    return this.docCard(docId).getByTitle('Delete document');
  }

  async deleteDocument(docId: string): Promise<void> {
    await this.deleteButton(docId).click();
    const dlg = this.dialog('Delete this document?');
    await expect(dlg).toBeVisible();
    await dlg.getByRole('button', { name: 'Yes, delete' }).click();
    await expect(dlg).not.toBeVisible();
  }

  // ── Query builder + results ──────────────────────────────────────────────────
  queryBuilder(): Locator {
    return this.page.getByRole('region', { name: 'Property query builder' });
  }

  runButton(): Locator {
    return this.page.getByRole('button', { name: /^Run/ });
  }

  async run(): Promise<void> {
    await this.runButton().click();
  }

  resultsRegion(): Locator {
    return this.page.getByRole('region', { name: 'Query results' });
  }

  docCard(docId: string): Locator {
    return this.resultsRegion().getByTestId('doc-card').filter({ hasText: docId });
  }

  codeModeTab(): Locator {
    return this.page.getByRole('tab', { name: 'Code' });
  }

  builderModeTab(): Locator {
    return this.page.getByRole('tab', { name: 'Builder' });
  }

  codeEditor(): Locator {
    // The card container and its <textarea> share the accessible name; the
    // textarea is the actual editor control (role=textbox).
    return this.page.getByRole('textbox', { name: 'Property query code editor' });
  }
}
