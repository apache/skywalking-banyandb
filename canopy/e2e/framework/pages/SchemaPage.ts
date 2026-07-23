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

// SchemaPage — page object for the metadata catalog CRUD surfaces:
// TypeOverviewPage (group cards), GroupPage (resource list + meta chips),
// ResourceDetailPage, and the Group/Measure/Stream/Trace modals.
//
// Locators are semantic: modals are role="dialog" named by their title; group
// cards and resource rows are role="button" named by the resource; catalog
// buttons expose aria-pressed; group meta chips carry data-testid keyed by the
// stat (the redesign added these landmarks). Text inputs are reached by their
// placeholders (the form Fields render label text without htmlFor).

import { expect, type Locator, type Page } from '@playwright/test';
import { BasePage } from './BasePage.js';

// URL segment for a catalog: 'measures' | 'streams' | 'traces'.
export type MetaType = 'measures' | 'streams' | 'traces';
export type CatalogButton = 'MEASURE' | 'STREAM' | 'TRACE' | 'PROPERTY';

const NAME_PLACEHOLDER: Record<MetaType, string> = {
  measures: 'service_cpm_minute',
  streams: 'access_log',
  traces: 'segment_traces',
};

const SINGULAR: Record<MetaType, string> = {
  measures: 'measure',
  streams: 'stream',
  traces: 'trace',
};

export class SchemaPage extends BasePage {
  constructor(page: Page) {
    super(page);
  }

  // ── Navigation ─────────────────────────────────────────────────────────────
  async gotoOverview(type: MetaType): Promise<void> {
    await this.page.goto(`/metadata/${type}`);
    await expect(this.newGroupButton()).toBeVisible();
  }

  async gotoGroup(type: MetaType, group: string): Promise<void> {
    await this.page.goto(`/metadata/${type}/${group}`);
    await expect(this.pageTitle(group)).toBeVisible();
  }

  async gotoResource(type: MetaType, group: string, name: string): Promise<void> {
    await this.page.goto(`/metadata/${type}/${group}/${name}`);
    await expect(this.pageTitle(name)).toBeVisible();
  }

  // ── Landmarks ──────────────────────────────────────────────────────────────
  pageTitle(name: string): Locator {
    return this.page.getByRole('heading', { name: new RegExp(name) });
  }

  newGroupButton(): Locator {
    return this.page.getByRole('button', { name: 'New group' }).first();
  }

  groupCard(name: string): Locator {
    return this.page.getByRole('button', { name: new RegExp(name) });
  }

  resourceRow(name: string): Locator {
    return this.page.getByRole('button', { name: new RegExp(name) });
  }

  newResourceButton(type: MetaType): Locator {
    const singular = SINGULAR[type];
    return this.page
      .getByRole('button', { name: new RegExp(`New ${singular}`, 'i') })
      .or(this.page.getByRole('button', { name: new RegExp(`Create ${singular}`, 'i') }))
      .first();
  }

  editGroupButton(): Locator {
    return this.page.getByRole('button', { name: 'Edit group' });
  }

  deleteGroupButton(): Locator {
    return this.page.getByRole('button', { name: 'Delete group' });
  }

  // Resource-detail page action (single, unambiguous "Edit"/"Delete").
  editResourceButton(): Locator {
    return this.page.getByRole('button', { name: 'Edit', exact: true });
  }

  deleteResourceButton(): Locator {
    return this.page.getByRole('button', { name: 'Delete', exact: true });
  }

  // Group meta chip value, e.g. metaChip('shards'), metaChip('ttl'), metaChip('stages').
  metaChip(key: string): Locator {
    return this.page.getByTestId(`meta-${key}`);
  }

  emptyState(): Locator {
    return this.page.getByText(/No .* (yet|in this group)|Group not found|No matches/);
  }

  // ── Resource list / pagination ──────────────────────────────────────────────
  resourceRows(): Locator {
    return this.page.getByTestId('res-row');
  }

  groupSearch(): Locator {
    return this.page.getByPlaceholder(/^Filter/);
  }

  pagerPage(): Locator {
    return this.page.getByTestId('pager-page');
  }

  pagerPrev(): Locator {
    return this.page.getByRole('button', { name: /Prev/ });
  }

  pagerNext(): Locator {
    return this.page.getByRole('button', { name: /Next/ });
  }

  // ── Dialogs ────────────────────────────────────────────────────────────────
  dialog(name: string | RegExp): Locator {
    return this.page.getByRole('dialog', { name });
  }

  // ── Group flows ────────────────────────────────────────────────────────────
  async createGroup(type: MetaType, name: string, catalog: CatalogButton): Promise<void> {
    await this.newGroupButton().click();
    const dlg = this.dialog('New group');
    await expect(dlg).toBeVisible();
    await dlg.getByPlaceholder('sw_metric').fill(name);
    await dlg.getByRole('button', { name: catalog }).click();
    await expect(dlg.getByRole('button', { name: catalog })).toHaveAttribute('aria-pressed', 'true');
    await dlg.getByRole('button', { name: 'Create group' }).click();
  }

  // Open the pre-filled edit-group modal and wait until GroupForm hydrated it.
  async openEditGroup(): Promise<Locator> {
    await this.editGroupButton().click();
    const dlg = this.dialog('Edit group');
    await expect(dlg).toBeVisible();
    // GroupForm marks the form data-initialized once it has pre-filled.
    await expect(dlg).toHaveAttribute('data-initialized', 'true');
    return dlg;
  }

  async deleteGroup(name: string): Promise<void> {
    await this.deleteGroupButton().click();
    const dlg = this.dialog('Delete group');
    await expect(dlg).toBeVisible();
    await dlg.getByRole('textbox').fill(name);
    await dlg.getByRole('button', { name: 'Delete group' }).click();
  }

  // ── Measure flow ───────────────────────────────────────────────────────────
  async createMeasure(name: string, tag: string, indexMode = true): Promise<void> {
    await this.newResourceButton('measures').click();
    const dlg = this.dialog('Create measure');
    await expect(dlg).toBeVisible();
    await dlg.getByPlaceholder(NAME_PLACEHOLDER.measures).fill(name);
    await dlg.getByPlaceholder('tag_name').first().fill(tag);
    // Pick the tag as the entity via the availability picker.
    await dlg.getByRole('button', { name: tag, exact: true }).click();
    if (indexMode) {
      await dlg.getByRole('checkbox', { name: /Enable index mode/i }).check();
    }
    await dlg.getByRole('button', { name: 'Create measure' }).click();
  }

  // ── Stream flow ────────────────────────────────────────────────────────────
  async createStream(name: string, tag: string): Promise<void> {
    await this.newResourceButton('streams').click();
    const dlg = this.dialog('Create stream');
    await expect(dlg).toBeVisible();
    await dlg.getByPlaceholder(NAME_PLACEHOLDER.streams).fill(name);
    await dlg.getByPlaceholder('tag_name').first().fill(tag);
    await dlg.getByRole('button', { name: tag, exact: true }).click();
    await dlg.getByRole('button', { name: 'Create stream' }).click();
  }

  // ── Trace flow ─────────────────────────────────────────────────────────────
  async createTrace(name: string, tags: readonly string[]): Promise<void> {
    await this.newResourceButton('traces').click();
    const dlg = this.dialog('Create trace');
    await expect(dlg).toBeVisible();
    await dlg.getByPlaceholder(NAME_PLACEHOLDER.traces).fill(name);
    const tagInputs = dlg.getByPlaceholder('tag_name');
    await tagInputs.nth(0).fill(tags[0]);
    await dlg.getByRole('button', { name: 'Add tag' }).click();
    await tagInputs.nth(1).fill(tags[1]);
    await dlg.getByRole('button', { name: 'Add tag' }).click();
    await tagInputs.nth(2).fill(tags[2]);
    // Reserved-tag mapping selects are properly labelled (getByLabel works).
    await dlg.getByLabel(/Trace ID tag/).selectOption(tags[0]);
    await dlg.getByLabel(/Span ID tag/).selectOption(tags[1]);
    await dlg.getByLabel(/Timestamp tag/).selectOption(tags[2]);
    await dlg.getByRole('button', { name: 'Create trace' }).click();
  }

  // Delete a resource from its detail page (opens the typed delete dialog).
  async deleteResource(type: MetaType): Promise<void> {
    await this.deleteResourceButton().click();
    const singular = SINGULAR[type];
    const dlg = this.dialog(new RegExp(`Delete ${singular}`, 'i'));
    await expect(dlg).toBeVisible();
    await dlg.getByRole('button', { name: new RegExp(`Delete ${singular}`, 'i') }).click();
  }
}
