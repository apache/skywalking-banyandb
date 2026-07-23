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

// PipelinesPage — page object for the Pipelines surfaces (docs/pipelines-design.md):
// the generic overview (/pipelines), the cross-group TopN list
// (/pipelines/topn — TopNList.tsx), the TopN detail page
// (/pipelines/topn/:group/:name — TopNDetail.tsx) and their create/edit/
// delete modals (TopNForms.tsx).
//
// Locators mirror the other *Page.ts conventions: modals are role="dialog"
// named by their title, filters are native <select>s reached by their
// aria-label, and the two markup gaps this milestone found (no stable handle
// for a TopNList row, and duplicate accessible-name text across the detail
// page's meta-chip row vs. its Ranking block) were closed with a
// data-testid="topn-row" and a role="region" landmark respectively, rather
// than falling back to a CSS/XPath selector.

import { expect, type Locator, type Page } from '@playwright/test';
import { BasePage } from './BasePage.js';

export type TopNRank = 'topN' | 'bottomN' | 'both';

const RANK_SORT: Record<TopNRank, string> = {
  topN: 'SORT_DESC',
  bottomN: 'SORT_ASC',
  both: 'SORT_UNSPECIFIED',
};

export interface CreateTopNOpts {
  readonly name: string;
  readonly sourceMeasure: string;
  readonly fieldName: string;
  readonly rank?: TopNRank;
  readonly groupByTags?: readonly string[];
}

export interface EditTopNOpts {
  readonly fieldName?: string;
  readonly rank?: TopNRank;
  readonly countersNumber?: number;
}

export class PipelinesPage extends BasePage {
  constructor(page: Page) {
    super(page);
  }

  // ── Navigation ─────────────────────────────────────────────────────────────
  async gotoOverview(): Promise<void> {
    await this.page.goto('/pipelines');
    await expect(this.page.getByRole('heading', { level: 1, name: 'Pipelines' })).toBeVisible();
  }

  async gotoList(): Promise<void> {
    await this.page.goto('/pipelines/topn');
    await expect(this.page.getByRole('heading', { level: 1, name: 'TopN' })).toBeVisible();
  }

  async gotoDetail(group: string, name: string): Promise<void> {
    await this.page.goto(`/pipelines/topn/${group}/${name}`);
    await expect(this.page.getByRole('heading', { level: 1, name })).toBeVisible();
  }

  // The app shell's <main> landmark. The Sidebar (docs/pipelines-design.md §3)
  // has its own always-visible "TopN" nav button, so any locator matching on
  // the "TopN" label must be scoped to main content — otherwise it's a
  // strict-mode violation (two "TopN" buttons on screen at once).
  main(): Locator {
    return this.page.getByRole('main');
  }

  // ── Overview ─────────────────────────────────────────────────────────────
  topNTypeCard(): Locator {
    return this.main().getByRole('button', { name: /TopN/ });
  }

  async openTopNType(): Promise<void> {
    await this.topNTypeCard().click();
    await expect(this.page.getByRole('heading', { level: 1, name: 'TopN' })).toBeVisible();
  }

  // ── List filters ───────────────────────────────────────────────────────────
  nameFilter(): Locator {
    return this.page.getByPlaceholder('Filter by name');
  }

  groupFilter(): Locator {
    return this.page.getByLabel('Group', { exact: true });
  }

  sourceFilter(): Locator {
    return this.page.getByLabel('Source', { exact: true });
  }

  rankFilter(): Locator {
    return this.page.getByLabel('Rank', { exact: true });
  }

  // ── List rows ──────────────────────────────────────────────────────────────
  row(name: string): Locator {
    return this.page.getByTestId('topn-row').filter({ hasText: name });
  }

  async openRow(name: string): Promise<void> {
    await this.row(name).click();
  }

  newAggregationButton(): Locator {
    return this.page.getByRole('button', { name: 'New aggregation' });
  }

  dialog(name: string | RegExp): Locator {
    return this.page.getByRole('dialog', { name });
  }

  // ── Create / edit / delete (list page) ──────────────────────────────────────
  async createAggregation(opts: CreateTopNOpts): Promise<void> {
    await this.newAggregationButton().click();
    const dlg = this.dialog('Create TopN aggregation');
    await expect(dlg).toBeVisible();
    await dlg.getByPlaceholder('service_cpm_minute_topn').fill(opts.name);
    await this.fillAggregationForm(dlg, opts);
    await dlg.getByRole('button', { name: 'Create aggregation' }).click();
    await expect(dlg).not.toBeVisible();
  }

  // Shared fill logic for source measure / ranked field / rank direction /
  // group-by tags, used by both create (via createAggregation) and edit.
  //
  // Combobox.tsx renders its option listbox through a React portal into
  // document.body (DropdownPanel), so the <li role="option"> elements are NOT
  // DOM descendants of the dialog — `dlg.getByRole('option', ...)` never
  // resolves and hangs until timeout. Options must be looked up page-scoped;
  // the combobox input itself stays dialog-scoped since it isn't portaled.
  private async fillAggregationForm(
    dlg: Locator,
    opts: { sourceMeasure?: string; fieldName?: string; rank?: TopNRank; groupByTags?: readonly string[] },
  ): Promise<void> {
    if (opts.sourceMeasure) {
      await dlg.getByRole('combobox', { name: 'Source measure' }).fill(opts.sourceMeasure);
      await this.page.getByRole('option', { name: opts.sourceMeasure }).click();
    }
    if (opts.fieldName) {
      await dlg.getByRole('combobox', { name: 'Ranked field' }).fill(opts.fieldName);
      await this.page.getByRole('option', { name: opts.fieldName }).click();
    }
    if (opts.rank) {
      await dlg.getByRole('button', { name: new RegExp(RANK_SORT[opts.rank]) }).click();
    }
    for (const tag of opts.groupByTags ?? []) {
      await dlg.getByRole('combobox', { name: 'Group by tags' }).fill(tag);
      await this.page.getByRole('option', { name: tag }).click();
    }
  }

  editButton(): Locator {
    return this.page.getByRole('button', { name: 'Edit' });
  }

  deleteButton(): Locator {
    return this.page.getByRole('button', { name: 'Delete' });
  }

  async openEdit(): Promise<Locator> {
    await this.editButton().click();
    const dlg = this.dialog('Edit TopN aggregation');
    await expect(dlg).toBeVisible();
    return dlg;
  }

  async editAggregation(opts: EditTopNOpts): Promise<void> {
    const dlg = await this.openEdit();
    await this.fillAggregationForm(dlg, opts);
    if (opts.countersNumber != null) {
      await dlg.getByLabel('Counters number').fill(String(opts.countersNumber));
    }
    await dlg.getByRole('button', { name: 'Save changes' }).click();
    await expect(dlg).not.toBeVisible();
  }

  async deleteAggregation(name: string): Promise<void> {
    await this.deleteButton().click();
    const dlg = this.dialog('Delete TopN aggregation');
    await expect(dlg).toBeVisible();
    await dlg.getByRole('textbox').fill(name);
    await dlg.getByRole('button', { name: 'Delete aggregation' }).click();
    await expect(dlg).not.toBeVisible();
  }

  // ── Detail page ──────────────────────────────────────────────────────────
  // The rank/sort/field/counters meta-chip row — scoped via its own region
  // landmark since "SORT_DESC"/"1000"/etc. recur verbatim in the Ranking
  // block further down the page (a same-name collision, the exact pitfall
  // TESTING.md calls out for unscoped getByLabel/getByText).
  summaryRegion(): Locator {
    return this.page.getByRole('region', { name: 'TopN aggregation summary' });
  }

  // The Group-by-tags chip block — also its own region landmark, since a
  // bare single-letter tag name like "id" has no reliable text-node boundary
  // to target directly (it sits beside a "1." ordinal chip with no
  // whitespace separator in the accessible text).
  groupByRegion(): Locator {
    return this.page.getByRole('region', { name: 'Group by tags' });
  }

  runQueryButton(): Locator {
    return this.page.getByRole('button', { name: /Run Top-N query/i });
  }

  // Named by its exact "Open {group}/{name}" title — a bare /^Open /
  // regex also matches the unrelated "Run Top-N query" button (title="Open
  // the Query console pre-filled to rank this measure"), a second instance
  // of the same collision pattern this milestone kept running into.
  sourceMeasureLink(group: string, name: string): Locator {
    return this.page.getByTitle(`Open ${group}/${name}`, { exact: true });
  }
}
