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

// LifecyclePage — page object for the lifecycle-stages editor inside the
// Group create/edit modal. Each stage card is a role="group" named "Stage N",
// its number/interval inputs carry aria-labels, and the unit toggles expose
// aria-pressed (all added by the redesign) so stages are fully addressable
// without CSS classes or positional nth() indexing.

import { expect, type Locator, type Page } from '@playwright/test';
import { BasePage } from './BasePage.js';

export interface StageValues {
  name?: string;
  shards?: number;
  segNum?: number;
  ttlNum?: number;
  nodeSelector?: string;
}

export class LifecyclePage extends BasePage {
  constructor(page: Page) {
    super(page);
  }

  addStageButton(): Locator {
    return this.page.getByRole('button', { name: /Add lifecycle stage/i });
  }

  // Zero-based index → the "Stage N" (1-based) role=group landmark.
  stage(i: number): Locator {
    return this.page.getByRole('group', { name: `Stage ${i + 1}`, exact: true });
  }

  stageCards(): Locator {
    return this.page.getByRole('group', { name: /^Stage \d+$/ });
  }

  sectionTitle(): Locator {
    return this.page.getByText('Lifecycle stages');
  }

  async addStage(): Promise<void> {
    await this.addStageButton().click();
  }

  async removeStage(i: number): Promise<void> {
    await this.stage(i).getByRole('button', { name: 'Remove' }).click();
  }

  async fillStage(i: number, v: StageValues): Promise<void> {
    const card = this.stage(i);
    if (v.name !== undefined) await card.getByLabel('Stage name').fill(v.name);
    if (v.shards !== undefined) await card.getByLabel('Stage shards').fill(String(v.shards));
    if (v.segNum !== undefined) await card.getByLabel('Stage segment interval value').fill(String(v.segNum));
    if (v.ttlNum !== undefined) await card.getByLabel('Stage TTL value').fill(String(v.ttlNum));
    if (v.nodeSelector !== undefined) await card.getByLabel('Node selector').fill(v.nodeSelector);
  }

  // Unit toggle group ("Stage segment interval" | "Stage TTL") within a card.
  unitGroup(i: number, which: 'Stage segment interval' | 'Stage TTL'): Locator {
    return this.stage(i).getByRole('group', { name: which });
  }

  async setUnit(i: number, which: 'Stage segment interval' | 'Stage TTL', unit: 'Hour' | 'Day'): Promise<void> {
    await this.unitGroup(i, which).getByRole('button', { name: unit }).click();
  }

  async expectUnitSelected(i: number, which: 'Stage segment interval' | 'Stage TTL', unit: 'Hour' | 'Day'): Promise<void> {
    await expect(this.unitGroup(i, which).getByRole('button', { name: unit })).toHaveAttribute('aria-pressed', 'true');
  }

  closeCheckbox(i: number): Locator {
    return this.stage(i).getByRole('checkbox', { name: /Close non-live segments/i });
  }

  defaultCheckbox(i: number): Locator {
    return this.stage(i).getByRole('checkbox', { name: /Mark as default stage/i });
  }
}
