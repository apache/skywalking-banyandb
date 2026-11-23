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

import { expect, type Locator, type Page } from '@playwright/test';
import { BasePage } from './BasePage.js';

export class SamplingPage extends BasePage {
  constructor(page: Page) {
    super(page);
  }

  async gotoList(): Promise<void> {
    await this.page.goto('./pipelines/sampling');
    await expect(this.page.getByRole('heading', { level: 1, name: 'Sampling', exact: true })).toBeVisible();
  }

  async gotoDetail(group: string): Promise<void> {
    await this.page.goto(`./pipelines/sampling/${encodeURIComponent(group)}`);
    await expect(this.page.getByRole('heading', { level: 1, name: group })).toBeVisible();
  }

  async openCreatePipeline(): Promise<Locator> {
    await this.page.getByRole('button', { name: 'New sampling pipeline', exact: true }).click();
    const dialog = this.page.getByRole('dialog', { name: 'Create sampling pipeline' });
    return dialog;
  }

  async createPipeline(group: string, sampler?: { name: string; path: string }): Promise<void> {
    const dialog = await this.openCreatePipeline();
    await dialog.getByLabel('Trace group', { exact: true }).selectOption(group);
    if (sampler) {
      await dialog.getByRole('button', { name: '+ Add sampler', exact: true }).click();
      await dialog.getByLabel('Name', { exact: true }).fill(sampler.name);
      await dialog.getByLabel('Plugin file (.so)', { exact: true }).fill(sampler.path);
    }
    await dialog.getByLabel('MERGE grace', { exact: true }).fill('7200s');
    await dialog.getByRole('button', { name: 'Create pipeline', exact: true }).click();
    await expect(dialog).toBeHidden();
    await expect(this.page).toHaveURL(new RegExp(`/pipelines/sampling/${group}$`));
  }

  async reload(): Promise<void> {
    await this.page.reload();
    await expect(this.page.getByRole('button', { name: 'Edit', exact: true })).toBeVisible();
  }

  async openEditPipeline(): Promise<Locator> {
    await this.page.getByRole('button', { name: 'Edit', exact: true }).click();
    const dialog = this.page.getByRole('dialog', { name: 'Edit sampling pipeline' });
    await expect(dialog).toBeVisible();
    return dialog;
  }

  async expectSavedMergeGrace(grace: string): Promise<void> {
    const dialog = await this.openEditPipeline();
    await expect(dialog.getByLabel('MERGE grace', { exact: true })).toHaveValue(grace);
    await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
    await expect(dialog).toBeHidden();
  }

  async expectSavedSampler(sampler: { name: string; path: string }): Promise<void> {
    const dialog = await this.openEditPipeline();
    await dialog.getByRole('button', { name: 'Expand sampler 1', exact: true }).click();
    await expect(dialog.getByLabel('Name', { exact: true })).toHaveValue(sampler.name);
    await expect(dialog.getByLabel('Plugin file (.so)', { exact: true })).toHaveValue(sampler.path);
  }

  async expectPipelineAbsent(group: string): Promise<void> {
    await expect(this.page.getByRole('link', { name: group, exact: true })).toHaveCount(0);
  }

  async expectGroupAvailableForPipeline(group: string): Promise<void> {
    const dialog = await this.openCreatePipeline();
    await dialog.getByLabel('Trace group', { exact: true }).selectOption(group);
    this.page.once('dialog', (confirmation) => confirmation.accept());
    await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
    await expect(dialog).toBeHidden();
  }

  async expectCreationDismissed(): Promise<void> {
    await expect(this.page.getByRole('dialog', { name: 'Create sampling pipeline' })).toBeHidden();
    await expect(this.page).toHaveURL(/\/pipelines\/sampling$/);
  }

  async removePipeline(): Promise<void> {
    this.page.once('dialog', (dialog) => dialog.accept());
    await this.page.getByRole('button', { name: 'Remove', exact: true }).click();
    await expect(this.page).toHaveURL(/\/pipelines\/sampling$/);
  }
}
