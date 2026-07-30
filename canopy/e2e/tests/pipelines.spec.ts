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

// pipelines.spec.ts — Pipelines + Pipelines/TopN journey (docs/pipelines-design.md):
// overview -> TopN list -> create a TopNAggregation over a seeded measure ->
// detail renders it (rank/sort/field/counters/group-by) -> edit -> delete.
// The source measure is seeded via SeedFactory.createMeasure (schema-only,
// over HTTP) since a topn-agg's sourceMeasure must exist first; the
// aggregation itself is created/edited/deleted through the real UI, not the
// seed factory, so this spec exercises TopNForms.tsx end-to-end.

import { test, expect } from '../framework/fixtures.js';

test.describe('Pipelines + TopN CRUD @e2e @pipelines @seed', () => {
  test('the Pipelines overview links into the TopN list', async ({ pipelinesPage }) => {
    await pipelinesPage.gotoOverview();
    await pipelinesPage.openTopNType();
    await expect(pipelinesPage.page).toHaveURL(/\/pipelines\/topn$/);
  });

  test('create -> detail -> edit -> delete a TopN aggregation over a seeded measure', async ({ pipelinesPage, seed }) => {
    const group = await seed.createGroup('e2e-topn');
    const measure = await seed.createMeasure(group, 'm', ['value', 'count']);

    await pipelinesPage.gotoList();
    // Scope the list + the "New aggregation" CTA to this group, so the create
    // modal locks Identity/Group to it and pre-fills Source group to match —
    // a TopNAggregation must live in the same group as the measure it ranks.
    await pipelinesPage.groupFilter().selectOption(group);

    const name = seed.uniqueName('e2e-topn-agg');
    await pipelinesPage.createAggregation({
      name,
      sourceMeasure: measure,
      fieldName: 'value',
      rank: 'bottomN',
      groupByTags: ['id'],
    });
    seed.trackResource(`/api/v1/topn-agg/schema/${encodeURIComponent(group)}/${encodeURIComponent(name)}`);

    // Create navigates straight to the new aggregation's detail page.
    await expect(pipelinesPage.page).toHaveURL(new RegExp(`/pipelines/topn/${group}/${name}$`));

    const summary = pipelinesPage.summaryRegion();
    await expect(summary.getByText('bottomN')).toBeVisible();
    await expect(summary.getByText('SORT_ASC')).toBeVisible();
    await expect(summary.getByText('value', { exact: true })).toBeVisible();
    await expect(summary.getByText('1000')).toBeVisible(); // default counters_number
    await expect(pipelinesPage.sourceMeasureLink(group, measure)).toBeVisible();
    await expect(pipelinesPage.groupByRegion()).toContainText('id'); // group-by tag chip

    // Edit: swap the ranked field + direction, bump counters.
    await pipelinesPage.editAggregation({ fieldName: 'count', rank: 'topN', countersNumber: 500 });
    await expect(summary.getByText('topN')).toBeVisible();
    await expect(summary.getByText('SORT_DESC')).toBeVisible();
    await expect(summary.getByText('count', { exact: true })).toBeVisible();
    await expect(summary.getByText('500')).toBeVisible();

    // The edit is visible from the cross-group list too.
    await pipelinesPage.gotoList();
    await expect(pipelinesPage.row(name)).toBeVisible();
    await expect(pipelinesPage.row(name)).toContainText('topN');

    // Delete from the detail page — type-to-confirm, then back on the list.
    await pipelinesPage.gotoDetail(group, name);
    await pipelinesPage.deleteAggregation(name);
    await expect(pipelinesPage.page).toHaveURL(/\/pipelines\/topn$/);
    await expect(pipelinesPage.row(name)).toHaveCount(0);
  });

  test('filters the TopN list by name and rank direction', async ({ pipelinesPage, seed }) => {
    const group = await seed.createGroup('e2e-topn');
    const measure = await seed.createMeasure(group, 'm', ['value']);
    const topName = await seed.createTopNAggregation(group, measure, 'value', 'e2e-top', { fieldValueSort: 'SORT_DESC' });
    const bottomName = await seed.createTopNAggregation(group, measure, 'value', 'e2e-bottom', { fieldValueSort: 'SORT_ASC' });

    await pipelinesPage.gotoList();
    await pipelinesPage.groupFilter().selectOption(group);
    await expect(pipelinesPage.row(topName)).toBeVisible();
    await expect(pipelinesPage.row(bottomName)).toBeVisible();

    await pipelinesPage.nameFilter().fill('e2e-bottom');
    await expect(pipelinesPage.row(bottomName)).toBeVisible();
    await expect(pipelinesPage.row(topName)).toHaveCount(0);

    await pipelinesPage.nameFilter().fill('');
    await pipelinesPage.rankFilter().selectOption('SORT_DESC');
    await expect(pipelinesPage.row(topName)).toBeVisible();
    await expect(pipelinesPage.row(bottomName)).toHaveCount(0);
  });
});
