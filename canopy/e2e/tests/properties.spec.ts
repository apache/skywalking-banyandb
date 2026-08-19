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

// properties.spec.ts — Property CRUD + query journey (MP milestone):
// create a collection -> Apply a document -> query (builder) renders it in
// DocList -> edit -> delete document -> delete the collection. Property data
// seeds over REST (Apply) via SeedFactory.seedPropertyDoc — no gRPC seeder
// needed, per docs/property-design.md §9.

import { test, expect } from '../framework/fixtures.js';

test.describe('property CRUD + query @e2e @property @seed', () => {
  test('creates a property collection via the UI', async ({ propertyPage, seed }) => {
    const group = await seed.createGroup('e2e-prop', 'CATALOG_PROPERTY');
    const name = seed.uniqueName('e2e-coll');
    await propertyPage.gotoGroup(group);
    await propertyPage.createCollection(group, name);
    seed.trackResource(`./api/v1/property/schema/${encodeURIComponent(group)}/${encodeURIComponent(name)}`);
    await expect(propertyPage.page).toHaveURL(new RegExp(`/properties/${group}/${name}`));
  });

  test('Apply -> query (builder) renders the document in DocList -> edit -> delete', async ({ propertyPage, seed, page }) => {
    const group = await seed.createGroup('e2e-prop', 'CATALOG_PROPERTY');
    const name = await seed.createPropertySchema(group);

    await propertyPage.gotoDetail(group, name);

    // Apply a document via the UI.
    await propertyPage.applyDocument('doc-1', 'menu_name', 'Home');
    await propertyPage.run();
    await expect(propertyPage.docCard('doc-1')).toBeVisible();
    await expect(propertyPage.docCard('doc-1')).toContainText('Home');

    // Edit it — the tag value updates and the change survives a re-query.
    await propertyPage.editDocument('doc-1', 'Dashboard');
    await propertyPage.run();
    await expect(propertyPage.docCard('doc-1')).toContainText('Dashboard');

    // Delete it — the card disappears after the query re-runs.
    await propertyPage.deleteDocument('doc-1');
    await propertyPage.run();
    await expect(propertyPage.docCard('doc-1')).toHaveCount(0);
    await expect(page.getByText(/No documents match this query/i)).toBeVisible();
  });

  test('a seeded document is discoverable via the code-mode query', async ({ propertyPage, seed }) => {
    const group = await seed.createGroup('e2e-prop', 'CATALOG_PROPERTY');
    const name = await seed.createPropertySchema(group);
    await seed.seedPropertyDoc(group, name, 'seeded-1', { menu_name: 'Reports' });

    await propertyPage.gotoDetail(group, name);
    await propertyPage.codeModeTab().click();
    await expect(propertyPage.codeEditor()).toBeVisible();
    await propertyPage.run();
    await expect(propertyPage.docCard('seeded-1')).toBeVisible();
    await expect(propertyPage.docCard('seeded-1')).toContainText('Reports');
  });

  test('WHERE menu_name = value filters the builder query to a matching document', async ({ propertyPage, seed }) => {
    const group = await seed.createGroup('e2e-prop', 'CATALOG_PROPERTY');
    const name = await seed.createPropertySchema(group);
    await seed.seedPropertyDoc(group, name, 'doc-a', { menu_name: 'Alpha' });
    await seed.seedPropertyDoc(group, name, 'doc-b', { menu_name: 'Beta' });

    await propertyPage.gotoDetail(group, name);
    // First run with no filter discovers the `menu_name` tag key for the WHERE dropdown.
    await propertyPage.run();
    await expect(propertyPage.docCard('doc-a')).toBeVisible();
    await expect(propertyPage.docCard('doc-b')).toBeVisible();

    await propertyPage.page.getByRole('button', { name: /WHERE/ }).click();
    await propertyPage.page.getByRole('button', { name: 'Add condition' }).click();
    await propertyPage.page.getByLabel('Field').selectOption('menu_name');
    await propertyPage.page.getByLabel('Value').fill('Alpha');
    await propertyPage.run();
    await expect(propertyPage.docCard('doc-a')).toBeVisible();
    await expect(propertyPage.docCard('doc-b')).toHaveCount(0);
  });

  test('deletes the property collection (type-to-confirm)', async ({ propertyPage, seed }) => {
    const group = await seed.createGroup('e2e-prop', 'CATALOG_PROPERTY');
    const name = await seed.createPropertySchema(group);
    await propertyPage.gotoDetail(group, name);
    await propertyPage.deleteCollection(name);
    await expect(propertyPage.page).toHaveURL(new RegExp(`/properties/${group}$`));
  });
});
