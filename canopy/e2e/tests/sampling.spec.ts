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

import { test, expect } from '../framework/fixtures.js';

test.describe('Sampling pipeline persistence @e2e @sampling @smoke @seed', () => {
  test('add, reload, preserve through a group TTL edit, and remove', async ({ samplingPage, schemaPage, seed, request }) => {
    const group = await seed.createGroup('e2e-sampling', 'CATALOG_TRACE');
    const groupPath = `./api/v1/group/schema/${encodeURIComponent(group)}`;

    await samplingPage.gotoList();
    await samplingPage.createPipeline(group);
    await samplingPage.reload();
    await samplingPage.expectSavedMergeGrace('2h');

    const createdResponse = await request.get(groupPath);
    expect(createdResponse.ok()).toBeTruthy();
    const created = (await createdResponse.json()).group;
    expect(created.pipeline.enabled).toBe(true);
    expect(created.pipeline.mergeGrace).toBe('7200s');
    expect(created.pipeline.enabledEvents).toContain('PIPELINE_EVENT_MERGE');
    expect(created.resourceOpts.ttl.num).toBe(7);

    await schemaPage.gotoGroup('traces', group);
    await schemaPage.updateGroupTtl(14);

    const updatedResponse = await request.get(groupPath);
    expect(updatedResponse.ok()).toBeTruthy();
    const updated = (await updatedResponse.json()).group;
    expect(updated.resourceOpts.ttl.num).toBe(14);
    expect(updated.pipeline).toEqual(created.pipeline);

    await samplingPage.gotoDetail(group);
    await samplingPage.removePipeline();
    await samplingPage.expectPipelineAbsent(group);
    await samplingPage.gotoList();
    await samplingPage.expectPipelineAbsent(group);
    await samplingPage.expectGroupAvailableForPipeline(group);
    const removedResponse = await request.get(groupPath);
    expect(removedResponse.ok()).toBeTruthy();
    const removed = (await removedResponse.json()).group;
    expect(removed.pipeline ?? null).toBeNull();
    expect(removed.catalog).toBe('CATALOG_TRACE');
    expect(removed.resourceOpts).toEqual(updated.resourceOpts);
  });

  test('recover an expired session and persist a new sampler', async ({ page, context, samplingPage, loginPage, seed, request }) => {
    const group = await seed.createGroup('e2e-sampling-session', 'CATALOG_TRACE');
    const sampler = { name: 'keep-all', path: 'alwayskeepsampler.so' };
    const now = new Date();
    await page.clock.setFixedTime(now);
    const loaded = page.waitForResponse((response) =>
      response.url().endsWith('/api/v1/group/schema/lists') && response.status() === 200);
    await samplingPage.gotoList();
    await loaded;

    // Drop only this browser's cookie; the seed fixture and parallel tests keep
    // their sessions. Age the group cache so opening the form makes a real request.
    await context.clearCookies();
    await page.clock.setFixedTime(new Date(now.getTime() + 60_000));
    const rejected = page.waitForResponse((response) =>
      response.url().endsWith('/api/v1/group/schema/lists') && response.status() === 401);
    await samplingPage.openCreatePipeline();
    expect((await (await rejected).json()).error).toBe('unauthenticated');
    await loginPage.expectLoaded();
    await samplingPage.expectCreationDismissed();

    // Recover in place: navigating to /login would hide a stale SPA session bug.
    await loginPage.fill();
    await loginPage.submit().click();
    await expect(loginPage.username()).toBeHidden();
    await samplingPage.createPipeline(group, sampler);
    await samplingPage.reload();

    const response = await request.get(`./api/v1/group/schema/${encodeURIComponent(group)}`);
    expect(response.ok()).toBeTruthy();
    const saved = (await response.json()).group;
    expect(saved.pipeline.plugins).toEqual([
      expect.objectContaining({ name: sampler.name, sampler: expect.objectContaining({ path: sampler.path, abiVersion: 1 }) }),
    ]);
    await samplingPage.expectSavedSampler(sampler);
  });

});
