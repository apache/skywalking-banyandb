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
import type { MonitorOverview } from 'canopy-shared';

test('monitor displays real cluster routing and disk observations', async ({ page, request }, testInfo) => {
  const response = await request.get('./api/monitor/overview');
  expect(response.ok()).toBeTruthy();
  expect(response.headers()['cache-control']).toBe('no-store');
  const overview = await response.json() as MonitorOverview;
  expect(overview.refreshIntervalMs).toBeGreaterThanOrEqual(5000);
  expect(overview.refreshIntervalMs).toBeLessThanOrEqual(300000);
  expect(overview.cluster.status).toBe('ok');
  expect(overview.storage.status).toBe('ok');
  if (overview.storage.status !== 'ok' || overview.cluster.status !== 'ok') {
    throw new Error('Real BanyanDB cluster and metrics endpoints must be available');
  }
  expect(overview.storage.data.disks.length).toBeGreaterThan(0);
  expect(overview.storage.data.uptimes?.length).toBeGreaterThan(0);

  await page.goto('./');
  await page.getByRole('navigation', { name: 'Main navigation' }).getByRole('button', { name: 'Monitor', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Monitor', exact: true })).toBeVisible();
  await expect(page.getByRole('table', { name: 'Disk usage by node and path' })).toBeVisible();
  await expect(page.getByRole('table', { name: 'Uptime by metrics identity' })).toBeVisible();
  for (const disk of overview.storage.data.disks) {
    await expect(page.getByRole('table', { name: 'Disk usage by node and path' }).getByText(disk.path, { exact: true })).toBeVisible();
  }
  if (overview.cluster.data.nodes.length === 0) {
    await expect(page.getByText('No cluster nodes reported.')).toBeVisible();
  } else {
    const table = page.getByRole('table', { name: 'Cluster nodes' });
    await expect(table).toBeVisible();
    for (const node of overview.cluster.data.nodes) {
      await expect(table.getByRole('rowheader', { name: node.name, exact: true }).first()).toBeVisible();
    }
  }
  const refreshResponse = page.waitForResponse(res => res.url().includes('/api/monitor/overview') && res.request().method() === 'GET');
  await page.getByRole('button', { name: 'Refresh', exact: true }).click();
  expect((await refreshResponse).ok()).toBeTruthy();
  await expect(page.getByRole('button', { name: 'Refresh', exact: true })).toBeEnabled();
  const screenshot = testInfo.outputPath('monitor-overview.png');
  await page.screenshot({ path: screenshot, fullPage: true });
  await testInfo.attach('monitor-overview', { path: screenshot, contentType: 'image/png' });
});
