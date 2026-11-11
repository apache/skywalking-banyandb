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

import React from 'react';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Group, MonitorSnapshot } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { MonitorPage, formatBytes, formatRate, formatUptime, shortPath } from './MonitorPage.js';

vi.mock('../data/api.js', () => ({ apiDataSource: { listGroups: vi.fn() } }));

const GiB = 1024 ** 3;
const snapshot: MonitorSnapshot = {
  mode: 'standalone',
  source: 'banyandb-0:2121',
  scrapedAt: 0,
  nodes: [{
    id: 'banyandb-0:2121',
    role: 'standalone',
    uptimeSeconds: 2 * 86400 + 3 * 3600,
    cpuCores: 16,
    cpuUsedCores: 4,
    rssBytes: 8 * GiB,
    memoryUsedRatio: 0.52,
    goroutines: 2610,
    disk: [{ path: '/data/measure', usedBytes: 90 * GiB, totalBytes: 100 * GiB }],
    write: { perSecond: 48200 },
    query: { perSecond: 312, avgLatencyMs: 41 },
    errorsPerMinute: 0,
    cpuHistory: [{ t: 1, v: 3 }, { t: 2, v: 4 }],
  }],
  groups: [{ name: 'sw_metric', catalog: 'measure', bytes: 2 * GiB }],
};
const groups: Group[] = [
  { name: 'sw_metric', catalog: 'CATALOG_MEASURE', resourceOpts: { shardNum: 2 } },
  { name: 'sw_props', catalog: 'CATALOG_PROPERTY', resourceOpts: { shardNum: 1 } },
];

function respond(status: number, body: unknown) {
  vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify(body), { status }));
}

function mount() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}><MonitorPage /></QueryClientProvider>);
}

beforeEach(() => vi.mocked(apiDataSource.listGroups).mockResolvedValue({ groups }));
afterEach(() => vi.restoreAllMocks());

it('shows the standalone node, its storage and group sizes', async () => {
  respond(200, snapshot);
  mount();
  const node = await screen.findByRole('button', { name: /banyandb-0:2121/ });
  expect(node).toHaveTextContent('25%'); // 4 of 16 cores
  expect(node).toHaveTextContent('90%');
  expect(node).toHaveTextContent('48.2k/s');
  expect(node).toHaveTextContent('41ms');
  // Disk at 90% is past the 85% warning, so the node and the page read as degraded.
  expect(screen.getByRole('heading', { name: /Node monitor/ })).toHaveTextContent('degraded');
  const row = (await screen.findByRole('cell', { name: 'sw_metric' })).closest('tr')!;
  expect(row).toHaveTextContent('measure22.0 GiB');
  expect(screen.queryByRole('cell', { name: 'sw_props' })).not.toBeInTheDocument();
});

it('tells a read-only user how to open the page instead of polling', async () => {
  respond(403, { error: 'forbidden', message: 'Monitor is limited to administrators.' });
  mount();
  expect(await screen.findByRole('alert')).toHaveTextContent('CANOPY_MONITOR_ROLES=admin,readonly');
  expect(screen.queryByRole('group', { name: 'Auto refresh' })).not.toBeInTheDocument();
});

it('names the failing scrape when the metrics endpoint is down', async () => {
  respond(502, { error: 'upstream_error', message: 'GET /metrics failed with ECONNREFUSED', path: '/metrics' });
  mount();
  const alert = await screen.findByRole('alert');
  expect(alert).toHaveTextContent('Cannot reach the node');
  expect(alert).toHaveTextContent('GET /metrics failed with ECONNREFUSED');
  expect(within(alert).getByRole('button', { name: 'Retry now' })).toBeInTheDocument();
});

it('opens the node drawer and closes it with Escape', async () => {
  respond(200, snapshot);
  mount();
  fireEvent.click(await screen.findByRole('button', { name: /banyandb-0:2121/ }));
  const drawer = screen.getByRole('dialog', { name: 'Node banyandb-0:2121' });
  expect(drawer).toHaveTextContent('/data/measure');
  expect(drawer).toHaveTextContent('Disk90.0 GiB · 90%');
  expect(drawer).toHaveTextContent('90.0 GiB / 100 GiB');
  expect(within(drawer).getByRole('img', { name: /CPU cores used/ })).toBeInTheDocument();
  fireEvent.keyDown(window, { key: 'Escape' });
  expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
});

it('counts paths on one filesystem once', async () => {
  const shared = { usedBytes: 40 * GiB, totalBytes: 100 * GiB };
  respond(200, { ...snapshot, nodes: [{ ...snapshot.nodes[0], disk: [{ path: '/data/measure', ...shared }, { path: '/data/stream', ...shared }] }] });
  mount();
  expect(await screen.findByRole('button', { name: /banyandb-0:2121/ })).toHaveTextContent('40%');
  expect(screen.getByText('Disk').nextSibling).toHaveTextContent('40.0 GiB / 100 GiB');
});

it('formats bytes, rates, uptime and paths', () => {
  expect(shortPath('/var/lib/banyandb/measure/data')).toBe('…/measure/data');
  expect(shortPath('/data')).toBe('/data');
  expect(formatBytes(undefined)).toBe('—');
  expect(formatBytes(1536)).toBe('1.5 KiB');
  expect(formatBytes(3 * 1024 ** 4)).toBe('3.0 TiB');
  expect(formatRate(0)).toBe('0.0/s');
  expect(formatRate(1250)).toBe('1.3k/s');
  expect(formatUptime(90)).toBe('1m');
  expect(formatUptime(3 * 86400 + 7200)).toBe('3d 2h');
});
