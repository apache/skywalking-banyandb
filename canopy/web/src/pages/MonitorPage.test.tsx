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
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Group, MonitorSnapshot } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { MonitorPage, formatBytes, formatRate, formatUptime, nodeHealth, shortPath } from './MonitorPage.js';

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

// Braced: a function returned from beforeEach would run as a teardown.
beforeEach(() => {
  vi.mocked(apiDataSource.listGroups).mockResolvedValue({ groups });
});
afterEach(() => vi.restoreAllMocks());

it('shows the standalone node, its storage and group sizes', async () => {
  respond(200, snapshot);
  mount();
  const node = await screen.findByRole('button', { name: /banyandb-0:2121/ });
  expect(node).toHaveTextContent('25%'); // 4 of 16 cores
  expect(node).toHaveTextContent('90%');
  expect(node).toHaveTextContent('48.2k/s');
  expect(node).toHaveTextContent('avg 41ms');
  // The fullest path at 90% is past the 85% warning, so the node and the page read as degraded.
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

it('judges health from the fullest path, CPU and missing readings, not just averages', () => {
  const base = snapshot.nodes[0];
  const paths = [{ path: '/a', usedBytes: 99, totalBytes: 100 }, { path: '/b', usedBytes: 0, totalBytes: 10_000 }];
  expect(nodeHealth({ ...base, cpuUsedCores: undefined, memoryUsedRatio: undefined, disk: [] }).status).toBe('unknown');
  expect(nodeHealth({ ...base, cpuUsedCores: 15, disk: [{ path: '/a', usedBytes: 1, totalBytes: 10 }] }).status).toBe('danger');
  expect(nodeHealth({ ...base, disk: paths })).toMatchObject({ disk: 99, status: 'danger' });
  expect(nodeHealth({ ...base, errorsPerMinute: 2, disk: [{ path: '/a', usedBytes: 1, totalBytes: 10 }] }).status).toBe('warn');
  expect(nodeHealth({ ...base, disk: [{ path: '/a', usedBytes: 1, totalBytes: 10 }] }).status).toBe('ok');
});

it('marks retained values stale after a failed refresh, on the card and in the drawer', async () => {
  vi.spyOn(globalThis, 'fetch')
    .mockResolvedValueOnce(new Response(JSON.stringify(snapshot), { status: 200 }))
    .mockResolvedValue(new Response(JSON.stringify({ error: 'upstream_error', message: 'GET /metrics failed with ECONNREFUSED', path: '/metrics' }), { status: 502 }));
  mount();
  const card = await screen.findByRole('button', { name: /banyandb-0:2121/ });
  expect(card).toHaveTextContent('Degraded');
  fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
  await waitFor(() => expect(card).toHaveTextContent(/Stale · as of/));
  expect(card).not.toHaveTextContent('Degraded');
  expect(screen.getByRole('heading', { name: /Node monitor/ })).toHaveTextContent('unknown');
  expect(screen.getByRole('alert')).toHaveTextContent('from the last successful scrape');
  fireEvent.click(card);
  expect(screen.getByRole('dialog')).toHaveTextContent(/standalone · Stale · as of/);
});

it('says why the group table is missing when group metadata fails', async () => {
  respond(200, snapshot);
  vi.mocked(apiDataSource.listGroups).mockRejectedValue(new Error('upstream timed out'));
  mount();
  expect(await screen.findByText(/Group details are unavailable: upstream timed out/)).toBeInTheDocument();
  expect(screen.getByRole('button', { name: /banyandb-0:2121/ })).toBeInTheDocument();
});

it('keeps focus in the drawer, survives the clock, and returns focus to the card on Escape', async () => {
  respond(200, snapshot);
  mount();
  const card = await screen.findByRole('button', { name: /banyandb-0:2121/ });
  card.focus();
  fireEvent.click(card);
  const drawer = screen.getByRole('dialog', { name: 'Node banyandb-0:2121' });
  const close = within(drawer).getByRole('button', { name: 'Close node details' });
  await waitFor(() => expect(close).toHaveFocus());
  expect(drawer).toHaveTextContent('/data/measure');
  expect(drawer).toHaveTextContent('90.0 GiB / 100 GiB');
  expect(drawer).toHaveTextContent('Fullest path90%');
  expect(within(drawer).getByRole('img', { name: /CPU cores used/ })).toBeInTheDocument();
  fireEvent.keyDown(document, { key: 'Tab' });
  expect(close).toHaveFocus();
  fireEvent.keyDown(document, { key: 'Tab', shiftKey: true });
  expect(close).toHaveFocus();
  // The page clock ticks every second; it must not refocus the drawer.
  const focus = vi.spyOn(HTMLElement.prototype, 'focus');
  await new Promise((r) => setTimeout(r, 1200));
  expect(focus).not.toHaveBeenCalled();
  fireEvent.keyDown(document, { key: 'Escape' });
  expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  expect(card).toHaveFocus();
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
