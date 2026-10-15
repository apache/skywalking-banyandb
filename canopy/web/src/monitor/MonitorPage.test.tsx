/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership. The ASF licenses this file to You under
 * the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

import React from 'react';
import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { focusManager, QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { MonitorOverview } from 'canopy-shared';
import { MonitorPage } from '../pages/MonitorPage.js';
import { diskUsage, fetchOverview, formatBytes, formatUptime, refreshInterval } from './overview.js';

const overview: MonitorOverview = {
  observedAt: '2026-10-15T08:00:00.000Z', refreshIntervalMs: 10_000, experimentalSections: false,
  cluster: { status: 'ok', data: { nodes: [
    { name: 'data-0', route: 'hot', roles: ['data'], address: 'data-0:17912', state: 'active', apiVersion: 'v1', fileFormatVersion: '1.0.0' },
    { name: 'data-1', route: '', roles: ['data'], address: '', state: 'registered' },
    { name: 'data-2', route: 'hot', roles: ['data'], address: 'data-2:17912', state: 'evictable' },
  ] } },
  storage: { status: 'ok', data: {
    uptimes: [{ node: 'data-pod-0/banyandb', seconds: 3723 }],
    disks: [{ node: 'data-0', path: '/data', totalBytes: 1024, usedBytes: 512 }],
    metrics: [{ name: 'banyandb_measure_total_file_parts', labels: { group: 'metrics' }, value: 7 }],
  } },
};

const clients: QueryClient[] = [];
function mount() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  clients.push(client);
  return render(<QueryClientProvider client={client}><MonitorPage /></QueryClientProvider>);
}

function reply(data: MonitorOverview = overview) {
  return { ok: true, status: 200, json: async () => data } as Response;
}

afterEach(() => {
  cleanup();
  clients.forEach((client) => client.clear());
  clients.length = 0;
  focusManager.setFocused(undefined);
  vi.useRealTimers();
  vi.unstubAllGlobals();
  document.querySelector('base')?.remove();
});

it('shows reported routing states, disk usage and labeled storage metrics', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(reply()));
  mount();
  expect(await screen.findByRole('table', { name: 'Cluster nodes' })).toBeInTheDocument();
  expect(screen.getByText('Active route')).toBeInTheDocument();
  expect(screen.getByText('Registered (no active route)')).toBeInTheDocument();
  expect(screen.getByText('Evictable')).toBeInTheDocument();
  expect(screen.getByText('50.0%')).toBeInTheDocument();
  expect(screen.getByText('512 B')).toBeInTheDocument();
  expect(screen.getByText('group=metrics')).toBeInTheDocument();
  const uptimes = screen.getByRole('table', { name: 'Uptime by metrics identity' });
  expect(within(uptimes).getByText('data-pod-0/banyandb')).toBeInTheDocument();
  expect(within(uptimes).getByText('1h 2m 3s')).toBeInTheDocument();
  expect(screen.getByRole('columnheader', { name: 'API version' })).toBeInTheDocument();
  expect(screen.getByRole('columnheader', { name: 'File format' })).toBeInTheDocument();
  expect(screen.getByRole('cell', { name: 'v1' })).toBeInTheDocument();
  expect(screen.getByRole('cell', { name: '1.0.0' })).toBeInTheDocument();
  const registered = screen.getByRole('row', { name: /data-1/ });
  expect(within(registered).getAllByRole('cell', { name: 'Unavailable' })).toHaveLength(3);
  expect(screen.queryByText('Experimental')).not.toBeInTheDocument();
});

it('keeps cluster visible when storage collection fails', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(reply({ ...overview, storage: { status: 'error', message: 'Metrics source timed out' } })));
  mount();
  expect(await screen.findByRole('table', { name: 'Cluster nodes' })).toBeInTheDocument();
  expect(screen.getByRole('alert')).toHaveTextContent('Storage unavailable: Metrics source timed out');
  expect(screen.getByText('Uptime unavailable: Metrics source timed out')).toBeInTheDocument();
});

it('keeps storage visible when cluster collection fails', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(reply({ ...overview, cluster: { status: 'error', message: 'Registry unavailable' } })));
  mount();
  expect(await screen.findByRole('table', { name: 'Disk usage by node and path' })).toBeInTheDocument();
  expect(screen.getByRole('alert')).toHaveTextContent('Cluster unavailable: Registry unavailable');
});

it('reports empty sources and only shows advanced sections when enabled', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(reply({ ...overview, experimentalSections: true,
    cluster: { status: 'ok', data: { nodes: [] } }, storage: { status: 'ok', data: { disks: [], metrics: [] } },
  })));
  mount();
  expect(await screen.findByText('No cluster nodes reported.')).toBeInTheDocument();
  expect(screen.getByText('No uptime metrics reported.')).toBeInTheDocument();
  expect(screen.getByText('No disk usage metrics reported.')).toBeInTheDocument();
  expect(screen.getByText('No storage size or parts metrics reported.')).toBeInTheDocument();
  for (const name of ['Query', 'Events', 'Trace']) {
    expect(within(screen.getByRole('region', { name: `${name} monitoring` })).getByText(/not connected to a data source/)).toBeInTheDocument();
  }
});

it.each([401, 403])('shows an actionable denied state for HTTP %s without polling', async (status) => {
  const fetch = vi.fn().mockResolvedValue({ ok: false, status });
  vi.stubGlobal('fetch', fetch);
  mount();
  expect(await screen.findByRole('alert')).toHaveTextContent(status === 401 ? 'Sign in again' : 'Ask your administrator');
  expect(screen.queryByRole('table')).not.toBeInTheDocument();
  expect(fetch).toHaveBeenCalledTimes(1);
});

it('preserves stale data on a failed manual refresh and recovers on the next refresh', async () => {
  const fetch = vi.fn().mockResolvedValueOnce(reply()).mockRejectedValueOnce(new Error('Network unavailable')).mockResolvedValue(reply());
  vi.stubGlobal('fetch', fetch);
  mount();
  await screen.findByRole('table', { name: 'Cluster nodes' });
  fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
  expect(await screen.findByRole('alert')).toHaveTextContent('The data below is stale. Network unavailable');
  expect(screen.getByRole('table', { name: 'Cluster nodes' })).toBeInTheDocument();
  fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
  await waitFor(() => expect(screen.queryByRole('alert')).not.toBeInTheDocument());
});

it('hides cached data when a refresh loses monitoring permission', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(reply()).mockResolvedValue({ ok: false, status: 403 }));
  mount();
  await screen.findByRole('table', { name: 'Cluster nodes' });
  fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
  await screen.findByRole('alert');
  expect(screen.queryByRole('table')).not.toBeInTheDocument();
});

it('shows loading and aborts its request when unmounted', async () => {
  let requestSignal: AbortSignal | undefined;
  vi.stubGlobal('fetch', vi.fn((_path, options) => {
    requestSignal = options.signal;
    return new Promise(() => {});
  }));
  const view = mount();
  expect(screen.getByRole('status')).toHaveTextContent('Loading monitoring data');
  view.unmount();
  expect(requestSignal?.aborted).toBe(true);
});

it('uses the mounted Canopy base path for monitor requests', async () => {
  const base = document.createElement('base');
  base.href = '/console/';
  document.head.append(base);
  const fetch = vi.fn().mockResolvedValue(reply());
  vi.stubGlobal('fetch', fetch);
  const signal = new AbortController().signal;
  await fetchOverview(signal);
  expect(fetch).toHaveBeenCalledWith('/console/api/monitor/overview', { signal });
});

it('polls at the bounded server interval only while visible and mounted', async () => {
  vi.useFakeTimers();
  focusManager.setFocused(true);
  const fetch = vi.fn().mockResolvedValue(reply({ ...overview, refreshIntervalMs: 1 }));
  vi.stubGlobal('fetch', fetch);
  const view = mount();
  await act(async () => { await vi.advanceTimersByTimeAsync(1); });
  expect(fetch).toHaveBeenCalledTimes(1);
  await act(async () => { await vi.advanceTimersByTimeAsync(4_998); });
  expect(fetch).toHaveBeenCalledTimes(1);
  await act(async () => { await vi.advanceTimersByTimeAsync(2); });
  expect(fetch).toHaveBeenCalledTimes(2);
  focusManager.setFocused(false);
  await act(async () => { await vi.advanceTimersByTimeAsync(10_000); });
  expect(fetch).toHaveBeenCalledTimes(2);
  view.unmount();
  focusManager.setFocused(true);
  await act(async () => { await vi.advanceTimersByTimeAsync(10_000); });
  expect(fetch).toHaveBeenCalledTimes(2);
});

it('bounds server refresh configuration and rejects invalid disk ratios', () => {
  expect(refreshInterval(undefined)).toBe(10_000);
  expect(refreshInterval(NaN)).toBe(10_000);
  expect(refreshInterval(100)).toBe(5_000);
  expect(refreshInterval(600_000)).toBe(300_000);
  expect(refreshInterval(30_000)).toBe(30_000);
  for (const [used, total] of [[null, null], [1, 0], [2, 1], [-1, 2], [Infinity, 2]]) {
    expect(diskUsage(used, total)).toBe('Unavailable');
  }
  expect(formatBytes(null)).toBe('Unavailable');
  expect(formatBytes(-1)).toBe('Unavailable');
  expect(formatBytes(0)).toBe('0 B');
  expect(formatBytes(0.5)).toBe('0.5 B');
  expect(formatUptime(0)).toBe('0s');
  expect(formatUptime(90_061)).toBe('1d 1h 1m 1s');
  expect(formatUptime(3_600)).toBe('1h');
  expect(formatUptime(-1)).toBe('Unavailable');
  expect(formatUptime(Infinity)).toBe('Unavailable');
});
