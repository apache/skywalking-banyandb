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
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes } from 'react-router';
import type { Group } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import { SamplingDetail, SamplingList } from './SamplingPages.js';

vi.mock('../data/api.js', () => ({ apiDataSource: { listGroups: vi.fn(), updateGroup: vi.fn() } }));
vi.mock('../auth/AuthContext.js', () => ({ useAuth: vi.fn() }));
const group: Group = { name: 'traces', catalog: 'CATALOG_TRACE', resourceOpts: { shardNum: 1 },
  pipeline: { enabled: true, stages: [{ stage: 'hot', plugins: [{ name: 'keep-errors', sampler: { path: 'errors.so', abiVersion: 1 } }] }] } };
function mount(detail = false) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[detail ? '/pipelines/sampling/traces' : '/pipelines/sampling']}>
    <Routes><Route path="/pipelines/sampling" element={<SamplingList />} /><Route path="/pipelines/sampling/:group" element={<SamplingDetail />} /></Routes>
  </MemoryRouter></QueryClientProvider>);
}
beforeEach(() => {
  vi.mocked(apiDataSource.listGroups).mockResolvedValue({ groups: [group,
    { name: 'unconfigured', catalog: 'CATALOG_TRACE', resourceOpts: { shardNum: 1 } },
    { name: 'metrics', catalog: 'CATALOG_MEASURE', resourceOpts: { shardNum: 1 } }] });
  vi.mocked(useAuth).mockReturnValue({ session: { user: 'admin', role: 'admin', banyanVersion: null }, loading: false, setSession: vi.fn() });
});
afterEach(() => vi.restoreAllMocks());
it('lists only configured pipelines, including disabled ones, and opens lifecycle-only details', async () => {
  vi.mocked(apiDataSource.listGroups).mockResolvedValue({ groups: [group,
    { ...group, name: 'disabled', pipeline: { enabled: false } },
    { name: 'unconfigured', catalog: 'CATALOG_TRACE', resourceOpts: { shardNum: 1 } },
    { ...group, name: 'metrics', catalog: 'CATALOG_MEASURE' }] });
  mount();
  await screen.findByRole('link', { name: 'traces' });
  expect(screen.queryByRole('link', { name: 'unconfigured' })).not.toBeInTheDocument();
  expect(screen.queryByText('metrics')).not.toBeInTheDocument();
  expect(screen.getByRole('link', { name: 'disabled' })).toBeInTheDocument();
  expect(screen.queryByRole('option', { name: 'Not configured' })).not.toBeInTheDocument();
  fireEvent.change(screen.getByRole('combobox'), { target: { value: 'Disabled' } });
  expect(screen.queryByRole('link', { name: 'traces' })).not.toBeInTheDocument();
  expect(screen.getByRole('link', { name: 'disabled' })).toBeInTheDocument();
  fireEvent.change(screen.getByRole('combobox'), { target: { value: 'All' } });
  fireEvent.click(screen.getByRole('link', { name: 'traces' }));
  expect(await screen.findByRole('heading', { name: 'Settings' })).toBeInTheDocument();
  expect(screen.getAllByText('off')).toHaveLength(2);
  expect(screen.getByText('keep-errors')).toBeInTheDocument();
});
it('removes only pipeline from the latest complete group', async () => {
  vi.spyOn(window, 'confirm').mockReturnValue(true);
  mount(true);
  await screen.findByRole('button', { name: 'Remove' });
  const latest = { ...group, resourceOpts: { shardNum: 3, ttl: { unit: 'UNIT_DAY' as const, num: 14 } } };
  vi.mocked(apiDataSource.listGroups).mockResolvedValue({ groups: [latest] });
  fireEvent.click(screen.getByRole('button', { name: 'Remove' }));
  await waitFor(() => expect(apiDataSource.updateGroup).toHaveBeenCalledWith('traces', { group: {
    metadata: { name: 'traces' }, catalog: group.catalog, resourceOpts: latest.resourceOpts,
  } }));
  const request = vi.mocked(apiDataSource.updateGroup).mock.calls.at(-1)![1];
  expect(Object.hasOwn(request.group, 'pipeline')).toBe(false);
});
it('retains the detail and shows rejected removal errors', async () => {
  vi.spyOn(window, 'confirm').mockReturnValue(true);
  vi.mocked(apiDataSource.updateGroup).mockRejectedValueOnce(new Error('Backend rejected update'));
  mount(true);
  fireEvent.click(await screen.findByRole('button', { name: 'Remove' }));
  expect(await screen.findByRole('alert')).toHaveTextContent('Backend rejected update');
  expect(screen.getByRole('heading', { name: 'Settings' })).toBeInTheDocument();
});
it('hides mutation controls for read-only users', async () => {
  vi.mocked(useAuth).mockReturnValue({ session: { user: 'reader', role: 'readonly', banyanVersion: null }, loading: false, setSession: vi.fn() });
  mount(); await screen.findByRole('link', { name: 'traces' });
  expect(screen.queryByRole('button', { name: /New sampling|Edit|Remove|Configure/ })).not.toBeInTheDocument();
});

it.each([false, true])('labels schema targets as migration-only in list/detail (detail=%s)', async (detail) => {
  mount(detail);
  expect(await screen.findByText('Migration targets')).toBeInTheDocument();
  expect(screen.getByText('Schema selectors apply only to stage sampling during lifecycle migration. MERGE and FINALIZE apply to all trace schemas in the group.')).toBeInTheDocument();
});
