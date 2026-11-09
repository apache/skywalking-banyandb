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
import { fireEvent, render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes } from 'react-router';
import type { Group } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { PipelinesPage } from './PipelinesPage.js';

vi.mock('../data/api.js', () => ({ apiDataSource: { listGroups: vi.fn(), listTopNAggregations: vi.fn() } }));
const groups: Group[] = [
  { name: 'metrics', catalog: 'CATALOG_MEASURE', resourceOpts: { shardNum: 1 } },
  { name: 'enabled-traces', catalog: 'CATALOG_TRACE', resourceOpts: { shardNum: 1 }, pipeline: { enabled: true } },
  { name: 'disabled-traces', catalog: 'CATALOG_TRACE', resourceOpts: { shardNum: 1 }, pipeline: { enabled: false } },
  { name: 'unconfigured', catalog: 'CATALOG_TRACE', resourceOpts: { shardNum: 1 } },
  { name: 'logs', catalog: 'CATALOG_STREAM', resourceOpts: { shardNum: 1 }, pipeline: { enabled: true } },
];
function mount() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}><MemoryRouter initialEntries={['/pipelines']}>
    <Routes>
      <Route path="/pipelines" element={<PipelinesPage />} />
      <Route path="/pipelines/sampling" element={<h1>Sampling list</h1>} />
      <Route path="/pipelines/sampling/:group" element={<h1>Sampling detail</h1>} />
    </Routes>
  </MemoryRouter></QueryClientProvider>);
}
beforeEach(() => {
  vi.mocked(apiDataSource.listGroups).mockResolvedValue({ groups });
  vi.mocked(apiDataSource.listTopNAggregations).mockResolvedValue([{ metadata: { name: 'top-latency', group: 'metrics' } }]);
});
afterEach(() => vi.clearAllMocks());

it('counts only configured trace pipelines, including disabled ones, alongside TopN', async () => {
  mount();
  await screen.findByRole('button', { name: /top-latency/ });
  expect(screen.getByText('pipelines', { exact: true }).parentElement).toHaveTextContent('3pipelines');
  expect(screen.getByText('pipeline types').parentElement).toHaveTextContent('2pipeline types');
  expect(screen.getByText('groups in use').parentElement).toHaveTextContent('3groups in use');
  expect(screen.getByRole('button', { name: /^Sampling Controls/ })).toHaveTextContent('2 pipelines');
  expect(screen.getByRole('button', { name: /enabled-traces.*Enabled/ })).toBeInTheDocument();
  expect(screen.getByRole('button', { name: /disabled-traces.*Disabled/ })).toBeInTheDocument();
  expect(screen.queryByText('unconfigured')).not.toBeInTheDocument();
  expect(screen.queryByText('logs')).not.toBeInTheDocument();
  expect(apiDataSource.listTopNAggregations).toHaveBeenCalledWith('metrics');
  expect(apiDataSource.listTopNAggregations).toHaveBeenCalledTimes(1);
});
it.each(['type card', 'view all'])('opens the sampling list from its %s', async (entry) => {
  mount();
  const button = await screen.findByRole('button', { name: entry === 'type card' ? /^Sampling Controls/ : 'View all Sampling pipelines' });
  fireEvent.click(button);
  expect(await screen.findByRole('heading', { name: 'Sampling list' })).toBeInTheDocument();
});
it('opens a configured sampling pipeline from recent items', async () => {
  mount();
  fireEvent.click(await screen.findByRole('button', { name: /disabled-traces.*Disabled/ }));
  expect(await screen.findByRole('heading', { name: 'Sampling detail' })).toBeInTheDocument();
});
it('keeps the sampling type available without configured trace pipelines', async () => {
  vi.mocked(apiDataSource.listGroups).mockResolvedValue({ groups: groups.filter((group) => !group.pipeline) });
  mount();
  await screen.findByRole('button', { name: /top-latency/ });
  expect(screen.getByRole('button', { name: /^Sampling Controls/ })).toHaveTextContent('0 pipelines');
  expect(screen.queryByRole('button', { name: 'View all Sampling pipelines' })).not.toBeInTheDocument();
});
