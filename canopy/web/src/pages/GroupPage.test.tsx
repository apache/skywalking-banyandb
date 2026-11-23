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
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Group } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import { GroupPage } from './GroupPage.js';

vi.mock('../data/api.js', () => ({ apiDataSource: { listGroups: vi.fn(), listResourcesInGroup: vi.fn() } }));
vi.mock('../auth/AuthContext.js', () => ({ useAuth: vi.fn() }));
vi.mock('../pipelines/SamplingForm.js', () => ({
  SamplingFormModal: ({ group }: { group: Group }) => <div role="dialog">Sampling for {group.name}</div>,
}));

function renderGroup(group: Group, role: 'admin' | 'readonly' = 'admin') {
  vi.mocked(useAuth).mockReturnValue({ session: { user: 'tester', role, banyanVersion: null }, loading: false, setSession: vi.fn() });
  vi.mocked(apiDataSource.listGroups).mockResolvedValue({ groups: [group] });
  vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([]);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={client}><MemoryRouter>
    <GroupPage type={group.catalog === 'CATALOG_TRACE' ? 'traces' : 'measures'} groupName={group.name} />
  </MemoryRouter></QueryClientProvider>);
}

const traceGroup: Group = { name: 'sw_trace', catalog: 'CATALOG_TRACE', resourceOpts: { shardNum: 2 } };

describe('GroupPage sampling entry', () => {
  it.each([true, false])('opens sampling editor for a trace group (configured: %s)', async (configured) => {
    renderGroup({ ...traceGroup, ...(configured ? { pipeline: { enabled: true } } : {}) });
    expect(await screen.findByRole('link', { name: 'Sampling' })).toHaveAttribute('href', '/pipelines/sampling/sw_trace');
    expect(screen.getByText(configured ? 'Enabled' : 'Not configured')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: configured ? 'Edit sampling' : 'Configure sampling' }));
    expect(screen.getByRole('dialog')).toHaveTextContent('Sampling for sw_trace');
  });

  it('allows readonly users to view sampling without edit controls', async () => {
    renderGroup(traceGroup, 'readonly');
    await screen.findByRole('link', { name: 'Sampling' });
    expect(screen.queryByRole('button', { name: /sampling/i })).not.toBeInTheDocument();
  });

  it('does not expose sampling for other catalogs', async () => {
    renderGroup({ ...traceGroup, catalog: 'CATALOG_MEASURE' });
    await screen.findByRole('heading', { name: /sw_trace/ });
    expect(screen.queryByRole('link', { name: 'Sampling' })).not.toBeInTheDocument();
  });
});
