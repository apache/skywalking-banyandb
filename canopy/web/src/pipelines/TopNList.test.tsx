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

// Component tests for TopNList (/pipelines/topn) — the cross-group TopN
// aggregation list: loading/empty states, name/rank filtering, the
// "source measure no longer exists" danger badge, canWrite-gated actions,
// and row-click navigation to the detail route.

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import type { TopNAggregationSchema, MeasureSchema } from 'canopy-shared';
import { FieldType, EncodingMethod, CompressMethod } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import { TopNList } from './TopNList.js';

vi.mock('../data/api.js', () => ({
  apiDataSource: {
    listGroups: vi.fn(),
    listTopNAggregations: vi.fn(),
    listResourcesInGroup: vi.fn(),
  },
}));

vi.mock('../auth/AuthContext.js', () => ({
  useAuth: vi.fn(),
}));

const GROUPS = { groups: [{ name: 'sw_metric', catalog: 'CATALOG_MEASURE', resourceOpts: { shardNum: 1 } }] };

const AGG_A: TopNAggregationSchema = {
  metadata: { name: 'service_cpm_topn', group: 'sw_metric' },
  sourceMeasure: { group: 'sw_metric', name: 'service_cpm_minute' },
  fieldName: 'value',
  fieldValueSort: 'SORT_DESC',
  groupByTagNames: ['service'],
  countersNumber: 1000,
  lruSize: 10,
};

const AGG_B: TopNAggregationSchema = {
  metadata: { name: 'endpoint_latency_bottomn', group: 'sw_metric' },
  sourceMeasure: { group: 'sw_metric', name: 'missing_measure' },
  fieldName: 'latency',
  fieldValueSort: 'SORT_ASC',
  groupByTagNames: [],
  countersNumber: 500,
};

const MEASURE: MeasureSchema = {
  metadata: { name: 'service_cpm_minute', group: 'sw_metric' },
  tagFamilies: [],
  fields: [{ name: 'value', fieldType: FieldType.INT, encodingMethod: EncodingMethod.GORILLA, compressionMethod: CompressMethod.ZSTD }],
  entity: { tagNames: ['id'] },
  interval: '1m',
};

function makeWrapper() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  function Wrapper({ children }: { children: React.ReactNode }) {
    return (
      <QueryClientProvider client={qc}>
        <MemoryRouter initialEntries={['/pipelines/topn']}>
          <Routes>
            <Route path="/pipelines/topn" element={children} />
            <Route path="/pipelines/topn/:group/:name" element={<div>DETAIL ROUTE</div>} />
            <Route path="/pipelines" element={<div>PIPELINES OVERVIEW</div>} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    );
  }
  Wrapper.displayName = 'TestWrapper';
  return Wrapper;
}

function mockAuth(role: 'admin' | 'readonly' | null) {
  vi.mocked(useAuth).mockReturnValue({
    session: role ? { user: 'u', role, banyanVersion: null } : null,
    loading: false,
    setSession: vi.fn(),
  });
}

describe('TopNList', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.listGroups).mockResolvedValue(GROUPS as never);
    vi.mocked(apiDataSource.listTopNAggregations).mockResolvedValue([AGG_A, AGG_B]);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([MEASURE] as never);
    mockAuth('admin');
  });

  it('renders both aggregation rows once loaded', async () => {
    render(<TopNList />, { wrapper: makeWrapper() });
    expect(await screen.findByText('service_cpm_topn')).toBeInTheDocument();
    expect(screen.getByText('endpoint_latency_bottomn')).toBeInTheDocument();
    expect(screen.getByText('2 aggregations')).toBeInTheDocument();
  });

  it('flags a source measure that no longer exists with the danger chip', async () => {
    render(<TopNList />, { wrapper: makeWrapper() });
    await screen.findByText('service_cpm_topn');
    // AGG_B's source ("missing_measure") isn't in the resolved measure list.
    expect(screen.getByTitle('Source measure no longer exists')).toBeInTheDocument();
  });

  it('filters the list by name', async () => {
    render(<TopNList />, { wrapper: makeWrapper() });
    await screen.findByText('service_cpm_topn');
    fireEvent.change(screen.getByPlaceholderText('Filter by name'), { target: { value: 'endpoint' } });
    await waitFor(() => expect(screen.queryByText('service_cpm_topn')).not.toBeInTheDocument());
    expect(screen.getByText('endpoint_latency_bottomn')).toBeInTheDocument();
    expect(screen.getByText('1 of 2')).toBeInTheDocument();
  });

  it('filters the list by rank direction', async () => {
    render(<TopNList />, { wrapper: makeWrapper() });
    await screen.findByText('service_cpm_topn');
    fireEvent.change(screen.getByLabelText('Rank'), { target: { value: 'SORT_ASC' } });
    await waitFor(() => expect(screen.queryByText('service_cpm_topn')).not.toBeInTheDocument());
    expect(screen.getByText('endpoint_latency_bottomn')).toBeInTheDocument();
  });

  it('shows the empty state when there are no aggregations at all', async () => {
    vi.mocked(apiDataSource.listTopNAggregations).mockResolvedValue([]);
    render(<TopNList />, { wrapper: makeWrapper() });
    expect(await screen.findByText('No TopN aggregations yet')).toBeInTheDocument();
  });

  it('hides New/Edit/Delete actions for a readonly session', async () => {
    mockAuth('readonly');
    render(<TopNList />, { wrapper: makeWrapper() });
    await screen.findByText('service_cpm_topn');
    expect(screen.queryByRole('button', { name: /New aggregation/i })).not.toBeInTheDocument();
    expect(screen.queryByTitle('Edit')).not.toBeInTheDocument();
    expect(screen.queryByTitle('Delete')).not.toBeInTheDocument();
  });

  it('shows New/Edit/Delete actions for an admin session', async () => {
    render(<TopNList />, { wrapper: makeWrapper() });
    await screen.findByText('service_cpm_topn');
    expect(screen.getByRole('button', { name: /New aggregation/i })).toBeInTheDocument();
    expect(screen.getAllByTitle('Edit').length).toBe(2);
    expect(screen.getAllByTitle('Delete').length).toBe(2);
  });

  it('navigates to the detail route when a row is clicked', async () => {
    render(<TopNList />, { wrapper: makeWrapper() });
    const row = await screen.findByText('service_cpm_topn');
    fireEvent.click(row.closest('.topn-row')!);
    expect(await screen.findByText('DETAIL ROUTE')).toBeInTheDocument();
  });
});
