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

// Component tests for TopNDetail (/pipelines/topn/:group/:name) — rank/sort/
// field/counters chips, source-measure card (existing vs. missing), criteria
// chips, the not-found state, canWrite-gated Edit/Delete, and the
// "Run Top-N query" deep-link into /query with the expected seed state.

import React from 'react';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import type { TopNAggregationSchema, MeasureSchema } from 'canopy-shared';
import { FieldType, EncodingMethod, CompressMethod } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import { TopNDetail } from './TopNDetail.js';

// topn-shared.tsx's flattenTopNCriteria (used by TopNDetail) reuses the real
// data/api.js's decodePropertyTagValue codec — keep the actual implementation
// via importOriginal rather than re-deriving its {str,int,...} unwrap logic
// here, and only stub the two apiDataSource methods this page calls.
vi.mock('../data/api.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../data/api.js')>();
  return {
    ...actual,
    apiDataSource: {
      getTopNAggregation: vi.fn(),
      listResourcesInGroup: vi.fn(),
    },
  };
});

vi.mock('../auth/AuthContext.js', () => ({
  useAuth: vi.fn(),
}));

const AGG: TopNAggregationSchema = {
  metadata: { name: 'service_cpm_topn', group: 'sw_metric' },
  sourceMeasure: { group: 'sw_metric', name: 'service_cpm_minute' },
  fieldName: 'value',
  fieldValueSort: 'SORT_DESC',
  groupByTagNames: ['service', 'region'],
  criteria: {
    le: {
      op: 'LOGICAL_OP_AND',
      left: { condition: { name: 'service', op: 'BINARY_OP_EQ', value: { str: { value: 'checkout' } } } },
      right: { condition: { name: 'status', op: 'BINARY_OP_GT', value: { int: { value: '200' } } } },
    },
  },
  countersNumber: 1000,
  lruSize: 10,
};

const MEASURE: MeasureSchema = {
  metadata: { name: 'service_cpm_minute', group: 'sw_metric' },
  tagFamilies: [],
  fields: [{ name: 'value', fieldType: FieldType.INT, encodingMethod: EncodingMethod.GORILLA, compressionMethod: CompressMethod.ZSTD }],
  entity: { tagNames: ['id'] },
  interval: '1m',
};

function LocationProbe() {
  const loc = useLocation();
  return <div data-testid="location-state">{JSON.stringify(loc.state)}</div>;
}

function makeWrapper() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  function Wrapper({ children }: { children: React.ReactNode }) {
    return (
      <QueryClientProvider client={qc}>
        <MemoryRouter initialEntries={['/pipelines/topn/sw_metric/service_cpm_topn']}>
          <Routes>
            <Route path="/pipelines/topn/sw_metric/service_cpm_topn" element={children} />
            <Route path="/pipelines/topn" element={<div>TOPN LIST ROUTE</div>} />
            <Route path="/query" element={<LocationProbe />} />
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

// Loads and settles on the detail page's h1 (unique — the breadcrumb repeats
// the same name as a plain <span>, not a heading), used as the "ready" signal
// before scoping into a specific chip/chip-row block.
async function findLoaded(name = 'service_cpm_topn') {
  return screen.findByRole('heading', { level: 1, name });
}

describe('TopNDetail', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.getTopNAggregation).mockResolvedValue(AGG);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([MEASURE] as never);
    mockAuth('admin');
  });

  it('renders rank/sort/field/counters chips', async () => {
    render(<TopNDetail groupName="sw_metric" aggName="service_cpm_topn" />, { wrapper: makeWrapper() });
    await findLoaded();
    // Scope to the top meta-chip row — "SORT_DESC"/"1000"/"value" are each
    // repeated verbatim further down in the Ranking/Source-measure blocks.
    const meta = within(screen.getByRole('region', { name: 'TopN aggregation summary' }));
    expect(meta.getByText('topN')).toBeInTheDocument();
    expect(meta.getByText('SORT_DESC')).toBeInTheDocument();
    expect(meta.getByText('value')).toBeInTheDocument();
    expect(meta.getByText('1000')).toBeInTheDocument();
    expect(meta.getByText('10')).toBeInTheDocument();
  });

  it('renders the group-by tags in order', async () => {
    render(<TopNDetail groupName="sw_metric" aggName="service_cpm_topn" />, { wrapper: makeWrapper() });
    await findLoaded();
    // "service" also appears as a criteria condition's tag further down —
    // scope to the Group-by-tags block specifically.
    const block = screen.getByText('Group by tags').closest('.detail-block') as HTMLElement;
    expect(within(block).getByText('service')).toBeInTheDocument();
    expect(within(block).getByText('region')).toBeInTheDocument();
  });

  it('renders the criteria as AND-joined condition chips', async () => {
    render(<TopNDetail groupName="sw_metric" aggName="service_cpm_topn" />, { wrapper: makeWrapper() });
    await findLoaded();
    const block = screen.getByText('Criteria').closest('.detail-block') as HTMLElement;
    expect(within(block).getByText('service')).toBeInTheDocument();
    expect(within(block).getByText('checkout')).toBeInTheDocument();
    expect(within(block).getByText('AND')).toBeInTheDocument();
    expect(within(block).getByText('status')).toBeInTheDocument();
    expect(within(block).getByText('200')).toBeInTheDocument();
  });

  it('renders the "no criteria" note when criteria is absent', async () => {
    vi.mocked(apiDataSource.getTopNAggregation).mockResolvedValue({ ...AGG, criteria: undefined });
    render(<TopNDetail groupName="sw_metric" aggName="service_cpm_topn" />, { wrapper: makeWrapper() });
    expect(await screen.findByText(/No criteria/)).toBeInTheDocument();
  });

  it('links the source measure card when the source measure exists', async () => {
    render(<TopNDetail groupName="sw_metric" aggName="service_cpm_topn" />, { wrapper: makeWrapper() });
    const link = await screen.findByTitle('Open sw_metric/service_cpm_minute');
    expect(link.tagName).toBe('BUTTON');
  });

  it('flags a missing source measure instead of a link', async () => {
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([]);
    render(<TopNDetail groupName="sw_metric" aggName="service_cpm_topn" />, { wrapper: makeWrapper() });
    expect(await screen.findByText(/measure no longer exists/)).toBeInTheDocument();
  });

  it('renders the not-found state for a missing aggregation', async () => {
    vi.mocked(apiDataSource.getTopNAggregation).mockRejectedValue(new Error('404'));
    render(<TopNDetail groupName="sw_metric" aggName="ghost" />, { wrapper: makeWrapper() });
    expect(await screen.findByText('Aggregation not found')).toBeInTheDocument();
  });

  it('hides Edit/Delete for a readonly session', async () => {
    mockAuth('readonly');
    render(<TopNDetail groupName="sw_metric" aggName="service_cpm_topn" />, { wrapper: makeWrapper() });
    await findLoaded();
    expect(screen.queryByRole('button', { name: 'Edit' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Delete' })).not.toBeInTheDocument();
  });

  it('shows Edit/Delete for an admin session', async () => {
    render(<TopNDetail groupName="sw_metric" aggName="service_cpm_topn" />, { wrapper: makeWrapper() });
    await findLoaded();
    expect(screen.getByRole('button', { name: 'Edit' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Delete' })).toBeInTheDocument();
  });

  it('"Run Top-N query" deep-links into /query with the topn seed state', async () => {
    render(<TopNDetail groupName="sw_metric" aggName="service_cpm_topn" />, { wrapper: makeWrapper() });
    const runBtn = await screen.findByRole('button', { name: /Run Top-N query/i });
    fireEvent.click(runBtn);
    const probe = await screen.findByTestId('location-state');
    expect(JSON.parse(probe.textContent!)).toEqual({
      seed: { catalog: 'topn', group: 'sw_metric', resource: 'service_cpm_topn' },
    });
  });
});
