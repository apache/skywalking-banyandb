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
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';

import { apiDataSource } from '../data/api.js';
import { QueryConsole } from './QueryConsole.js';
import type { QBBuilderState } from './bydbql.js';

vi.mock('../data/api.js', () => ({
  apiDataSource: {
    listGroups: vi.fn(),
    listResourcesInGroup: vi.fn(),
    listTopNAggregations: vi.fn(),
    listIndexRuleBindings: vi.fn(),
    listIndexRules: vi.fn(),
    runQuery: vi.fn(),
  },
}));

const GROUPS = [{ name: 'g1', catalog: 'CATALOG_MEASURE', resourceOpts: { shardNum: 1 } }];

const STALE_STATE: QBBuilderState = {
  catalog: 'measures',
  group: 'g1',
  resource: 'old_measure',
  select: [{ field: 'old_field', fn: 'MEAN' }],
  projection: ['old_tag'],
  where: { combinator: 'AND', children: [{ tag: 'old_tag', op: 'BINARY_OP_EQ', value: 'old-value' }] },
  groupBy: ['old_tag'],
  time: { mode: 'relative', rel: '-30m', from: '', to: '' },
  orderField: 'time',
  orderDir: 'DESC',
  limit: 100,
  offset: 0,
  trace: false,
  topN: 10,
  aggFn: '',
  fromAgg: null,
  fromResource: 'g1/old_measure',
};

function makeWrapper() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  function Wrapper({ children }: { readonly children: React.ReactNode }) {
    return <MemoryRouter><QueryClientProvider client={queryClient}>{children}</QueryClientProvider></MemoryRouter>;
  }
  Wrapper.displayName = 'QueryConsoleTestWrapper';
  return Wrapper;
}

beforeEach(() => {
  localStorage.clear();
  vi.clearAllMocks();
  localStorage.setItem('canopy.query.v3', JSON.stringify({ builder: STALE_STATE }));
  vi.mocked(apiDataSource.listGroups).mockResolvedValue({ groups: GROUPS } as never);
  vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([
    {
      metadata: { name: 'old_measure', group: 'g1' },
      tagFamilies: [{ tags: [{ name: 'old_tag', type: 'TAG_TYPE_STRING' }] }],
      fields: [{ name: 'old_field' }],
    },
    {
      metadata: { name: 'new_measure', group: 'g1' },
      tagFamilies: [{ tags: [{ name: 'new_tag', type: 'TAG_TYPE_STRING' }] }],
      fields: [{ name: 'new_field' }],
    },
  ] as never);
  vi.mocked(apiDataSource.listTopNAggregations).mockResolvedValue([] as never);
  vi.mocked(apiDataSource.listIndexRuleBindings).mockResolvedValue([] as never);
  vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([] as never);
});

describe('QueryConsole fuzzy resource pick', () => {
  it('keeps the fuzzy-pick reset baseline for dependent clauses', async () => {
    render(<QueryConsole />, { wrapper: makeWrapper() });

    fireEvent.change(screen.getByLabelText('Search resources by name'), { target: { value: 'new_measure' } });
    const fuzzyResult = await within(screen.getByRole('listbox')).findByRole('option');
    fireEvent.click(fuzzyResult);

    const query = await screen.findByTitle(/FROM MEASURE new_measure/);
    const generated = query.getAttribute('title') ?? '';
    expect(generated).toContain('SELECT new_tag');
    expect(generated).not.toContain('old_tag');
    expect(generated).not.toContain('old_field');
    expect(generated).not.toContain('WHERE');
    expect(generated).not.toContain('GROUP BY');
  });
});
