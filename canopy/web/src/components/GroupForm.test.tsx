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
import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { apiDataSource } from '../data/api.js';
import { GroupForm } from './GroupForm.js';

vi.mock('../data/api.js', () => ({
  apiDataSource: {
    listGroups: vi.fn(),
    createGroup: vi.fn(),
    updateGroup: vi.fn(),
    deleteGroup: vi.fn(),
  },
}));

function makeWrapper() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  function Wrapper({ children }: { children: React.ReactNode }) {
    return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
  }
  Wrapper.displayName = 'TestWrapper';
  return Wrapper;
}

const MOCK_GROUP = {
  name: 'mygroup',
  catalog: 'CATALOG_MEASURE',
  resourceOpts: {
    shardNum: 2,
    segmentInterval: { num: 1, unit: 'UNIT_DAY' },
    ttl: { num: 7, unit: 'UNIT_DAY' },
  },
};

describe('GroupForm — edit mode immutable fields', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.listGroups).mockResolvedValue({ groups: [MOCK_GROUP] } as never);
  });

  it('name is read-only', () => {
    render(
      <GroupForm mode="edit" initialName="mygroup" onClose={vi.fn()} />,
      { wrapper: makeWrapper() },
    );
    const nameInput = screen.getByDisplayValue('mygroup');
    expect(nameInput).toHaveAttribute('readonly');
  });

  it('catalog seg-btn buttons are disabled in edit mode', () => {
    render(
      <GroupForm mode="edit" initialName="mygroup" onClose={vi.fn()} />,
      { wrapper: makeWrapper() },
    );
    // In edit mode the catalog seg-btns are rendered but disabled (not hidden)
    expect(screen.getByRole('button', { name: 'MEASURE' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'STREAM' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'TRACE' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'PROPERTY' })).toBeDisabled();
  });

  it('MEASURE button is active (is-on) in edit mode for a measure group', () => {
    render(
      <GroupForm mode="edit" initialName="mygroup" onClose={vi.fn()} />,
      { wrapper: makeWrapper() },
    );
    expect(screen.getByRole('button', { name: 'MEASURE' })).toHaveClass('is-on');
  });
});
