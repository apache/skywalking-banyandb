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
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { apiDataSource } from '../data/api.js';
import { IndexRuleBindingForm } from './IndexRuleBindingForm.js';

vi.mock('../data/api.js', () => ({
  apiDataSource: {
    getIndexRuleBinding: vi.fn(),
    listIndexRules: vi.fn(),
    createIndexRuleBinding: vi.fn(),
    updateIndexRuleBinding: vi.fn(),
    deleteIndexRuleBinding: vi.fn(),
  },
}));

function makeWrapper() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
}

const MOCK_BINDING = {
  metadata: { name: 'binding-1', group: 'sw_metric' },
  rules: ['by_host'],
  subject: { name: 'sw_metric_data', catalog: 'CATALOG_MEASURE' },
  beginAt: '2026-01-01T00:00:00Z',
  expireAt: '2026-02-01T00:00:00Z',
};

describe('IndexRuleBindingForm — edit mode', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.getIndexRuleBinding).mockResolvedValue(MOCK_BINDING);
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([
      { metadata: { name: 'by_host', group: 'sw_metric' }, tags: ['host'], type: 'INDEX_TYPE_TREE' },
    ] as never);
  });

  it('name input is read-only in edit mode', async () => {
    render(
      <IndexRuleBindingForm mode="edit" groupName="sw_metric" initialName="binding-1" onClose={vi.fn()} />,
      { wrapper: makeWrapper() },
    );
    const nameInput = await screen.findByDisplayValue('binding-1');
    expect(nameInput).toHaveAttribute('readonly');
  });

  it('loads subject + rules + window from the existing binding', async () => {
    render(
      <IndexRuleBindingForm mode="edit" groupName="sw_metric" initialName="binding-1" onClose={vi.fn()} />,
      { wrapper: makeWrapper() },
    );
    expect(await screen.findByDisplayValue('sw_metric_data')).toBeInTheDocument();
    expect(screen.getByDisplayValue('2026-01-01T00:00:00Z')).toBeInTheDocument();
    expect(screen.getByDisplayValue('2026-02-01T00:00:00Z')).toBeInTheDocument();
  });
});

describe('IndexRuleBindingForm — create mode validation', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([
      { metadata: { name: 'by_host', group: 'g' }, tags: ['host'], type: 'INDEX_TYPE_TREE' },
    ] as never);
  });

  it('rejects expireAt <= beginAt', async () => {
    render(<IndexRuleBindingForm mode="create" groupName="g" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    const textboxes = screen.getAllByRole('textbox');
    fireEvent.change(textboxes[0], { target: { value: 'binding-1' } });
    // subject name
    fireEvent.change(textboxes[1], { target: { value: 'sw_data' } });
    // begin
    fireEvent.change(textboxes[2], { target: { value: '2026-02-01T00:00:00Z' } });
    // expire (before begin)
    fireEvent.change(textboxes[3], { target: { value: '2026-01-01T00:00:00Z' } });
    // pick a rule
    fireEvent.click(await screen.findByRole('button', { name: 'by_host' }));
    fireEvent.click(screen.getByRole('button', { name: /create/i }));
    expect(await screen.findByText(/Expire time must be after/)).toBeInTheDocument();
  });

  it('rejects when no rule is selected', async () => {
    render(<IndexRuleBindingForm mode="create" groupName="g" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    const textboxes = screen.getAllByRole('textbox');
    fireEvent.change(textboxes[0], { target: { value: 'binding-1' } });
    fireEvent.change(textboxes[1], { target: { value: 'sw_data' } });
    fireEvent.change(textboxes[2], { target: { value: '2026-01-01T00:00:00Z' } });
    fireEvent.change(textboxes[3], { target: { value: '2026-02-01T00:00:00Z' } });
    fireEvent.click(screen.getByRole('button', { name: /create/i }));
    expect(await screen.findByText(/At least one rule is required/)).toBeInTheDocument();
  });
});

describe('IndexRuleBindingForm — happy path', () => {
  it('submits a valid create', async () => {
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([
      { metadata: { name: 'by_host', group: 'g' }, tags: ['host'], type: 'INDEX_TYPE_TREE' },
    ] as never);
    vi.mocked(apiDataSource.createIndexRuleBinding).mockResolvedValue(MOCK_BINDING);
    const onClose = vi.fn();
    render(<IndexRuleBindingForm mode="create" groupName="g" onClose={onClose} />, { wrapper: makeWrapper() });
    const textboxes = screen.getAllByRole('textbox');
    fireEvent.change(textboxes[0], { target: { value: 'binding-1' } });
    fireEvent.change(textboxes[1], { target: { value: 'sw_data' } });
    fireEvent.change(textboxes[2], { target: { value: '2026-01-01T00:00:00Z' } });
    fireEvent.change(textboxes[3], { target: { value: '2026-02-01T00:00:00Z' } });
    fireEvent.click(await screen.findByRole('button', { name: 'by_host' }));
    fireEvent.click(screen.getByRole('button', { name: /create/i }));
    await waitFor(() => {
      expect(apiDataSource.createIndexRuleBinding).toHaveBeenCalledWith({
        indexRuleBinding: {
          metadata: { name: 'binding-1', group: 'g' },
          rules: ['by_host'],
          subject: { name: 'sw_data', catalog: 'CATALOG_MEASURE' },
          beginAt: '2026-01-01T00:00:00Z',
          expireAt: '2026-02-01T00:00:00Z',
        },
      });
    });
  });
});

describe('IndexRuleBindingForm — delete mode', () => {
  it('renders the danger modal with the binding name', () => {
    render(<IndexRuleBindingForm mode="delete" groupName="g" initialName="binding-1" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    expect(screen.getByText('Delete index rule binding')).toBeInTheDocument();
    expect(screen.getByText('binding-1')).toBeInTheDocument();
  });
});
