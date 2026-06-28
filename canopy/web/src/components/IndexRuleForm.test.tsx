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
import { IndexType } from 'canopy-shared';
import { IndexRuleForm } from './IndexRuleForm.js';

vi.mock('../data/api.js', () => ({
  apiDataSource: {
    getIndexRule: vi.fn(),
    listIndexRules: vi.fn(),
    listIndexRuleBindings: vi.fn(),
    listResourcesInGroup: vi.fn(),
    createIndexRule: vi.fn(),
    updateIndexRule: vi.fn(),
    deleteIndexRule: vi.fn(),
  },
}));

function makeWrapper() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
}

const MOCK_RULE = {
  metadata: { name: 'by_host', group: 'sw_metric' },
  tags: ['host'],
  type: IndexType.TREE,
};

describe('IndexRuleForm — edit mode immutable fields', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.getIndexRule).mockResolvedValue(MOCK_RULE);
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([MOCK_RULE] as never);
    vi.mocked(apiDataSource.listIndexRuleBindings).mockResolvedValue([]);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([]);
  });

  it('name input is read-only in edit mode', async () => {
    render(
      <IndexRuleForm mode="edit" groupName="sw_metric" initialName="by_host" onClose={vi.fn()} />,
      { wrapper: makeWrapper() },
    );
    const nameInput = await screen.findByDisplayValue('by_host');
    expect(nameInput).toHaveAttribute('readonly');
  });

  it('loads tags + type from the existing rule', async () => {
    render(
      <IndexRuleForm mode="edit" groupName="sw_metric" initialName="by_host" onClose={vi.fn()} />,
      { wrapper: makeWrapper() },
    );
    // The "host" tag renders as a selected chip — multiple buttons can match
    // the /host/i regex, so wait specifically for the "is-on" selected chip.
    const hostChip = await waitFor(() => {
      const candidates = screen.getAllByRole('button', { name: /host/i });
      const selected = candidates.find((b) => b.classList.contains('is-on'));
      if (!selected) throw new Error('selected host chip not found');
      return selected;
    });
    expect(hostChip).toHaveClass('is-on');
    // Type cards are queried by their unique hint text (lowercase label).
    const treeCard = await screen.findByRole('button', { name: /Hierarchical tree/ });
    expect(treeCard).toHaveClass('is-on');
  });
});

describe('IndexRuleForm — create mode validation', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([]);
    vi.mocked(apiDataSource.listIndexRuleBindings).mockResolvedValue([]);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([]);
  });

  it('requires a name', async () => {
    render(<IndexRuleForm mode="create" groupName="g" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    fireEvent.click(screen.getByRole('button', { name: 'Create index rule' }));
    expect(await screen.findByText(/Name is required/)).toBeInTheDocument();
    expect(apiDataSource.createIndexRule).not.toHaveBeenCalled();
  });

  it('requires at least one tag', async () => {
    render(<IndexRuleForm mode="create" groupName="g" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    const nameInput = screen.getAllByRole('textbox')[0];
    fireEvent.change(nameInput, { target: { value: 'by_host' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create index rule' }));
    expect(await screen.findByText(/At least one tag is required/)).toBeInTheDocument();
  });
});

describe('IndexRuleForm — happy path create', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([]);
    vi.mocked(apiDataSource.listIndexRuleBindings).mockResolvedValue([]);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([]);
  });

  it('submits a valid create via the custom-tag input', async () => {
    vi.mocked(apiDataSource.createIndexRule).mockResolvedValue(MOCK_RULE);
    const onClose = vi.fn();
    render(<IndexRuleForm mode="create" groupName="sw_metric" onClose={onClose} />, { wrapper: makeWrapper() });
    // Fill name.
    const nameInput = screen.getAllByRole('textbox')[0];
    fireEvent.change(nameInput, { target: { value: 'by_host' } });
    // Add 'host' as a custom tag (no group-resource tag picker).
    const tagInput = screen.getByPlaceholderText('tag_name');
    fireEvent.change(tagInput, { target: { value: 'host' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add' }));
    // Submit — default type is INVERTED.
    fireEvent.click(screen.getByRole('button', { name: 'Create index rule' }));
    await waitFor(() => {
      expect(apiDataSource.createIndexRule).toHaveBeenCalledWith({
        indexRule: {
          metadata: { name: 'by_host', group: 'sw_metric' },
          tags: ['host'],
          type: 'TYPE_INVERTED',
          analyzer: '',
          noSort: false,
        },
      });
    });
  });
});

describe('IndexRuleForm — analyzer is locked when not INVERTED', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([]);
    vi.mocked(apiDataSource.listIndexRuleBindings).mockResolvedValue([]);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([]);
  });

  it('is enabled by default (INVERTED) and locked after switching to TREE', () => {
    render(<IndexRuleForm mode="create" groupName="g" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    // Default type is INVERTED — analyzer select is editable.
    const analyzerSelect = screen.getByDisplayValue('— none —');
    expect(analyzerSelect).not.toBeDisabled();
    // Switch to TREE — analyzer select becomes locked.
    fireEvent.click(screen.getByRole('button', { name: /Hierarchical tree/ }));
    expect(analyzerSelect).toBeDisabled();
  });
});

describe('IndexRuleForm — noSort checkbox', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([]);
    vi.mocked(apiDataSource.listIndexRuleBindings).mockResolvedValue([]);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([]);
  });

  it('toggles noSort on the submit payload', async () => {
    vi.mocked(apiDataSource.createIndexRule).mockResolvedValue(MOCK_RULE);
    render(<IndexRuleForm mode="create" groupName="sw_metric" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    const nameInput = screen.getAllByRole('textbox')[0];
    fireEvent.change(nameInput, { target: { value: 'by_host' } });
    // Add 'host' as a custom tag.
    const tagInput = screen.getByPlaceholderText('tag_name');
    fireEvent.change(tagInput, { target: { value: 'host' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add' }));
    // Toggle No sort checkbox.
    const noSort = screen.getByRole('checkbox');
    fireEvent.click(noSort);
    fireEvent.click(screen.getByRole('button', { name: 'Create index rule' }));
    await waitFor(() => {
      expect(apiDataSource.createIndexRule).toHaveBeenCalledWith(
        expect.objectContaining({
          indexRule: expect.objectContaining({ noSort: true }),
        }),
      );
    });
  });
});

describe('IndexRuleForm — delete mode', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([]);
    vi.mocked(apiDataSource.listIndexRuleBindings).mockResolvedValue([]);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([]);
  });

  it('renders the danger modal with the rule name', () => {
    render(<IndexRuleForm mode="delete" groupName="g" initialName="by_host" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    // The title and the danger button both say "Delete index rule" — assert at
    // least one is present rather than using the strict getByText.
    expect(screen.getAllByText('Delete index rule').length).toBeGreaterThan(0);
    expect(screen.getByText('by_host')).toBeInTheDocument();
  });

  it('calls deleteIndexRule on confirm', async () => {
    vi.mocked(apiDataSource.deleteIndexRule).mockResolvedValue(undefined);
    const onClose = vi.fn();
    render(<IndexRuleForm mode="delete" groupName="g" initialName="by_host" onClose={onClose} />, { wrapper: makeWrapper() });
    fireEvent.click(screen.getByRole('button', { name: /delete/i }));
    await waitFor(() => expect(apiDataSource.deleteIndexRule).toHaveBeenCalledWith('g', 'by_host'));
  });
});
