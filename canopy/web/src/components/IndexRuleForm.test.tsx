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
    expect(await screen.findByText('host')).toBeInTheDocument();
    const treeBtn = screen.getByRole('button', { name: 'TREE' });
    expect(treeBtn).toHaveClass('is-on');
  });
});

describe('IndexRuleForm — create mode validation', () => {
  it('requires a name', async () => {
    render(<IndexRuleForm mode="create" groupName="g" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    fireEvent.click(screen.getByRole('button', { name: /create/i }));
    expect(await screen.findByText(/Name is required/)).toBeInTheDocument();
    expect(apiDataSource.createIndexRule).not.toHaveBeenCalled();
  });

  it('requires at least one tag', async () => {
    render(<IndexRuleForm mode="create" groupName="g" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    // Provide a valid name so we exercise the tags check.
    const nameInput = screen.getAllByRole('textbox')[0];
    fireEvent.change(nameInput, { target: { value: 'by_host' } });
    fireEvent.click(screen.getByRole('button', { name: /create/i }));
    expect(await screen.findByText(/At least one tag is required/)).toBeInTheDocument();
  });
});

describe('IndexRuleForm — happy path create', () => {
  it('submits a valid create', async () => {
    vi.mocked(apiDataSource.createIndexRule).mockResolvedValue(MOCK_RULE);
    const onClose = vi.fn();
    render(<IndexRuleForm mode="create" groupName="sw_metric" onClose={onClose} />, { wrapper: makeWrapper() });
    // Fill name
    const nameInput = screen.getAllByRole('textbox')[0];
    fireEvent.change(nameInput, { target: { value: 'by_host' } });
    // Add tag
    fireEvent.change(screen.getByPlaceholderText('tag_name'), { target: { value: 'host' } });
    fireEvent.click(screen.getByRole('button', { name: /add tag/i }));
    fireEvent.click(screen.getByRole('button', { name: /create/i }));
    await waitFor(() => {
      expect(apiDataSource.createIndexRule).toHaveBeenCalledWith({
        indexRule: {
          metadata: { name: 'by_host', group: 'sw_metric' },
          tags: ['host'],
          type: 'INDEX_TYPE_TREE',
        },
      });
    });
  });
});

describe('IndexRuleForm — analyzer is only shown for INVERTED', () => {
  it('does not show analyzer input for TREE', () => {
    render(<IndexRuleForm mode="create" groupName="g" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    expect(screen.queryByPlaceholderText('keyword')).toBeNull();
  });

  it('shows analyzer input after switching to INVERTED', () => {
    render(<IndexRuleForm mode="create" groupName="g" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    fireEvent.click(screen.getByRole('button', { name: 'INVERTED' }));
    expect(screen.getByPlaceholderText('keyword')).toBeInTheDocument();
  });
});

describe('IndexRuleForm — delete mode', () => {
  it('renders the danger modal with the rule name', () => {
    render(<IndexRuleForm mode="delete" groupName="g" initialName="by_host" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    expect(screen.getByText('Delete index rule')).toBeInTheDocument();
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
