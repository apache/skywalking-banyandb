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
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { apiDataSource } from '../data/api.js';
import { TraceForm } from './TraceForm.js';

vi.mock('../data/api.js', () => ({
  apiDataSource: {
    createTrace: vi.fn(),
    updateTrace: vi.fn(),
    deleteResource: vi.fn(),
    getResource: vi.fn(),
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

const GROUP = 'test-group';

describe('TraceForm — create mode validation', () => {
  it('requires a name', async () => {
    render(<TraceForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    fireEvent.click(screen.getByRole('button', { name: /Create trace/i }));
    await waitFor(() => expect(screen.getByText('Name is required.')).toBeInTheDocument());
  });

  it('requires all tag names to be non-empty', async () => {
    const user = userEvent.setup();
    render(<TraceForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    await user.type(screen.getAllByRole('textbox')[0], 'mytrace');
    // default tag row has empty name — submit without filling it
    fireEvent.click(screen.getByRole('button', { name: /Create trace/i }));
    await waitFor(() => expect(screen.getByText('All tags must have a name.')).toBeInTheDocument());
  });

  it('rejects tag names containing "#"', async () => {
    const user = userEvent.setup();
    render(<TraceForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    await user.type(screen.getAllByRole('textbox')[0], 'mytrace');
    await user.type(screen.getByPlaceholderText('tag_name'), 'bad#tag');
    fireEvent.click(screen.getByRole('button', { name: /Create trace/i }));
    await waitFor(() => expect(screen.getByText('Tag name "bad#tag" must not contain "#".')).toBeInTheDocument());
  });

  it('requires trace ID tag to be selected', async () => {
    const user = userEvent.setup();
    render(<TraceForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    await user.type(screen.getAllByRole('textbox')[0], 'mytrace');
    await user.type(screen.getByPlaceholderText('tag_name'), 't1');
    // all role tag selects left at default "— select —"
    fireEvent.click(screen.getByRole('button', { name: /Create trace/i }));
    await waitFor(() => expect(screen.getByText('Trace ID tag name is required.')).toBeInTheDocument());
  });

  it('requires span ID tag to be selected', async () => {
    const user = userEvent.setup();
    render(<TraceForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    await user.type(screen.getAllByRole('textbox')[0], 'mytrace');
    await user.type(screen.getByPlaceholderText('tag_name'), 't1');
    fireEvent.change(screen.getByLabelText(/Trace ID tag/), { target: { value: 't1' } });
    fireEvent.click(screen.getByRole('button', { name: /Create trace/i }));
    await waitFor(() => expect(screen.getByText('Span ID tag name is required.')).toBeInTheDocument());
  });

  it('requires timestamp tag to be selected', async () => {
    const user = userEvent.setup();
    render(<TraceForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    await user.type(screen.getAllByRole('textbox')[0], 'mytrace');
    await user.type(screen.getByPlaceholderText('tag_name'), 't1');
    fireEvent.change(screen.getByLabelText(/Trace ID tag/), { target: { value: 't1' } });
    fireEvent.change(screen.getByLabelText(/Span ID tag/), { target: { value: 't1' } });
    fireEvent.click(screen.getByRole('button', { name: /Create trace/i }));
    await waitFor(() => expect(screen.getByText('Timestamp tag name is required.')).toBeInTheDocument());
  });
});

describe('TraceForm — edit mode immutable fields', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.getResource).mockReturnValue(new Promise(() => {}) as never);
  });

  it('name is read-only and pre-filled with the resource name', () => {
    render(
      <TraceForm mode="edit" groupName={GROUP} initialName="mytrace" onClose={vi.fn()} />,
      { wrapper: makeWrapper() },
    );
    const nameInput = screen.getAllByRole('textbox')[0];
    expect(nameInput).toHaveAttribute('readonly');
    expect(nameInput).toHaveValue('mytrace');
  });
});

describe('TraceForm — edit mode pre-fill', () => {
  it('populates tags and role selects from the fetched schema', async () => {
    vi.mocked(apiDataSource.getResource).mockResolvedValue({
      metadata: { name: 'mytrace', group: GROUP },
      tags: [
        { name: 'tid', type: 'TAG_TYPE_STRING' as never },
        { name: 'sid', type: 'TAG_TYPE_STRING' as never },
        { name: 'ts',  type: 'TAG_TYPE_STRING' as never },
      ],
      traceIdTagName: 'tid',
      spanIdTagName: 'sid',
      timestampTagName: 'ts',
    } as never);

    render(
      <TraceForm mode="edit" groupName={GROUP} initialName="mytrace" onClose={vi.fn()} />,
      { wrapper: makeWrapper() },
    );

    // Wait for useEffect to run after the query resolves
    await waitFor(() =>
      expect(screen.getAllByPlaceholderText('tag_name')[0]).toHaveValue('tid'),
    );
    expect(screen.getAllByPlaceholderText('tag_name')[1]).toHaveValue('sid');
    expect(screen.getAllByPlaceholderText('tag_name')[2]).toHaveValue('ts');
    // The role-selects are <select> elements whose selected <option> shows the
    // tag name; `getAllByDisplayValue` finds them alongside the text inputs.
    expect(screen.getAllByDisplayValue('tid').length).toBeGreaterThanOrEqual(2);
    expect(screen.getAllByDisplayValue('sid').length).toBeGreaterThanOrEqual(2);
    expect(screen.getAllByDisplayValue('ts').length).toBeGreaterThanOrEqual(2);
  });
});
