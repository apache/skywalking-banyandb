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
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { apiDataSource } from '../data/api.js';
import { StreamForm } from './StreamForm.js';

vi.mock('../data/api.js', () => ({
  apiDataSource: {
    createStream: vi.fn(),
    updateStream: vi.fn(),
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

// Drive controlled inputs synchronously. `fireEvent.change` sets the whole value
// in one committed React update, so the subsequent submit always validates against
// the intended state — unlike async `userEvent.type`, whose per-keystroke timing
// can race the synchronous submit under load and flake the suite.
function setValue(el: Element | null, value: string) {
  fireEvent.change(el as HTMLElement, { target: { value } });
}

const GROUP = 'test-group';

describe('StreamForm — create mode validation', () => {
  it('requires a name', async () => {
    render(<StreamForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    fireEvent.click(screen.getByRole('button', { name: /Create stream/i }));
    await waitFor(() => expect(screen.getByText('Name is required.')).toBeInTheDocument());
  });

  it('requires each tag family to have a name', async () => {
    render(<StreamForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    setValue(screen.getAllByRole('textbox')[0], 'mystream');
    setValue(screen.getByPlaceholderText('Family name'), '');
    fireEvent.click(screen.getByRole('button', { name: /Create stream/i }));
    await waitFor(() => expect(screen.getByText('Each tag family must have a name.')).toBeInTheDocument());
  });

  it('requires all tag names to be non-empty', async () => {
    render(<StreamForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    setValue(screen.getAllByRole('textbox')[0], 'mystream');
    // default family='default', tag name is empty — submit without filling it
    fireEvent.click(screen.getByRole('button', { name: /Create stream/i }));
    await waitFor(() => expect(screen.getByText('All tags in family "default" must have names.')).toBeInTheDocument());
  });

  it('rejects tag names containing "#"', async () => {
    render(<StreamForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    setValue(screen.getAllByRole('textbox')[0], 'mystream');
    setValue(screen.getByPlaceholderText('tag_name'), 'bad#tag');
    fireEvent.click(screen.getByRole('button', { name: /Create stream/i }));
    await waitFor(() => expect(screen.getByText('Tag name "bad#tag" must not contain "#".')).toBeInTheDocument());
  });

  it('requires at least one entity tag', async () => {
    render(<StreamForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    setValue(screen.getAllByRole('textbox')[0], 'mystream');
    setValue(screen.getByPlaceholderText('tag_name'), 'mytag');
    // valid name + valid tag, but no entity tag selected
    fireEvent.click(screen.getByRole('button', { name: /Create stream/i }));
    await waitFor(() => expect(screen.getByText('At least one entity tag is required.')).toBeInTheDocument());
  });
});

describe('StreamForm — edit mode immutable fields', () => {
  beforeEach(() => {
    // Keep the getResource query pending so edit pre-fill doesn't interfere with assertions
    vi.mocked(apiDataSource.getResource).mockReturnValue(new Promise(() => {}) as never);
  });

  it('name is read-only and pre-filled with the resource name', () => {
    render(
      <StreamForm mode="edit" groupName={GROUP} initialName="mystream" onClose={vi.fn()} />,
      { wrapper: makeWrapper() },
    );
    const nameInput = screen.getAllByRole('textbox')[0];
    expect(nameInput).toHaveAttribute('readonly');
    expect(nameInput).toHaveValue('mystream');
  });
});
