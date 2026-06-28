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
import { MeasureForm } from './MeasureForm.js';

vi.mock('../data/api.js', () => ({
  apiDataSource: {
    createMeasure: vi.fn(),
    updateMeasure: vi.fn(),
    deleteResource: vi.fn(),
    getResource: vi.fn(),
  },
}));

function makeWrapper() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
}

function submitForm(container: HTMLElement) {
  fireEvent.submit(container.querySelector('#measure-form') as HTMLFormElement);
}

const GROUP = 'test-group';

describe('MeasureForm — create mode validation', () => {
  it('requires a name', async () => {
    const { container } = render(<MeasureForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    submitForm(container);
    await waitFor(() => expect(screen.getByText('Name is required.')).toBeInTheDocument());
  });

  it('requires each tag family to have a name', async () => {
    const user = userEvent.setup();
    const { container } = render(<MeasureForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    await user.type(screen.getByPlaceholderText('service_cpm_minute'), 'mymeasure');
    await user.clear(screen.getByPlaceholderText('Family name'));
    submitForm(container);
    await waitFor(() => expect(screen.getByText('Each tag family must have a name.')).toBeInTheDocument());
  });

  it('requires all tag names to be non-empty', async () => {
    const user = userEvent.setup();
    const { container } = render(<MeasureForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    await user.type(screen.getByPlaceholderText('service_cpm_minute'), 'mymeasure');
    // default family='default', tag name is empty — submit without filling it
    submitForm(container);
    await waitFor(() => expect(screen.getByText('All tags in family "default" must have names.')).toBeInTheDocument());
  });

  it('rejects tag names containing "#"', async () => {
    const user = userEvent.setup();
    const { container } = render(<MeasureForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    await user.type(screen.getByPlaceholderText('service_cpm_minute'), 'mymeasure');
    await user.type(screen.getByPlaceholderText('tag_name'), 'bad#tag');
    submitForm(container);
    await waitFor(() => expect(screen.getByText('Tag name "bad#tag" must not contain "#".')).toBeInTheDocument());
  });

  it('requires all field names to be non-empty when fields are present', async () => {
    const user = userEvent.setup();
    const { container } = render(<MeasureForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    await user.type(screen.getByPlaceholderText('service_cpm_minute'), 'mymeasure');
    await user.type(screen.getByPlaceholderText('tag_name'), 'mytag');
    await user.click(screen.getByRole('button', { name: /Add field/ }));
    // field name input is empty — submit without filling it
    submitForm(container);
    await waitFor(() => expect(screen.getByText('All fields must have a name.')).toBeInTheDocument());
  });

  it('requires at least one entity tag', async () => {
    const user = userEvent.setup();
    const { container } = render(<MeasureForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    await user.type(screen.getByPlaceholderText('service_cpm_minute'), 'mymeasure');
    await user.type(screen.getByPlaceholderText('tag_name'), 'mytag');
    // valid name + valid tag, no fields, but no entity tag selected
    submitForm(container);
    await waitFor(() => expect(screen.getByText('Select at least one entity tag.')).toBeInTheDocument());
  });

  it('does not offer COMPRESSION_METHOD_UNSPECIFIED for new fields', async () => {
    const user = userEvent.setup();
    const { container } = render(<MeasureForm mode="create" groupName={GROUP} onClose={vi.fn()} />, { wrapper: makeWrapper() });
    await user.click(screen.getByRole('button', { name: /Add field/ }));
    expect(container.querySelector('option[value="COMPRESSION_METHOD_UNSPECIFIED"]')).toBeNull();
  });
});

describe('MeasureForm — edit mode immutable fields', () => {
  beforeEach(() => {
    // Keep the getResource query pending so edit pre-fill doesn't interfere with assertions
    vi.mocked(apiDataSource.getResource).mockReturnValue(new Promise(() => {}) as never);
  });

  it('name is read-only and pre-filled with the resource name', () => {
    render(
      <MeasureForm mode="edit" groupName={GROUP} initialName="mymeasure" onClose={vi.fn()} />,
      { wrapper: makeWrapper() },
    );
    const nameInput = screen.getByPlaceholderText('service_cpm_minute');
    expect(nameInput).toHaveAttribute('readonly');
    expect(nameInput).toHaveValue('mymeasure');
  });
});
