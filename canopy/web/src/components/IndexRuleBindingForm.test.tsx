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
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { apiDataSource } from '../data/api.js';
import { IndexRuleBindingForm } from './IndexRuleBindingForm.js';

vi.mock('../data/api.js', () => ({
  apiDataSource: {
    getIndexRuleBinding: vi.fn(),
    listIndexRules: vi.fn(),
    listResourcesInGroup: vi.fn(),
    createIndexRuleBinding: vi.fn(),
    updateIndexRuleBinding: vi.fn(),
    deleteIndexRuleBinding: vi.fn(),
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

const BEGIN_MS = Date.parse('2026-01-01T00:00:00Z');
const EXPIRE_MS = Date.parse('2026-02-01T00:00:00Z');
const FAR_FUTURE_MS = Date.parse('2099-12-31T00:00:00Z');

// What datetime-local will display for the above, in UTC (jsdom default TZ).
const BEGIN_LOCAL = '2026-01-01T00:00';
const EXPIRE_LOCAL = '2026-02-01T00:00';

const MOCK_BINDING = {
  metadata: { name: 'binding-1', group: 'sw_metric' },
  rules: ['by_host'],
  subject: { name: 'sw_metric_data', catalog: 'CATALOG_MEASURE' },
  beginAt: BEGIN_MS,
  expireAt: EXPIRE_MS,
};

const MOCK_RULE = {
  metadata: { name: 'by_host', group: 'sw_metric' },
  tags: ['host'],
  type: 'TYPE_TREE',
} as never;

const MOCK_RESOURCE = {
  metadata: { name: 'sw_metric_data', group: 'sw_metric', catalog: 'CATALOG_MEASURE' },
};

// Open the subject dropdown and wait for its option to appear.
async function pickSubject(name: string) {
  const inputs = screen.getAllByRole('combobox');
  fireEvent.focus(inputs[0]);
  const option = await screen.findByRole('option', { name });
  fireEvent.click(option);
}

// Open the rules dropdown and click an option.
async function pickRule(name: string) {
  const inputs = screen.getAllByRole('combobox');
  fireEvent.focus(inputs[1]);
  const option = await screen.findByRole('option', { name });
  fireEvent.click(option);
}

describe('IndexRuleBindingForm — edit mode', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.getIndexRuleBinding).mockResolvedValue(MOCK_BINDING);
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([MOCK_RULE]);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([MOCK_RESOURCE] as never);
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
    // Subject input (first combobox) shows the loaded subject name.
    const subjectInput = await waitFor(() => {
      const inputs = screen.getAllByRole('combobox') as HTMLInputElement[];
      if (inputs[0].value !== 'sw_metric_data') throw new Error('subject not yet loaded');
      return inputs[0];
    });
    expect(subjectInput).toHaveValue('sw_metric_data');
    // Rule chip appears for the loaded rule.
    expect(screen.getByRole('button', { name: /by_host/ })).toBeInTheDocument();
    // Datetime-local inputs render the formatted timestamp.
    expect(screen.getByDisplayValue(BEGIN_LOCAL)).toBeInTheDocument();
    expect(screen.getByDisplayValue(EXPIRE_LOCAL)).toBeInTheDocument();
  });
});

describe('IndexRuleBindingForm — create mode validation', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([MOCK_RULE]);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([MOCK_RESOURCE] as never);
  });

  it('rejects expireAt <= beginAt', async () => {
    render(<IndexRuleBindingForm mode="create" groupName="g" type="measures" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    fireEvent.change(screen.getByPlaceholderText('binding_name'), { target: { value: 'binding-1' } });
    await pickSubject('sw_metric_data');
    // Uncheck "Never expires" so the expire input becomes editable.
    fireEvent.click(screen.getByRole('checkbox'));
    // Set begin to a LATER date than expire.
    const dateInputs = document.querySelectorAll<HTMLInputElement>('input[type="datetime-local"]');
    fireEvent.change(dateInputs[0], { target: { value: EXPIRE_LOCAL } });
    fireEvent.change(dateInputs[1], { target: { value: BEGIN_LOCAL } });
    await pickRule('by_host');
    fireEvent.click(screen.getByRole('button', { name: /create/i }));
    expect(await screen.findByText(/Expire time must be after/)).toBeInTheDocument();
  });

  it('rejects when no rule is selected', async () => {
    render(<IndexRuleBindingForm mode="create" groupName="g" type="measures" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    fireEvent.change(screen.getByPlaceholderText('binding_name'), { target: { value: 'binding-1' } });
    await pickSubject('sw_metric_data');
    // Uncheck "Never expires" so the expire input becomes editable.
    fireEvent.click(screen.getByRole('checkbox'));
    const dateInputs = document.querySelectorAll<HTMLInputElement>('input[type="datetime-local"]');
    fireEvent.change(dateInputs[0], { target: { value: BEGIN_LOCAL } });
    fireEvent.change(dateInputs[1], { target: { value: EXPIRE_LOCAL } });
    fireEvent.click(screen.getByRole('button', { name: /create/i }));
    expect(await screen.findByText(/At least one rule is required/)).toBeInTheDocument();
  });
});

describe('IndexRuleBindingForm — happy path', () => {
  it('submits a valid create', async () => {
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([MOCK_RULE]);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([MOCK_RESOURCE] as never);
    vi.mocked(apiDataSource.createIndexRuleBinding).mockResolvedValue(MOCK_BINDING);
    const onClose = vi.fn();
    render(<IndexRuleBindingForm mode="create" groupName="g" type="measures" onClose={onClose} />, { wrapper: makeWrapper() });
    fireEvent.change(screen.getByPlaceholderText('binding_name'), { target: { value: 'binding-1' } });
    await pickSubject('sw_metric_data');
    // Uncheck "Never expires" so the expire input is editable.
    fireEvent.click(screen.getByRole('checkbox'));
    const dateInputs = document.querySelectorAll<HTMLInputElement>('input[type="datetime-local"]');
    fireEvent.change(dateInputs[0], { target: { value: BEGIN_LOCAL } });
    fireEvent.change(dateInputs[1], { target: { value: EXPIRE_LOCAL } });
    await pickRule('by_host');
    fireEvent.click(screen.getByRole('button', { name: /create/i }));
    await waitFor(() => {
      expect(apiDataSource.createIndexRuleBinding).toHaveBeenCalledWith({
        indexRuleBinding: {
          metadata: { name: 'binding-1', group: 'g' },
          rules: ['by_host'],
          subject: { name: 'sw_metric_data', catalog: 'CATALOG_MEASURE' },
          beginAt: BEGIN_MS,
          expireAt: EXPIRE_MS,
        },
      });
    });
  });
});

describe('IndexRuleBindingForm — delete mode', () => {
  it('renders the danger modal with the binding name', () => {
    render(<IndexRuleBindingForm mode="delete" groupName="g" initialName="binding-1" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    // Title and danger button both say "Delete binding" — assert at least one match.
    expect(screen.getAllByText('Delete binding').length).toBeGreaterThan(0);
    expect(screen.getByText('binding-1')).toBeInTheDocument();
  });
});

describe('IndexRuleBindingForm — never-expires toggle', () => {
  it('shows the never-expires indicator instead of an input when checked, and uses FAR_FUTURE on submit', async () => {
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([MOCK_RULE]);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([MOCK_RESOURCE] as never);
    vi.mocked(apiDataSource.createIndexRuleBinding).mockResolvedValue(MOCK_BINDING);
    render(<IndexRuleBindingForm mode="create" groupName="g" type="measures" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    // When never-expires is checked there is only one datetime-local input
    // (begin-at); the expire-at is rendered as the .f-never-expires indicator.
    const dateInputs = document.querySelectorAll<HTMLInputElement>('input[type="datetime-local"]');
    expect(dateInputs).toHaveLength(1);
    expect(document.querySelector('.f-never-expires')).toBeInTheDocument();
    // Fill the rest and submit; expireAt should be FAR_FUTURE.
    fireEvent.change(screen.getByPlaceholderText('binding_name'), { target: { value: 'binding-1' } });
    await pickSubject('sw_metric_data');
    fireEvent.change(dateInputs[0], { target: { value: BEGIN_LOCAL } });
    await pickRule('by_host');
    fireEvent.click(screen.getByRole('button', { name: /create/i }));
    await waitFor(() => {
      expect(apiDataSource.createIndexRuleBinding).toHaveBeenCalledWith(
        expect.objectContaining({
          indexRuleBinding: expect.objectContaining({ expireAt: FAR_FUTURE_MS }),
        }),
      );
    });
  });

  it('replaces the sentinel with a 30-day default when user unchecks never-expires', async () => {
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([MOCK_RULE]);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([MOCK_RESOURCE] as never);
    render(<IndexRuleBindingForm mode="create" groupName="g" type="measures" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    // Default state: never-expires checked, expireAt hidden behind indicator.
    expect(document.querySelector('.f-never-expires')).toBeInTheDocument();
    // Uncheck — the indicator should disappear and the date input appear.
    fireEvent.click(screen.getByRole('checkbox'));
    expect(document.querySelector('.f-never-expires')).toBeNull();
    const dateInputs = document.querySelectorAll<HTMLInputElement>('input[type="datetime-local"]');
    expect(dateInputs).toHaveLength(2);
    // The expire-at input should default to roughly begin + 30 days, NOT the
    // 2099-12-31 FAR_FUTURE sentinel.
    const expireValue = dateInputs[1].value;
    expect(expireValue).not.toMatch(/^2099-12-31/);
    expect(expireValue.length).toBeGreaterThan(0);
  });
});

describe('IndexRuleBindingForm — combobox fuzzy filter', () => {
  beforeEach(() => {
    vi.mocked(apiDataSource.listIndexRules).mockResolvedValue([
      { metadata: { name: 'by_host', group: 'g' } },
      { metadata: { name: 'by_service_id', group: 'g' } },
      { metadata: { name: 'by_path', group: 'g' } },
    ] as never);
    vi.mocked(apiDataSource.listResourcesInGroup).mockResolvedValue([
      { metadata: { name: 'sw_metric_data', group: 'g', catalog: 'CATALOG_MEASURE' } },
      { metadata: { name: 'sw_service_data', group: 'g', catalog: 'CATALOG_MEASURE' } },
      { metadata: { name: 'sw_path_data', group: 'g', catalog: 'CATALOG_MEASURE' } },
    ] as never);
  });

  it('filters the rules dropdown by fuzzy query', async () => {
    const user = userEvent.setup();
    render(<IndexRuleBindingForm mode="create" groupName="g" type="measures" onClose={vi.fn()} />, { wrapper: makeWrapper() });
    await waitFor(() => {
      expect(screen.getAllByRole('combobox')).toHaveLength(2);
    });
    const rulesInput = screen.getAllByRole('combobox')[1];
    // Real user interaction: click to focus, type each character.
    await user.click(rulesInput);
    await user.type(rulesInput, 'bsi');
    // Subsequence query 'b','s','i' matches "by_service_id" only.
    const opt = await screen.findByRole('option', { name: /by_service_id/ });
    expect(opt).toBeInTheDocument();
    expect(screen.queryByRole('option', { name: /by_host/ })).toBeNull();
    expect(screen.queryByRole('option', { name: /by_path/ })).toBeNull();
  });
});
