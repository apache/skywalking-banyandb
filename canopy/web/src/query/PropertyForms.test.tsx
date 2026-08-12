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

// PropertyForms.test.tsx — component coverage for the property document CRUD
// modals: the tag editor (add/remove/type dropdown), validateEntry, and the
// wiring to apiDataSource (Apply/Delete, mocked — no live network).

import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { apiDataSource } from '../data/api.js';
import {
  PropertyEntryModal, PropertyCollectionModal, DeletePropertyEntryModal, validateEntry,
} from './PropertyForms.js';
import type { PropertyDocument } from 'canopy-shared';

vi.mock('../data/api.js', async () => {
  const actual = await vi.importActual<typeof import('../data/api.js')>('../data/api.js');
  return {
    ...actual,
    apiDataSource: {
      applyPropertyDocument: vi.fn(() => Promise.resolve({ created: true, tagsNum: 1 })),
      deletePropertyDocument: vi.fn(() => Promise.resolve()),
      createPropertySchema: vi.fn(() => Promise.resolve({ metadata: { name: 'x', group: 'sw' }, tags: [] })),
      deletePropertySchema: vi.fn(() => Promise.resolve()),
    },
  };
});

function Wrapper({ children }: { children: React.ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
}

function renderIn(ui: React.ReactElement) {
  return render(ui, { wrapper: Wrapper });
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe('validateEntry', () => {
  it('requires an id on create', () => {
    const e = validateEntry({ id: '', tags: [{ key: 'a', valueType: 'str', value: '1', _uid: 1 }] }, { mode: 'create' });
    expect(e.id).toMatch(/required/i);
  });

  it('rejects an id with illegal characters', () => {
    const e = validateEntry({ id: 'bad id!', tags: [{ key: 'a', valueType: 'str', value: '1', _uid: 1 }] }, { mode: 'create' });
    expect(e.id).toBeTruthy();
  });

  it('does not require an id on edit', () => {
    const e = validateEntry({ id: '', tags: [{ key: 'a', valueType: 'str', value: '1', _uid: 1 }] }, { mode: 'edit' });
    expect(e.id).toBeUndefined();
  });

  it('requires at least one tag', () => {
    const e = validateEntry({ id: 'x', tags: [] }, { mode: 'create' });
    expect(e.tagsEmpty).toBeTruthy();
  });

  it('flags duplicate tag keys', () => {
    const e = validateEntry({
      id: 'x',
      tags: [
        { key: 'a', valueType: 'str', value: '1', _uid: 1 },
        { key: 'a', valueType: 'str', value: '2', _uid: 2 },
      ],
    }, { mode: 'create' });
    expect(e.tags?.[1]?.key).toMatch(/duplicate/i);
  });

  it('flags an already-taken id', () => {
    const e = validateEntry({ id: 'dup', tags: [{ key: 'a', valueType: 'str', value: '1', _uid: 1 }] },
      { mode: 'create', existingIds: new Set(['dup']) });
    expect(e.id).toMatch(/already exists/i);
  });
});

describe('PropertyEntryModal — create (Apply)', () => {
  it('adds a tag row, removes one, and applies the document with encoded tag values', async () => {
    renderIn(
      <PropertyEntryModal
        mode="create"
        groupName="sw"
        propName="ui_menu"
        onClose={() => {}}
        onApplied={() => {}}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText('General-Service'), { target: { value: 'doc-1' } });
    fireEvent.change(screen.getByPlaceholderText('key'), { target: { value: 'menu_name' } });
    fireEvent.change(screen.getByPlaceholderText('value'), { target: { value: 'Home' } });

    fireEvent.click(screen.getByText('+ Add tag'));
    const keyInputs = screen.getAllByPlaceholderText('key');
    expect(keyInputs).toHaveLength(2);
    fireEvent.change(keyInputs[1], { target: { value: 'total' } });
    const valueInputs = screen.getAllByPlaceholderText('value');
    fireEvent.change(valueInputs[1], { target: { value: '5' } });
    // switch the second tag's type to int
    const typeSelects = screen.getAllByRole('combobox');
    fireEvent.change(typeSelects[1], { target: { value: 'int' } });

    fireEvent.click(screen.getByRole('button', { name: 'Apply document' }));

    await waitFor(() => expect(apiDataSource.applyPropertyDocument).toHaveBeenCalledTimes(1));
    const [group, name, id, req] = (apiDataSource.applyPropertyDocument as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(group).toBe('sw');
    expect(name).toBe('ui_menu');
    expect(id).toBe('doc-1');
    expect(req.strategy).toBe('STRATEGY_REPLACE');
    expect(req.property.tags).toEqual([
      { key: 'menu_name', value: { str: { value: 'Home' } } },
      { key: 'total', value: { int: { value: '5' } } },
    ]);
  });

  it('blocks submit and shows an error when a tag key is missing', async () => {
    renderIn(
      <PropertyEntryModal mode="create" groupName="sw" propName="ui_menu" onClose={() => {}} onApplied={() => {}} />,
    );
    fireEvent.change(screen.getByPlaceholderText('General-Service'), { target: { value: 'doc-1' } });
    // leave the tag key blank
    fireEvent.click(screen.getByRole('button', { name: 'Apply document' }));
    expect(await screen.findByText('Required')).toBeInTheDocument();
    expect(apiDataSource.applyPropertyDocument).not.toHaveBeenCalled();
  });
});

describe('PropertyEntryModal — edit', () => {
  it('locks the id and pre-fills existing tags', async () => {
    const entry: PropertyDocument = { id: 'doc-1', tags: [{ key: 'menu_name', valueType: 'str', value: 'Home' }] };
    renderIn(
      <PropertyEntryModal mode="edit" groupName="sw" propName="ui_menu" entry={entry} onClose={() => {}} onApplied={() => {}} />,
    );
    const idInput = screen.getByDisplayValue('doc-1') as HTMLInputElement;
    expect(idInput).toBeDisabled();
    expect(screen.getByDisplayValue('menu_name')).toBeInTheDocument();
    expect(screen.getByDisplayValue('Home')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
    await waitFor(() => expect(apiDataSource.applyPropertyDocument).toHaveBeenCalledTimes(1));
    const [, , id] = (apiDataSource.applyPropertyDocument as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(id).toBe('doc-1');
  });
});

describe('DeletePropertyEntryModal', () => {
  it('confirms and calls deletePropertyDocument', async () => {
    const onDeleted = vi.fn();
    renderIn(
      <DeletePropertyEntryModal
        groupName="sw"
        propName="ui_menu"
        entry={{ id: 'doc-1', tags: [] }}
        onClose={() => {}}
        onDeleted={onDeleted}
      />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Yes, delete' }));
    await waitFor(() => expect(apiDataSource.deletePropertyDocument).toHaveBeenCalledWith('sw', 'ui_menu', 'doc-1'));
    await waitFor(() => expect(onDeleted).toHaveBeenCalled());
  });
});

describe('PropertyCollectionModal', () => {
  it('validates the name and creates the collection', async () => {
    const onClose = vi.fn();
    renderIn(
      <PropertyCollectionModal groupName="sw" existingNames={new Set(['taken'])} onClose={onClose} />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Create property' }));
    expect(await screen.findByText(/required/i)).toBeInTheDocument();

    fireEvent.change(screen.getByPlaceholderText('temp_data'), { target: { value: 'taken' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create property' }));
    expect(await screen.findByText(/already exists/i)).toBeInTheDocument();
    expect(apiDataSource.createPropertySchema).not.toHaveBeenCalled();

    fireEvent.change(screen.getByPlaceholderText('temp_data'), { target: { value: 'new_prop' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create property' }));
    await waitFor(() => expect(apiDataSource.createPropertySchema).toHaveBeenCalledWith({
      property: { metadata: { name: 'new_prop', group: 'sw' } },
    }));
    await waitFor(() => expect(onClose).toHaveBeenCalledWith({ name: 'new_prop' }));
  });
});
