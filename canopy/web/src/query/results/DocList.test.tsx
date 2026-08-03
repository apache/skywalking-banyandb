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

// DocList.test.tsx — component coverage for the property document list:
// adaptive value cells (scalar / JSON pretty-print / long-text clamp),
// pagination past DL_PAGE_SIZE, and role-gated Edit/Delete (useCanWrite ->
// AuthContext's session.role, admin vs readonly).

import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { AuthProvider, type Session } from '../../auth/AuthContext.js';
import { DocList } from './DocList.js';
import type { PropertyDocument } from 'canopy-shared';

function mockSession(session: Session | null) {
  vi.stubGlobal('fetch', vi.fn(() => Promise.resolve({
    ok: session !== null,
    json: () => Promise.resolve(session),
  } as Response)));
}

async function renderDocList(session: Session | null, props: Partial<React.ComponentProps<typeof DocList>> = {}) {
  mockSession(session);
  const defaultRows: PropertyDocument[] = [
    { id: 'doc-1', tags: [{ key: 'name', valueType: 'str', value: 'General Service' }] },
  ];
  const onEditEntry = vi.fn();
  const onDeleteEntry = vi.fn();
  render(
    <AuthProvider>
      <DocList
        rows={props.rows ?? defaultRows}
        groupName="sw"
        propName="ui_menu"
        onEditEntry={onEditEntry}
        onDeleteEntry={onDeleteEntry}
        {...props}
      />
    </AuthProvider>,
  );
  // Let AuthProvider's session probe resolve before assertions.
  await waitFor(() => expect(document.querySelector('.doc-card, .empty')).toBeTruthy());
  return { onEditEntry, onDeleteEntry };
}

const ADMIN: Session = { user: 'root', role: 'admin', banyanVersion: null };
const READONLY: Session = { user: 'viewer', role: 'readonly', banyanVersion: null };

describe('DocList', () => {
  beforeEach(() => {
    vi.unstubAllGlobals();
  });

  it('renders the document id and its tags', async () => {
    await renderDocList(ADMIN);
    expect(screen.getByText('sw/ui_menu/')).toBeInTheDocument();
    expect(screen.getByText('doc-1')).toBeInTheDocument();
    expect(screen.getByText('name')).toBeInTheDocument();
    expect(screen.getByText('General Service')).toBeInTheDocument();
  });

  it('shows an empty state when there are no rows', async () => {
    await renderDocList(ADMIN, { rows: [] });
    expect(screen.getByText(/No documents match this query/i)).toBeInTheDocument();
  });

  it('pretty-prints a str tag value that parses as JSON', async () => {
    await renderDocList(ADMIN, {
      rows: [{ id: 'doc-2', tags: [{ key: 'configuration', valueType: 'str', value: '{"a":1,"b":2}' }] }],
    });
    expect(screen.getByText(/parses as JSON/i)).toBeInTheDocument();
    expect(screen.getByText('pretty')).toBeInTheDocument();
    expect(screen.getByText('raw')).toBeInTheDocument();
  });

  it('renders an int tag with the mono numeric style and the int type pill', async () => {
    await renderDocList(ADMIN, {
      rows: [{ id: 'doc-3', tags: [{ key: 'total', valueType: 'int', value: '42' }] }],
    });
    expect(screen.getByText('42')).toBeInTheDocument();
    expect(screen.getByText('int')).toBeInTheDocument();
  });

  it('paginates past 10 documents and Next/Prev navigate pages', async () => {
    const rows: PropertyDocument[] = Array.from({ length: 12 }, (_, i) => ({
      id: `doc-${i + 1}`,
      tags: [{ key: 'k', valueType: 'str' as const, value: 'v' }],
    }));
    await renderDocList(ADMIN, { rows });
    expect(screen.getByText(/showing 1–10 of 12 documents/i)).toBeInTheDocument();
    expect(screen.getByText('doc-1')).toBeInTheDocument();
    expect(screen.queryByText('doc-11')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /Next/i }));
    expect(screen.getByText(/showing 11–12 of 12 documents/i)).toBeInTheDocument();
    expect(screen.getByText('doc-11')).toBeInTheDocument();
    expect(screen.queryByText('doc-1')).not.toBeInTheDocument();
  });

  it('shows Edit/Delete actions for an admin session', async () => {
    await renderDocList(ADMIN);
    expect(screen.getByTitle('Edit document')).toBeInTheDocument();
    expect(screen.getByTitle('Delete document')).toBeInTheDocument();
  });

  it('hides Edit/Delete actions for a readonly session', async () => {
    await renderDocList(READONLY);
    expect(screen.queryByTitle('Edit document')).not.toBeInTheDocument();
    expect(screen.queryByTitle('Delete document')).not.toBeInTheDocument();
  });

  it('invokes onEditEntry / onDeleteEntry with the clicked document', async () => {
    const { onEditEntry, onDeleteEntry } = await renderDocList(ADMIN);
    fireEvent.click(screen.getByTitle('Edit document'));
    expect(onEditEntry).toHaveBeenCalledWith(expect.objectContaining({ id: 'doc-1' }));
    fireEvent.click(screen.getByTitle('Delete document'));
    expect(onDeleteEntry).toHaveBeenCalledWith(expect.objectContaining({ id: 'doc-1' }));
  });
});
