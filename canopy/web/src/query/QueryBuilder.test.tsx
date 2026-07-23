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

// QueryBuilder — WHERE condition UI. Focused on the MATCH-only analyzer +
// operator inputs (the rest of the WHERE→BydbQL generation is covered by
// where.test.ts / bydbql.test.ts).

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryBuilder, type QueryBuilderProps } from './QueryBuilder.js';
import type { QBBuilderState, QBWhereLeafWithConn } from './bydbql.js';

function stateWithLeaf(leaf: QBWhereLeafWithConn): QBBuilderState {
  return {
    catalog: 'streams',
    group: 'g1',
    resource: 'logs',
    select: [],
    projection: [],
    where: { combinator: 'AND', children: [leaf] },
    groupBy: [],
    time: { mode: 'relative', rel: '-30m', from: '', to: '' },
    orderField: 'time',
    orderDir: 'DESC',
    limit: 100,
    offset: 0,
    trace: false,
    topN: 10,
    aggFn: '',
    fromAgg: null,
    fromResource: null,
  };
}

function renderBuilder(state: QBBuilderState, onChange = vi.fn()) {
  const props: QueryBuilderProps = {
    state,
    onChange,
    tags: ['name', 'message'],
    fields: [],
    groupNames: ['g1'],
    resourceNames: ['logs'],
    topnAggNames: [],
    groups: [],
    groupResources: new Map(),
    groupTopnAggs: new Map(),
    onPickResource: vi.fn(),
    isRunning: false,
    onEjectToCode: vi.fn(),
    onRun: vi.fn(),
    hasRun: false, // full clause editors (no accordion) so the WHERE row is visible
    compact: false,
    setCompact: vi.fn(),
    openSection: null,
    setOpenSection: vi.fn(),
  };
  render(<MemoryRouter><QueryBuilder {...props} /></MemoryRouter>);
  return { onChange };
}

describe('QueryBuilder WHERE — MATCH analyzer + operator fields', () => {
  it('shows the Analyzer + Match operator controls only for the MATCH operator', () => {
    renderBuilder(stateWithLeaf({ tag: 'name', op: 'BINARY_OP_MATCH', value: 'nodea' }));
    expect(screen.getByLabelText('Analyzer')).toBeInTheDocument();
    expect(screen.getByLabelText('Match operator')).toBeInTheDocument();
  });

  it('hides the MATCH controls for a non-MATCH operator', () => {
    renderBuilder(stateWithLeaf({ tag: 'name', op: 'BINARY_OP_EQ', value: 'nodea' }));
    expect(screen.queryByLabelText('Analyzer')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Match operator')).not.toBeInTheDocument();
  });

  it('propagates the analyzer edit onto the WHERE leaf', () => {
    const { onChange } = renderBuilder(stateWithLeaf({ tag: 'name', op: 'BINARY_OP_MATCH', value: 'nodea' }));
    fireEvent.change(screen.getByLabelText('Analyzer'), { target: { value: 'standard' } });
    const call = onChange.mock.calls.find((c) => c[0]?.where);
    expect(call).toBeTruthy();
    expect((call![0].where.children[0] as QBWhereLeafWithConn).analyzer).toBe('standard');
  });

  it('propagates the match-operator selection onto the WHERE leaf', () => {
    const { onChange } = renderBuilder(stateWithLeaf({ tag: 'name', op: 'BINARY_OP_MATCH', value: 'nodea' }));
    fireEvent.change(screen.getByLabelText('Match operator'), { target: { value: 'OR' } });
    const call = onChange.mock.calls.find((c) => c[0]?.where);
    expect(call).toBeTruthy();
    expect((call![0].where.children[0] as QBWhereLeafWithConn).matchOp).toBe('OR');
  });
});
