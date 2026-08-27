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
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { useState } from 'react';
import { MemoryRouter } from 'react-router';
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

function staleMeasureState(): QBBuilderState {
  return {
    ...stateWithLeaf({ tag: 'old_tag', op: 'BINARY_OP_EQ', value: 'old-value' }),
    catalog: 'measures',
    resource: 'old_measure',
    select: [{ field: 'old_field', fn: 'MEAN' }],
    projection: ['old_tag'],
    groupBy: ['old_tag'],
    fromResource: 'g1/old_measure',
  };
}

/** Mirrors #14043: SELECT entity_id carried from a prior resource into Trace zipkin_span. */
function staleTraceState(): QBBuilderState {
  return {
    ...stateWithLeaf({ tag: 'entity_id', op: 'BINARY_OP_EQ', value: 'svc-1' }),
    catalog: 'traces',
    group: 'sw_zipkin',
    resource: 'old_span',
    select: [],
    projection: ['entity_id'],
    groupBy: [],
    orderField: 'time',
    fromResource: 'sw_zipkin/old_span',
  };
}

function StatefulQueryBuilder({
  autoPickOnGroupChange,
  initialState = staleMeasureState(),
}: {
  readonly autoPickOnGroupChange?: boolean;
  readonly initialState?: QBBuilderState;
}) {
  const [state, setState] = useState(initialState);
  const isTrace = state.catalog === 'traces';
  const tags = isTrace
    ? (state.resource === 'zipkin_span' ? ['trace_id', 'span_id', 'name'] : ['entity_id', 'trace_id'])
    : (state.resource === 'new_measure' ? ['new_tag'] : ['old_tag']);
  const fields = isTrace
    ? []
    : (state.resource === 'new_measure' ? ['new_field'] : ['old_field']);
  const resourceNames = isTrace
    ? ['old_span', 'zipkin_span']
    : (state.group === 'g2' ? ['new_measure'] : ['old_measure', 'new_measure']);
  const groupNames = isTrace ? ['sw_zipkin'] : ['g1', 'g2'];
  const onChange = (patch: Partial<QBBuilderState>) => {
    setState((current) => {
      const next = { ...current, ...patch };
      return autoPickOnGroupChange && next.group === 'g2' && !next.resource
        ? { ...next, resource: 'new_measure' }
        : next;
    });
  };

  return (
    <MemoryRouter>
      <QueryBuilder
        state={state}
        onChange={onChange}
        tags={tags}
        fields={fields}
        groupNames={groupNames}
        resourceNames={resourceNames}
        topnAggNames={[]}
        groups={[]}
        groupResources={new Map()}
        groupTopnAggs={new Map()}
        onPickResource={vi.fn()}
        isRunning={false}
        onEjectToCode={vi.fn()}
        onRun={vi.fn()}
        hasRun={false}
        compact={false}
        setCompact={vi.fn()}
        openSection={null}
        setOpenSection={vi.fn()}
      />
    </MemoryRouter>
  );
}

function queryPreview(container: HTMLElement): HTMLElement {
  const preview = container.querySelector('.qb-gen-line');
  if (!preview) throw new Error('Generated query preview was not rendered');
  return preview as HTMLElement;
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

describe('QueryBuilder resource changes', () => {
  it('clears dependent clauses when the resource dropdown selects a new schema', async () => {
    const { container } = render(<StatefulQueryBuilder />);

    fireEvent.change(screen.getByLabelText('Resource'), { target: { value: 'new_measure' } });

    await waitFor(() => expect(queryPreview(container)).toHaveTextContent('FROM MEASURE new_measure'));
    const query = queryPreview(container).getAttribute('title') ?? '';
    expect(query).toContain('SELECT new_tag');
    expect(query).not.toContain('old_tag');
    expect(query).not.toContain('old_field');
    expect(query).not.toContain('WHERE');
    expect(query).not.toContain('GROUP BY');
  });

  it('does not carry clauses into an auto-selected resource after changing groups', async () => {
    const { container } = render(<StatefulQueryBuilder autoPickOnGroupChange />);

    fireEvent.change(screen.getByLabelText('Group'), { target: { value: 'g2' } });

    await waitFor(() => expect(screen.getByLabelText('Resource')).toHaveValue('new_measure'));
    const query = queryPreview(container).getAttribute('title') ?? '';
    expect(query).toContain('SELECT new_tag');
    expect(query).not.toContain('old_tag');
    expect(query).not.toContain('old_field');
    expect(query).not.toContain('WHERE');
    expect(query).not.toContain('GROUP BY');
  });

  it('drops a stale SELECT tag when switching Trace resources (#14043)', async () => {
    const { container } = render(<StatefulQueryBuilder initialState={staleTraceState()} />);

    fireEvent.change(screen.getByLabelText('Resource'), { target: { value: 'zipkin_span' } });

    await waitFor(() => expect(queryPreview(container)).toHaveTextContent('FROM TRACE zipkin_span'));
    const query = queryPreview(container).getAttribute('title') ?? '';
    // Empty projection on Trace expands to the new resource's tag list (not SELECT *).
    expect(query).toContain('SELECT trace_id, span_id, name');
    expect(query).not.toContain('entity_id');
    expect(query).toContain('WHERE trace_id');
    expect(query).not.toContain('ORDER BY');
  });
});

// ORDER BY resolves to an index-rule NAME server-side, so the dropdown is
// confined to the rules bound to the current resource once they load.
describe('QueryBuilder ORDER BY — index-rule-confined options', () => {
  const orderProps = {
    tags: ['name', 'message', 'trace_id'],
    orderableRules: ['name'],
  };

  it('offers only time + bound index rules once loaded', () => {
    const s = stateWithLeaf({ tag: 'name', op: 'BINARY_OP_EQ', value: 'x' });
    const props: QueryBuilderProps = {
      state: s, onChange: vi.fn(), ...orderProps,
      fields: [], groupNames: ['g1'], resourceNames: ['logs'], topnAggNames: [],
      groups: [], groupResources: new Map(), groupTopnAggs: new Map(),
      onPickResource: vi.fn(), isRunning: false, onEjectToCode: vi.fn(), onRun: vi.fn(),
    };
    render(<MemoryRouter><QueryBuilder {...props} /></MemoryRouter>);
    const options = (screen.getByLabelText('Order field') as HTMLSelectElement).querySelectorAll('option');
    expect([...options].map((o) => o.value)).toEqual(['', 'time', 'name']);
  });

  it('keeps a manually cleared orderField — empty means no ORDER BY', () => {
    const s = { ...stateWithLeaf({ tag: 'name', op: 'BINARY_OP_EQ', value: 'x' }), orderField: '' };
    const onChange = vi.fn();
    const props: QueryBuilderProps = {
      state: s, onChange, ...orderProps,
      fields: [], groupNames: ['g1'], resourceNames: ['logs'], topnAggNames: [],
      groups: [], groupResources: new Map(), groupTopnAggs: new Map(),
      onPickResource: vi.fn(), isRunning: false, onEjectToCode: vi.fn(), onRun: vi.fn(),
    };
    render(<MemoryRouter><QueryBuilder {...props} /></MemoryRouter>);
    expect(onChange).not.toHaveBeenCalled();
    expect((screen.getByLabelText('Order field') as HTMLSelectElement).value).toBe('');
  });

  it('resets a stale orderField to a bound rule', () => {
    const s = { ...stateWithLeaf({ tag: 'name', op: 'BINARY_OP_EQ', value: 'x' }), orderField: 'trace_id' };
    const onChange = vi.fn();
    const props: QueryBuilderProps = {
      state: s, onChange, ...orderProps,
      fields: [], groupNames: ['g1'], resourceNames: ['logs'], topnAggNames: [],
      groups: [], groupResources: new Map(), groupTopnAggs: new Map(),
      onPickResource: vi.fn(), isRunning: false, onEjectToCode: vi.fn(), onRun: vi.fn(),
    };
    render(<MemoryRouter><QueryBuilder {...props} /></MemoryRouter>);
    expect(onChange).toHaveBeenCalledWith({ orderField: 'time' });
  });

  it('drops the time alias for traces (ORDER BY stays optional), defaulting to the timestampTagName rule', () => {
    const s: QBBuilderState = {
      ...stateWithLeaf({ tag: 'trace_id', op: 'BINARY_OP_EQ', value: '' }),
      catalog: 'traces', resource: 'segment', orderField: 'time',
    };
    const onChange = vi.fn();
    const props: QueryBuilderProps = {
      state: s, onChange,
      tags: ['trace_id', 'start_time', 'latency'], orderableRules: ['start_time', 'latency'],
      traceTimestampTag: 'start_time',
      fields: [], groupNames: ['sw_trace'], resourceNames: ['segment'], topnAggNames: [],
      groups: [], groupResources: new Map(), groupTopnAggs: new Map(),
      onPickResource: vi.fn(), isRunning: false, onEjectToCode: vi.fn(), onRun: vi.fn(),
    };
    render(<MemoryRouter><QueryBuilder {...props} /></MemoryRouter>);
    // 'time' is not a valid trace order on the distributed path — reset to
    // the schema's timestampTagName (which has a bound rule).
    expect(onChange).toHaveBeenCalledWith({ orderField: 'start_time' });
    const options = (screen.getByLabelText('Order field') as HTMLSelectElement).querySelectorAll('option');
    expect([...options].map((o) => o.value)).toEqual(['', 'start_time', 'latency']);
    // ORDER BY is optional for traces too — a trace_id filter makes it
    // unnecessary, and QueryConsole clears the order field when one is added.
    const orderRow = screen.getByText('ORDER BY').closest('.qb-section');
    expect(orderRow?.textContent ?? '').toContain('optional');
  });
});
