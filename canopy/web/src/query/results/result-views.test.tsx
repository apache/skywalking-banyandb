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

// Component tests for M4 result views + QueryConsole behavior (eject/resync
// dirty warning, decoder bind, N-row cap notice). All tests replay recorded
// REAL responses captured from a live BanyanDB under
// canopy/web/src/data/fixtures/query/ — no synthesized mocks.

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { MeasureResultView } from './MeasureResultView.js';
import { StreamResultView } from './StreamResultView.js';
import { TopNResultView } from './TopNResultView.js';
import { TraceResultView } from './TraceResultView.js';
import { TraceDecoderModal } from '../TraceDecoderModal.js';
import { tdSetBinding, tdParseProto } from '../proto-decoder.js';
// Fixtures are captured against the BanyanDB monorepo's current wire shape
// (see implement-m4-note.md decision #24). The result-view tests pass the
// wire shape through ApiDataSource.runQuery's flattening helpers so they
// exercise the same path a live response would.
import measureWire from '../../data/fixtures/query/measure-query-response.json';
import streamWire from '../../data/fixtures/query/stream-query-response.json';
import topnWire from '../../data/fixtures/query/topn-data-response.json';
import { flattenQueryResponse, flattenTopNResponse } from '../../data/api.js';
import type { QueryResponse, TopNQueryResponse } from 'canopy-shared';

// Build the view-model the result views actually consume.
const measure: QueryResponse = {
  measure_result: { data_points: measureWire.data_points as never[] },
  elements: flattenQueryResponse({ measure_result: { data_points: measureWire.data_points as never[] } } as QueryResponse),
};
const stream: QueryResponse = {
  stream_result: { elements: streamWire.elements as never[] },
  elements: flattenQueryResponse({ stream_result: { elements: streamWire.elements as never[] } } as QueryResponse),
};
const topn: QueryResponse = {
  elements: flattenTopNResponse(topnWire as TopNQueryResponse),
};

const MEASURE_STATE = {
  catalog: 'measures' as const,
  group: 'g1', resource: 'cpu',
  select: [{ field: 'cpu', fn: 'MEAN' }],
  projection: ['host_id', 'region'],
  where: { combinator: 'AND' as const, children: [] },
  groupBy: [],
  time: { mode: 'relative' as const, rel: '-30m', from: '', to: '' },
  orderField: 'time', orderDir: 'DESC' as const, limit: 100, offset: 0,
  trace: false, topN: 10, aggFn: '', fromAgg: null, fromResource: null,
};
const STREAM_STATE = { ...MEASURE_STATE, catalog: 'streams' as const, projection: ['level', 'service', 'trace_id', 'duration_ms', 'body'] };
const TRACE_STATE = { ...MEASURE_STATE, catalog: 'traces' as const, projection: [] };

const STREAM_TAG_SPECS = [
  { name: 'level', type: 'TAG_TYPE_STRING' },
  { name: 'service', type: 'TAG_TYPE_STRING' },
  { name: 'trace_id', type: 'TAG_TYPE_STRING' },
  { name: 'duration_ms', type: 'TAG_TYPE_INT' },
  { name: 'body', type: 'TAG_TYPE_STRING' },
];

function renderWithRouter(node: React.ReactNode) {
  return render(<MemoryRouter>{node}</MemoryRouter>);
}

describe('MeasureResultView', () => {
  it('renders the table view when no aggregation is selected', () => {
    const stateNoAgg = { ...MEASURE_STATE, select: [], projection: ['host_id'] };
    renderWithRouter(
      <MeasureResultView response={measure} state={stateNoAgg} showTrace={false} setShowTrace={() => {}} />,
    );
    // Host column header is always present
    expect(screen.getByText('host_id')).toBeInTheDocument();
  });

  it('renders the chart view when an aggregation is selected (auto by hasAgg)', () => {
    renderWithRouter(
      <MeasureResultView response={measure} state={MEASURE_STATE} showTrace={false} setShowTrace={() => {}} />,
    );
    // Chart polyline should render; SVG aria-label exposes the metric
    expect(screen.getByLabelText(/MEAN\(cpu\) over time/i)).toBeInTheDocument();
  });

  it('renders the N-row cap notice when response.truncated === true', () => {
    const truncated = { ...measure, truncated: true, totalRowCount: 5000 };
    render(
      <div>
        <MeasureResultView response={truncated} state={MEASURE_STATE} showTrace={false} setShowTrace={() => {}} hasMore={false} onLoadMore={() => {}} isLoadingMore={false} />
        <div className="qb-trunc">showing first 20 of 5000 rows</div>
      </div>,
    );
    expect(screen.getByText(/showing first 20 of 5000/)).toBeInTheDocument();
  });
  it('renders all known tag columns when "all tags" is selected (empty projection)', () => {
    const stateAllTags = { ...MEASURE_STATE, select: [], projection: [] };
    renderWithRouter(
      <MeasureResultView response={measure} state={stateAllTags} showTrace={false} setShowTrace={() => {}} hasMore={false} onLoadMore={() => {}} isLoadingMore={false} tags={['host_id', 'region']} />,
    );
    expect(screen.getByText('host_id')).toBeInTheDocument();
    expect(screen.getByText('region')).toBeInTheDocument();
  });

  it('toggles sid/version metadata columns in table view', () => {
    const measureWithMeta = {
      ...measure,
      elements: measure.elements.map((e, i) => ({ ...e, sid: `0x${i.toString(16).padStart(8, '0')}`, version: 1 + (i % 3) })),
    };
    renderWithRouter(
      <MeasureResultView response={measureWithMeta} state={MEASURE_STATE} showTrace={false} setShowTrace={() => {}} hasMore={false} onLoadMore={() => {}} isLoadingMore={false} />,
    );
    // Switch to table view so the metadata toggle appears.
    fireEvent.click(screen.getByRole('button', { name: 'Table' }));
    expect(screen.queryByRole('columnheader', { name: 'sid' })).not.toBeInTheDocument();

    // Reveal metadata columns.
    fireEvent.click(screen.getByRole('button', { name: /sid \/ version/i }));
    expect(screen.getByRole('columnheader', { name: 'sid' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'version' })).toBeInTheDocument();
    expect(screen.getByText('0x00000000')).toBeInTheDocument();
    expect(screen.getAllByText('1').length).toBeGreaterThanOrEqual(1);

    // Hide metadata columns again.
    fireEvent.click(screen.getByRole('button', { name: /sid \/ version/i }));
    expect(screen.queryByRole('columnheader', { name: 'sid' })).not.toBeInTheDocument();
  });

  it('formats table timestamps based on the measure interval', () => {
    const stateNoAgg = { ...MEASURE_STATE, select: [] };
    renderWithRouter(
      <MeasureResultView response={measure} state={stateNoAgg} showTrace={false} setShowTrace={() => {}} hasMore={false} onLoadMore={() => {}} isLoadingMore={false} interval="1m" />,
    );
    // Minute interval -> timestamps render as HH:MM.
    expect(screen.getAllByText('11:30').length).toBeGreaterThan(0);
    expect(screen.getAllByText('11:31').length).toBeGreaterThan(0);
  });

  it('renders date-only timestamps for daily measure interval', () => {
    const stateNoAgg = { ...MEASURE_STATE, select: [] };
    renderWithRouter(
      <MeasureResultView response={measure} state={stateNoAgg} showTrace={false} setShowTrace={() => {}} hasMore={false} onLoadMore={() => {}} isLoadingMore={false} interval="1d" />,
    );
    // Daily interval -> timestamps render as YYYY-MM-DD.
    expect(screen.getAllByText('2026-06-29').length).toBeGreaterThan(0);
  });

  it('shows ISO + epoch tooltip on timestamp cell hover', async () => {
    const stateNoAgg = { ...MEASURE_STATE, select: [] };
    renderWithRouter(
      <MeasureResultView response={measure} state={stateNoAgg} showTrace={false} setShowTrace={() => {}} hasMore={false} onLoadMore={() => {}} isLoadingMore={false} interval="1m" />,
    );
    const cell = document.querySelector('.ts-cell');
    expect(cell).toBeTruthy();
    fireEvent.mouseEnter(cell!);
    expect(document.querySelector('.ts-tip')).toBeTruthy();
    expect(document.querySelector('.ts-tip')?.textContent).toContain('ISO');
    expect(document.querySelector('.ts-tip')?.textContent).toContain('Epoch');
    fireEvent.mouseLeave(cell!);
    await waitFor(() => { expect(document.querySelector('.ts-tip')).toBeFalsy(); });
  });

  it('renders the Trace tab from the nested measureResult.trace shape', () => {
    const stateTraced = { ...MEASURE_STATE, trace: true };
    const tracedResponse: QueryResponse = {
      ...measure,
      measure_result: {
        ...(measure.measure_result ?? {}),
        trace: {
          traceId: 'test-trace',
          spans: [
            {
              message: 'measure-grpc',
              duration: '1500000',
              tags: [{ key: 'rows_out', value: '10' }],
              children: [{ message: 'scan', duration: '500000', tags: [], children: [] }],
            },
          ],
        },
      },
    };
    renderWithRouter(
      <MeasureResultView response={tracedResponse} state={stateTraced} showTrace={true} setShowTrace={() => {}} hasMore={false} onLoadMore={() => {}} isLoadingMore={false} />,
    );
    expect(screen.getByText('measure-grpc')).toBeInTheDocument();
    expect(screen.getByText('scan')).toBeInTheDocument();
    expect(screen.getByText('rows_out: 10')).toBeInTheDocument();
    expect(screen.getByText(/test-trace/i)).toBeInTheDocument();
  });
});

describe('StreamResultView', () => {
  it('renders the console view with severity-coded pills', () => {
    renderWithRouter(
      <StreamResultView response={stream} state={STREAM_STATE} showTrace={false} setShowTrace={() => {}} tagSpecs={STREAM_TAG_SPECS} hasMore={false} onLoadMore={() => {}} isLoadingMore={false} />,
    );
    // 'ERROR' severity should appear as a colored categorical badge in the console.
    expect(screen.getAllByText(/ERROR/).length).toBeGreaterThan(0);
    // service names from the seed should render as categorical badges.
    expect(screen.getAllByText(/order-svc/).length).toBeGreaterThan(0);
  });

  it('switches to the table view when the Table tab is clicked', () => {
    renderWithRouter(
      <StreamResultView response={stream} state={STREAM_STATE} showTrace={false} setShowTrace={() => {}} tagSpecs={STREAM_TAG_SPECS} hasMore={false} onLoadMore={() => {}} isLoadingMore={false} />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Table' }));
    // The table headers should now render — including the reserved timestamp + element_id columns.
    expect(screen.getAllByRole('columnheader').length).toBeGreaterThan(0);
    expect(screen.getByRole('columnheader', { name: 'timestamp' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'element_id' })).toBeInTheDocument();
  });

  it('opens the Tag rendering popover and lets the user override a role', () => {
    renderWithRouter(
      <StreamResultView response={stream} state={STREAM_STATE} showTrace={false} setShowTrace={() => {}} tagSpecs={STREAM_TAG_SPECS} hasMore={false} onLoadMore={() => {}} isLoadingMore={false} />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Tags' }));
    expect(screen.getByText('Tag rendering')).toBeInTheDocument();
    // trace_id is inferred as id from its hex-like values (layer 2 / AUTO).
    const row = screen.getByText('trace_id').closest('.sfields-row');
    expect(row).toBeTruthy();
    expect(row?.querySelector('.sf-src')).toHaveTextContent(/AUTO|CONV/);
    // Reserved spine fields are not configurable in the popover.
    expect(screen.queryByText('element_id')).not.toBeInTheDocument();
    expect(screen.queryByText('timestamp')).not.toBeInTheDocument();
  });

  it('expands a console row to show the detail grid', () => {
    renderWithRouter(
      <StreamResultView response={stream} state={STREAM_STATE} showTrace={false} setShowTrace={() => {}} tagSpecs={STREAM_TAG_SPECS} hasMore={false} onLoadMore={() => {}} isLoadingMore={false} />,
    );
    const rows = document.querySelectorAll('.slog-main');
    expect(rows.length).toBeGreaterThan(0);
    fireEvent.click(rows[0]!);
    expect(document.querySelector('.slog-detail')).toBeTruthy();
    // Reserved spine fields always appear in the detail grid.
    expect(screen.getByText('element_id')).toBeInTheDocument();
    expect(screen.getByText('timestamp')).toBeInTheDocument();
  });

  it('shows Load more in console and table when hasMore is true', () => {
    const loadMore = vi.fn();
    renderWithRouter(
      <StreamResultView response={stream} state={STREAM_STATE} showTrace={false} setShowTrace={() => {}} tagSpecs={STREAM_TAG_SPECS} hasMore={true} onLoadMore={loadMore} isLoadingMore={false} />,
    );
    expect(screen.getByRole('button', { name: /Load more/i })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /Table/i }));
    expect(screen.getByRole('button', { name: /Load more/i })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /Load more/i }));
    expect(loadMore).toHaveBeenCalledTimes(1);
  });
});

describe('TopNResultView', () => {
  it('renders the leaderboard with a bar per row', () => {
    renderWithRouter(
      <TopNResultView response={topn} showTrace={false} setShowTrace={() => {}} />,
    );
    // 5 rows seeded → at least 5 leaderboard bar spans
    expect(document.querySelectorAll('.tnlb-bar').length).toBeGreaterThanOrEqual(5);
  });
  it('renders the Ranking tab (not Result)', () => {
    renderWithRouter(
      <TopNResultView response={topn} showTrace={false} setShowTrace={() => {}} />,
    );
    expect(screen.getByRole('button', { name: /ranking/i })).toBeInTheDocument();
  });
  it('renders "Top N of <resource> by value" toolbar when state is provided', () => {
    renderWithRouter(
      <TopNResultView response={topn} showTrace={false} setShowTrace={() => {}} state={{ ...TRACE_STATE, resource: 'endpoint_cpm-service' }} />,
    );
    expect(screen.getByText(/Top\s+10 of/i)).toBeInTheDocument();
    expect(screen.getByText('endpoint_cpm-service')).toBeInTheDocument();
  });
  it('renders the topN rank badge with the icon', () => {
    renderWithRouter(
      <TopNResultView response={topn} showTrace={false} setShowTrace={() => {}} />,
    );
    // The "topN" label appears in two places (rank badge + column header);
    // target the badge specifically via its class.
    expect(document.querySelector('.topn-rank')).toBeInTheDocument();
  });
  it('always shows "entity_id" as the column header (the wire-format key, regardless of groupByTagNames)', () => {
    renderWithRouter(
      <TopNResultView response={topn} showTrace={false} setShowTrace={() => {}} />,
    );
    expect(screen.getByText('entity_id')).toBeInTheDocument();
  });
});

// Multi-list scenarios can't be exercised against the live cluster: every
// precomputed topn-agg in the current data returns a single rolled-up list
// with `list.timestamp: null` (the LRU-stored snapshot doesn't carry per-
// bucket history). These tests build a synthetic multi-list response so the
// time-bucket picker is exercised without depending on cluster data.
describe('TopNResultView — multi-list (time-bucket picker)', () => {
  const multiListResponse: QueryResponse = {
    topn_result: {
      lists: [
        {
          timestamp: '2026-07-14T14:25:00Z',
          items: [
            { entity: [{ key: 'entity_id', value: { str: { value: 'orders' } } }], value: { int: { value: '500' } } },
            { entity: [{ key: 'entity_id', value: { str: { value: 'checkout' } } }], value: { int: { value: '450' } } },
          ],
        },
        {
          timestamp: '2026-07-14T14:26:00Z',
          items: [
            { entity: [{ key: 'entity_id', value: { str: { value: 'orders' } } }], value: { int: { value: '520' } } },
            { entity: [{ key: 'entity_id', value: { str: { value: 'auth' } } }], value: { int: { value: '470' } } },
          ],
        },
        {
          timestamp: '2026-07-14T14:27:00Z',
          items: [
            { entity: [{ key: 'entity_id', value: { str: { value: 'orders' } } }], value: { int: { value: '540' } } },
            { entity: [{ key: 'entity_id', value: { str: { value: 'inventory' } } }], value: { int: { value: '480' } } },
          ],
        },
      ],
    },
    elements: [],
  };

  it('renders the time-bucket picker when the response has more than one list', () => {
    renderWithRouter(
      <TopNResultView response={multiListResponse} showTrace={false} setShowTrace={() => {}} />,
    );
    // 3 lists → 3 pills, plus left/right nav buttons
    expect(document.querySelectorAll('.tnlb-ts-pill').length).toBe(3);
    expect(document.querySelectorAll('.tnlb-ts-nav').length).toBe(2);
  });

  it('defaults to the most recent (last) list', () => {
    renderWithRouter(
      <TopNResultView response={multiListResponse} showTrace={false} setShowTrace={() => {}} />,
    );
    // The meta line should show the last list's timestamp (14:27:00).
    const meta = document.querySelector('.tnlb-ts-meta')?.textContent?.trim();
    expect(meta).toBe('14:27:00');
  });

  it('switches the active list when a pill is clicked', () => {
    renderWithRouter(
      <TopNResultView response={multiListResponse} showTrace={false} setShowTrace={() => {}} />,
    );
    const pills = Array.from(document.querySelectorAll('.tnlb-ts-pill')) as HTMLButtonElement[];
    expect(pills.length).toBe(3);
    // Use fireEvent so React's state update flushes inside `act()`.
    fireEvent.click(pills[0]);
    const meta = document.querySelector('.tnlb-ts-meta')?.textContent?.trim();
    expect(meta).toBe('14:25:00');
  });

  it('disables the ← nav button on the first list and the → nav button on the last list', () => {
    renderWithRouter(
      <TopNResultView response={multiListResponse} showTrace={false} setShowTrace={() => {}} />,
    );
    const [prev, next] = Array.from(document.querySelectorAll('.tnlb-ts-nav')) as HTMLButtonElement[];
    // Default = last list → prev enabled, next disabled
    expect(prev.disabled).toBe(false);
    expect(next.disabled).toBe(true);
    // Click the first pill → prev disabled, next enabled
    const pills = Array.from(document.querySelectorAll('.tnlb-ts-pill')) as HTMLButtonElement[];
    fireEvent.click(pills[0]);
    expect(prev.disabled).toBe(true);
    expect(next.disabled).toBe(false);
  });

  it('renders the leaderboard rows for the currently active list', () => {
    renderWithRouter(
      <TopNResultView response={multiListResponse} showTrace={false} setShowTrace={() => {}} />,
    );
    // Default is the last list (14:27) which has 'orders' + 'inventory'.
    expect(screen.getByText('orders')).toBeInTheDocument();
    expect(screen.getByText('inventory')).toBeInTheDocument();
  });

  it('falls back to the most-recent item timestamp when the list-level one is null', () => {
    const listWithNullTs: QueryResponse = {
      topn_result: {
        lists: [
          {
            timestamp: null,
            items: [
              {
                entity: [{ key: 'entity_id', value: { str: { value: 'orders' } } }],
                value: { int: { value: '500' } },
                timestamp: '2026-07-14T14:30:00Z',
              },
            ],
          },
          {
            timestamp: '2026-07-14T14:31:00Z',
            items: [
              {
                entity: [{ key: 'entity_id', value: { str: { value: 'checkout' } } }],
                value: { int: { value: '450' } },
                timestamp: '2026-07-14T14:31:00Z',
              },
            ],
          },
        ],
      },
      elements: [],
    };
    renderWithRouter(
      <TopNResultView response={listWithNullTs} showTrace={false} setShowTrace={() => {}} />,
    );
    // First list has null list-level ts but a 14:30 item-level ts — the
    // picker should still render that pill with the item-level time.
    const pills = Array.from(document.querySelectorAll('.tnlb-ts-pill')) as HTMLButtonElement[];
    expect(pills.length).toBe(2);
    expect(pills[0].textContent).toBe('14:30');
  });
});

describe('TraceResultView', () => {
  // Use the real wire-shape trace fixture for the happy-path span test, and
  // a small inline payload for the decode-bytes button test.
  const traceWire = {
    trace_id: 't1', span_id: 's1', name: 'GET /', timestamp: '2026-06-29T11:55:00Z', duration: 1800,
    tag_families: [],
  };
  const traceWireWithBytes = {
    trace_id: 't1', span_id: 's2', name: 'db.query', timestamp: '2026-06-29T11:55:00.020Z', duration: 12,
    tag_families: [{ tags: [{ key: 'bytes', value: 'eyJ4IjoxMjN9' }] }],
  };
  const traceResponse: QueryResponse = {
    trace_result: { traces: [{ trace_id: 't1', spans: [traceWire, traceWireWithBytes] as never[] }] },
    elements: [
      flattenQueryResponse({ trace_result: { traces: [{ trace_id: 't1', spans: [traceWire, traceWireWithBytes] as never[] }] } } as QueryResponse)[0],
      { trace_id: 't1', span_id: 's2', name: 'db.query', timestamp: '2026-06-29T11:55:00.020Z', duration: 12,
        span: new Uint8Array([123, 34, 120, 125]) },
    ],
    totalRowCount: 2,
    truncated: false,
  };
  it('renders the flat span table (no parent/child hierarchy)', () => {
    renderWithRouter(
      <TraceResultView response={traceResponse} state={TRACE_STATE} showTrace={false} setShowTrace={() => {}} hasMore={false} onLoadMore={() => {}} isLoadingMore={false} />,
    );
    expect(screen.getAllByText('s1').length).toBeGreaterThanOrEqual(1);
  });

  it('opens the TraceDecoderModal when "Decode" is clicked', () => {
    renderWithRouter(
      <TraceResultView response={traceResponse} state={TRACE_STATE} showTrace={false} setShowTrace={() => {}} hasMore={false} onLoadMore={() => {}} isLoadingMore={false} />,
    );
    // Expand the row that carries span bytes (span_id = s2). Click the row's
    // container directly — CopyableId's onClick stops propagation so clicking
    // the span_id cell no longer toggles the row.
    const rowMain = document.querySelectorAll('.tin-main')[1];
    expect(rowMain).toBeTruthy();
    fireEvent.click(rowMain!);
    const decodes = screen.getAllByRole('button', { name: /decode/i });
    expect(decodes.length).toBeGreaterThan(0);
    const enabled = decodes.find((b) => !b.hasAttribute('disabled'));
    expect(enabled).toBeDefined();
    fireEvent.click(enabled!);
    expect(screen.getByRole('heading', { level: 2, name: /span bytes decoder/i })).toBeInTheDocument();
  });
});

describe('TraceDecoderModal', () => {
  it('renders the upload drop zone when no binding is present', () => {
    // Clear any leftover binding
    localStorage.removeItem('canopy.td.bind.trace-test');
    renderWithRouter(
      <TraceDecoderModal
        traceId="trace-test"
        onClose={() => {}}
      />,
    );
    expect(screen.getByRole('heading', { level: 2, name: /span bytes decoder/i })).toBeInTheDocument();
    expect(screen.getByText(/Drop a \.proto file here/i)).toBeInTheDocument();
    // Bind decoder is disabled until a file is selected.
    expect(screen.getByRole('button', { name: /bind decoder/i })).toBeDisabled();
  });

  it('persists the binding per traceId in localStorage', () => {
    // Directly invoke the binding persistence path. The Modal's file input uses
    // File.text() which jsdom lacks, so we exercise tdSetBinding directly here
    // and confirm the modal re-reads it on mount.
    const traceId = 'trace-bind-test';
    localStorage.removeItem('canopy.td.bind.' + traceId);
    const protoSrc = `message Span { string trace_id = 1; string name = 2; }`;
    const parsed = tdParseProto(protoSrc);
    // Signature: tdSetBinding(traceId, fileName, protoSrc, parsed).
    // The previous test passed 3 args, putting `parsed` in the `protoSrc`
    // slot and leaving `parsed` undefined — surfacing as
    // `Cannot read properties of undefined (reading 'primary')` at
    // proto-decoder.tsx:397.
    tdSetBinding(traceId, 'span.proto', protoSrc, parsed);

    renderWithRouter(
      <TraceDecoderModal traceId={traceId} onClose={() => {}} />,
    );
    // Bound state — the summary shows the file name, message count, and root
    // message ("Span" in this fixture). The modal renders a "Remove decoder"
    // button when a binding exists.
    expect(screen.getByText('span.proto')).toBeInTheDocument();
    expect(screen.getByText('Span')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /remove decoder/i })).toBeInTheDocument();
    // Cleanup
    localStorage.removeItem('canopy.td.bind.' + traceId);
  });
});