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

// MeasureResultView.tsx — measure query result view (Table + Chart).
// Ported from .handoff-import/banyandb/project/measure-results.jsx.
// Table is time-major + GROUP BY aggregates; Chart is a hand-rolled SVG
// time-series (auto by hasAgg). Trace tab surfaces the query trace when on.

import React, { useMemo } from 'react';
import type { QueryResponse } from 'canopy-shared';
import type { QBBuilderState } from '../bydbql.js';

interface Props {
  readonly response: QueryResponse;
  readonly state: QBBuilderState;
  readonly showTrace: boolean;
  readonly setShowTrace: (v: boolean) => void;
}

type Elem = Record<string, unknown>;

export function MeasureResultView({ response, state, showTrace, setShowTrace }: Props) {
  const elements = (response.elements ?? []) as readonly Elem[];
  const hasAgg = (state.select ?? []).some((r) => r.fn);
  const [view, setView] = React.useState<'table' | 'chart'>(hasAgg ? 'chart' : 'table');
  React.useEffect(() => { setView(hasAgg ? 'chart' : 'table'); }, [hasAgg]);

  // Determine column set from projection + select
  const columns = useMemo(() => {
    const tagCols = state.projection;
    const fieldCols = (state.select ?? []).map((r) => r.fn ? `${r.fn.toLowerCase()}(${r.field})` : r.field);
    return [...tagCols, ...fieldCols];
  }, [state.projection, state.select]);

  return (
    <div className="rv-root">
      <div className="rv-tabs">
        <button type="button" className={'rv-tab' + (view === 'table' ? ' is-on' : '')} onClick={() => setView('table')}>Table</button>
        <button type="button" className={'rv-tab' + (view === 'chart' ? ' is-on' : '')} onClick={() => setView('chart')}>Chart</button>
        {state.trace && (
          <button type="button" className={'rv-tab' + (showTrace ? ' is-on' : '')} onClick={() => setShowTrace(!showTrace)}>Trace</button>
        )}
      </div>
      {showTrace && state.trace ? (
        <TracePanel response={response} />
      ) : view === 'table' ? (
        <TableView elements={elements} columns={columns} />
      ) : (
        <ChartView elements={elements} columns={columns} />
      )}
    </div>
  );
}

function TableView({ elements, columns }: { elements: readonly Elem[]; columns: readonly string[] }) {
  if (elements.length === 0) return <div className="rv-empty">No rows.</div>;
  return (
    <div className="rv-table-wrap">
      <table className="rv-table">
        <thead>
          <tr>
            {columns.map((c) => <th key={c}>{c}</th>)}
          </tr>
        </thead>
        <tbody>
          {elements.map((e, i) => (
            <tr key={i}>
              {columns.map((c) => <td key={c}>{String(e[c] ?? '')}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ChartView({ elements, columns }: { elements: readonly Elem[]; columns: readonly string[] }) {
  // Render a simple SVG time-series: x = index, y = first numeric column
  const numCol = columns.find((c) => elements.some((e) => typeof e[c] === 'number')) ?? columns[columns.length - 1];
  const xs = elements.map((_, i) => i);
  const ys = elements.map((e) => Number(e[numCol] ?? 0));
  if (ys.length === 0) return <div className="rv-empty">No data.</div>;
  const min = Math.min(...ys);
  const max = Math.max(...ys);
  const range = max - min || 1;
  const W = 800, H = 240;
  const pad = 24;
  const xStep = (W - pad * 2) / Math.max(1, xs.length - 1);
  const points = ys.map((y, i) => {
    const px = pad + i * xStep;
    const py = H - pad - ((y - min) / range) * (H - pad * 2);
    return `${px.toFixed(1)},${py.toFixed(1)}`;
  }).join(' ');
  return (
    <div className="rv-chart-wrap">
      <svg className="rv-chart" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" role="img" aria-label={`${numCol} over time`}>
        <line x1={pad} y1={H - pad} x2={W - pad} y2={H - pad} className="rv-axis" />
        <line x1={pad} y1={pad} x2={pad} y2={H - pad} className="rv-axis" />
        <polyline points={points} className="rv-line" />
      </svg>
      <div className="rv-chart-meta">{numCol}: min {min.toFixed(2)} · max {max.toFixed(2)}</div>
    </div>
  );
}

function TracePanel({ response }: { response: QueryResponse }) {
  const trace = (response as unknown as { trace?: unknown }).trace;
  return (
    <div className="rv-trace">
      <pre>{JSON.stringify(trace ?? {}, null, 2)}</pre>
    </div>
  );
}