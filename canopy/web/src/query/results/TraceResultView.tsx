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

// TraceResultView.tsx — trace query result view (span table + inspector + bytes).
// Ported from .handoff-import/banyandb/project/trace-results.jsx.
// Flat span table — QueryResponse has no parent/child hierarchy — with
// expandable span inspector + opaque-bytes inspector (TraceDecoderModal).

import React, { useMemo, useState } from 'react';
import type { QueryResponse } from 'canopy-shared';
import type { QBBuilderState } from '../bydbql.js';
import { TraceDecoderModal } from '../TraceDecoderModal.js';
import { srRenderValue } from '../role-infer.js';
import { ResultPanel } from './ResultPanel.js';
import { ResultEmpty } from './ResultEmpty.js';
import { TraceView, TraceDisabled } from './TraceView.js';

type Elem = Record<string, unknown>;

interface Props {
  readonly response: QueryResponse;
  readonly state: QBBuilderState;
  readonly showTrace: boolean;
  readonly setShowTrace: (v: boolean) => void;
  readonly execMs?: number;
}

export function TraceResultView({ response, state, showTrace, setShowTrace, execMs }: Props) {
  const elements = (response.elements ?? []) as readonly Elem[];
  const [view, setView] = useState<'spans' | 'grouped' | 'json'>('spans');
  const [expanded, setExpanded] = useState<number | null>(null);
  const [decoding, setDecoding] = useState<{ traceId: string; bytes: Uint8Array } | null>(null);

  const cols = ['trace_id', 'span_id', 'name', 'timestamp', 'duration'];

  const onExport = () => {
    const blob = new Blob([JSON.stringify(response, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `trace-result-${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const actions = (
    <>
      <button type="button" className={'result-view-btn' + (view === 'spans' ? ' is-on' : '')} onClick={() => setView('spans')}>Spans</button>
      <button type="button" className={'result-view-btn' + (view === 'grouped' ? ' is-on' : '')} onClick={() => setView('grouped')}>Group by trace</button>
      <button type="button" className={'result-view-btn' + (view === 'json' ? ' is-on' : '')} onClick={() => setView('json')}>Raw JSON</button>
      <button type="button" className="result-view-btn" onClick={onExport}>Export</button>
    </>
  );

  return (
    <ResultPanel catalog="traces" response={response} execMs={execMs} traceEnabled={state.trace} showTrace={showTrace} setShowTrace={setShowTrace} actions={actions}>
      {showTrace ? (
        state.trace ? <TraceView response={response} /> : <TraceDisabled />
      ) : view === 'json' ? (
        <div className="rv-trace"><pre>{JSON.stringify(response, null, 2)}</pre></div>
      ) : view === 'grouped' ? (
        <GroupedTraceView elements={elements} cols={cols} expanded={expanded} setExpanded={setExpanded} setDecoding={setDecoding} />
      ) : elements.length === 0 ? (
        <ResultEmpty title="No spans" text="The trace query matched no spans in this window." />
      ) : (
        <div className="rv-table-wrap">
          <table className="rv-table">
            <thead>
              <tr>{cols.map((c) => <th key={c}>{c}</th>)}<th></th></tr>
            </thead>
            <tbody>
              {elements.map((e, i) => (
                <React.Fragment key={i}>
                  <tr>
                    {cols.map((c) => <td key={c}>{srRenderValue('text', e[c]).display}</td>)}
                    <td>
                      <button type="button" className="qb-btn qb-btn-ghost" onClick={() => setExpanded(expanded === i ? null : i)}>
                        {expanded === i ? 'Hide' : 'Inspect'}
                      </button>
                      <DecodeButton element={e} onDecode={(traceId, bytes) => setDecoding({ traceId, bytes })} />
                    </td>
                  </tr>
                  {expanded === i && (
                    <tr className="rv-inspect-row">
                      <td colSpan={cols.length + 1}>
                        <pre className="rv-inspect">{JSON.stringify(e, null, 2)}</pre>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {decoding && (
        <TraceDecoderModal
          traceId={decoding.traceId}
          bytes={decoding.bytes}
          onClose={() => setDecoding(null)}
        />
      )}
    </ResultPanel>
  );
}

function DecodeButton({ element, onDecode }: { element: Elem; onDecode: (traceId: string, bytes: Uint8Array) => void }) {
  const disabled = !element.bytes || (Array.isArray(element.bytes) && element.bytes.length === 0);
  return (
    <button
      type="button"
      className="qb-btn qb-btn-ghost"
      disabled={disabled}
      onClick={() => {
        const raw = element.bytes as Uint8Array | number[] | undefined;
        const bytes = raw instanceof Uint8Array
          ? raw
          : new Uint8Array(Array.isArray(raw) ? raw : []);
        onDecode(String(element.trace_id ?? ''), bytes);
      }}
    >
      Decode bytes
    </button>
  );
}

function GroupedTraceView({ elements, cols, expanded, setExpanded, setDecoding }: {
  elements: readonly Elem[];
  cols: readonly string[];
  expanded: number | null;
  setExpanded: (v: number | null) => void;
  setDecoding: (v: { traceId: string; bytes: Uint8Array } | null) => void;
}) {
  const grouped = useMemo(() => {
    const map = new Map<string, Elem[]>();
    for (const e of elements) {
      const tid = String(e.trace_id ?? 'unknown');
      if (!map.has(tid)) map.set(tid, []);
      map.get(tid)!.push(e);
    }
    return [...map.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  }, [elements]);

  if (grouped.length === 0) return <ResultEmpty title="No spans" text="The trace query matched no spans in this window." />;

  return (
    <div className="rv-table-wrap">
      {grouped.map(([traceId, spans]) => (
        <div key={traceId} className="rv-trace-group">
          <div className="rv-trace-group-head">
            <span className="rv-trace-group-id mono">{traceId}</span>
            <span className="rv-trace-group-count">{spans.length} span{spans.length === 1 ? '' : 's'}</span>
          </div>
          <table className="rv-table">
            <thead>
              <tr>{cols.map((c) => <th key={c}>{c}</th>)}<th></th></tr>
            </thead>
            <tbody>
              {spans.map((e, i) => {
                const globalIdx = elements.indexOf(e);
                return (
                  <React.Fragment key={i}>
                    <tr>
                      {cols.map((c) => <td key={c}>{srRenderValue('text', e[c]).display}</td>)}
                      <td>
                        <button type="button" className="qb-btn qb-btn-ghost" onClick={() => setExpanded(expanded === globalIdx ? null : globalIdx)}>
                          {expanded === globalIdx ? 'Hide' : 'Inspect'}
                        </button>
                        <DecodeButton element={e} onDecode={(traceId, bytes) => setDecoding({ traceId, bytes })} />
                      </td>
                    </tr>
                    {expanded === globalIdx && (
                      <tr className="rv-inspect-row">
                        <td colSpan={cols.length + 1}>
                          <pre className="rv-inspect">{JSON.stringify(e, null, 2)}</pre>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  );
}