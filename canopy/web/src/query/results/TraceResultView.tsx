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

import React, { useState } from 'react';
import type { QueryResponse } from 'canopy-shared';
import type { QBBuilderState } from '../bydbql.js';
import { TraceDecoderModal } from '../TraceDecoderModal.js';
import { srRenderValue } from '../role-infer.js';

type Elem = Record<string, unknown>;

interface Props {
  readonly response: QueryResponse;
  readonly state: QBBuilderState;
  readonly showTrace: boolean;
  readonly setShowTrace: (v: boolean) => void;
}

export function TraceResultView({ response, state, showTrace, setShowTrace }: Props) {
  const elements = (response.elements ?? []) as readonly Elem[];
  const [expanded, setExpanded] = useState<number | null>(null);
  const [decoding, setDecoding] = useState<{ traceId: string; bytes: Uint8Array } | null>(null);

  const cols = ['trace_id', 'span_id', 'name', 'timestamp', 'duration'];

  return (
    <div className="rv-root">
      <div className="rv-tabs">
        <button type="button" className="rv-tab is-on">Spans</button>
        {state.trace && (
          <button type="button" className={'rv-tab' + (showTrace ? ' is-on' : '')} onClick={() => setShowTrace(!showTrace)}>Trace</button>
        )}
      </div>
      {showTrace && state.trace ? (
        <pre className="rv-trace">{JSON.stringify((response as unknown as { trace?: unknown }).trace ?? {}, null, 2)}</pre>
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
                      <button
                        type="button"
                        className="qb-btn qb-btn-ghost"
                        disabled={!e.bytes || (Array.isArray(e.bytes) && e.bytes.length === 0)}
                        onClick={() => {
                          const raw = e.bytes as Uint8Array | number[] | undefined;
                          const bytes = raw instanceof Uint8Array
                            ? raw
                            : new Uint8Array(Array.isArray(raw) ? raw : []);
                          setDecoding({ traceId: String(e.trace_id ?? ''), bytes });
                        }}
                      >
                        Decode bytes
                      </button>
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
    </div>
  );
}