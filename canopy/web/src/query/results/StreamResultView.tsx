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

// StreamResultView.tsx — stream query result view (Console + Table + Fields + binary).
// Ported from .handoff-import/banyandb/project/stream-results.jsx.
// Tag rendering uses the 4-layer role-inference ladder from role-infer.ts —
// never keys on tag name alone.

import React, { useMemo, useState } from 'react';
import type { QueryResponse } from 'canopy-shared';
import type { QBBuilderState } from '../bydbql.js';
import { srInferRole, srSeverityColor, srHttpColor, srRenderValue, type SR_ROLE } from '../role-infer.js';
import { tdHexDump, tdToBase64 } from '../proto-decoder.js';
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

export function StreamResultView({ response, state, showTrace, setShowTrace, execMs }: Props) {
  const [view, setView] = useState<'console' | 'table' | 'json'>('console');
  const [inspecting, setInspecting] = useState<{ tag: string; bytes: Uint8Array } | null>(null);

  const elements = useMemo(() => (response.elements ?? []) as readonly Elem[], [response]);

  // Build a per-tag inferred role from sample values (layer 1 + 2 + 3).
  const tags = useMemo(
    () => (state.projection.length ? state.projection : Object.keys(elements[0] ?? {})),
    [state.projection, elements],
  );
  const roles = useMemo(() => {
    const map: Record<string, SR_ROLE> = {};
    for (const t of tags) {
      const sample = elements.map((e) => e[t]).filter((v) => v !== undefined);
      const inf = srInferRole(t, 'TAG_TYPE_STRING', sample, {});
      map[t] = inf.role;
    }
    return map;
  }, [tags, elements]);

  const onExport = () => {
    const blob = new Blob([JSON.stringify(response, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `stream-result-${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const actions = (
    <>
      <button type="button" className={'result-view-btn' + (view === 'console' ? ' is-on' : '')} onClick={() => setView('console')}>Console</button>
      <button type="button" className={'result-view-btn' + (view === 'table' ? ' is-on' : '')} onClick={() => setView('table')}>Table</button>
      <button type="button" className={'result-view-btn' + (view === 'json' ? ' is-on' : '')} onClick={() => setView('json')}>Raw JSON</button>
      <button type="button" className="result-view-btn" onClick={onExport}>Export</button>
    </>
  );

  return (
    <ResultPanel catalog="streams" response={response} execMs={execMs} traceEnabled={state.trace} showTrace={showTrace} setShowTrace={setShowTrace} actions={actions}>
      {showTrace ? (
        state.trace ? <TraceView response={response} /> : <TraceDisabled />
      ) : view === 'console' ? (
        <ConsoleView elements={elements} roles={roles} setInspecting={setInspecting} />
      ) : view === 'table' ? (
        <TableView elements={elements} roles={roles} />
      ) : (
        <div className="rv-trace"><pre>{JSON.stringify(response, null, 2)}</pre></div>
      )}

      {inspecting && (
        <BinaryInspector tag={inspecting.tag} bytes={inspecting.bytes} onClose={() => setInspecting(null)} />
      )}
    </ResultPanel>
  );
}

function ConsoleView({ elements, roles, setInspecting }: {
  elements: readonly Elem[];
  roles: Record<string, SR_ROLE>;
  setInspecting: (v: { tag: string; bytes: Uint8Array }) => void;
}) {
  if (elements.length === 0) return <ResultEmpty title="No events" text="The stream query matched no events in this window." />;
  return (
    <div className="rv-console">
      {elements.map((e, i) => {
        const ts = e.timestamp ?? e.ts ?? e.time;
        const timeStr = typeof ts === 'number' ? new Date(ts).toISOString() : String(ts ?? '');
        return (
          <div key={i} className="rv-console-row">
            <span className="rv-console-ts">{timeStr}</span>
            {Object.entries(roles).map(([tag, role]) => {
              const v = e[tag];
              const r = srRenderValue(role, v);
              const color = role === 'severity' ? srSeverityColor(v)
                : role === 'http_status' ? srHttpColor(v)
                : undefined;
              if (role === 'binary' && v instanceof Uint8Array) {
                return (
                  <button key={tag} type="button" className="rv-pill" onClick={() => setInspecting({ tag, bytes: v })}>
                    {tag}: {r.display}
                  </button>
                );
              }
              return (
                <span key={tag} className={'rv-pill rv-pill-' + role} style={color ? { color } : undefined}>
                  {tag}: <span className="rv-pill-val">{r.display}</span>
                </span>
              );
            })}
          </div>
        );
      })}
    </div>
  );
}

function TableView({ elements, roles }: { elements: readonly Elem[]; roles: Record<string, SR_ROLE> }) {
  if (elements.length === 0) return <ResultEmpty title="No events" text="The stream query matched no events in this window." />;
  const cols = Object.keys(roles);
  return (
    <div className="rv-table-wrap">
      <table className="rv-table">
        <thead>
          <tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr>
        </thead>
        <tbody>
          {elements.map((e, i) => (
            <tr key={i}>
              {cols.map((c) => <td key={c}>{srRenderValue(roles[c], e[c]).display}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function BinaryInspector({ tag, bytes, onClose }: { tag: string; bytes: Uint8Array; onClose: () => void }) {
  const rows = tdHexDump(bytes);
  const b64 = tdToBase64(bytes);
  return (
    <div className="modal-backdrop" role="dialog" aria-modal="true" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2 className="modal-title">Binary: {tag} ({bytes.length} bytes)</h2>
          <button type="button" className="qb-btn qb-btn-ghost" onClick={onClose}>Close</button>
        </header>
        <div className="rv-binary">
          <pre className="rv-binary-hex">
            {rows.map((r: { off: string; hex: string; ascii: string }, i: number) => (
              <div key={i}><span className="rv-off">{r.off}</span>  {r.hex}  <span className="rv-ascii">{r.ascii}</span></div>
            ))}
          </pre>
          <details>
            <summary>base64 ({b64.length} chars)</summary>
            <pre className="rv-binary-b64">{b64}</pre>
          </details>
        </div>
        <p className="rv-binary-note">
          BanyanDB stores DATA_BINARY opaquely. Bind a .proto on the trace view to decode span payloads.
        </p>
      </div>
    </div>
  );
}