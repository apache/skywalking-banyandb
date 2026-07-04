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

type Elem = Record<string, unknown>;

interface Props {
  readonly response: QueryResponse;
  readonly state: QBBuilderState;
  readonly showTrace: boolean;
  readonly setShowTrace: (v: boolean) => void;
}

export function StreamResultView({ response, state, showTrace, setShowTrace }: Props) {
  const [view, setView] = useState<'console' | 'table' | 'fields'>('console');
  const [overrides, setOverrides] = useState<Record<string, SR_ROLE>>({});
  const [inspecting, setInspecting] = useState<{ tag: string; bytes: Uint8Array } | null>(null);

  const elements = useMemo(() => (response.elements ?? []) as readonly Elem[], [response]);

  // Build a per-tag inferred role from sample values (layer 1 + 2 + 3 + 4).
  const tags = useMemo(
    () => (state.projection.length ? state.projection : Object.keys(elements[0] ?? {})),
    [state.projection, elements],
  );
  const roles = useMemo(() => {
    const map: Record<string, SR_ROLE> = {};
    for (const t of tags) {
      const sample = elements.map((e) => e[t]).filter((v) => v !== undefined);
      const inf = srInferRole(t, 'TAG_TYPE_STRING', sample, overrides);
      map[t] = inf.role;
    }
    return map;
  }, [tags, elements, overrides]);

  return (
    <div className="rv-root">
      <div className="rv-tabs">
        <button type="button" className={'rv-tab' + (view === 'console' ? ' is-on' : '')} onClick={() => setView('console')}>Console</button>
        <button type="button" className={'rv-tab' + (view === 'table' ? ' is-on' : '')} onClick={() => setView('table')}>Table</button>
        <button type="button" className={'rv-tab' + (view === 'fields' ? ' is-on' : '')} onClick={() => setView('fields')}>Fields</button>
        {state.trace && (
          <button type="button" className={'rv-tab' + (showTrace ? ' is-on' : '')} onClick={() => setShowTrace(!showTrace)}>Trace</button>
        )}
      </div>

      {showTrace && state.trace ? (
        <pre className="rv-trace">{JSON.stringify((response as unknown as { trace?: unknown }).trace ?? {}, null, 2)}</pre>
      ) : view === 'console' ? (
        <ConsoleView elements={elements} roles={roles} setInspecting={setInspecting} />
      ) : view === 'table' ? (
        <TableView elements={elements} roles={roles} />
      ) : (
        <FieldsPanel tags={tags} roles={roles} overrides={overrides} onChange={setOverrides} />
      )}

      {inspecting && (
        <BinaryInspector tag={inspecting.tag} bytes={inspecting.bytes} onClose={() => setInspecting(null)} />
      )}
    </div>
  );
}

function ConsoleView({ elements, roles, setInspecting }: {
  elements: readonly Elem[];
  roles: Record<string, SR_ROLE>;
  setInspecting: (v: { tag: string; bytes: Uint8Array }) => void;
}) {
  if (elements.length === 0) return <div className="rv-empty">No events.</div>;
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
  if (elements.length === 0) return <div className="rv-empty">No events.</div>;
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

function FieldsPanel({ tags, roles, overrides, onChange }: {
  tags: readonly string[];
  roles: Record<string, SR_ROLE>;
  overrides: Record<string, SR_ROLE>;
  onChange: (next: Record<string, SR_ROLE>) => void;
}) {
  const ROLE_OPTIONS: SR_ROLE[] = ['text', 'severity', 'http_status', 'service', 'id', 'duration_ms', 'duration_ns', 'time', 'body', 'binary', 'array', 'number'];
  const setRole = (tag: string, role: SR_ROLE | null) => {
    const next = { ...overrides };
    if (role === null) delete next[tag]; else next[tag] = role;
    onChange(next);
  };
  return (
    <div className="rv-fields">
      <p className="rv-fields-meta">Per-tag role overrides (Layer 4 of the role-inference ladder). Set overrides win over inferred roles.</p>
      <table className="rv-table">
        <thead>
          <tr><th>Tag</th><th>Inferred</th><th>Override</th></tr>
        </thead>
        <tbody>
          {tags.map((t) => (
            <tr key={t}>
              <td>{t}</td>
              <td><span className={'rv-pill rv-pill-' + roles[t]}>{roles[t]}</span></td>
              <td>
                <select
                  aria-label={`Role override for ${t}`}
                  value={overrides[t] ?? ''}
                  onChange={(e) => setRole(t, (e.target.value || null) as SR_ROLE | null)}
                >
                  <option value="">— inferred —</option>
                  {ROLE_OPTIONS.map((r) => <option key={r} value={r}>{r}</option>)}
                </select>
              </td>
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