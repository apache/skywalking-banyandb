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
// Refactored to match the handoff stream-result design:
//   - 4-layer role inference (type → value → convention → user override)
//   - One-line summary with colored pills (service / id / severity / status / number)
//     plus a body text segment
//   - Expandable row detail grid
//   - "Tag rendering" popover to toggle summary visibility and override roles
//   - Tags / Console / Table view-mode switcher
//
// Tag types come from the stream schema (passed as `tagSpecs`). When schema
// types are unavailable, every tag falls back to TAG_TYPE_STRING.

import React, { useMemo, useRef, useState } from 'react';
import type { QueryResponse } from 'canopy-shared';
import type { QBBuilderState } from '../bydbql.js';
import {
  srInferRole,
  srRenderValue,
  srLayerLabel,
  srSeverityColor,
  srHttpColor,
  SR_ROLE_OPTIONS,
  type SR_ROLE,
  type SR_TAG_TYPE,
} from '../role-infer.js';
import { tdHexDump, tdToBase64 } from '../proto-decoder.js';
import { ResultPanel } from './ResultPanel.js';
import { ResultEmpty } from './ResultEmpty.js';
import { TraceView, TraceDisabled } from './TraceView.js';

type Elem = Record<string, unknown>;

interface TagSpec {
  readonly name: string;
  readonly type: SR_TAG_TYPE | string;
}

interface StreamTagConfig {
  readonly name: string;
  readonly type: string;
  readonly role: SR_ROLE;
  readonly layer: 1 | 2 | 3 | 4;
  readonly visible: boolean;
}

interface Props {
  readonly response: QueryResponse;
  readonly state: QBBuilderState;
  readonly showTrace: boolean;
  readonly setShowTrace: (v: boolean) => void;
  readonly execMs?: number;
  /** Schema tag specs (name + type) so the view can infer roles from type. */
  readonly tagSpecs?: readonly TagSpec[];
}

const ALL_ROLES: readonly SR_ROLE[] = SR_ROLE_OPTIONS;

const ROLE_LABEL: Record<SR_ROLE, string> = {
  severity: 'severity',
  http_status: 'status',
  service: 'service',
  id: 'id',
  duration_ms: 'duration',
  duration_ns: 'duration',
  time: 'time',
  body: 'body',
  binary: 'binary',
  array: 'array',
  number: 'number',
  text: 'text',
};

function typeLabel(type: string): string {
  if (!type) return 'string';
  const normalized = type.replace(/^TAG_TYPE_/, '').toLowerCase();
  switch (normalized) {
    case 'int':
    case 'int64': return 'int';
    case 'data_binary': return 'binary';
    case 'string_array':
    case 'int64_array': return 'array';
    default: return normalized;
  }
}

function isSummaryRole(role: SR_ROLE): boolean {
  return role === 'service' || role === 'id' || role === 'number' ||
    role === 'severity' || role === 'http_status' || role === 'duration_ms' || role === 'duration_ns';
}

function defaultVisible(role: SR_ROLE): boolean {
  return isSummaryRole(role);
}

function storageKey(group: string, resource: string): string {
  return `canopy.stream.render.${group}.${resource}`;
}

export function StreamResultView({ response, state, showTrace, setShowTrace, execMs, tagSpecs }: Props) {
  const [view, setView] = useState<'console' | 'table' | 'json'>('console');
  const [expanded, setExpanded] = useState<Set<number>>(new Set());
  const [inspecting, setInspecting] = useState<{ tag: string; bytes: Uint8Array } | null>(null);
  const [showFields, setShowFields] = useState(false);
  const popoverRef = useRef<HTMLDivElement | null>(null);

  const elements = useMemo(() => (response.elements ?? []) as readonly Elem[], [response]);

  // Tags to render: explicit projection, or all keys from the first element.
  const tagNames = useMemo(
    () => (state.projection.length ? state.projection : Object.keys(elements[0] ?? {})),
    [state.projection, elements],
  );

  // Build a type map from schema specs; missing tags default to STRING.
  const typeMap = useMemo(() => {
    const map: Record<string, string> = {};
    for (const t of tagSpecs ?? []) map[t.name] = t.type;
    return map;
  }, [tagSpecs]);

  // Load persisted overrides + visibility once per resource.
  const persistedKey = storageKey(state.group, state.resource);
  const persisted = useMemo(() => {
    try {
      const raw = localStorage.getItem(persistedKey);
      if (raw) {
        const parsed = JSON.parse(raw) as { overrides?: Record<string, SR_ROLE>; visible?: Record<string, boolean> };
        return { overrides: parsed.overrides ?? {}, visible: parsed.visible ?? {} };
      }
    } catch { /* ignore */ }
    return { overrides: {}, visible: {} };
  }, [persistedKey]);

  const [overrides, setOverrides] = useState<Record<string, SR_ROLE>>(persisted.overrides);
  const [visibility, setVisibility] = useState<Record<string, boolean>>(persisted.visible);

  // Persist whenever the user changes a setting.
  React.useEffect(() => {
    try {
      localStorage.setItem(persistedKey, JSON.stringify({ overrides, visible: visibility }));
    } catch { /* quota */ }
  }, [persistedKey, overrides, visibility]);

  // Resolve per-tag config (type / inferred role / override / visibility).
  const tagConfig = useMemo((): readonly StreamTagConfig[] => {
    return tagNames.map((name) => {
      const type = typeMap[name] ?? 'TAG_TYPE_STRING';
      const sample = elements.map((e) => e[name]).filter((v) => v !== undefined);
      const inferred = srInferRole(name, type, sample, overrides);
      const visible = visibility[name] ?? defaultVisible(inferred.role);
      return { name, type, role: inferred.role, layer: inferred.layer, visible };
    });
  }, [tagNames, typeMap, elements, overrides, visibility]);

  const configMap = useMemo(() => {
    const map: Record<string, StreamTagConfig> = {};
    for (const c of tagConfig) map[c.name] = c;
    return map;
  }, [tagConfig]);

  const setOverride = (name: string, role: SR_ROLE) => {
    setOverrides((prev) => {
      const next = { ...prev };
      if (role) next[name] = role;
      else delete next[name];
      return next;
    });
  };

  const toggleVisible = (name: string) => {
    setVisibility((prev) => ({ ...prev, [name]: !(prev[name] ?? defaultVisible(configMap[name]?.role ?? 'text')) }));
  };

  const onExport = () => {
    const blob = new Blob([JSON.stringify(response, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `stream-result-${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const count = elements.length;
  const total = response.totalRowCount ?? count;
  const subBar = (
    <div className="sr-toolbar">
      <span className="sr-count">
        {count > 0 ? `showing ${count.toLocaleString('en-US')} of ${total.toLocaleString('en-US')}` : 'no results'}
        {state.orderField && ` · order by ${state.orderField} ${state.orderDir.toLowerCase()}`}
      </span>
      <div className="sr-tool-right">
        <div className="sf-wrap" ref={popoverRef}>
          <button
            type="button"
            className={'sf-btn' + (showFields ? ' is-on' : '')}
            onClick={() => setShowFields((v) => !v)}
            title="Tag rendering"
          >
            <IconTag width={13} height={13} /> Tags
          </button>
          {showFields && (
            <TagRenderingPopover
              config={tagConfig}
              onClose={() => setShowFields(false)}
              onToggle={toggleVisible}
              onRoleChange={setOverride}
            />
          )}
        </div>
        <div className="sr-view-seg" role="tablist" aria-label="Result view">
          <button
            type="button"
            className={'sr-view-btn' + (view === 'console' ? ' is-on' : '')}
            onClick={() => setView('console')}
          >
            <IconConsole width={13} height={13} /> Console
          </button>
          <button
            type="button"
            className={'sr-view-btn' + (view === 'table' ? ' is-on' : '')}
            onClick={() => setView('table')}
          >
            <IconRows width={13} height={13} /> Table
          </button>
        </div>
      </div>
    </div>
  );

  return (
    <ResultPanel catalog="streams" response={response} execMs={execMs} traceEnabled={state.trace} showTrace={showTrace} setShowTrace={setShowTrace} subBar={subBar}>
      {showTrace ? (
        state.trace ? <TraceView response={response} /> : <TraceDisabled />
      ) : view === 'console' ? (
        <ConsoleView
          elements={elements}
          config={tagConfig}
          expanded={expanded}
          setExpanded={setExpanded}
          setInspecting={setInspecting}
        />
      ) : view === 'table' ? (
        <TableView elements={elements} config={tagConfig} setInspecting={setInspecting} />
      ) : (
        <div className="rv-trace"><pre>{JSON.stringify(response, null, 2)}</pre></div>
      )}

      {inspecting && (
        <BinaryInspector tag={inspecting.tag} bytes={inspecting.bytes} onClose={() => setInspecting(null)} />
      )}
    </ResultPanel>
  );
}

function ConsoleView({ elements, config, expanded, setExpanded, setInspecting }: {
  elements: readonly Elem[];
  config: readonly StreamTagConfig[];
  expanded: Set<number>;
  setExpanded: (v: Set<number>) => void;
  setInspecting: (v: { tag: string; bytes: Uint8Array }) => void;
}) {
  if (elements.length === 0) return <ResultEmpty title="No events" text="The stream query matched no events in this window." />;

  const toggle = (i: number) => {
    const next = new Set(expanded);
    if (next.has(i)) next.delete(i);
    else next.add(i);
    setExpanded(next);
  };

  // Body tag is the first tag with role 'body'; fallback to the first 'text'
  // tag that is not a metadata column (element_id / timestamp) so
  // endpoint-like columns become the summary line.
  const bodyTag = config.find((c) => c.role === 'body') ??
    config.find((c) => c.role === 'text' && !/^(element_id|timestamp|ts|time)$/.test(c.name));

  return (
    <div className="slog">
      {elements.map((e, i) => {
        const tsRaw = e.timestamp ?? e.ts ?? e.time;
        const timeStr = formatTimestamp(tsRaw);
        const isOpen = expanded.has(i);
        return (
          <div key={i} className={'slog-row' + (isOpen ? ' is-open' : '')}>
            <div className="slog-main" onClick={() => toggle(i)}>
              <span className="slog-chev">{isOpen ? '▼' : '▶'}</span>
              <span className="slog-ts">{timeStr}</span>
              <span className="slog-line">
                <span className="slog-badges">
                  {config.filter((c) => c.visible && c.role !== 'body').map((c) => (
                    <ValuePill key={c.name} tag={c.name} role={c.role} value={e[c.name]} setInspecting={setInspecting} />
                  ))}
                </span>
                {bodyTag && (
                  <span className="slog-body">
                    {srRenderValue(bodyTag.role, e[bodyTag.name]).display}
                  </span>
                )}
              </span>
            </div>
            {isOpen && (
              <div className="slog-detail">
                {config.map((c) => (
                  <div key={c.name} className="slog-kv">
                    <span className="slog-k">{c.name}</span>
                    <span className="slog-v">
                      <ValuePill tag={c.name} role={c.role} value={e[c.name]} setInspecting={setInspecting} />
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function TableView({ elements, config, setInspecting }: {
  elements: readonly Elem[];
  config: readonly StreamTagConfig[];
  setInspecting: (v: { tag: string; bytes: Uint8Array }) => void;
}) {
  if (elements.length === 0) return <ResultEmpty title="No events" text="The stream query matched no events in this window." />;
  const cols = config.map((c) => c.name);
  return (
    <div className="rv-table-wrap">
      <table className="rv-table">
        <thead>
          <tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr>
        </thead>
        <tbody>
          {elements.map((e, i) => (
            <tr key={i}>
              {cols.map((c) => {
                const cfg = config.find((x) => x.name === c)!;
                return (
                  <td key={c}>
                    <ValuePill tag={c} role={cfg.role} value={e[c]} setInspecting={setInspecting} />
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ValuePill({ tag, role, value, setInspecting }: {
  tag: string;
  role: SR_ROLE;
  value: unknown;
  setInspecting?: (v: { tag: string; bytes: Uint8Array }) => void;
}) {
  const rendered = srRenderValue(role, value);
  const color = role === 'severity' ? srSeverityColor(value)
    : role === 'http_status' ? srHttpColor(value)
    : rendered.color;

  if (role === 'binary' && value instanceof Uint8Array) {
    return (
      <button type="button" className="sbin sbin-btn" onClick={() => setInspecting?.({ tag, bytes: value })}>
        <IconBinary width={11} height={11} /> {rendered.display} <span className="sbin-caret">▾</span>
      </button>
    );
  }

  if (role === 'array' && Array.isArray(value)) {
    return (
      <span className="sarr">
        {value.map((v, i) => (
          <span key={i} className="scat">{String(v)}</span>
        ))}
      </span>
    );
  }

  const style: React.CSSProperties = color ? {
    background: color,
    color: isLightColor(color) ? '#08100a' : undefined,
    borderColor: 'transparent',
  } : {};

  return (
    <span className={'scat scat-' + role} style={style} title={rendered.title}>
      {rendered.display}
    </span>
  );
}

function isLightColor(color: string): boolean {
  // Heuristic: the service palette and id blue are light enough that dark text
  // reads better. Semantic vars are not colors, so they return false.
  if (color.startsWith('var(')) return false;
  const light = ['#3d82f6', '#6b9fff', '#56c2d6', '#7fc296', '#3cc8b4', '#9b8cf0', '#d98a3c', '#e0707a', '#c77dbf', '#d8b13c'];
  return light.includes(color);
}

function formatTimestamp(raw: unknown): string {
  if (raw == null) return '';
  if (typeof raw === 'number') {
    const d = new Date(raw > 1e12 ? raw : raw * 1000);
    return d.toISOString().replace('T', ' ').replace('Z', '');
  }
  const d = new Date(String(raw));
  if (Number.isNaN(d.getTime())) return String(raw);
  return d.toISOString().replace('T', ' ').replace('Z', '');
}

function TagRenderingPopover({ config, onClose, onToggle, onRoleChange }: {
  config: readonly StreamTagConfig[];
  onClose: () => void;
  onToggle: (name: string) => void;
  onRoleChange: (name: string, role: SR_ROLE) => void;
}) {
  // Close when clicking outside the popover.
  const selfRef = useRef<HTMLDivElement | null>(null);
  React.useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (selfRef.current && !selfRef.current.contains(e.target as Node)) onClose();
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [onClose]);

  return (
    <>
      <div className="sf-backdrop" onClick={onClose} />
      <div className="sfields-pop" ref={selfRef}>
        <div className="sfields-head">
          <span>Tag rendering</span>
          <span className="mono">on line · type → shown as</span>
        </div>
        {config.map((c) => (
          <div key={c.name} className="sfields-row">
            <button
              type="button"
              className={'sf-pin' + (c.visible ? ' is-on' : '')}
              onClick={() => onToggle(c.name)}
              title={c.visible ? 'Hide from one-line summary' : 'Show in one-line summary'}
            >
              {c.visible ? <IconEye width={13} height={13} /> : <IconEyeOff width={13} height={13} />}
            </button>
            <span className="sf-name" title={c.name}>{c.name}</span>
            <span className="sf-type">{typeLabel(c.type)}</span>
            <span className="sf-arrow">→</span>
            <select
              className="sf-sel"
              value={c.role}
              onChange={(e) => onRoleChange(c.name, e.target.value as SR_ROLE)}
            >
              {ALL_ROLES.map((r) => <option key={r} value={r}>{ROLE_LABEL[r]}</option>)}
            </select>
            <span className={'sf-src ' + srLayerLabel(c.layer).toLowerCase()}>{srLayerLabel(c.layer)}</span>
          </div>
        ))}
        <p className="sfields-note">
          The <b>eye</b> picks which tags ride the one-line summary; the rest fold into the row&apos;s detail.
          BanyanDB stores only the <span className="mono">TagType</span> — &quot;shown as&quot; is inferred from value
          cardinality &amp; shape (<span className="mono">AUTO</span>), with well-known fields
          (<span className="mono">CONV</span>) and your overrides (<span className="mono">SET</span>) on top.
        </p>
      </div>
    </>
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

const IconRows = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M3 6h18M3 12h18M3 18h18" />
  </svg>
);
const IconConsole = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M4 6h16M4 12h10M4 18h16" />
  </svg>
);
const IconTag = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M20.6 13.4 12 21l-9-9V4h8l9.6 9.4z" />
    <circle cx="7.5" cy="7.5" r="1.5" />
  </svg>
);
const IconEye = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M2 12s3.6-7 10-7 10 7 10 7-3.6 7-10 7-10-7-10-7z" />
    <circle cx="12" cy="12" r="3" />
  </svg>
);
const IconEyeOff = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M10.6 6.2A9.7 9.7 0 0 1 12 5c6.4 0 10 7 10 7a17 17 0 0 1-3.2 4M6.3 7.4A17 17 0 0 0 2 12s3.6 7 10 7a9.7 9.7 0 0 0 4-.9M9.9 9.9a3 3 0 0 0 4.2 4.2" />
    <path d="M3 3l18 18" />
  </svg>
);
const IconBinary = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M8 16v-8M12 20V4M16 16v-8" />
  </svg>
);
