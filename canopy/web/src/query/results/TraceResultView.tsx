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

// TraceResultView.tsx — trace query result view (flat repeated Span + inspector).
// Refactored to match the handoff trace-results design:
//   - flat span table with start_time, span_id, summary
//   - expandable row showing all tags in a grid + opaque span bytes inspector
//   - role inference reused from streams (type → value → convention → user override)
//   - "Tag rendering" popover to tune summary visibility and override roles
//   - proto binding control to decode span bytes against a user-provided .proto

import React, { useMemo, useRef, useState } from 'react';
import type { QueryResponse } from 'canopy-shared';
import type { QBBuilderState } from '../bydbql.js';
import {
  srInferRole,
  srRenderValue,
  srLayerLabel,
  srCatColor,
  srConventionFor,
  isValidRole,
  SR_ROLE_OPTIONS,
  type SR_ROLE,
  type SR_TAG_TYPE,
} from '../role-infer.js';

import { ResultPanel } from './ResultPanel.js';
import { ResultEmpty } from './ResultEmpty.js';
import { TraceView, TraceDisabled } from './TraceView.js';
import { TraceDecoderModal } from '../TraceDecoderModal.js';
import { tdHexDump, tdGetBinding, tdDecode, tdHighlightJSON, type TDBinding } from '../proto-decoder.js';

type Elem = Record<string, unknown>;

interface TagSpec {
  readonly name: string;
  readonly type: SR_TAG_TYPE | string;
}

interface TraceTagConfig {
  readonly name: string;
  readonly type: string;
  readonly role: SR_ROLE;
  readonly inferred: SR_ROLE;
  readonly layer: 1 | 2 | 3 | 4;
  readonly visible: boolean;
  readonly reserved?: 'trace' | 'span' | 'time' | 'parent';
}

interface Props {
  readonly response: QueryResponse;
  readonly state: QBBuilderState;
  readonly showTrace: boolean;
  readonly setShowTrace: (v: boolean) => void;
  readonly execMs?: number;
  /** Schema tag specs (name + type) so the view can infer roles from type. */
  readonly tagSpecs?: readonly TagSpec[];
  /** True when the last page returned a full LIMIT — i.e. more rows likely exist. */
  readonly hasMore: boolean;
  /** Called when the user clicks "Load more" — should re-run with offset+=limit. */
  readonly onLoadMore: () => void;
  /** Disables the button + shows a spinner label while the next page is in flight. */
  readonly isLoadingMore: boolean;
}

const ROLE_LABEL: Record<SR_ROLE | 'auto', string> = {
  auto: 'auto',
  cat: 'badge',
  body: 'body',
  id: 'id',
  numeric: 'number',
  time: 'time',
  list: 'list',
  binary: 'binary',
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
    case 'int_array':
    case 'int64_array': return 'array';
    default: return normalized;
  }
}

function storageKey(group: string, resource: string): string {
  return `canopy.trace.render.${group}.${resource}`;
}

function normalizeBytes(raw: unknown): Uint8Array | null {
  if (!raw) return null;
  if (raw instanceof Uint8Array) return raw;
  if (Array.isArray(raw)) return new Uint8Array(raw);
  if (typeof raw === 'string') {
    try {
      const s = atob(raw);
      const bytes = new Uint8Array(s.length);
      for (let i = 0; i < s.length; i++) bytes[i] = s.charCodeAt(i);
      return bytes;
    } catch {
      return null;
    }
  }
  return null;
}

function formatTimestamp(raw: unknown): string {
  if (raw == null) return '';
  let n: number;
  if (typeof raw === 'number') {
    n = raw > 1e12 ? raw : raw * 1000;
  } else {
    n = Date.parse(String(raw));
  }
  if (!Number.isFinite(n)) return String(raw);
  const d = new Date(n);
  return `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}:${String(d.getUTCSeconds()).padStart(2, '0')}.${String(d.getUTCMilliseconds()).padStart(3, '0')}`;
}

function reservedKind(name: string, tsField: string): TraceTagConfig['reserved'] {
  if (name === tsField) return 'time';
  if (name === 'trace_id') return 'trace';
  if (name === 'span_id') return 'span';
  if (name === 'parent_span_id') return 'parent';
  return undefined;
}

function reservedRole(name: string, tsField: string): SR_ROLE | null {
  const kind = reservedKind(name, tsField);
  if (!kind) return null;
  return kind === 'time' ? 'time' : 'id';
}

function reservedLabel(kind: NonNullable<TraceTagConfig['reserved']>): string {
  switch (kind) {
    case 'trace': return 'TRACE ID';
    case 'span': return 'SPAN ID';
    case 'parent': return 'PARENT SPAN ID';
    case 'time': return 'TIMESTAMP';
  }
}

function formatBytes(len: number): string {
  if (len === 0) return '0 bytes';
  if (len < 1024) return `${len} bytes`;
  if (len < 1024 * 1024) return `${(len / 1024).toFixed(len < 10 * 1024 ? 1 : 0)} KB`;
  return `${(len / (1024 * 1024)).toFixed(1)} MB`;
}

/** Decide which projected tags surface on the one-line summary vs. stay folded
 *  into the expanded detail. Driven by display ROLE, never by tag name. */
function trAutoPick(config: readonly Pick<TraceTagConfig, 'name' | 'role'>[]): Set<string> {
  const picked = new Set<string>();
  config
    .filter((c) => c.role === 'cat')
    .sort((a, b) => Number(srConventionFor(b.name) != null) - Number(srConventionFor(a.name) != null))
    .slice(0, 3)
    .forEach((c) => picked.add(c.name));
  const body = config.find((c) => c.role === 'body' && !picked.has(c.name))
    ?? config.find((c) => c.role === 'text' && !picked.has(c.name));
  if (body) picked.add(body.name);
  config
    .filter((c) => c.role === 'numeric' && !picked.has(c.name))
    .slice(0, 2)
    .forEach((c) => picked.add(c.name));
  return picked;
}

export function TraceResultView({ response, state, showTrace, setShowTrace, execMs, tagSpecs, hasMore, onLoadMore, isLoadingMore }: Props) {
  const [expanded, setExpanded] = useState<Set<number>>(new Set());
  const [showFields, setShowFields] = useState(false);
  const [decoding, setDecoding] = useState<{ resource: string; bytes: Uint8Array } | null>(null);
  const [binding, setBinding] = useState<TDBinding | null>(() => tdGetBinding(state.resource));
  const popoverRef = useRef<HTMLDivElement | null>(null);

  // Reload binding when the trace resource changes.
  React.useEffect(() => {
    setBinding(tdGetBinding(state.resource));
  }, [state.resource]);

  const elements = useMemo(() => (response.elements ?? []) as readonly Elem[], [response]);

  // Build a type map from schema specs; missing tags default to STRING.
  const typeMap = useMemo(() => {
    const map: Record<string, string> = {};
    for (const t of tagSpecs ?? []) map[t.name] = t.type;
    return map;
  }, [tagSpecs]);

  // Reserved spine fields: trace_id / span_id / timestamp.
  const tsField = useMemo(() => {
    const fromSchema = tagSpecs?.find((t) => t.type === 'TAG_TYPE_TIMESTAMP')?.name;
    if (fromSchema) return fromSchema;
    const first = elements[0] ?? {};
    if ('start_time' in first) return 'start_time';
    if ('timestamp' in first) return 'timestamp';
    return 'timestamp';
  }, [tagSpecs, elements]);

  // Reserved spine fields rendered in the row header (timestamp + span_id) and
  // the summary line, but they still appear inside the expanded detail card.
  const reservedNames = useMemo(() => {
    const set = new Set<string>(['trace_id', 'span_id', 'parent_span_id']);
    if (tsField) set.add(tsField);
    return set;
  }, [tsField]);

  // Configurable tag names = projected/element keys minus reserved spine fields.
  const tagNames = useMemo(() => {
    return (state.projection.length ? state.projection : Object.keys(elements[0] ?? {}))
      .filter((n) => !reservedNames.has(n) && n !== 'span');
  }, [state.projection, elements, reservedNames]);

  // Detail card shows every projected/element tag except the opaque `span` bytes.
  const detailTagNames = useMemo(() => {
    return (state.projection.length ? state.projection : Object.keys(elements[0] ?? {})).filter((n) => n !== 'span');
  }, [state.projection, elements]);

  // Load persisted overrides + visibility once per resource.
  const persistedKey = storageKey(state.group, state.resource);
  const persisted = useMemo(() => {
    try {
      const raw = localStorage.getItem(persistedKey);
      if (raw) {
        const parsed = JSON.parse(raw) as { overrides?: Record<string, string>; visible?: Record<string, boolean> };
        const overrides: Record<string, SR_ROLE> = {};
        if (parsed.overrides) {
          for (const [k, v] of Object.entries(parsed.overrides)) {
            if (isValidRole(v)) overrides[k] = v;
          }
        }
        return { overrides, visible: parsed.visible ?? {} };
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

  // Reset overrides/visibility when the resource changes.
  React.useEffect(() => {
    setOverrides(persisted.overrides);
    setVisibility(persisted.visible);
  }, [state.group, state.resource]); // eslint-disable-line react-hooks/exhaustive-deps

  // Resolve per-tag config (type / inferred role / override / visibility).
  const inferredConfig = useMemo((): readonly TraceTagConfig[] => {
    return tagNames.map((name) => {
      const type = typeMap[name] ?? 'TAG_TYPE_STRING';
      const sample = elements.map((e) => e[name]).filter((v) => v !== undefined);
      const inferred = srInferRole(name, type, sample, overrides);
      return { name, type, role: inferred.role, inferred: inferred.role, layer: inferred.layer, visible: false };
    });
  }, [tagNames, typeMap, elements, overrides]);

  const tagConfig = useMemo((): readonly TraceTagConfig[] => {
    const autoPicked = trAutoPick(inferredConfig);
    return inferredConfig.map((c) => ({
      ...c,
      visible: visibility[c.name] ?? autoPicked.has(c.name),
    }));
  }, [inferredConfig, visibility]);

  // Detail grid config: every tag except `span`, with reserved spine fields
  // pinned to their semantic roles and decorated with type badges.
  const detailConfig = useMemo((): readonly TraceTagConfig[] => {
    return detailTagNames.map((name) => {
      const configurable = tagConfig.find((c) => c.name === name);
      if (configurable) return { ...configurable, reserved: undefined };
      const type = typeMap[name] ?? 'TAG_TYPE_STRING';
      const rrole = reservedRole(name, tsField);
      if (rrole) {
        return { name, type, role: rrole, inferred: rrole, layer: 4, visible: false, reserved: reservedKind(name, tsField) };
      }
      const sample = elements.map((e) => e[name]).filter((v) => v !== undefined);
      const inferred = srInferRole(name, type, sample, overrides);
      return { name, type, role: inferred.role, inferred: inferred.role, layer: inferred.layer, visible: false };
    });
  }, [detailTagNames, tagConfig, typeMap, elements, overrides, tsField]);

  const setOverride = (name: string, role: SR_ROLE | null) => {
    setOverrides((prev) => {
      const next = { ...prev };
      if (role) next[name] = role;
      else delete next[name];
      return next;
    });
  };

  const toggleVisible = (name: string) => {
    setVisibility((prev) => ({ ...prev, [name]: !(prev[name] ?? trAutoPick(inferredConfig).has(name)) }));
  };

  const count = elements.length;
  const total = response.totalRowCount ?? 0;
  const hasReliableTotal = total > count;

  // Bytes of the first expanded row, used by the toolbar Decode button.
  const firstExpandedBytes = useMemo(() => {
    for (const idx of expanded) {
      const rowBytes = normalizeBytes(elements[idx]?.span);
      if (rowBytes && rowBytes.length > 0) return rowBytes;
    }
    return null;
  }, [expanded, elements]);

  const subBar = (
    <div className="mr-toolbar">
      <span className="mr-tool-label">
        flat <span className="mono strong">repeated Span</span>
        {count > 0 && (
          <>
            {' · '}
            {hasReliableTotal
              ? `${count.toLocaleString('en-US')} of ${total.toLocaleString('en-US')} spans`
              : `${count.toLocaleString('en-US')} spans`}
          </>
        )}
        {state.orderField && ` · order by ${state.orderField} ${state.orderDir.toLowerCase()}`}
      </span>
      <div className="mr-tool-right">
        <DecodeBytesButton
          bytes={firstExpandedBytes}
          binding={binding}
          onInspect={() => setDecoding({ resource: state.resource, bytes: firstExpandedBytes ?? new Uint8Array(0) })}
        />
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
              overrides={overrides}
              onClose={() => setShowFields(false)}
              onToggle={toggleVisible}
              onRoleChange={setOverride}
            />
          )}
        </div>
      </div>
    </div>
  );

  return (
    <ResultPanel catalog="traces" response={response} execMs={execMs} traceEnabled={state.trace} showTrace={showTrace} setShowTrace={setShowTrace} subBar={subBar} className="is-trace">
      {showTrace ? (
        state.trace ? <TraceView response={response} /> : <TraceDisabled />
      ) : elements.length === 0 ? (
        <ResultEmpty title="No spans" text="The trace query matched no spans in this window." />
      ) : (
        <>
          <div className="tin">
            <div className="tin-colhead">
              <span />
              <span>{tsField}</span>
              <span>span_id</span>
              <span>summary · expand for all tags + span bytes</span>
            </div>
            {elements.map((e, i) => (
              <TraceInspectorRow
                key={i}
                index={i}
                element={e}
                tsField={tsField}
                resource={state.resource}
                config={tagConfig}
                detailConfig={detailConfig}
                binding={binding}
                expanded={expanded}
                setExpanded={setExpanded}
                setDecoding={setDecoding}
              />
            ))}
          </div>
          {hasMore && (
            <div className="rv-loadmore">
              <button type="button" className="rv-loadmore-btn" onClick={onLoadMore} disabled={isLoadingMore}>
                {isLoadingMore ? 'Loading…' : `Load more (${count} shown)`}
              </button>
            </div>
          )}
          <div className="slog-foot">
            <span>
              {count} span{count === 1 ? '' : 's'} · <span className="mono">Span = tags + span bytes</span>
              {' · reserved '}<span className="mono faint">trace_id · span_id · {tsField}</span>
            </span>
            <span className="mr-tool-right mono faint">flat repeated Span · LIMIT {state.limit}</span>
          </div>
        </>
      )}
      {decoding && (
        <TraceDecoderModal
          traceId={decoding.resource}
          onClose={() => setDecoding(null)}
          onChange={setBinding}
        />
      )}

    </ResultPanel>
  );
}

function TraceInspectorRow({ index, element, tsField, resource, config, detailConfig, binding, expanded, setExpanded, setDecoding }: {
  index: number;
  element: Elem;
  tsField: string;
  resource: string;
  config: readonly TraceTagConfig[];
  detailConfig: readonly TraceTagConfig[];
  binding: TDBinding | null;
  expanded: Set<number>;
  setExpanded: (v: Set<number>) => void;
  setDecoding: (v: { resource: string; bytes: Uint8Array } | null) => void;
}) {
  const isOpen = expanded.has(index);
  const toggle = () => {
    const next = new Set(expanded);
    if (next.has(index)) next.delete(index);
    else next.add(index);
    setExpanded(next);
  };

  const visible = config.filter((c) => c.visible);
  const badgeCols = visible.filter((c) => c.role === 'cat');
  const bodyCol = visible.find((c) => c.role === 'body') ?? visible.find((c) => c.role === 'text');
  const metaCols = visible.filter((c) => c.role === 'numeric');
  const bodyVal = bodyCol ? element[bodyCol.name] : undefined;
  const spanId = String(element.span_id ?? '');
  const ts = formatTimestamp(element[tsField]);
  const bytes = normalizeBytes(element.span);

  return (
    <div className={'tin-row' + (isOpen ? ' is-open' : '')}>
      <div className="tin-main" onClick={toggle}>
        <span className="slog-chev">{isOpen ? '▾' : '▸'}</span>
        <span className="slog-ts mono">{ts}</span>
        <span className="tin-sid mono" title={`span_id = ${spanId}`}>{spanId}</span>
        <div className="slog-line">
          {badgeCols.length > 0 && (
            <span className="slog-badges">{badgeCols.map((c) => <ValuePill key={c.name} tag={c.name} role={c.role} value={element[c.name]} />)}</span>
          )}
          <span className={'slog-body' + (bodyCol && bodyCol.role === 'body' ? '' : ' is-fallback')}>
            {String(bodyVal ?? '')}
          </span>
          {metaCols.length > 0 && (
            <span className="slog-meta">{metaCols.map((c) => (
              <span key={c.name} className="slog-metacell"><ValuePill tag={c.name} role={c.role} value={element[c.name]} /></span>
            ))}</span>
          )}
        </div>
      </div>
      {isOpen && (
        <div className="tin-detail">
          <div className="tin-col">
            <div className="tin-col-h mono">TAGS · MODEL.V1.TAG[{detailConfig.length}] <span className="faint">key→value</span></div>
            <div className="tin-tags">
              {detailConfig.map((c) => (
                <div key={c.name} className="tin-tag">
                  <span className="tin-k mono">
                    {c.name}
                    {c.reserved && <span className={`tin-res is-${c.reserved}`}>{reservedLabel(c.reserved)}</span>}
                  </span>
                  <span className="tin-v"><ValuePill tag={c.name} role={c.role} value={element[c.name]} /></span>
                </div>
              ))}
            </div>
          </div>
          <div className="tin-col">
            {binding ? (
              <TDSpanDecoded bytes={bytes} element={element} binding={binding} tsField={tsField} />
            ) : (
              <>
                <div className="tin-col-h mono">SPAN · BYTES <span className="faint">opaque·not indexed</span></div>
                <SpanBytesPanel bytes={bytes} />
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function ValuePill({ tag, role, value }: {
  tag: string;
  role: SR_ROLE;
  value: unknown;
}) {
  if (value == null || value === '') return <span className="mono faint">∅</span>;

  if (role === 'binary') {
    const bytes = normalizeBytes(value);
    if (!bytes) return <span className="mono faint">∅</span>;
    return <span className="sbin">binary ({bytes.length} bytes)</span>;
  }

  if (role === 'list' && Array.isArray(value)) {
    return (
      <span className="sarr">
        {value.map((v, i) => (
          <span key={i} className="scat">{String(v)}</span>
        ))}
      </span>
    );
  }

  switch (role) {
    case 'time':
      return <span className="snum dim">{formatTimestamp(value)}</span>;
    case 'numeric': {
      const rendered = srRenderValue(role, value, tag);
      return <span className="snum strong">{rendered.display}</span>;
    }
    case 'id': {
      const rendered = srRenderValue(role, value, tag);
      return <span className="sid" title={rendered.title}>{rendered.display}</span>;
    }
    case 'cat': {
      const color = srCatColor(tag, value);
      return (
        <span
          className="scat"
          style={{ color, background: `color-mix(in srgb, ${color} 15%, transparent)`, borderColor: `color-mix(in srgb, ${color} 32%, transparent)` }}
        >
          {String(value)}
        </span>
      );
    }
    case 'body':
      return <span className="sbody">{String(value)}</span>;
    default:
      return <span className="mono dim">{String(value)}</span>;
  }
}

function DecodeBytesButton({ bytes, binding, onInspect }: { bytes: Uint8Array | null; binding: TDBinding | null; onInspect: () => void }) {
  const hasBytes = (bytes?.length ?? 0) > 0;
  const bound = !!binding;
  return (
    <button
      type="button"
      className={'sf-btn td-btn' + (bound ? ' is-bound' : '')}
      onClick={onInspect}
      disabled={!hasBytes}
      title={bound ? `Decoder bound: ${binding.fileName}` : hasBytes ? 'Decode span bytes against a bound .proto' : 'Expand a row with span bytes to decode'}
    >
      <IconBinary width={12} height={12} />
      {bound ? binding.fileName : 'Decode bytes'}
      {bound && <span className="td-dot" />}
    </button>
  );
}

const BYTES_PREVIEW_LIMIT = 64;

function copyText(text: string): boolean {
  try {
    if (navigator.clipboard) {
      navigator.clipboard.writeText(text).catch(() => { /* ignore */ });
      return true;
    }
  } catch { /* ignore */ }
  try {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
    return true;
  } catch {
    return false;
  }
}

function toBase64(bytes: Uint8Array): string {
  let s = '';
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
  try { return btoa(s); } catch { return s; }
}

interface PopPos {
  readonly left: number;
  readonly top: number;
  readonly maxH: number;
}

function SpanBytesPanel({ bytes }: { bytes: Uint8Array | null }) {
  const [open, setOpen] = React.useState(false);
  const [pos, setPos] = React.useState<PopPos | null>(null);
  const [copied, setCopied] = React.useState<'b64' | 'hex' | ''>('');
  const btnRef = React.useRef<HTMLButtonElement | null>(null);
  const len = bytes?.length ?? 0;
  const popoverWidth = 380;

  React.useEffect(() => {
    if (!open) return;
    const close = () => setOpen(false);
    window.addEventListener('scroll', close, true);
    window.addEventListener('resize', close);
    return () => {
      window.removeEventListener('scroll', close, true);
      window.removeEventListener('resize', close);
    };
  }, [open]);

  if (!bytes || len === 0) {
    return <div className="tin-raw"><div className="tin-raw-empty">(no span bytes)</div></div>;
  }

  const previewBytes = bytes.slice(0, BYTES_PREVIEW_LIMIT);
  const rows = tdHexDump(previewBytes, 12);
  const remaining = Math.max(0, len - BYTES_PREVIEW_LIMIT);

  const toggle = () => {
    if (open) {
      setOpen(false);
      return;
    }
    const rect = btnRef.current?.getBoundingClientRect();
    const left = rect ? Math.max(12, Math.min(rect.left, window.innerWidth - popoverWidth - 12)) : 12;
    const top = rect ? rect.bottom + 6 : 12;
    const maxH = Math.max(140, window.innerHeight - top - 18);
    setPos({ left, top, maxH });
    setOpen(true);
  };

  const copy = (what: 'b64' | 'hex', text: string) => {
    if (copyText(text)) {
      setCopied(what);
      setTimeout(() => setCopied(''), 1100);
    }
  };

  return (
    <span className="sbin-wrap">
      <button
        ref={btnRef}
        type="button"
        className={'tin-raw-size' + (open ? ' is-open' : '')}
        onClick={toggle}
        aria-expanded={open}
        title="Opaque DATA_BINARY — BanyanDB stores no encoding. Click to inspect bytes."
      >
        <IconBinary width={12} height={12} />{formatBytes(len)}<span className="sbin-caret">{open ? '▾' : '▸'}</span>
      </button>
      {open && pos && (
        <>
          <span className="sbin-backdrop" onClick={() => setOpen(false)} />
          <span
            className="sbin-pop"
            style={{ position: 'fixed', left: pos.left, top: pos.top, maxHeight: pos.maxH, width: popoverWidth }}
          >
            <span className="sbin-pop-head">
              <span className="mono">DATA_BINARY · {len.toLocaleString('en-US')} bytes</span>
              <span className="sbin-acts">
                <button type="button" onClick={() => copy('b64', toBase64(bytes))}>{copied === 'b64' ? 'copied' : 'base64'}</button>
                <button type="button" onClick={() => copy('hex', rows.map((r) => r.hex).join(' '))}>{copied === 'hex' ? 'copied' : 'hex'}</button>
              </span>
            </span>
            <span className="sbin-hex">
              {rows.map((r, i) => (
                <span key={i} className="sbin-hexrow">
                  <span className="sbin-off">{r.off}</span>
                  <span className="sbin-bytes">{r.hex}</span>
                  <span className="sbin-ascii">{r.ascii}</span>
                </span>
              ))}
            </span>
            <span className="sbin-note">
              {remaining > 0 ? `+ ${remaining.toLocaleString('en-US')} more bytes. ` : ''}
              Bytes are opaque to BanyanDB – decode in the client that wrote them.
            </span>
          </span>
        </>
      )}
    </span>
  );
}

function TDSpanDecoded({ bytes, element, binding, tsField }: { bytes: Uint8Array | null; element: Elem; binding: TDBinding; tsField: string }) {
  const [showRaw, setShowRaw] = React.useState(false);
  const decoded = React.useMemo(() => {
    const spanLike = {
      traceId: element.trace_id,
      spanId: element.span_id,
      parentSpanId: element.parent_span_id,
      timestamp: element[tsField] ?? element.timestamp,
      durationMs: element.duration_ms,
      service: element.service,
      endpoint: element.endpoint,
      status: element.status,
    };
    return tdDecode(binding, spanLike);
  }, [element, binding]);

  return (
    <>
      <div className="tin-col-h mono">
        SPAN · {showRaw ? 'BYTES' : 'DECODED'}
        <span className="faint">{showRaw ? 'opaque·not indexed' : `via ${binding.fileName} · ${binding.primary}`}</span>
        <button
          type="button"
          className="td-raw-toggle"
          onClick={() => setShowRaw((v) => !v)}
          title={showRaw ? 'Show the decoded payload' : 'Show the raw opaque bytes'}
        >
          {showRaw ? 'decoded' : 'raw bytes'}
        </button>
      </div>
      {showRaw ? (
        <SpanBytesPanel bytes={bytes} />
      ) : (
        <div className="td-decoded">
          <pre className="td-code"><code>{tdHighlightJSON(decoded)}</code></pre>
        </div>
      )}
    </>
  );
}

function TagRenderingPopover({ config, overrides, onClose, onToggle, onRoleChange }: {
  config: readonly TraceTagConfig[];
  overrides: Record<string, SR_ROLE>;
  onClose: () => void;
  onToggle: (name: string) => void;
  onRoleChange: (name: string, role: SR_ROLE | null) => void;
}) {
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
              value={overrides[c.name] ?? 'auto'}
              onChange={(e) => {
                const v = e.target.value;
                onRoleChange(c.name, v === 'auto' ? null : (v as SR_ROLE));
              }}
            >
              <option value="auto">auto · {ROLE_LABEL[c.inferred]}</option>
              {SR_ROLE_OPTIONS.filter((r) => r !== 'auto').map((r) => (
                <option key={r} value={r}>{ROLE_LABEL[r as SR_ROLE]}</option>
              ))}
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
  <svg {...p} viewBox="0 0 24 24" fill="currentColor" stroke="none">
    <text x="50%" y="54%" dominantBaseline="middle" textAnchor="middle" fontFamily="monospace" fontSize="12" fontWeight="700">{'{ }'}</text>
  </svg>
);
