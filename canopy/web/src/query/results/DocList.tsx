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

// DocList.tsx — Property document list, "field rows" style (option A of the
// Property Document Styles exploration). One row per tag with an adaptive
// value cell: scalars inline, str values that parse as JSON get an embedded
// highlighted block (pretty <-> raw, copy, expand/collapse), long prose wraps
// with a clamp. BanyanDB has no JSON type — the stored type pill stays str; a
// dashed badge marks detection. The list paginates past DL_PAGE_SIZE.
// Ported from .handoff-import/banyandb/project/doc-list.jsx (window-global
// JSX -> ES module TSX). `RoleContext.canWrite` -> useCanWrite() (AuthContext.js).

import React from 'react';
import { useCanWrite } from '../../auth/AuthContext.js';
import { IconEdit, IconTrash, IconCheck } from '../../components/icons.js';
import { looksLikeJSON, PROP_VALUE_LABEL } from '../property-util.js';
import type { PropertyDocument, PropertyDocTag } from 'canopy-shared';

const DL_PAGE_SIZE = 10;

const IconExpand = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M8 3H5a2 2 0 0 0-2 2v3M16 3h3a2 2 0 0 1 2 2v3M8 21H5a2 2 0 0 1-2-2v-3M16 21h3a2 2 0 0 0 2-2v-3" />
  </svg>
);
const IconCopy = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <rect x="9" y="9" width="12" height="12" rx="2" />
    <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
  </svg>
);
const IconBraces = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M8 3a2 2 0 0 0-2 2v3a2 2 0 0 1-2 2 2 2 0 0 1 2 2v3a2 2 0 0 0 2 2M16 3a2 2 0 0 1 2 2v3a2 2 0 0 0 2 2 2 2 0 0 0-2 2v3a2 2 0 0 1-2 2" />
  </svg>
);

interface HighlightPart { readonly cls: string; readonly s: string; }

/* ---------------- value detection (presentation, not schema) ---------------- */
interface Detected {
  readonly kind: 'json' | 'text' | 'scalar';
  readonly val: string;
  readonly parsed?: unknown;
}
function dlDetect(tag: PropertyDocTag): Detected {
  const val = tag.value ?? '';
  if (tag.valueType !== 'str') return { kind: 'scalar', val };
  if (looksLikeJSON(val)) {
    try {
      return { kind: 'json', val, parsed: JSON.parse(val) as unknown };
    } catch { /* fall through */ }
  }
  if (val.length > 80 || val.indexOf('\n') !== -1) return { kind: 'text', val };
  return { kind: 'scalar', val };
}

/* ---------------- JSON pretty-printer + highlighter ---------------- */
function dlHighlight(value: unknown): HighlightPart[][] {
  const out: HighlightPart[][] = [];
  let line: HighlightPart[] = [];
  const push = (cls: string, s: string) => line.push({ cls, s });
  const nl = () => { out.push(line); line = []; };
  const pad = (d: number) => push('tok-punc', '  '.repeat(d));
  const walk = (v: unknown, d: number) => {
    if (v === null) { push('tok-kw', 'null'); return; }
    if (typeof v === 'boolean') { push('tok-kw', String(v)); return; }
    if (typeof v === 'number') { push('tok-num', String(v)); return; }
    if (typeof v === 'string') { push('tok-str', JSON.stringify(v)); return; }
    if (Array.isArray(v)) {
      const flat = v.every((x) => typeof x !== 'object' || x === null);
      push('tok-punc', '[');
      if (v.length && (!flat || JSON.stringify(v).length > 48)) {
        nl();
        v.forEach((x, i) => { pad(d + 1); walk(x, d + 1); if (i < v.length - 1) push('tok-punc', ','); nl(); });
        pad(d);
      } else {
        v.forEach((x, i) => { walk(x, d); if (i < v.length - 1) push('tok-punc', ', '); });
      }
      push('tok-punc', ']');
      return;
    }
    const obj = v as Record<string, unknown>;
    const keys = Object.keys(obj);
    push('tok-punc', '{');
    if (keys.length) {
      nl();
      keys.forEach((k, i) => {
        pad(d + 1);
        push('tok-key', JSON.stringify(k));
        push('tok-punc', ': ');
        const child = obj[k];
        if (child && typeof child === 'object' && !Array.isArray(child) && JSON.stringify(child).length <= 76) {
          push('tok-punc', '{ ');
          const ck = Object.keys(child as Record<string, unknown>);
          ck.forEach((c, ci) => {
            push('tok-key', JSON.stringify(c));
            push('tok-punc', ': ');
            walk((child as Record<string, unknown>)[c], d + 1);
            if (ci < ck.length - 1) push('tok-punc', ', ');
          });
          push('tok-punc', ' }');
        } else {
          walk(child, d + 1);
        }
        if (i < keys.length - 1) push('tok-punc', ',');
        nl();
      });
      pad(d);
    }
    push('tok-punc', '}');
  };
  walk(value, 0);
  nl();
  return out.filter((l) => l.length);
}

function DLCode({ lines }: { readonly lines: readonly (readonly HighlightPart[])[] }) {
  return (
    <pre className="pd-code">
      {lines.map((parts, i) => (
        <span key={i} className="ln">
          <span className="ln-n">{i + 1}</span>
          <span className="ln-c">{parts.map((p, j) => <span key={j} className={p.cls}>{p.s}</span>)}</span>
        </span>
      ))}
    </pre>
  );
}

/* ---------------- small controls ---------------- */
function DLCopy({ text }: { readonly text: string }) {
  const [state, setState] = React.useState<'ok' | 'fail' | null>(null);
  const flash = (s: 'ok' | 'fail') => { setState(s); setTimeout(() => setState(null), 1400); };
  const copy = async () => {
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        const ta = document.createElement('textarea');
        ta.value = text;
        document.body.appendChild(ta);
        ta.select();
        const ok = document.execCommand('copy');
        document.body.removeChild(ta);
        if (!ok) throw new Error('copy rejected');
      }
      flash('ok');
    } catch { flash('fail'); }
  };
  return (
    <button type="button" className="pd-ibtn" onClick={() => void copy()}>
      {state === 'ok' ? <IconCheck size={12} /> : <IconCopy width={12} height={12} />}{' '}
      {state === 'ok' ? 'copied' : state === 'fail' ? 'copy failed' : 'copy'}
    </button>
  );
}

function DLDet({ kind }: { readonly kind: 'json' | 'text' }) {
  if (kind === 'json') return <span className="pd-det" title="This str parses as JSON — pretty-printed for display">{'{}'} json</span>;
  return <span className="pd-det is-text" title="Long text — wrapped for display">¶ text</span>;
}

/* ---------------- adaptive value cells ---------------- */
function DLJsonValue({ det }: { readonly det: Detected }) {
  const lines = React.useMemo(() => dlHighlight(det.parsed), [det.val]); // eslint-disable-line react-hooks/exhaustive-deps
  const rawLines = React.useMemo(() => det.val.split('\n').map((s) => [{ cls: 'tok-punc', s }]), [det.val]);
  const big = lines.length > 8;
  const [open, setOpen] = React.useState(!big);
  const [raw, setRaw] = React.useState(false);
  const shown = raw ? rawLines : lines;
  return (
    <div className={'fr-block' + (open ? '' : ' is-collapsed')}>
      <div className="fr-block-bar">
        <span style={{ color: 'var(--tok-key)' }}><IconBraces width={12} height={12} /></span>
        <span className="pd-meta">str · parses as JSON · {lines.length} lines</span>
        <span className="pd-gap" />
        <div className="pd-seg" role="group" aria-label="JSON display mode">
          <button type="button" className={raw ? '' : 'is-on'} onClick={() => setRaw(false)}>pretty</button>
          <button type="button" className={raw ? 'is-on' : ''} onClick={() => setRaw(true)}>raw</button>
        </div>
        <DLCopy text={det.val} />
        {big && (
          <button type="button" className="pd-ibtn" onClick={() => setOpen(!open)}>
            <IconExpand width={12} height={12} /> {open ? 'collapse' : 'expand'}
          </button>
        )}
      </div>
      <div
        className="fr-block-body"
        onClick={open ? undefined : () => setOpen(true)}
        style={open ? undefined : { cursor: 'pointer' }}
        title={open ? undefined : 'Click to expand'}
      >
        <DLCode lines={shown} />
      </div>
    </div>
  );
}

function DLTextValue({ det }: { readonly det: Detected }) {
  const paras = det.val.split(/\n+/).filter(Boolean);
  const long = det.val.length > 220 || paras.length > 1;
  const [open, setOpen] = React.useState(!long);
  return (
    <div>
      <div className={'fr-prose' + (open ? '' : ' is-clamped')}>
        {open ? paras.map((p, i) => <span key={i}>{i > 0 && <><br /><br /></>}{p}</span>) : paras[0]}
      </div>
      {long && (
        <button type="button" className="fr-more" onClick={() => setOpen(!open)}>{open ? 'collapse ▴' : 'show all ▾'}</button>
      )}
    </div>
  );
}

function DLValue({ tag }: { readonly tag: PropertyDocTag }) {
  const det = dlDetect(tag);
  if (det.kind === 'json') return <DLJsonValue det={det} />;
  if (det.kind === 'text') return <DLTextValue det={det} />;
  if (tag.valueType === 'int') return <span className="fr-val-int mono">{det.val}</span>;
  return <span className="mono">{det.val === '' ? '∅' : det.val}</span>;
}

/* ---------------- document card ---------------- */
interface DocCardProps {
  readonly entry: PropertyDocument;
  readonly projection?: readonly string[];
  readonly groupName: string;
  readonly propName: string;
  readonly onEdit: () => void;
  readonly onDelete: () => void;
}
function DocCard({ entry, projection, groupName, propName, onEdit, onDelete }: DocCardProps) {
  const canWrite = useCanWrite();
  const tags = projection && projection.length
    ? (entry.tags ?? []).filter((t) => projection.includes(t.key))
    : (entry.tags ?? []);
  return (
    <div className="doc-card" data-testid="doc-card">
      <div className="doc-head">
        <span className="doc-id">
          <span className="doc-id-key mono">{groupName}/{propName}/</span>
          <span className="doc-id-val mono">{entry.id}</span>
        </span>
        <span className="doc-actions">
          <span className="doc-tagcount">{(entry.tags ?? []).length} tag{(entry.tags ?? []).length !== 1 ? 's' : ''}</span>
          {canWrite && (
            <>
              <button type="button" className="rc-act" title="Edit document" onClick={onEdit}><IconEdit size={15} /></button>
              <button type="button" className="rc-act is-danger" title="Delete document" onClick={onDelete}><IconTrash size={15} /></button>
            </>
          )}
        </span>
      </div>
      <div className="fr-rows">
        {tags.map((t, i) => {
          const det = dlDetect(t);
          return (
            <div key={t.key + i} className="fr-row">
              <span className="fr-keycol">
                <span className="fr-key">{t.key}</span>
                <span className={'pd-type' + (t.valueType === 'int' ? ' is-int' : '')}>{PROP_VALUE_LABEL(t.valueType)}</span>
                {(det.kind === 'json' || det.kind === 'text') && <DLDet kind={det.kind} />}
              </span>
              <div className="fr-val"><DLValue tag={t} /></div>
            </div>
          );
        })}
        {!tags.length && (
          <div className="fr-row">
            <span className="fr-keycol" />
            <span className="dim" style={{ fontStyle: 'italic', fontSize: 12.5 }}>{projection?.length ? 'no projected tags' : 'no tags'}</span>
          </div>
        )}
      </div>
    </div>
  );
}

/* ---------------- the paged list ---------------- */
export interface DocListProps {
  readonly rows: readonly PropertyDocument[];
  readonly projection?: readonly string[];
  readonly groupName: string;
  readonly propName: string;
  readonly onEditEntry: (entry: PropertyDocument) => void;
  readonly onDeleteEntry: (entry: PropertyDocument) => void;
}
export function DocList({ rows, projection, groupName, propName, onEditEntry, onDeleteEntry }: DocListProps) {
  const [page, setPage] = React.useState(0);
  const pages = Math.max(1, Math.ceil(rows.length / DL_PAGE_SIZE));
  const cur = Math.min(page, pages - 1); // clamp when a query shrinks the list
  React.useEffect(() => { if (page !== cur) setPage(cur); }, [page, cur]);
  const start = cur * DL_PAGE_SIZE;
  const slice = rows.slice(start, start + DL_PAGE_SIZE);
  return (
    <div className="doc-list" role="region" aria-label="Query results">
      {slice.map((e) => (
        <DocCard
          key={e.id}
          entry={e}
          projection={projection}
          groupName={groupName}
          propName={propName}
          onEdit={() => onEditEntry(e)}
          onDelete={() => onDeleteEntry(e)}
        />
      ))}
      {rows.length > DL_PAGE_SIZE && (
        <div className="doc-pager">
          <span className="mono">showing {start + 1}–{Math.min(start + DL_PAGE_SIZE, rows.length)} of {rows.length} documents</span>
          <span className="doc-pager-btns">
            <button type="button" className="pg-btn" disabled={cur === 0} onClick={() => setPage(cur - 1)}>← Prev</button>
            <span className="doc-pager-page mono">{cur + 1} / {pages}</span>
            <button type="button" className="pg-btn" disabled={cur >= pages - 1} onClick={() => setPage(cur + 1)}>Next →</button>
          </span>
        </div>
      )}
      {rows.length === 0 && (
        <div className="empty">
          <p className="empty-title">No documents match this query</p>
        </div>
      )}
    </div>
  );
}
