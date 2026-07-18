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

// PropertyQuery.tsx — BydbQL query console scoped to a single Property
// collection. Ported from
// .handoff-import/banyandb/project/property-query.jsx: same QBSection
// accordion clauses (fold to one-line summaries after the first run), same
// pinned qb-foot with a one-line generated preview, same code-mode chrome —
// reusing QBSection/QBChips (qb-parts.js) and CodeEditor verbatim.
//
// ADAPTATION FROM THE HANDOFF: the mock's pqExecuteState filtered an
// in-memory `entries` prop; this port calls the real property/v1 Query RPC
// (queryPropertyDocuments) via property-bydbql.ts's pqBuildQueryRequest. Per
// docs/property-design.md §5, the Code tab is display-only in v1 (the
// builder is the source of truth for what actually executes) — code mode
// still round-trips through pqParseCode so hand-edited queries run too.

import React from 'react';
import { QBSection, QBChips } from './qb-parts.js';
import { CodeEditor } from './CodeEditor.js';
import { useRunPropertyQuery } from '../data/hooks.js';
import {
  PQ_ID, PROP_OPS, PROP_OP, QB_COMBINATORS, qbIsGroup, qbConn,
  pqNewCond, pqNewGroup, pqWhereRoot, pqDefault, buildPropertyBydbQL, pqParseCode, pqBuildQueryRequest,
  type PQBuilderState, type QBWhereNode, type QBWhereGroupWithConn, type QBWhereLeafWithConn,
} from './property-bydbql.js';
import type { PropertyDocument } from 'canopy-shared';

const IconClose = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M6 6l12 12M18 6 6 18" />
  </svg>
);
const IconPlus = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M12 5v14M5 12h14" />
  </svg>
);
const IconParens = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M9 4 H6 a2 2 0 0 0 -2 2 v3 a3 3 0 0 1 -3 3 a3 3 0 0 1 3 3 v3 a2 2 0 0 0 2 2 H9" />
    <path d="M15 4 h3 a2 2 0 0 1 2 2 v3 a3 3 0 0 0 3 3 a3 3 0 0 0 -3 3 v3 a2 2 0 0 1 -2 2 H15" />
  </svg>
);
const IconChev = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="m9 6 6 6-6 6" />
  </svg>
);
const IconPlay = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M6 4l14 8-14 8z" />
  </svg>
);
const IconArrowRight = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M5 12h14M13 5l7 7-7 7" />
  </svg>
);
const IconFormat = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M4 6h16M4 12h10M4 18h16" />
  </svg>
);
const IconQuery = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="4" width="18" height="16" rx="2" />
    <path d="M7 9l3 3-3 3" />
    <path d="M13 15h4" />
  </svg>
);

/* ===================================================================== */
/* WHERE builder UI — Property flavor (tag dropdown carries ID; no MATCH) */

interface PQConditionProps {
  readonly node: QBWhereLeafWithConn;
  readonly tagOptions: readonly { readonly value: string; readonly label: string }[];
  readonly onChange: (n: QBWhereLeafWithConn) => void;
  readonly onRemove: () => void;
}
function PQCondition({ node, tagOptions, onChange, onRemove }: PQConditionProps) {
  return (
    <div className="qb-cond">
      <span className="qb-select-wrap">
        <select aria-label="Field" value={node.tag} onChange={(e) => onChange({ ...node, tag: e.target.value })}>
          {tagOptions.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
        </select>
        <span className="qb-select-chev"><IconChev width={13} height={13} /></span>
      </span>
      <span className="qb-select-wrap">
        <select aria-label="Operator" value={node.op} onChange={(e) => onChange({ ...node, op: e.target.value })}>
          {PROP_OPS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
        </select>
        <span className="qb-select-chev"><IconChev width={13} height={13} /></span>
      </span>
      <input
        className="qb-input mono"
        aria-label="Value"
        value={node.value}
        placeholder={(node.op === 'BINARY_OP_IN' || node.op === 'BINARY_OP_NOT_IN') ? 'a, b, c' : 'value'}
        onChange={(e) => onChange({ ...node, value: e.target.value })}
      />
      <span className="qb-gap" />
      <button type="button" className="qb-del" title="Remove condition" onClick={onRemove}><IconClose width={14} height={14} /></button>
    </div>
  );
}

interface PQWhereGroupProps {
  readonly node: QBWhereGroupWithConn;
  readonly tagOptions: readonly { readonly value: string; readonly label: string }[];
  readonly depth: number;
  readonly onChange: (n: QBWhereGroupWithConn) => void;
  readonly onRemove?: () => void;
}
function PQWhereGroup({ node, tagOptions, depth, onChange, onRemove }: PQWhereGroupProps) {
  const children = node.children;
  const setChild = (i: number, c: QBWhereNode) => onChange({ ...node, children: children.map((x, idx) => (idx === i ? c : x)) });
  const delChild = (i: number) => onChange({ ...node, children: children.filter((_, idx) => idx !== i) });
  const addCond = () => onChange({ ...node, children: [...children, children.length ? { ...pqNewCond(), conn: 'AND' as const } : pqNewCond()] });
  const addGroup = () => onChange({ ...node, children: [...children, children.length ? { ...pqNewGroup(), conn: 'AND' as const } : pqNewGroup()] });
  const isRoot = depth === 0;
  return (
    <div className={'qb-group' + (isRoot ? ' is-root' : '')}>
      <div className="qb-group-head">
        <span className="qb-group-tag">{isRoot ? 'Conditions' : 'Group'}</span>
        {!isRoot && (
          <>
            <span className="qb-gap" />
            <button type="button" className="qb-del" title="Remove group" onClick={onRemove}><IconClose width={14} height={14} /></button>
          </>
        )}
      </div>
      <div className="qb-group-children">
        {children.length === 0 && <span className="qb-dim">no conditions yet</span>}
        {children.map((c, i) => (
          <div key={i} className="qb-child">
            {i > 0 && (
              <div className="qb-conn-seg" role="group" aria-label="Connector to previous condition"
                title="AND binds tighter than OR — use a group for explicit parentheses">
                {QB_COMBINATORS.map((cb) => (
                  <button
                    key={cb.value}
                    type="button"
                    className={'qb-conn-btn' + (qbConn(node, c) === cb.value ? ' is-on' : '')}
                    onClick={() => setChild(i, { ...c, conn: cb.value })}
                  >
                    {cb.label}
                  </button>
                ))}
              </div>
            )}
            {qbIsGroup(c) ? (
              <PQWhereGroup node={c} tagOptions={tagOptions} depth={depth + 1} onChange={(nc) => setChild(i, nc)} onRemove={() => delChild(i)} />
            ) : (
              <PQCondition node={c as QBWhereLeafWithConn} tagOptions={tagOptions} onChange={(nc) => setChild(i, nc)} onRemove={() => delChild(i)} />
            )}
          </div>
        ))}
      </div>
      <div className="qb-group-foot">
        <button type="button" className="qb-add" onClick={addCond}><IconPlus width={13} height={13} /> Add condition</button>
        <button type="button" className="qb-add" onClick={addGroup}><IconParens width={13} height={13} /> Add group</button>
      </div>
    </div>
  );
}

/* ===================================================================== */

function pqStoreKey(groupName: string, propName: string): string {
  return 'canopy.propq.' + groupName + '::' + propName;
}
interface PQPersisted {
  readonly mode: 'builder' | 'code';
  readonly builder: PQBuilderState;
  readonly code: string;
  readonly codeDirty: boolean;
}
function pqLoad(groupName: string, propName: string): PQPersisted {
  try {
    const raw = localStorage.getItem(pqStoreKey(groupName, propName));
    if (raw) {
      const p = JSON.parse(raw) as Partial<PQPersisted>;
      if (p?.builder) return { mode: p.mode ?? 'builder', builder: p.builder, code: p.code ?? '', codeDirty: p.codeDirty ?? false };
    }
  } catch { /* ignore */ }
  return { mode: 'builder', builder: pqDefault(), code: '', codeDirty: false };
}

export interface PropertyQueryResult {
  readonly documents: readonly PropertyDocument[];
  readonly error: string | null;
}

export interface PropertyQueryProps {
  readonly groupName: string;
  readonly propName: string;
  /** Tag names observed across this collection's documents (schema-free union). */
  readonly tags: readonly string[];
  readonly onResult: (res: PropertyQueryResult) => void;
  /** Bumped by the parent after a document CRUD action so the last-applied query re-runs. */
  readonly refreshToken?: number;
}

export function PropertyQuery({ groupName, propName, tags, onResult, refreshToken }: PropertyQueryProps) {
  const boot = React.useMemo(() => pqLoad(groupName, propName), [groupName, propName]);
  const [mode, setMode] = React.useState<'builder' | 'code'>(boot.mode);
  const [b, setB] = React.useState<PQBuilderState>(boot.builder);
  const [code, setCode] = React.useState(boot.code);
  const [codeDirty, setCodeDirty] = React.useState(boot.codeDirty);
  const [status, setStatus] = React.useState<{ ms?: string; n?: number; error?: string } | null>(null);
  const [ran, setRan] = React.useState(false);
  const [openKw, setOpenKw] = React.useState<string | null>(null);
  const runMutation = useRunPropertyQuery();

  const generated = React.useMemo(() => buildPropertyBydbQL(b, propName, groupName), [b, propName, groupName]);
  const set = (patch: Partial<PQBuilderState>) => setB((c) => ({ ...c, ...patch }));

  const tagOptions = React.useMemo(
    () => [{ value: PQ_ID, label: 'ID — document id' }, ...tags.map((n) => ({ value: n, label: n }))],
    [tags],
  );

  // persist
  React.useEffect(() => {
    try { localStorage.setItem(pqStoreKey(groupName, propName), JSON.stringify({ mode, builder: b, code, codeDirty })); } catch { /* quota */ }
  }, [mode, b, code, codeDirty, groupName, propName]);

  const where = pqWhereRoot(b);
  const setWhere = (node: QBWhereGroupWithConn) => set({ where: node });
  const addFirstCond = () => setWhere({ combinator: 'AND', children: [pqNewCond()] });

  const ejectToCode = () => { setCode(generated); setCodeDirty(false); setMode('code'); setStatus(null); };
  const backToBuilder = () => {
    if (codeDirty && code.trim() && code.trim() !== generated.trim()) {
      // matches the handoff's confirm-before-discard UX (window.confirm, same as QueryConsole.tsx)
      if (!window.confirm('Switch back to the visual builder?\n\nManual edits to the query will be discarded and the query will be regenerated from the builder.')) return;
    }
    setCodeDirty(false); setMode('builder'); setStatus(null);
  };
  const reset = () => {
    setB(pqDefault()); setCode(''); setCodeDirty(false);
    setStatus(null); setMode('builder'); setRan(false); setOpenKw(null);
  };

  const run = React.useCallback(() => {
    setOpenKw(null); // fold all clauses — the document list gets the viewport
    const t0 = performance.now();
    let next: PQBuilderState;
    if (mode === 'code') {
      try {
        next = pqParseCode(code);
        setCodeDirty(false);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        onResult({ documents: [], error: message });
        setStatus({ error: message });
        return;
      }
    } else {
      next = b;
    }
    const req = pqBuildQueryRequest(next, groupName, propName);
    runMutation.mutate(req, {
      onSuccess: (res) => {
        const ms = Math.max(0.1, performance.now() - t0);
        setRan(true);
        setStatus({ ms: ms.toFixed(1), n: res.documents.length });
        onResult({ documents: res.documents, error: null });
      },
      onError: (err) => {
        setStatus({ error: err.message });
        onResult({ documents: [], error: err.message });
      },
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps -- runMutation identity is stable across renders (useMutation)
  }, [mode, code, b, groupName, propName, onResult]);

  const runRef = React.useRef(run);
  runRef.current = run;
  // Re-run the last-applied query after a document CRUD action (Apply/Delete)
  // bumps refreshToken — mirrors the handoff's "last applied query re-executes
  // when documents change" behavior, now driven by a real re-query instead of
  // re-filtering a static `entries` prop.
  React.useEffect(() => {
    if (refreshToken === undefined || !ran) return;
    runRef.current();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- only re-run on refreshToken bumps, not every render
  }, [refreshToken]);

  // Cmd/Ctrl+Enter to run, in either mode — mirrors QueryConsole's shortcut.
  React.useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); runRef.current(); }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  const accOn = ran;
  const countConds = (n: QBWhereNode): number => (qbIsGroup(n) ? n.children.reduce((s, c) => s + countConds(c), 0) : 1);
  const condTxt = (c: QBWhereLeafWithConn) => (c.tag === PQ_ID ? 'ID' : c.tag) + ' ' + PROP_OP(c.op).sql + ' ' + (c.value !== '' ? c.value : '…');
  const nConds = where.children.length ? countConds(where) : 0;
  const qbConnSummaryOf = (root: QBWhereGroupWithConn): 'AND' | 'OR' | 'mixed' => {
    const conns = root.children.slice(1).map((c) => qbConn(root, c));
    if (conns.length === 0) return 'AND';
    return conns.every((x) => x === conns[0]) ? conns[0] : 'mixed';
  };
  const sums = {
    from: 'property ' + propName + ' in ' + groupName,
    select: (b.projection ?? []).length ? b.projection.join(', ') : 'all tags',
    where: nConds === 0 ? 'no filters'
      : nConds === 1 && !qbIsGroup(where.children[0]) ? condTxt(where.children[0] as QBWhereLeafWithConn)
      : nConds + ' conditions · ' + qbConnSummaryOf(where),
    order: (b.orderField ? (b.orderField === PQ_ID ? 'ID' : b.orderField) + ' ' + (b.orderDir || 'ASC').toLowerCase() + ' · ' : '')
      + (b.limit ? 'limit ' + b.limit : 'all documents'),
  };
  const sp = (key: string, kw: string, sum: string) => ({
    acc: accOn, kw, sum, optional: key === 'where' || key === 'order',
    open: openKw === key, onToggle: () => setOpenKw(openKw === key ? null : key),
  });

  const isRunning = runMutation.isPending;

  return (
    <div className="propq">
      <div className="propq-bar">
        <div className="propq-bar-left">
          <IconQuery width={15} height={15} />
          <span className="propq-bar-title">Query documents</span>
          <span className="propq-bar-sub">BydbQL · property</span>
        </div>
        <div className="qb-mode-seg" role="tablist" aria-label="Query mode">
          <button type="button" role="tab" aria-selected={mode === 'builder'} className={'qb-mode-btn' + (mode === 'builder' ? ' is-on' : '')}
            onClick={() => (mode === 'builder' ? undefined : backToBuilder())}>
            Builder
          </button>
          <button type="button" role="tab" aria-selected={mode === 'code'} className={'qb-mode-btn' + (mode === 'code' ? ' is-on' : '')}
            onClick={() => (mode === 'code' ? undefined : ejectToCode())}>
            Code
          </button>
        </div>
      </div>

      {mode === 'builder' ? (
        <div className="qb-card" role="region" aria-label="Property query builder">
          <QBSection {...sp('from', 'FROM', sums.from)} hint="Scoped to this property. Property queries don’t take a TIME clause.">
            <div className="qb-row qb-from-row">
              <span className="qb-inline-kw">PROPERTY</span>
              <span className="propq-lock mono">{propName}</span>
              <span className="qb-inline-kw">IN</span>
              <span className="propq-lock mono">{groupName}</span>
              <span className="qb-gap" />
              <span className="f-lock">locked</span>
            </div>
          </QBSection>

          <QBSection {...sp('select', 'SELECT', sums.select)} hint="Project specific tags onto each document, or return all tags.">
            <QBChips value={b.projection} options={tags} allLabel="all tags" onChange={(projection) => set({ projection })} />
          </QBSection>

          <QBSection {...sp('where', 'WHERE', sums.where)} hint="Filter by the document ID or by tag values. Combine with AND / OR, and nest groups for precedence.">
            {where.children.length === 0 ? (
              <button type="button" className="qb-add" onClick={addFirstCond}><IconPlus width={13} height={13} /> Add condition</button>
            ) : (
              <PQWhereGroup node={where} tagOptions={tagOptions} depth={0} onChange={setWhere} />
            )}
          </QBSection>

          <QBSection {...sp('order', 'ORDER BY', sums.order)} hint="Sort by a tag or the document ID, and cap the number of documents returned.">
            <div className="qb-row">
              <span className="qb-select-wrap">
                <select aria-label="Order field" value={b.orderField} onChange={(e) => set({ orderField: e.target.value })}>
                  <option value="">— unordered —</option>
                  {tagOptions.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
                </select>
                <span className="qb-select-chev"><IconChev width={13} height={13} /></span>
              </span>
              <div className="qb-dir-seg">
                <button type="button" className={'qb-dir-btn' + (b.orderDir === 'ASC' ? ' is-on' : '')} onClick={() => set({ orderDir: 'ASC' })} disabled={!b.orderField}>ASC</button>
                <button type="button" className={'qb-dir-btn' + (b.orderDir === 'DESC' ? ' is-on' : '')} onClick={() => set({ orderDir: 'DESC' })} disabled={!b.orderField}>DESC</button>
              </div>
              <span className="qb-gap" />
              <span className="qb-inline-kw">LIMIT</span>
              <input
                className="qb-input qb-num mono"
                type="number"
                min={1}
                aria-label="Limit"
                value={b.limit}
                placeholder="all"
                onChange={(e) => set({ limit: e.target.value === '' ? '' : Math.max(1, parseInt(e.target.value, 10) || 1) })}
              />
            </div>
          </QBSection>

          <div className="qb-foot">
            <span className="lang-pill">BydbQL</span>
            <code className="qb-gen-line mono" title={generated}>{generated.replace(/\s*\n\s*/g, ' ')}</code>
            <button type="button" className="qb-eject" onClick={ejectToCode}>
              Edit as code <IconArrowRight width={13} height={13} />
            </button>
            {status?.ms != null && <span className="rs ok propq-ran">executed in {status.ms} ms · {status.n} document{status.n === 1 ? '' : 's'}</span>}
            <button type="button" className="btn btn-ghost" onClick={reset} title="Reset query"><IconFormat width={15} height={15} /></button>
            <button type="button" className="btn btn-primary" disabled={isRunning} onClick={() => run()}>
              <IconPlay width={15} height={15} /> {isRunning ? 'Running…' : 'Run'}<kbd className="kbd">{typeof navigator !== 'undefined' && /Mac/i.test(navigator.platform) ? '⌘↵' : 'Ctrl↵'}</kbd>
            </button>
          </div>
        </div>
      ) : (
        <>
          <CodeEditor
            value={code}
            onChange={(v) => { setCode(v); setCodeDirty(true); }}
            hint={status?.error ? 'parse error — fix and run' : codeDirty ? 'edited — run to apply' : 'from builder'}
            ariaLabel="Property query code editor"
            toolbarRight={(
              <button type="button" className="btn btn-ghost" onClick={backToBuilder} title="Back to builder">
                <IconArrowRight width={15} height={15} style={{ transform: 'rotate(180deg)' }} /> Builder
              </button>
            )}
          />
          <div className="qb-foot">
            <button type="button" className="btn btn-ghost" onClick={() => { setCode(generated); setCodeDirty(false); setStatus(null); }}>
              <IconFormat width={15} height={15} /> Re-sync
            </button>
            <span className="qb-gap" />
            {status?.ms != null && <span className="rs ok propq-ran">executed in {status.ms} ms · {status.n} document{status.n === 1 ? '' : 's'}</span>}
            <button type="button" className="btn btn-primary" disabled={isRunning} onClick={() => run()}>
              <IconPlay width={15} height={15} /> {isRunning ? 'Running…' : 'Run'}<kbd className="kbd">{typeof navigator !== 'undefined' && /Mac/i.test(navigator.platform) ? '⌘↵' : 'Ctrl↵'}</kbd>
            </button>
          </div>
          {status?.error && <div className="propq-err">{status.error}</div>}
        </>
      )}
    </div>
  );
}
