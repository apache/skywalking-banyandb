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

// QueryBuilder.tsx — visual BydbQL query builder.
// Ported from .handoff-import/banyandb/project/query-builder.jsx. Faithful
// re-implementation: QB_CATALOG control + Resource picker + SELECT chips +
// WHERE tree + Time + ORDER BY + LIMIT + Trace toggle (footer button).
//
// The builder mutates a QBBuilderState from bydbql.ts. The consumer
// (QueryConsole) wires changes back into the shared state and re-generates
// BydbQL via buildBydbQL(state).

import React from 'react';
import {
  QB_CATALOGS, QB_CAT, QB_OPS, QB_OP, QB_AGGS, QB_TOPN_OPS, QB_TOPN_AGGS, QB_TIMES, QB_COMBINATORS,
  qbDataCatalog, qbIsGroup, qbNewCond, qbNewGroup, qbEmptyWhere, qbPruneWhere,
  qbSearchIndex, qbSearchResults, qbConnSummary,
  buildBydbQL,
  type QBWhereNode, type QBWhereLeafWithConn,
  type QBBuilderState, type QB_CATALOG_VALUE, type QBSearchHit, type GroupResourcesMap,
} from './bydbql.js';
import type { Group } from 'canopy-shared';

// Catalog icons — same paths as the handoff's icons.jsx (Measure / Stream /
// Trace / Top-N). Inline SVG so the rail matches without a separate import.
const IconMeasures = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M3 12h4l2-6 4 13 2-7h6" />
  </svg>
);
const IconStreams = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M3 8c2.5 0 2.5 2.4 5 2.4S10.5 8 13 8s2.5 2.4 5 2.4S20.5 8 21 8" />
    <path d="M3 15c2.5 0 2.5 2.4 5 2.4S10.5 15 13 15s2.5 2.4 5 2.4 2.5-2.4 3-2.4" />
  </svg>
);
const IconTraces = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="5" cy="6" r="2" />
    <circle cx="5" cy="18" r="2" />
    <circle cx="19" cy="12" r="2" />
    <path d="M7 6h6a4 4 0 0 1 4 4v.4M7 18h6a4 4 0 0 0 4-4v-.4" />
  </svg>
);
const IconTopN = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3.5" y="5" width="4.2" height="15" rx="1" />
    <rect x="9.9" y="9.5" width="4.2" height="10.5" rx="1" />
    <rect x="16.3" y="14" width="4.2" height="6" rx="1" />
  </svg>
);

const CATALOG_ICON: Record<QB_CATALOG_VALUE, React.FC<React.SVGProps<SVGSVGElement>>> = {
  measures: IconMeasures as React.FC<React.SVGProps<SVGSVGElement>>,
  streams: IconStreams as React.FC<React.SVGProps<SVGSVGElement>>,
  traces: IconTraces as React.FC<React.SVGProps<SVGSVGElement>>,
  topn: IconTopN as React.FC<React.SVGProps<SVGSVGElement>>,
};

const IconChev = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="m9 6 6 6-6 6" />
  </svg>
);
const IconCalendar = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="5" width="18" height="16" rx="2" />
    <path d="M3 10h18M8 3v4M16 3v4" />
  </svg>
);
const IconClock = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="9" />
    <path d="M12 7v5l3 2" />
  </svg>
);
const IconArrowRight = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M5 12h14M13 5l7 7-7 7" />
  </svg>
);
const IconClose = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M6 6l12 12M18 6 6 18" />
  </svg>
);
const IconSearch = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="11" cy="11" r="7" />
    <path d="m21 21-4.3-4.3" />
  </svg>
);
const IconPlay = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M6 4l14 8-14 8z" />
  </svg>
);
// Two parenthesised arcs — matches the handoff's IconParens that
// prefixes the "Add group" button so users can spot nested groups
// at a glance.
const IconParens = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M9 4 H6 a2 2 0 0 0 -2 2 v3 a3 3 0 0 1 -3 3 a3 3 0 0 1 3 3 v3 a2 2 0 0 0 2 2 H9" />
    <path d="M15 4 h3 a2 2 0 0 1 2 2 v3 a3 3 0 0 0 3 3 a3 3 0 0 0 -3 3 v3 a2 2 0 0 1 -2 2 H15" />
  </svg>
);

/* ---- fuzzy resource search box (From row) ---- */
// Render a string with matched characters highlighted via <mark className="qb-fz-hl">.
function QBHighlight({ text, marks }: { readonly text: string; readonly marks: readonly number[] }) {
  if (!marks || !marks.length) return <>{text}</>;
  const set = new Set<number>(marks);
  return (
    <>
      {text.split('').map((ch, i) => (set.has(i) ? <mark key={i} className="qb-fz-hl">{ch}</mark> : ch))}
    </>
  );
}

interface QBResourceSearchProps {
  readonly groups: readonly Group[];
  readonly groupResources: GroupResourcesMap;
  readonly onPick: (catalog: QB_CATALOG_VALUE, group: string, resource: string) => void;
}

// Search box: type a name, arrow/enter to jump to that resource across every
// catalog. Ported from the handoff's QBResourceSearch in
// .handoff-import/banyandb/project/query-builder.jsx.
function QBResourceSearch({ groups, groupResources, onPick }: QBResourceSearchProps) {
  const [q, setQ] = React.useState('');
  const [open, setOpen] = React.useState(false);
  const [active, setActive] = React.useState(0);
  const wrapRef = React.useRef<HTMLDivElement | null>(null);
  const inputRef = React.useRef<HTMLInputElement | null>(null);

  const index = React.useMemo(() => qbSearchIndex(groups, groupResources), [groups, groupResources]);
  const results = React.useMemo(() => qbSearchResults(index, q, 8), [index, q]);

  React.useEffect(() => { setActive(0); }, [q]);
  React.useEffect(() => {
    const onDoc = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, []);

  const pick = (it: QBSearchHit) => {
    onPick(it.catalog, it.group, it.resource);
    setQ('');
    setOpen(false);
    if (inputRef.current) inputRef.current.blur();
  };

  const onKey = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Escape') { setOpen(false); return; }
    if (!results.length) return;
    if (e.key === 'ArrowDown') { e.preventDefault(); setActive((a) => Math.min(results.length - 1, a + 1)); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setActive((a) => Math.max(0, a - 1)); }
    else if (e.key === 'Enter') { e.preventDefault(); pick(results[active]); }
  };

  const showMenu = open && q.trim().length > 0;
  return (
    <div className="qb-search" ref={wrapRef}>
      <span className="qb-search-ico"><IconSearch width={15} height={15} /></span>
      <input
        ref={inputRef}
        className="qb-search-input"
        value={q}
        placeholder="Search any measure, stream, trace or Top-N by name…"
        aria-label="Search resources by name"
        autoComplete="off"
        spellCheck={false}
        onChange={(e) => { setQ(e.target.value); setOpen(true); }}
        onFocus={() => setOpen(true)}
        onKeyDown={onKey}
      />
      {q && (
        <button
          type="button"
          className="qb-search-clear"
          title="Clear search"
          aria-label="Clear search"
          onMouseDown={(e) => e.preventDefault()}
          onClick={() => { setQ(''); if (inputRef.current) inputRef.current.focus(); }}
        >
          <IconClose width={13} height={13} />
        </button>
      )}
      {showMenu && (
        <div className="qb-search-menu" role="listbox">
          {results.length === 0 ? (
            <div className="qb-search-empty">No measure, stream, trace or Top-N named &ldquo;{q.trim()}&rdquo;</div>
          ) : results.map((it, i) => {
            const Ico = CATALOG_ICON[it.catalog];
            return (
              <button
                key={`${it.catalog}/${it.group}/${it.resource}`}
                type="button"
                role="option"
                aria-selected={i === active}
                className={'qb-search-item' + (i === active ? ' is-active' : '')}
                onMouseEnter={() => setActive(i)}
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => pick(it)}
              >
                <span className="qb-search-badge">{Ico && <Ico width={13} height={13} />} {it.label}</span>
                <span className="qb-search-name mono"><QBHighlight text={it.resource} marks={it.marks} /></span>
                <span className="qb-search-grp mono">in <QBHighlight text={it.group} marks={it.gmarks} /></span>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

export interface QueryBuilderProps {
  readonly state: QBBuilderState;
  readonly onChange: (patch: Partial<QBBuilderState>) => void;
  readonly tags: readonly string[];
  readonly fields: readonly string[];
  readonly groupNames: readonly string[];
  readonly resourceNames: readonly string[];
  /** All groups (used by the From-row fuzzy search to enumerate every catalog). */
  readonly groups: readonly Group[];
  /** Pre-fetched resources keyed by `${dataCatalog}/${groupName}`. */
  readonly groupResources: GroupResourcesMap;
  /** Called when the user picks a hit in the From-row fuzzy search. */
  readonly onPickResource: (catalog: QB_CATALOG_VALUE, group: string, resource: string) => void;
  readonly isRunning: boolean;
  readonly onEjectToCode: () => void;
  readonly onRun: () => void;
  /** True once the query has been run at least once — collapses clauses into accordion summaries. */
  readonly hasRun?: boolean;
  /** Compact accordion mode (default true); false expands every clause like pre-run. */
  readonly compact?: boolean;
  readonly setCompact?: (v: boolean) => void;
  /** Key of the currently expanded accordion section, or null when all are collapsed. */
  readonly openSection?: string | null;
  readonly setOpenSection?: (v: string | null) => void;
}

/* ---- QBRow: one row in the WHERE list — either a leaf condition or a
   nested group. Renders the AND/OR connector (when not the first row),
   then the row body. Mirrors the handoff's QBWhereGroup / QBCondition
   recursive tree. ---- */
interface QBRowProps {
  readonly node: QBWhereNode;
  readonly tags: readonly string[];
  readonly fields: readonly string[];
  readonly ops?: readonly { value: string; label: string }[];
  readonly isFirst: boolean;
  readonly onChange: (next: QBWhereNode) => void;
  readonly onRemove: () => void;
  readonly onAddCond: () => void;
  readonly onAddGroup: () => void;
  readonly onUpdateCond: (patch: Partial<QBWhereLeafWithConn>) => void;
}

function QBRow({ node, tags, fields, ops, isFirst, onChange, onRemove, onAddCond, onAddGroup, onUpdateCond }: QBRowProps) {
  if (qbIsGroup(node)) {
    // nested group card
    return (
      <>
        {!isFirst && (
          <div className="qb-conn-seg" role="group" aria-label="Connector to previous condition" title="AND binds tighter than OR — use a group for explicit parentheses">
            {QB_COMBINATORS.map((cb) => (
              <button
                key={cb.value}
                type="button"
                className={'qb-conn-btn' + ((node.conn ?? 'AND') === cb.value ? ' is-on' : '')}
                onClick={() => onChange({ ...node, conn: cb.value as 'AND' | 'OR' })}
              >
                {cb.label}
              </button>
            ))}
          </div>
        )}
        <div className="qb-group-card">
          <div className="qb-group-head">
            <span className="qb-group-tag">Group</span>
            <span className="qb-gap" />
            <button type="button" className="qb-del" title="Remove group" onClick={onRemove} aria-label="Remove group">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
                <path d="M6 6l12 12M18 6 6 18" />
              </svg>
            </button>
          </div>
          <div className="qb-group-children">
            {node.children.length === 0 ? (
              <span className="qb-dim">no conditions yet</span>
            ) : (
              node.children.map((c, i) => (
                <QBRow
                  key={i}
                  node={c}
                  tags={tags}
                  fields={fields}
                  ops={ops}
                  isFirst={i === 0}
                  onChange={(nc) => onChange({ ...node, children: node.children.map((cc, idx) => (idx === i ? nc : cc)) })}
                  onRemove={() => onChange({ ...node, children: node.children.filter((_, idx) => idx !== i) })}
                  onAddCond={() => onChange({ ...node, children: [...node.children, qbNewCond(tags)] })}
                  onAddGroup={() => onChange({ ...node, children: [...node.children, qbNewGroup(tags)] })}
                  onUpdateCond={(patch) => onChange({ ...node, children: node.children.map((cc, idx) => (idx === i ? { ...cc, ...patch } : cc)) })}
                />
              ))
            )}
          </div>
          <div className="qb-group-foot">
            <button type="button" className="qb-add" onClick={onAddCond} disabled={!tags.length}>
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
                <path d="M12 5v14M5 12h14" />
              </svg>
              Add condition
            </button>
            <button type="button" className="qb-add" onClick={onAddGroup} disabled={!tags.length}>
              <IconParens width={13} height={13} />
              Add group
            </button>
          </div>
        </div>
      </>
    );
  }
  // leaf condition
  const leaf = node as QBWhereLeafWithConn;
  return (
    <>
      {!isFirst && (
        <div className="qb-conn-seg" role="group" aria-label="Connector to previous condition" title="AND binds tighter than OR — use a group for explicit parentheses">
          {QB_COMBINATORS.map((cb) => (
            <button
              key={cb.value}
              type="button"
              className={'qb-conn-btn' + ((leaf.conn ?? 'AND') === cb.value ? ' is-on' : '')}
              onClick={() => onChange({ ...leaf, conn: cb.value as 'AND' | 'OR' })}
            >
              {cb.label}
            </button>
          ))}
        </div>
      )}
      <div className="qb-cond">
        <span className="qb-select-wrap">
          <select aria-label="Tag" value={leaf.tag} onChange={(e) => onUpdateCond({ tag: e.target.value })}>
            <option value="">— tag —</option>
            {tags.map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
          <span className="qb-select-chev"><IconChev width={13} height={13} /></span>
        </span>
        <span className="qb-select-wrap">
          <select aria-label="Operator" value={leaf.op} onChange={(e) => onUpdateCond({ op: e.target.value })}>
            {(ops ?? QB_OPS).map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
          </select>
          <span className="qb-select-chev"><IconChev width={13} height={13} /></span>
        </span>
        <input
          className="qb-input mono"
          aria-label="Value"
          type="text"
          value={leaf.value}
          placeholder="value"
          onChange={(e) => onUpdateCond({ value: e.target.value })}
        />
        <span className="qb-gap" />
        <button type="button" className="qb-del" title="Remove condition" onClick={onRemove} aria-label="Remove condition">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
            <path d="M6 6l12 12M18 6 6 18" />
          </svg>
        </button>
      </div>
    </>
  );
}

interface QBSectionProps {
  readonly kw: string;
  readonly sum: string;
  readonly hint?: string;
  readonly optional?: boolean;
  readonly open: boolean;
  readonly acc: boolean;
  readonly onToggle: () => void;
  readonly children: React.ReactNode;
}

/** Clause wrapper: flat while composing, accordion row after the first run. */
function QBSection({ kw, sum, hint, optional, open, acc, onToggle, children }: QBSectionProps) {
  if (!acc) {
    return (
      <div className="qb-section">
        <div className="qb-section-h" title={hint}>
          <span>{kw}</span>
          {optional && <span className="qb-opt">optional</span>}
        </div>
        {children}
      </div>
    );
  }
  return (
    <div className={'qb-acc' + (open ? ' is-open' : '')}>
      <button type="button" className="qb-acc-head" title={hint} aria-expanded={open} onClick={onToggle}>
        <span className="qb-acc-chev">{open ? '▾' : '▸'}</span>
        <span className="qb-kw">{kw}</span>
        {!open && <span className="qb-acc-sum mono">{sum}</span>}
        <span className="qb-acc-edit">{open ? 'collapse' : 'edit'}</span>
      </button>
      {open && <div className="qb-acc-body"><div className="qb-body">{children}</div></div>}
    </div>
  );
}

function countConds(node: QBWhereNode): number {
  if (qbIsGroup(node)) return node.children.reduce((sum, c) => sum + countConds(c), 0);
  return 1;
}

function condText(c: QBWhereLeafWithConn): string {
  return `${c.tag} ${QB_OP(c.op).sql} ${c.value !== '' ? c.value : '…'}`;
}

function buildSummaries(state: QBBuilderState): Record<string, string> {
  const cat = QB_CAT(state.catalog);
  const fieldsTxt = (state.select ?? []).filter((r) => r.field).map((r) =>
    r.fn ? `${r.fn.toLowerCase()}(${r.field})` : r.field,
  ).join(', ');
  const tagsTxt = (state.projection ?? []).length ? state.projection.join(', ') : 'all tags';
  const where = state.where;
  const nConds = where.children.length ? countConds(where) : 0;
  const time = state.time;
  const timeTxt = time.mode === 'relative'
    ? (time.rel ? `last ${time.rel.replace(/^-/, '')}` : 'all time')
    : (time.from && time.to ? `${time.from} → ${time.to}`
      : time.from ? `since ${time.from}`
      : time.to ? `until ${time.to}`
      : 'all time');
  return {
    from: `${cat.kw.toLowerCase()} ${state.resource || '—'} in ${state.group || '—'}`,
    select: state.catalog === 'measures' ? (fieldsTxt ? `${tagsTxt} · ${fieldsTxt}` : tagsTxt) : tagsTxt,
    where: nConds === 0 ? 'no filters'
      : (nConds === 1 && !qbIsGroup(where.children[0])) ? condText(where.children[0] as QBWhereLeafWithConn)
      : `${nConds} conditions · ${qbConnSummary(where)}`,
    groupBy: (state.groupBy ?? []).length ? state.groupBy.join(', ') : 'no grouping',
    time: timeTxt,
    order: state.catalog === 'topn'
      ? `value ${(state.orderDir || 'DESC').toLowerCase()} · limit ${state.limit}${state.offset ? `, off ${state.offset}` : ''}`
      : state.orderField
        ? `${state.orderField} ${(state.orderDir || 'DESC').toLowerCase()} · limit ${state.limit}${state.offset ? `, off ${state.offset}` : ''}`
        : `no order · limit ${state.limit}${state.offset ? `, off ${state.offset}` : ''}`,
    top: `top ${state.topN} series`,
    agg: state.aggFn ? `${state.aggFn.toLowerCase()} over range` : 'pre-aggregated value',
    orderTopn: `value ${(state.orderDir || 'DESC').toLowerCase()}`,
  };
}

export function QueryBuilder({
  state, onChange, tags, fields, groupNames, resourceNames,
  groups, groupResources, onPickResource,
  isRunning, onEjectToCode, onRun,
  hasRun = false, compact = true, setCompact, openSection = null, setOpenSection,
}: QueryBuilderProps) {
  const cat = QB_CAT(state.catalog);
  const isTopN = state.catalog === 'topn';
  const isMeasure = state.catalog === 'measures';
  const where = state.where;
  const accOn = hasRun && compact;
  const sums = React.useMemo(() => buildSummaries(state), [state]);
  const sectionProps = (key: string, kw: string, sum: string, optional = false, hint = '') => ({
    kw,
    sum,
    hint,
    optional,
    acc: accOn,
    open: openSection === key,
    onToggle: () => setOpenSection?.(openSection === key ? null : key),
  });

  const setCatalog = (catalog: QB_CATALOG_VALUE) => {
    const first = groupNames[0] ?? '';
    onChange({
      catalog,
      group: first,
      resource: '',
      select: [],
      projection: catalog === 'measures' ? (tags[0] ? [tags[0]] : []) : [],
      where: qbEmptyWhere(),
      groupBy: [],
      orderField: catalog === 'traces' ? '' : 'time',
      orderDir: 'DESC',
      offset: 0,
      time: catalog === 'topn'
        ? { mode: 'relative', rel: '-30m', from: '', to: '' }
        : state.time,
      fromAgg: null,
      fromResource: null,
    });
  };
  const setGroup = (group: string) => onChange({ group, resource: '' });
  const setResource = (resource: string) => onChange({ resource });
  const toggleProjection = (tag: string) => {
    const cur = state.projection;
    const next = cur.includes(tag) ? cur.filter((t) => t !== tag) : [...cur, tag];
    onChange({ projection: next });
  };
  const toggleGroupBy = (tag: string) => {
    const cur = state.groupBy;
    const next = cur.includes(tag) ? cur.filter((t) => t !== tag) : [...cur, tag];
    onChange({ groupBy: next });
  };
  const setWhere = (children: readonly QBWhereNode[]) => {
    onChange({ where: { ...where, children } });
  };
  const setChildWhere = (i: number, child: QBWhereNode) => {
    setWhere(where.children.map((c, idx) => (idx === i ? child : c)));
  };
  const removeChild = (i: number) => {
    setWhere(where.children.filter((_, idx) => idx !== i));
  };
  const addCondition = () => setWhere([...where.children, qbNewCond(tags)]);
  // Add a parenthesized group (matches the handoff's `Add group` button).
  // qbNewGroup wraps a fresh empty condition, so the user lands inside
  // an AND-group with one row ready to fill in — the same UX as the
  // handoff's qbNewGroup(tags) helper.
  const addGroup = () => setWhere([...where.children, qbNewGroup(tags)]);
  const updateCondition = (i: number, patch: Partial<QBWhereLeafWithConn>) => {
    const next = where.children.map((c, idx) => (idx === i ? { ...c, ...patch } : c));
    setWhere(next);
  };
  const setTime = (patch: Partial<QBBuilderState['time']>) => onChange({ time: { ...state.time, ...patch } });
  const setOrder = (patch: Partial<Pick<QBBuilderState, 'orderField' | 'orderDir'>>) => onChange(patch);

  return (
    <div className="qb-card" data-catalog={state.catalog}>
      <div className="qb-rail-scroll">
        {hasRun && (
          <div className="qb-viewbar">
            <span>{compact ? 'Clauses collapsed to summaries — click one to edit' : 'Showing full clause editors'}</span>
            <button type="button" onClick={() => setCompact?.(!compact)}>
              {compact ? 'Expand all' : 'Collapse'}
            </button>
          </div>
        )}
      {/* ── FROM ─────────────────────────────────────────────── */}
      <QBSection {...sectionProps('from', 'FROM', sums.from, false, 'Pick what to query, then the group and resource it lives in.')}>
        <QBResourceSearch
          groups={groups}
          groupResources={groupResources}
          onPick={onPickResource}
        />
        <div className="qb-cat-seg" role="tablist" aria-label="Target type">
          {QB_CATALOGS.map((c) => {
            const Ico = CATALOG_ICON[c.value];
            return (
              <button
                key={c.value}
                type="button"
                role="tab"
                title={c.blurb}
                aria-selected={state.catalog === c.value}
                className={'qb-cat-btn' + (state.catalog === c.value ? ' is-on' : '')}
                onClick={() => setCatalog(c.value)}
              >
                {Ico && <Ico width={15} height={15} />}
                <span className="qb-cat-name">{c.label}</span>
              </button>
            );
          })}
        </div>
        <div className="qb-row qb-from-row">
          <span className="qb-inline-kw">{cat.kw}</span>
          <span className="qb-select-wrap">
            <select aria-label="Resource" value={state.resource} onChange={(e) => setResource(e.target.value)}>
              <option value="">— none —</option>
              {resourceNames.map((n) => <option key={n} value={n}>{n}</option>)}
            </select>
            <span className="qb-select-chev"><IconChev width={13} height={13} /></span>
          </span>
          <span className="qb-inline-kw">IN</span>
          <span className="qb-select-wrap">
            <select aria-label="Group" value={state.group} onChange={(e) => setGroup(e.target.value)}>
              <option value="">— none —</option>
              {groupNames.map((n) => <option key={n} value={n}>{n}</option>)}
            </select>
            <span className="qb-select-chev"><IconChev width={13} height={13} /></span>
          </span>
        </div>
      </QBSection>

      {/* ── SELECT ────────────────────────────────────────────── */}
      {!isTopN && (
        <QBSection {...sectionProps('select', 'SELECT', sums.select, false, state.catalog === 'measures'
          ? 'Project the tags to return — typically the entity tags — and optionally add fields with an aggregation.'
          : 'Project specific tags, or return all tags.')}>
          {isMeasure ? (
            <>
              <div className="qb-sub-label">Tags <span className="qb-sub-req">required</span></div>
              <div className="qb-chips">
                {/* "all tags" is the empty-projection sentinel: clicking it
                    means "include every tag on this resource". Matches the
                    handoff's qb-chip with the allLabel="all tags" branch
                    in QBChips; we render it as the first chip so the user
                    sees a clear "everything" option before the per-tag list. */}
                {tags.length > 0 && (
                  <button
                    type="button"
                    className={'qb-chip' + (state.projection.length === 0 ? ' is-on' : '')}
                    onClick={() => onChange({ projection: [] })}
                    title="Project every tag on this resource"
                  >
                    all tags
                  </button>
                )}
                {tags.map((t) => (
                  <button
                    key={t}
                    type="button"
                    className={'qb-chip mono' + (state.projection.includes(t) ? ' is-on' : '')}
                    onClick={() => toggleProjection(t)}
                  >
                    {t}
                  </button>
                ))}
              </div>
              <div className="qb-sub-label" style={{ marginTop: 8 }}>Fields <span className="qb-sub-opt">optional</span></div>
              <div className="qb-stack">
                {(state.select ?? []).map((row, i) => (
                  <div key={i} className="qb-row qb-select-row">
                    <span className="qb-select-wrap">
                      <select className="qb-select mono" aria-label="Aggregation" value={row.fn} onChange={(e) => {
                        const next = [...state.select];
                        next[i] = { ...row, fn: e.target.value };
                        onChange({ select: next });
                      }}>
                        {QB_AGGS.map((a) => <option key={a.value} value={a.value}>{a.label}</option>)}
                      </select>
                      <span className="qb-select-chev"><IconChev width={13} height={13} /></span>
                    </span>
                    <span className="qb-of">of</span>
                    <span className="qb-select-wrap">
                      <select className="qb-select mono" aria-label="Field" value={row.field} onChange={(e) => {
                        const next = [...state.select];
                        next[i] = { ...row, field: e.target.value };
                        onChange({ select: next });
                      }}>
                        {fields.length === 0 && <option value="">—</option>}
                        {fields.map((f) => <option key={f} value={f}>{f}</option>)}
                      </select>
                      <span className="qb-select-chev"><IconChev width={13} height={13} /></span>
                    </span>
                    <button type="button" className="qb-del" title="Remove field" onClick={() => onChange({ select: state.select.filter((_, idx) => idx !== i) })}>
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M6 6l12 12M18 6 6 18" />
                      </svg>
                    </button>
                  </div>
                ))}
                <button type="button" className="qb-add" onClick={() => onChange({ select: [...state.select, { field: fields[0] ?? '', fn: '' }] })} disabled={!fields.length}>
                  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M12 5v14M5 12h14" />
                  </svg>
                  Add field
                </button>
              </div>
            </>
          ) : (
            <div className="qb-chips">
              {tags.length > 0 && (
                <button
                  type="button"
                  className={'qb-chip' + (state.projection.length === 0 ? ' is-on' : '')}
                  onClick={() => onChange({ projection: [] })}
                  title="Project every tag on this resource"
                >
                  all tags
                </button>
              )}
              {tags.map((t) => (
                <button
                  key={t}
                  type="button"
                  className={'qb-chip mono' + (state.projection.includes(t) ? ' is-on' : '')}
                  onClick={() => toggleProjection(t)}
                >
                  {t}
                </button>
              ))}
            </div>
          )}
        </QBSection>
      )}

      {/* ── WHERE ────────────────────────────────────────────── */}
      <QBSection {...sectionProps('where', 'WHERE', sums.where, true, 'Filter by tag values. Pick AND / OR between each condition — AND binds tighter than OR; nest groups for explicit parentheses.')}
      >
        {where.children.map((c, i) => (
          <QBRow
            key={i}
            node={c}
            tags={tags}
            fields={fields}
            ops={isTopN ? QB_TOPN_OPS : QB_OPS}
            isFirst={i === 0}
            onChange={(nc) => setChildWhere(i, nc)}
            onRemove={() => removeChild(i)}
            onAddCond={() => {
              // append to the nested group if this row is a group;
              // otherwise treat as inserting a new root-level row
              if (qbIsGroup(c)) {
                setChildWhere(i, {
                  ...c,
                  children: [...c.children, qbNewCond(tags)],
                });
              } else {
                setWhere([...where.children.slice(0, i + 1), qbNewGroup(tags), ...where.children.slice(i + 1)]);
              }
            }}
            onAddGroup={() => {
              if (qbIsGroup(c)) {
                setChildWhere(i, {
                  ...c,
                  children: [...c.children, qbNewGroup(tags)],
                });
              } else {
                setWhere([...where.children.slice(0, i + 1), qbNewGroup(tags), ...where.children.slice(i + 1)]);
              }
            }}
            onUpdateCond={(patch) => updateCondition(i, patch)}
          />
        ))}
        <button type="button" className="qb-add" onClick={addCondition} disabled={!tags.length}>
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
            <path d="M12 5v14M5 12h14" />
          </svg>
          Add condition
        </button>
        {where.children.length > 0 && (
          <button type="button" className="qb-add" onClick={addGroup} disabled={!tags.length}>
            <IconParens width={13} height={13} />
            Add group
          </button>
        )}
      </QBSection>

      {/* ── TIME ─────────────────────────────────────────────── */}
      <QBSection {...sectionProps('time', 'TIME', sums.time, true, 'Bound the query to an explicit start/end window, or a recent relative range.')}>
        <div className="qb-time">
          <div className="qb-time-seg" role="tablist" aria-label="Time range mode">
            <button
              type="button"
              role="tab"
              className={'qb-time-tab' + (state.time.mode === 'absolute' ? ' is-on' : '')}
              onClick={() => setTime({ mode: 'absolute' })}
            >
              <IconCalendar width={14} height={14} />
              Absolute range
            </button>
            <button
              type="button"
              role="tab"
              className={'qb-time-tab' + (state.time.mode === 'relative' ? ' is-on' : '')}
              onClick={() => setTime({ mode: 'relative' })}
            >
              <IconClock width={14} height={14} />
              Relative
            </button>
          </div>
          {state.time.mode === 'absolute' ? (
            <div className="qb-time-abs">
              <label className="qb-time-field">
                <span className="qb-time-lbl">From</span>
                <input
                  className="qb-input mono qb-time-input"
                  type="datetime-local"
                  step="1"
                  value={state.time.from}
                  onChange={(e) => setTime({ from: e.target.value })}
                />
              </label>
              <span className="qb-time-arrow"><IconArrowRight width={14} height={14} /></span>
              <label className="qb-time-field">
                <span className="qb-time-lbl">To</span>
                <input
                  className="qb-input mono qb-time-input"
                  type="datetime-local"
                  step="1"
                  value={state.time.to}
                  onChange={(e) => setTime({ to: e.target.value })}
                />
              </label>
              {(state.time.from || state.time.to) && (
                <button
                  type="button"
                  className="qb-time-clear"
                  title="Clear range"
                  aria-label="Clear time range"
                  onClick={() => setTime({ from: '', to: '' })}
                >
                  <IconClose width={13} height={13} />
                </button>
              )}
            </div>
          ) : (
            <span className="qb-select-wrap">
              <select
                aria-label="Relative time range"
                value={state.time.rel}
                onChange={(e) => setTime({ rel: e.target.value })}
              >
                {QB_TIMES.map((t) => <option key={t.value} value={t.value}>{t.label}</option>)}
              </select>
              <span className="qb-select-chev"><IconChev width={13} height={13} /></span>
            </span>
          )}
        </div>
      </QBSection>

      {/* ── GROUP BY (measures only) ─────────────────────────── */}
      {isMeasure && (
        <QBSection {...sectionProps('groupby', 'GROUP BY', sums.groupBy, true, 'Aggregate separately per distinct combination of these tags.')}
        >
          <div className="qb-chips">
            <button
              type="button"
              className={'qb-chip' + (state.groupBy.length === 0 ? ' is-on' : '')}
              onClick={() => onChange({ groupBy: [] })}
              title="No aggregation grouping"
            >
              no grouping
            </button>
            {tags.map((t) => (
              <button
                key={t}
                type="button"
                className={'qb-chip mono' + (state.groupBy.includes(t) ? ' is-on' : '')}
                onClick={() => toggleGroupBy(t)}
              >
                {t}
              </button>
            ))}
          </div>
        </QBSection>
      )}

      {/* ── ORDER + LIMIT (measures / streams / traces) ──────── */}
      {!isTopN && (
        <QBSection {...sectionProps('order', 'ORDER BY', sums.order, true)}>
          <div className="qb-row">
            <span className="qb-select-wrap">
              <select aria-label="Order field" value={state.orderField} onChange={(e) => setOrder({ orderField: e.target.value })}>
                <option value="time">time</option>
                {(isMeasure ? fields : tags).map((f) => <option key={f} value={f}>{f}</option>)}
              </select>
              <span className="qb-select-chev"><IconChev width={13} height={13} /></span>
            </span>
            <div className="qb-dir-seg" role="group" aria-label="Order direction">
              <button
                type="button"
                className={'qb-dir-btn' + (state.orderDir === 'DESC' ? ' is-on' : '')}
                onClick={() => setOrder({ orderDir: 'DESC' })}
                disabled={!state.orderField}
              >DESC</button>
              <button
                type="button"
                className={'qb-dir-btn' + (state.orderDir === 'ASC' ? ' is-on' : '')}
                onClick={() => setOrder({ orderDir: 'ASC' })}
                disabled={!state.orderField}
              >ASC</button>
            </div>
            <label className="qb-inline-kw">LIMIT</label>
            <input
              className="qb-input qb-num mono"
              type="number"
              min={1}
              max={10000}
              aria-label="Limit"
              value={state.limit}
              onChange={(e) => onChange({ limit: Math.max(1, Number(e.target.value) || 0) })}
            />
            <label className="qb-inline-kw">OFFSET</label>
            <input
              className="qb-input qb-num mono"
              type="number"
              min={0}
              max={100000}
              aria-label="Offset"
              value={state.offset}
              onChange={(e) => onChange({ offset: Math.max(0, Number(e.target.value) || 0) })}
            />
          </div>
        </QBSection>
      )}

      {/* ── TOP-N rail: SHOW TOP / WHERE / AGGREGATE BY / ORDER BY */}
      {isTopN && (
        <React.Fragment>
          <QBSection {...sectionProps('top', 'SHOW TOP', sums.top)}>
            <div className="qb-row">
              <label className="qb-inline-kw">TOP</label>
              <input
                className="qb-input qb-num mono"
                type="number"
                min={1}
                max={1000}
                aria-label="Top N"
                value={state.topN}
                onChange={(e) => onChange({ topN: Math.max(1, Number(e.target.value) || 0) })}
              />
              <span className="qb-dim">series</span>
            </div>
          </QBSection>

          <QBSection {...sectionProps('agg', 'AGGREGATE BY', sums.agg, true, 'Apply an aggregation over the time range before ranking.')}
          >
            <span className="qb-select-wrap">
              <select aria-label="Aggregate function" value={state.aggFn} onChange={(e) => onChange({ aggFn: e.target.value })}>
                {QB_TOPN_AGGS.map((a) => <option key={a.value} value={a.value}>{a.label}</option>)}
              </select>
              <span className="qb-select-chev"><IconChev width={13} height={13} /></span>
            </span>
          </QBSection>

          <QBSection {...sectionProps('order', 'ORDER BY', sums.order, true)}>
            <div className="qb-row">
              <label className="qb-inline-kw">value</label>
              <div className="qb-dir-seg" role="group" aria-label="Order direction">
                <button
                  type="button"
                  className={'qb-dir-btn' + (state.orderDir === 'DESC' ? ' is-on' : '')}
                  onClick={() => setOrder({ orderDir: 'DESC' })}
                >DESC</button>
                <button
                  type="button"
                  className={'qb-dir-btn' + (state.orderDir === 'ASC' ? ' is-on' : '')}
                  onClick={() => setOrder({ orderDir: 'ASC' })}
                >ASC</button>
              </div>
              <label className="qb-inline-kw">LIMIT</label>
              <input
                className="qb-input qb-num mono"
                type="number"
                min={1}
                max={10000}
                aria-label="Limit"
                value={state.limit}
                onChange={(e) => onChange({ limit: Math.max(1, Number(e.target.value) || 0) })}
              />
              <label className="qb-inline-kw">OFFSET</label>
              <input
                className="qb-input qb-num mono"
                type="number"
                min={0}
                max={100000}
                aria-label="Offset"
                value={state.offset}
                onChange={(e) => onChange({ offset: Math.max(0, Number(e.target.value) || 0) })}
              />
            </div>
          </QBSection>
        </React.Fragment>
      )}

      </div>

      {/* ── Generated BydbQL footer (matches the handoff's qb-foot) ──
          Single row: Edit as code + Trace on the left, generated query
          preview pushed to the right, then Run. */}
      <div className="qb-foot">
        <button
          type="button"
          className="qb-eject"
          onClick={onEjectToCode}
          title="Eject the current builder state into raw BydbQL for editing"
        >
          Edit as code <IconArrowRight width={13} height={13} />
        </button>
        <button
          type="button"
          className={'qb-trace-btn' + (state.trace ? ' is-on' : '')}
          onClick={() => onChange({ trace: !state.trace })}
          title="Append WITH QUERY_TRACE to capture per-span execution timings"
        >
          <span className="qb-trace-dot" /> Trace
        </button>
        <span className="qb-gap" />
        <code className="qb-gen-line mono" title={buildBydbQL(state, tags)}>
          {buildBydbQL(state, tags).replace(/\s*\n\s*/g, ' ')}
        </code>
        <button
          type="button"
          className="btn btn-primary"
          disabled={isRunning}
          onClick={() => onRun()}
        >
          <IconPlay width={15} height={15} /> Run
        </button>
      </div>
    </div>
  );
}

// Re-export helper so QueryConsole can build a default state.
export { qbDataCatalog, qbPruneWhere };