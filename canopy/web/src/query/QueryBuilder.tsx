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
  QB_CATALOGS, QB_CAT, QB_OPS, QB_AGGS, QB_TIMES, QB_COMBINATORS,
  qbDataCatalog, qbIsGroup, qbNewCond, qbNewGroup, qbEmptyWhere, qbPruneWhere,
    buildBydbQL,
    type QBWhereNode, type QBWhereGroupWithConn, type QBWhereLeafWithConn,
  type QBBuilderState, type QB_CATALOG_VALUE, type QBWhereLeafWithConn,
} from './bydbql.js';

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
const IconFormat = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M3 6h18M3 12h12M3 18h6" />
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

export interface QueryBuilderProps {
  readonly state: QBBuilderState;
  readonly onChange: (patch: Partial<QBBuilderState>) => void;
  readonly tags: readonly string[];
  readonly fields: readonly string[];
  readonly groupNames: readonly string[];
  readonly resourceNames: readonly string[];
  readonly isRunning: boolean;
  readonly onEjectToCode: () => void;
  readonly onRun: () => void;
  readonly onReset: () => void;
}

/* ---- QBRow: one row in the WHERE list — either a leaf condition or a
   nested group. Renders the AND/OR connector (when not the first row),
   then the row body. Mirrors the handoff's QBWhereGroup / QBCondition
   recursive tree. ---- */
interface QBRowProps {
  readonly node: QBWhereNode;
  readonly index: number;
  readonly tags: readonly string[];
  readonly fields: readonly string[];
  readonly isFirst: boolean;
  readonly onChange: (next: QBWhereNode) => void;
  readonly onRemove: () => void;
  readonly onAddCond: () => void;
  readonly onAddGroup: () => void;
  readonly onUpdateCond: (patch: Partial<QBWhereLeafWithConn>) => void;
}

function QBRow({ node, index, tags, fields, isFirst, onChange, onRemove, onAddCond, onAddGroup, onUpdateCond }: QBRowProps) {
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
                  index={i}
                  tags={tags}
                  fields={fields}
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
            {QB_OPS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
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

export function QueryBuilder({
  state, onChange, tags, fields, groupNames, resourceNames,
  isRunning, onEjectToCode, onRun, onReset,
}: QueryBuilderProps) {
  const cat = QB_CAT(state.catalog);
  const isTopN = state.catalog === 'topn';
  const isMeasure = state.catalog === 'measures';
  const where = state.where;

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
      orderField: 'time',
      orderDir: 'DESC',
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
  const removeCondition = (i: number) => {
    setWhere(where.children.filter((_, idx) => idx !== i));
  };
  const setConn = (i: number, conn: 'AND' | 'OR') => {
    if (i === 0) return;
    updateCondition(i, { conn });
  };
  const setTime = (patch: Partial<QBBuilderState['time']>) => onChange({ time: { ...state.time, ...patch } });
  const setOrder = (patch: Partial<Pick<QBBuilderState, 'orderField' | 'orderDir'>>) => onChange(patch);

  return (
    <div className="qb-card" data-catalog={state.catalog}>
      {/* ── FROM ─────────────────────────────────────────────── */}
      <div className="qb-section">
        <div className="qb-section-h" title="Pick what to query, then the group and resource it lives in.">FROM</div>
        <div className="qb-search">
          <span className="qb-search-ico"><IconSearch width={15} height={15} /></span>
          <input
            className="qb-search-input"
            placeholder="Search any measure, stream, trace or Top-N by name…"
            aria-label="Search resources by name"
            autoComplete="off"
            spellCheck={false}
            value=""
            onChange={() => { /* future: fuzzy filter on the catalog list */ }}
          />
        </div>
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
      </div>

      {/* ── SELECT ────────────────────────────────────────────── */}
      {!isTopN && (
        <div className="qb-section">
          <div className="qb-section-h">SELECT</div>
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
        </div>
      )}

      {/* ── WHERE ────────────────────────────────────────────── */}
      {!isTopN && (
        <div className="qb-section">
          <div
            className="qb-section-h"
            title="Filter by tag values. Pick AND / OR between each condition — AND binds tighter than OR; nest groups for explicit parentheses."
          >
            <span>WHERE</span>
            <span className="qb-opt">optional</span>
          </div>
          {/* 'Conditions' group tag — matches the handoff's qb-group-head
              label on the root WHERE group. */}
          {where.children.length > 0 && (
            <div className="qb-group-head">
              <span className="qb-group-tag">Conditions</span>
            </div>
          )}
          {where.children.map((c, i) => (
            <QBRow
              key={i}
              node={c}
              index={i}
              tags={tags}
              fields={fields}
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
        </div>
      )}

      {/* ── TIME ─────────────────────────────────────────────── */}
      <div className="qb-section">
        <div
          className="qb-section-h"
          title="Bound the query to an explicit start/end window, or a recent relative range."
        >
          <span>TIME</span>
          <span className="qb-opt">optional</span>
        </div>
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
      </div>

      {/* ── GROUP BY (measures only) ─────────────────────────── */}
      {isMeasure && (
        <div className="qb-section">
          <div
            className="qb-section-h"
            title="Aggregate separately per distinct combination of these tags."
          >
            <span>GROUP BY</span>
            <span className="qb-opt">optional</span>
          </div>
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
        </div>
      )}

      {/* ── ORDER + LIMIT ────────────────────────────────────── */}
      {!isTopN && (
        <div className="qb-section">
          <div className="qb-section-h">ORDER BY</div>
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
          </div>
        </div>
      )}

      {/* ── TOP-N specifics ──────────────────────────────────── */}
      {isTopN && (
        <div className="qb-section">
          <div className="qb-section-h">TOP-N</div>
          <div className="qb-row">
            <label className="qb-inline-kw">TOP</label>
            <input type="number" min={1} max={1000} aria-label="Top N" value={state.topN} onChange={(e) => onChange({ topN: Math.max(1, Number(e.target.value) || 0) })} />
            <label className="qb-inline-kw">AGGREGATE BY</label>
            <select aria-label="Aggregate function" value={state.aggFn} onChange={(e) => onChange({ aggFn: e.target.value })}>
              <option value="">none</option>
              {['SUM', 'MEAN', 'COUNT', 'MAX', 'MIN'].map((a) => <option key={a} value={a}>{a}</option>)}
            </select>
            <select aria-label="Top-N order direction" value={state.orderDir} onChange={(e) => onChange({ orderDir: e.target.value as 'ASC' | 'DESC' })}>
              <option value="DESC">value DESC</option>
              <option value="ASC">value ASC</option>
            </select>
          </div>
        </div>
      )}

      {/* ── Generated BydbQL footer (matches the handoff's qb-foot) ──
          Shows the live query, a Trace toggle, and the Eject-to-code
          + Run controls — the same widgets the handoff's qb-foot
          renders. The handoff's `Edit as code` button is implemented
          as the parent (QueryConsole)'s ejectToCode which switches
          into the Code mode. */}
      <div className="qb-foot">
        <div className="qb-foot-row">
          <span className="lang-pill">BydbQL</span>
          <code className="qb-gen-line mono" title={buildBydbQL(state)}>
            {buildBydbQL(state).replace(/\s*\n\s*/g, ' ')}
          </code>
        </div>
        <div className="qb-foot-row">

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
          <button type="button" className="btn btn-ghost" title="Reset query" onClick={onReset}>
            <IconFormat width={15} height={15} />
          </button>
          <button
            type="button"
            className="btn btn-primary"
            disabled={isRunning}
            onClick={onRun}
          >
            <IconPlay width={15} height={15} /> Run
            <kbd className="kbd">{typeof navigator !== 'undefined' && /Mac/i.test(navigator.platform) ? '⌘↵' : 'Ctrl↵'}</kbd>
          </button>
        </div>
      </div>
    </div>
  );
}

// Re-export helper so QueryConsole can build a default state.
export { qbDataCatalog, qbPruneWhere };