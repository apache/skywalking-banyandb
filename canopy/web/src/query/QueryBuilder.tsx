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
// WHERE tree + Time + ORDER BY + LIMIT + WITH QUERY_TRACE toggle.
//
// The builder mutates a QBBuilderState from bydbql.ts. The consumer
// (QueryConsole) wires changes back into the shared state and re-generates
// BydbQL via buildBydbQL(state).

import React from 'react';
import {
  QB_CATALOGS, QB_CAT, QB_OPS, QB_AGGS, QB_TIMES, QB_COMBINATORS,
  qbDataCatalog, qbNewCond, qbEmptyWhere, qbPruneWhere,
  type QBBuilderState, type QB_CATALOG_VALUE, type QBWhereLeafWithConn,
} from './bydbql.js';

export interface QueryBuilderProps {
  readonly state: QBBuilderState;
  readonly onChange: (patch: Partial<QBBuilderState>) => void;
  readonly tags: readonly string[];
  readonly fields: readonly string[];
  readonly groupNames: readonly string[];
  readonly resourceNames: readonly string[];
}

export function QueryBuilder({ state, onChange, tags, fields, groupNames, resourceNames }: QueryBuilderProps) {
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
  const setWhere = (children: readonly QBWhereLeafWithConn[]) => {
    onChange({ where: { ...where, children } });
  };
  const addCondition = () => setWhere([...where.children, qbNewCond(tags)]);
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
        <div className="qb-section-h">FROM</div>
        <div className="qb-cat-seg" role="tablist" aria-label="Target type">
          {QB_CATALOGS.map((c) => (
            <button
              key={c.value}
              type="button"
              role="tab"
              title={c.blurb}
              aria-selected={state.catalog === c.value}
              className={'qb-cat-btn' + (state.catalog === c.value ? ' is-on' : '')}
              onClick={() => setCatalog(c.value)}
            >
              <span className="qb-cat-name">{c.label}</span>
            </button>
          ))}
        </div>
        <div className="qb-row qb-from-row">
          <span className="qb-inline-kw">{cat.kw}</span>
          <select aria-label="Resource" value={state.resource} onChange={(e) => setResource(e.target.value)}>
            <option value="">— none —</option>
            {resourceNames.map((n) => <option key={n} value={n}>{n}</option>)}
          </select>
          <span className="qb-inline-kw">IN</span>
          <select aria-label="Group" value={state.group} onChange={(e) => setGroup(e.target.value)}>
            <option value="">— none —</option>
            {groupNames.map((n) => <option key={n} value={n}>{n}</option>)}
          </select>
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
                {tags.map((t) => (
                  <button
                    key={t}
                    type="button"
                    className={'qb-chip' + (state.projection.includes(t) ? ' is-on' : '')}
                    onClick={() => toggleProjection(t)}
                  >
                    {t}
                  </button>
                ))}
              </div>
              <div className="qb-sub-label" style={{ marginTop: 8 }}>Fields <span className="qb-sub-opt">optional</span></div>
              {(state.select ?? []).map((row, i) => (
                <div key={i} className="qb-row qb-select-row">
                  <select aria-label="Aggregation" value={row.fn} onChange={(e) => {
                    const next = [...state.select];
                    next[i] = { ...row, fn: e.target.value };
                    onChange({ select: next });
                  }}>
                    {QB_AGGS.map((a) => <option key={a.value} value={a.value}>{a.label}</option>)}
                  </select>
                  <select aria-label="Field" value={row.field} onChange={(e) => {
                    const next = [...state.select];
                    next[i] = { ...row, field: e.target.value };
                    onChange({ select: next });
                  }}>
                    <option value="">—</option>
                    {fields.map((f) => <option key={f} value={f}>{f}</option>)}
                  </select>
                  <button type="button" className="qb-row-del" onClick={() => onChange({ select: state.select.filter((_, idx) => idx !== i) })}>×</button>
                </div>
              ))}
              <button type="button" className="qb-row-add" onClick={() => onChange({ select: [...state.select, { field: fields[0] ?? '', fn: '' }] })}>
                + Add field
              </button>
            </>
          ) : (
            <div className="qb-chips">
              {tags.map((t) => (
                <button
                  key={t}
                  type="button"
                  className={'qb-chip' + (state.projection.includes(t) ? ' is-on' : '')}
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
          <div className="qb-section-h">WHERE</div>
          {where.children.length === 0 && (
            <button type="button" className="qb-row-add" onClick={addCondition}>+ Add condition</button>
          )}
          {where.children.map((c, i) => (
            <div key={i} className="qb-row qb-cond-row">
              {i > 0 ? (
                <select
                  aria-label="Connector"
                  className="qb-conn-sel"
                  value={c.conn ?? where.combinator}
                  onChange={(e) => setConn(i, e.target.value as 'AND' | 'OR')}
                >
                  {QB_COMBINATORS.map((x) => <option key={x.value} value={x.value}>{x.value}</option>)}
                </select>
              ) : (
                <span className="qb-inline-kw">WHERE</span>
              )}
              <select aria-label="Tag" value={c.tag} onChange={(e) => updateCondition(i, { tag: e.target.value })}>
                <option value="">— tag —</option>
                {tags.map((t) => <option key={t} value={t}>{t}</option>)}
              </select>
              <select aria-label="Operator" value={c.op} onChange={(e) => updateCondition(i, { op: e.target.value })}>
                {QB_OPS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
              <input
                aria-label="Value"
                type="text"
                value={c.value}
                placeholder="value"
                onChange={(e) => updateCondition(i, { value: e.target.value })}
              />
              <button type="button" className="qb-row-del" onClick={() => removeCondition(i)} aria-label="Remove condition">×</button>
            </div>
          ))}
          {where.children.length > 0 && (
            <button type="button" className="qb-row-add" onClick={addCondition}>+ Add condition</button>
          )}
        </div>
      )}

      {/* ── TIME ─────────────────────────────────────────────── */}
      <div className="qb-section">
        <div className="qb-section-h">TIME</div>
        <div className="qb-row">
          <select aria-label="Time mode" value={state.time.mode} onChange={(e) => setTime({ mode: e.target.value as 'relative' | 'absolute' })}>
            <option value="relative">Relative</option>
            <option value="absolute">Absolute</option>
          </select>
          {state.time.mode === 'relative' ? (
            <select aria-label="Relative window" value={state.time.rel} onChange={(e) => setTime({ rel: e.target.value })}>
              {QB_TIMES.map((t) => <option key={t.value} value={t.value}>{t.label}</option>)}
            </select>
          ) : (
            <>
              <input type="text" placeholder="from (RFC3339)" aria-label="From" value={state.time.from} onChange={(e) => setTime({ from: e.target.value })} />
              <input type="text" placeholder="to (RFC3339)" aria-label="To" value={state.time.to} onChange={(e) => setTime({ to: e.target.value })} />
            </>
          )}
        </div>
      </div>

      {/* ── ORDER + LIMIT ────────────────────────────────────── */}
      {!isTopN && (
        <div className="qb-section">
          <div className="qb-section-h">ORDER BY</div>
          <div className="qb-row">
            <select aria-label="Order field" value={state.orderField} onChange={(e) => setOrder({ orderField: e.target.value })}>
              <option value="time">time</option>
              {(isMeasure ? fields : tags).map((f) => <option key={f} value={f}>{f}</option>)}
            </select>
            <select aria-label="Order direction" value={state.orderDir} onChange={(e) => setOrder({ orderDir: e.target.value as 'ASC' | 'DESC' })}>
              <option value="DESC">DESC</option>
              <option value="ASC">ASC</option>
            </select>
            <label className="qb-inline-kw">LIMIT</label>
            <input
              type="number"
              min={1}
              max={10000}
              aria-label="Limit"
              value={state.limit}
              onChange={(e) => onChange({ limit: Math.max(1, Number(e.target.value) || 0) })}
            />
          </div>
          <label className="qb-row qb-trace-row">
            <input type="checkbox" checked={state.trace} onChange={(e) => onChange({ trace: e.target.checked })} />
            <span>WITH QUERY_TRACE</span>
          </label>
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
    </div>
  );
}

// Re-export helper so QueryConsole can build a default state.
export { qbDataCatalog, qbPruneWhere };