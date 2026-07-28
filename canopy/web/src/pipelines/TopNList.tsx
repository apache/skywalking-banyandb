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

// TopNList.tsx — the TopN pipeline type's list page (/pipelines/topn):
// filter by name/group/source/direction, New/Edit/Delete. Ported from
// .handoff-import/banyandb/project/topn-page.jsx's TopNList (window-global
// JSX -> ES module TSX; the handoff's per-group "locked" drill-down variant
// is dropped — docs/pipelines-design.md §3 only routes the all-groups list
// and the detail page, not a per-group index).
//
// ADAPTATIONS: mock `groups` prop -> live listGroups + listTopNAggregations
// (one call per measure group, mirroring QueryConsole.tsx's group-topn-agg
// prefetch pattern); onNavigate -> useNavigate; New/Edit/Delete modals are
// owned locally (mirrors PropertyDetailPage.tsx's local ModalState, not
// App.tsx's central union — Pipelines is a self-contained route tree).

import React, { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router';
import { useQuery } from '@tanstack/react-query';

import type { TopNAggregationSchema } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import {
  IconTopN, IconSearch, IconPlus, IconEdit, IconTrash, IconGroup, IconChevron, IconAlert,
} from '../components/icons.js';
import { RankBadge, SORT_OPTS } from './topn-shared.js';
import { TopNFormModal, DeleteTopNModal } from './TopNForms.js';

const PAGE_SIZE = 10;

interface Row {
  readonly agg: TopNAggregationSchema;
  readonly group: string;
}

type ModalState =
  | { readonly kind: 'create'; readonly groupName?: string }
  | { readonly kind: 'edit'; readonly groupName: string; readonly agg: TopNAggregationSchema }
  | { readonly kind: 'delete'; readonly groupName: string; readonly agg: TopNAggregationSchema }
  | null;

function FilterSelect({ label, value, options, onChange }: {
  label: string;
  value: string;
  options: ReadonlyArray<{ value: string; label: string }>;
  onChange: (v: string) => void;
}) {
  return (
    <label className="topn-filter">
      <span className="topn-filter-label">{label}</span>
      <span className="topn-select-wrap">
        <select className="topn-select" value={value} onChange={(e) => onChange(e.target.value)} aria-label={label}>
          {options.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
        </select>
        <span className="topn-select-chev"><IconChevron size={13} /></span>
      </span>
    </label>
  );
}

export function TopNList() {
  const navigate = useNavigate();
  const { session } = useAuth();
  const canWrite = session?.role === 'admin';
  const [modal, setModal] = useState<ModalState>(null);

  const { data: groupsData } = useQuery({ queryKey: ['groups'], queryFn: () => apiDataSource.listGroups() });
  const measureGroups = useMemo(() => (groupsData?.groups ?? []).filter((g) => g.catalog === 'CATALOG_MEASURE'), [groupsData]);
  const measureGroupNames = useMemo(() => measureGroups.map((g) => g.name), [measureGroups]);

  const [rows, setRows] = useState<readonly Row[]>([]);
  const [loaded, setLoaded] = useState(false);
  useEffect(() => {
    if (measureGroupNames.length === 0) { setRows([]); setLoaded(true); return; }
    let cancelled = false;
    Promise.allSettled(
      measureGroupNames.map((g) => apiDataSource.listTopNAggregations(g).then((aggs) => ({ group: g, aggs }))),
    ).then((results) => {
      if (cancelled) return;
      const out: Row[] = [];
      for (const r of results) if (r.status === 'fulfilled') for (const agg of r.value.aggs) out.push({ agg, group: r.value.group });
      setRows(out);
      setLoaded(true);
    });
    return () => { cancelled = true; };
  }, [measureGroupNames]);

  // Measure names per group, so a row can flag a source measure that no
  // longer exists (like the handoff's srcExists check).
  const [measureNames, setMeasureNames] = useState<ReadonlyMap<string, readonly string[]>>(new Map());
  useEffect(() => {
    if (measureGroupNames.length === 0) return;
    let cancelled = false;
    Promise.allSettled(
      measureGroupNames.map((g) => apiDataSource.listResourcesInGroup('measures', g).then((rs) => ({ group: g, names: rs.map((r) => r.metadata.name) }))),
    ).then((results) => {
      if (cancelled) return;
      setMeasureNames((prev) => {
        const next = new Map(prev);
        for (const r of results) if (r.status === 'fulfilled') next.set(r.value.group, r.value.names);
        return next;
      });
    });
    return () => { cancelled = true; };
  }, [measureGroupNames]);

  const [nameQ, setNameQ] = useState('');
  const [groupF, setGroupF] = useState('all');
  const [sourceF, setSourceF] = useState('all');
  const [rankF, setRankF] = useState('all');

  const groupScoped = groupF === 'all' ? rows : rows.filter((r) => r.group === groupF);

  const sourceOptions = useMemo(() => {
    const out: string[] = [];
    for (const r of groupScoped) {
      const n = r.agg.sourceMeasure?.name;
      if (n && !out.includes(n)) out.push(n);
    }
    return out.sort();
  }, [groupScoped]);

  const q = nameQ.trim().toLowerCase();
  const filtered = groupScoped.filter((r) => {
    if (q && !r.agg.metadata.name.toLowerCase().includes(q)) return false;
    if (sourceF !== 'all' && r.agg.sourceMeasure?.name !== sourceF) return false;
    if (rankF !== 'all' && (r.agg.fieldValueSort ?? 'SORT_DESC') !== rankF) return false;
    return true;
  });

  const totalCount = rows.length;
  const filtersActive = !!q || sourceF !== 'all' || rankF !== 'all' || groupF !== 'all';
  const newTargetGroup = groupF !== 'all' ? groupF : undefined;

  // Client-side paging over the filtered list (same pattern as DocList):
  // clamps when a filter change shrinks the list under the current page.
  const [page, setPage] = useState(0);
  const pages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const cur = Math.min(page, pages - 1);
  useEffect(() => { if (page !== cur) setPage(cur); }, [page, cur]);
  const start = cur * PAGE_SIZE;
  const paged = filtered.slice(start, start + PAGE_SIZE);

  return (
    <div className="page-body">
      <header className="page-head">
        <div className="crumbs">
          <button className="crumb crumb-link" onClick={() => navigate('/pipelines')}>Pipelines</button>
          <span className="crumb-sep">/</span>
          <span className="crumb is-last">TopN</span>
        </div>
        <h1 className="page-title">TopN</h1>
        <p className="page-meta">Offline TopN statistics computed over measures across all groups.</p>
      </header>

      <div className="res-toolbar topn-toolbar">
        <div className="topn-filters">
          <div className="search-box">
            <IconSearch size={15} />
            <input placeholder="Filter by name" value={nameQ} onChange={(e) => setNameQ(e.target.value)} />
          </div>
          <FilterSelect label="Group" value={groupF}
            onChange={(val) => { setGroupF(val); setSourceF('all'); }}
            options={[{ value: 'all', label: 'All groups' }, ...measureGroupNames.map((n) => ({ value: n, label: n }))]} />
          <FilterSelect label="Source" value={sourceF} onChange={setSourceF}
            options={[{ value: 'all', label: 'All measures' }, ...sourceOptions.map((n) => ({ value: n, label: n }))]} />
          <FilterSelect label="Rank" value={rankF} onChange={setRankF}
            options={[{ value: 'all', label: 'Any direction' }, ...SORT_OPTS.map((s) => ({ value: s.value, label: s.rank }))]} />
        </div>
        <div className="res-toolbar-right">
          <span className="res-count">
            {filtersActive ? `${filtered.length} of ${totalCount}` : `${totalCount} ${totalCount === 1 ? 'aggregation' : 'aggregations'}`}
          </span>
          {canWrite && (
            <button className="btn btn-primary" onClick={() => setModal({ kind: 'create', groupName: newTargetGroup })}>
              <IconPlus size={16} /> New aggregation
            </button>
          )}
        </div>
      </div>

      {!loaded ? (
        <div className="empty">
          <span className="empty-ico spin"><IconTopN size={32} /></span>
          <div className="empty-title">Loading…</div>
        </div>
      ) : totalCount === 0 ? (
        <div className="empty">
          <span className="empty-ico"><IconTopN size={36} /></span>
          <div className="empty-title">No TopN aggregations yet</div>
          <p className="empty-text">Define a TopNAggregation over a measure to pre-compute ranked statistics.</p>
          {canWrite && (
            <button className="btn btn-primary" onClick={() => setModal({ kind: 'create', groupName: newTargetGroup })}>
              <IconPlus size={15} /> Create aggregation
            </button>
          )}
        </div>
      ) : filtered.length === 0 ? (
        <div className="empty">
          <span className="empty-ico"><IconSearch size={36} /></span>
          <div className="empty-title">No matches</div>
          <p className="empty-text">No aggregation matches the current filters.</p>
        </div>
      ) : (
        <>
          <div className="idx-table topn-x">
            <div className="topn-head">
              <span>Aggregation</span>
              <span>Group</span>
              <span>Source · field</span>
              <span>Rank</span>
              <span>Group by</span>
              <span className="idx-actions-h" />
            </div>
            {paged.map((r) => {
            const a = r.agg;
            const src = a.sourceMeasure;
            const srcExists = !!src && (measureNames.get(src.group) ?? []).includes(src.name);
            const path = `/pipelines/topn/${r.group}/${a.metadata.name}`;
            return (
              <div key={`${r.group}/${a.metadata.name}`} className="topn-row" role="button" tabIndex={0}
                data-testid="topn-row"
                onClick={() => navigate(path)}
                onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); navigate(path); } }}>
                <span className="idx-name-cell">
                  <span className="idx-ico"><IconTopN size={14} /></span>
                  <span className="idx-name mono">{a.metadata.name}</span>
                </span>

                <span className="idx-chiprow">
                  <span className="topn-group-chip mono" title={`Group ${r.group}`}>
                    <IconGroup size={11} /> {r.group}
                  </span>
                </span>

                <span className="topn-src-cell">
                  {srcExists ? (
                    <span className="subj-chip is-static" title={`${src!.group}/${src!.name}`}>{src!.name}</span>
                  ) : (
                    <span className="subj-chip is-danger" title="Source measure no longer exists">
                      <IconAlert size={11} /> {src?.name ?? '—'}
                    </span>
                  )}
                  <span className="topn-field mono">{a.fieldName}</span>
                </span>

                <span><RankBadge sort={a.fieldValueSort} /></span>

                <span className="idx-chiprow">
                  {(a.groupByTagNames ?? []).map((t) => <span key={t} className="idx-tag mono">{t}</span>)}
                  {!(a.groupByTagNames ?? []).length && <span className="idx-dim">none</span>}
                </span>

                <span className="idx-actions" onClick={(e) => e.stopPropagation()}>
                  {canWrite && (
                    <button className="idx-act" title="Edit" onClick={() => setModal({ kind: 'edit', groupName: r.group, agg: a })}>
                      <IconEdit size={15} />
                    </button>
                  )}
                  {canWrite && (
                    <button className="idx-act is-danger" title="Delete" onClick={() => setModal({ kind: 'delete', groupName: r.group, agg: a })}>
                      <IconTrash size={15} />
                    </button>
                  )}
                </span>
              </div>
            );
          })}
          </div>
          {filtered.length > PAGE_SIZE && (
            <div className="doc-pager">
              <span className="mono">showing {start + 1}–{Math.min(start + PAGE_SIZE, filtered.length)} of {filtered.length} aggregations</span>
              <span className="doc-pager-btns">
                <button type="button" className="pg-btn" disabled={cur === 0} onClick={() => setPage(cur - 1)}>← Prev</button>
                <span className="doc-pager-page mono">{cur + 1} / {pages}</span>
                <button type="button" className="pg-btn" disabled={cur >= pages - 1} onClick={() => setPage(cur + 1)}>Next →</button>
              </span>
            </div>
          )}
        </>
      )}

      {modal?.kind === 'create' && (
        <TopNFormModal mode="create" groupName={modal.groupName}
          onClose={(created) => {
            setModal(null);
            if (created) navigate(`/pipelines/topn/${created.metadata.group}/${created.metadata.name}`);
          }} />
      )}
      {modal?.kind === 'edit' && (
        <TopNFormModal mode="edit" groupName={modal.groupName} aggregation={modal.agg} onClose={() => setModal(null)} />
      )}
      {modal?.kind === 'delete' && (
        <DeleteTopNModal groupName={modal.groupName} aggregation={modal.agg} onClose={() => setModal(null)} />
      )}
    </div>
  );
}
