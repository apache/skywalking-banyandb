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

// TopNDetail.tsx — the TopN pipeline type's detail page
// (/pipelines/topn/:group/:name). Ported from
// .handoff-import/banyandb/project/topn-page.jsx's TopNDetail.
//
// ADAPTATIONS: mock `groups` lookup -> getTopNAggregation + a live
// listResourcesInGroup('measures', ...) existence check for the source
// measure; onNavigate -> useNavigate; "Run Top-N query" reuses the EXISTING
// GroupPage "Query this resource" deep-link seed
// (navigate('/query', { state: { seed: { catalog: 'topn', group, resource } } }))
// — QueryConsole.tsx already dispatches TopN queries to /v1/measure/topn, so
// there is no new query path here (docs/pipelines-design.md §4). Edit/Delete
// modals are owned locally, mirroring PropertyDetailPage.tsx.

import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';

import type { MeasureSchema } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import {
  IconTopN, IconEmpty, IconMeasures, IconSort, IconKey, IconFilter,
  IconPlay, IconArrowLeft, IconArrowRight, IconEdit, IconTrash, IconAlert,
} from '../components/icons.js';
import { CondChip, topNRank, topNSortLabel, flattenTopNCriteria, DEFAULT_COUNTERS } from './topn-shared.js';
import { TopNFormModal, DeleteTopNModal } from './TopNForms.js';

type ModalState = { readonly kind: 'edit' } | { readonly kind: 'delete' } | null;

export function TopNDetail({ groupName, aggName }: { readonly groupName: string; readonly aggName: string }) {
  const navigate = useNavigate();
  const { session } = useAuth();
  const canWrite = session?.role === 'admin';
  const [modal, setModal] = useState<ModalState>(null);

  const { data: agg, isLoading, isError } = useQuery({
    queryKey: ['topNAggregation', groupName, aggName],
    queryFn: () => apiDataSource.getTopNAggregation(groupName, aggName),
  });

  const src = agg?.sourceMeasure;
  const { data: sourceMeasures = [] } = useQuery({
    queryKey: ['resources', 'measures', src?.group ?? ''],
    queryFn: () => apiDataSource.listResourcesInGroup('measures', src!.group),
    enabled: !!src?.group,
  });
  const srcMeasure = sourceMeasures.find((m) => m.metadata.name === src?.name) as MeasureSchema | undefined;
  const srcExists = !!srcMeasure;
  const srcFieldNames = srcMeasure?.fields.map((f) => f.name) ?? [];

  if (isLoading) {
    return (
      <div className="page-body">
        <div className="empty">
          <span className="empty-ico spin"><IconTopN size={32} /></span>
          <div className="empty-title">Loading…</div>
        </div>
      </div>
    );
  }

  if (isError || !agg) {
    return (
      <div className="page-body">
        <header className="page-head">
          <div className="crumbs">
            <button className="crumb crumb-link" onClick={() => navigate('/pipelines')}>Pipelines</button>
            <span className="crumb-sep">/</span>
            <button className="crumb crumb-link" onClick={() => navigate('/pipelines/topn')}>TopN</button>
            <span className="crumb-sep">/</span>
            <span className="crumb is-last">{aggName}</span>
          </div>
          <h1 className="page-title">{aggName}</h1>
        </header>
        <div className="empty">
          <span className="empty-ico"><IconEmpty size={36} /></span>
          <div className="empty-title">Aggregation not found</div>
          <p className="empty-text">No TopNAggregation named {aggName} exists in {groupName}.</p>
        </div>
      </div>
    );
  }

  const counters = agg.countersNumber || DEFAULT_COUNTERS;
  const conditions = flattenTopNCriteria(agg.criteria);
  const openSource = () => srcExists && navigate(`/metadata/measures/${src!.group}/${src!.name}`);

  // Deep-link into the Query console, pre-filled to query this aggregation.
  // The seed shape/route mirror GroupPage.tsx's "Query this resource" action
  // exactly (state.seed.{catalog,group,resource}); QueryConsole resolves a
  // 'topn' catalog seed against the topn-agg registry for `group`, so the
  // group here is the AGGREGATION's own group (== its source measure's
  // group), not necessarily anything else.
  const runQuery = () => navigate('/query', { state: { seed: { catalog: 'topn', group: groupName, resource: aggName } } });

  return (
    <div className="page-body">
      <header className="page-head">
        <div className="crumbs">
          <button className="crumb crumb-link" onClick={() => navigate('/pipelines')}>Pipelines</button>
          <span className="crumb-sep">/</span>
          <button className="crumb crumb-link" onClick={() => navigate('/pipelines/topn')}>TopN</button>
          <span className="crumb-sep">/</span>
          <span className="crumb is-last">{aggName}</span>
        </div>
        <div className="page-title-row">
          <h1 className="page-title">{aggName}</h1>
          <div className="page-actions">
            {srcExists && (
              <button className="btn btn-primary" onClick={runQuery} title="Open the Query console pre-filled to rank this measure">
                <IconPlay size={15} /> Run Top-N query
              </button>
            )}
            <button className="btn btn-ghost" onClick={() => navigate('/pipelines/topn')}>
              <IconArrowLeft size={15} /> Back
            </button>
            {canWrite && (
              <button className="btn btn-ghost" onClick={() => setModal({ kind: 'edit' })}><IconEdit size={15} /> Edit</button>
            )}
            {canWrite && (
              <button className="btn btn-danger-ghost" onClick={() => setModal({ kind: 'delete' })}><IconTrash size={15} /> Delete</button>
            )}
          </div>
        </div>
        <p className="page-meta">TopNAggregation in measure group {groupName}.</p>
      </header>

      <div className="grp-meta" role="region" aria-label="TopN aggregation summary">
        <div className="meta-chip"><span className="meta-k">rank</span><span className="meta-v">{topNRank(agg.fieldValueSort)}</span></div>
        <div className="meta-chip"><span className="meta-k">sort</span><span className="meta-v">{topNSortLabel(agg.fieldValueSort)}</span></div>
        <div className="meta-chip"><span className="meta-k">field</span><span className="meta-v">{agg.fieldName}</span></div>
        <div className="meta-chip"><span className="meta-k">counters</span><span className="meta-v">{counters}</span></div>
        {agg.lruSize != null && (
          <div className="meta-chip"><span className="meta-k">lru size</span><span className="meta-v">{agg.lruSize}</span></div>
        )}
      </div>

      <div className="detail-block">
        <div className="detail-h"><IconMeasures size={15} /> Source measure</div>
        <div className="topn-source-card">
          {srcExists ? (
            <button className="topn-source-link" onClick={openSource} title={`Open ${src!.group}/${src!.name}`}>
              <span className="topn-source-key mono">{src!.group}/</span>
              <span className="topn-source-name mono">{src!.name}</span>
              <IconArrowRight size={14} />
            </button>
          ) : (
            <span className="topn-source-missing">
              <IconAlert size={14} />
              <span className="mono">{src?.group}/{src?.name}</span> — measure no longer exists
            </span>
          )}
          <span className="topn-source-field">
            ranks field <span className="mono">{agg.fieldName}</span>
            {srcExists && !srcFieldNames.includes(agg.fieldName ?? '') && (
              <span className="topn-warn-inline"><IconAlert size={12} /> not a field of the source</span>
            )}
          </span>
        </div>
      </div>

      <div className="detail-block">
        <div className="detail-h"><IconSort size={15} /> Ranking</div>
        <div className="chip-row">
          <span className={'topn-rank-lg is-' + (agg.fieldValueSort === 'SORT_ASC' ? 'bottomn' : agg.fieldValueSort === 'SORT_UNSPECIFIED' ? 'bothn' : 'topn')}>
            <IconTopN size={14} /> {topNRank(agg.fieldValueSort)}
          </span>
          <span className="ord-chip"><span className="topn-k">field_value_sort</span>{topNSortLabel(agg.fieldValueSort)}</span>
          <span className="ord-chip"><span className="topn-k">counters_number</span>{counters}</span>
        </div>
      </div>

      <div className="detail-block" role="region" aria-label="Group by tags">
        <div className="detail-h"><IconKey size={15} /> Group by tags</div>
        <div className="chip-row">
          {(agg.groupByTagNames ?? []).map((n, i) => (
            <span key={n} className="ord-chip"><span className="picker-ord">{i + 1}</span>{n}</span>
          ))}
          {!(agg.groupByTagNames ?? []).length && <span className="dim">no grouping — single global ranking</span>}
        </div>
      </div>

      <div className="detail-block">
        <div className="detail-h"><IconFilter size={15} /> Criteria</div>
        {conditions.length ? (
          <div className="topn-cond-row">
            {conditions.map((c, i) => (
              <React.Fragment key={i}>
                {i > 0 && <span className="topn-and">AND</span>}
                <CondChip tag={c.tag} op={c.op} value={c.value} />
              </React.Fragment>
            ))}
          </div>
        ) : (
          <div className="prop-note">
            <IconFilter size={15} />
            <span>No criteria — every data point in <span className="mono">{src?.name}</span> is aggregated.</span>
          </div>
        )}
      </div>

      {modal?.kind === 'edit' && (
        <TopNFormModal mode="edit" groupName={groupName} aggregation={agg} onClose={() => setModal(null)} />
      )}
      {modal?.kind === 'delete' && (
        <DeleteTopNModal groupName={groupName} aggregation={agg} onClose={() => setModal(null)}
          onDeleted={() => navigate('/pipelines/topn')} />
      )}
    </div>
  );
}
