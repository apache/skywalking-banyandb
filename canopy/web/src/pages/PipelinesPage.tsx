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

// PipelinesPage.tsx — the generic Pipelines section overview (/pipelines).
// Ported from .handoff-import/banyandb/project/pipelines.jsx (window-global
// JSX -> ES module TSX).
//
// The handoff drives the overview off a `PIPELINE_TYPES` registry so adding a
// future pipeline type (e.g. tail-sampling) means appending one entry rather
// than touching this page. v1 has exactly one type — TopN — so this port
// keeps that registry shape (see PIPELINE_TYPES below) but resolves it
// directly against the live registry (listGroups + listTopNAggregations)
// instead of a mock `groups` prop; a second type can plug in the same way
// the handoff's comment describes, without changing this file's structure.

import React, { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';

import type { TopNAggregationSchema } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { IconPipelines, IconTopN, IconArrowRight, IconGroup } from '../components/icons.js';
import { RankBadge } from '../pipelines/topn-shared.js';

interface PipelineItem {
  readonly name: string;
  readonly group: string;
  readonly path: string;
  readonly meta: React.ReactNode;
}

// Registry entry contract (mirrors the handoff's PIPELINE_TYPES doc comment):
// key/label/icon/noun(Plural)/desc describe the type; `items` is the flat
// list of every pipeline instance of this type, already resolved live.
interface PipelineTypeEntry {
  readonly key: string;
  readonly label: string;
  readonly icon: React.ComponentType<{ size?: number }>;
  readonly noun: string;
  readonly nounPlural: string;
  readonly desc: string;
  readonly items: readonly PipelineItem[];
}

function useTopNPipelineType(): PipelineTypeEntry {
  const { data: groupsData } = useQuery({ queryKey: ['groups'], queryFn: () => apiDataSource.listGroups() });
  const measureGroupNames = useMemo(
    () => (groupsData?.groups ?? []).filter((g) => g.catalog === 'CATALOG_MEASURE').map((g) => g.name),
    [groupsData],
  );

  const [aggsByGroup, setAggsByGroup] = useState<ReadonlyMap<string, readonly TopNAggregationSchema[]>>(new Map());
  useEffect(() => {
    if (measureGroupNames.length === 0) { setAggsByGroup(new Map()); return; }
    let cancelled = false;
    Promise.allSettled(
      measureGroupNames.map((g) => apiDataSource.listTopNAggregations(g).then((aggs) => ({ group: g, aggs }))),
    ).then((results) => {
      if (cancelled) return;
      const next = new Map<string, readonly TopNAggregationSchema[]>();
      for (const r of results) if (r.status === 'fulfilled') next.set(r.value.group, r.value.aggs);
      setAggsByGroup(next);
    });
    return () => { cancelled = true; };
  }, [measureGroupNames]);

  const items = useMemo<PipelineItem[]>(() => {
    const out: PipelineItem[] = [];
    for (const [group, aggs] of aggsByGroup) {
      for (const a of aggs) {
        out.push({
          name: a.metadata.name,
          group,
          path: `/pipelines/topn/${group}/${a.metadata.name}`,
          meta: <RankBadge sort={a.fieldValueSort} />,
        });
      }
    }
    return out;
  }, [aggsByGroup]);

  return {
    key: 'topn',
    label: 'TopN',
    icon: IconTopN,
    noun: 'aggregation',
    nounPlural: 'aggregations',
    desc: 'Maintains ranked counters over a measure field — Top N (largest), Bottom N (smallest), or both — optionally grouped by tags and filtered by criteria.',
    items,
  };
}

export function PipelinesPage() {
  const navigate = useNavigate();
  const topn = useTopNPipelineType();
  const byType = useMemo(() => [topn], [topn]);

  const all = useMemo(() => byType.flatMap((pt) => pt.items.map((it) => ({ pt, it }))), [byType]);
  const total = all.length;
  const groupsInUse = useMemo(() => new Set(all.map((x) => x.it.group)).size, [all]);
  const featured = all.slice(0, 4);

  return (
    <div className="page-body">
      <header className="page-head">
        <h1 className="page-title">Pipelines</h1>
        <p className="page-meta">Continuous processing that maintains derived results over stored data.</p>
      </header>

      <div className="pipe-intro">
        <p className="pipe-lead">
          Pipelines run continuously over stored data to maintain derived results — computed as data arrives,
          so they are ready at query time instead of being computed on demand. Each pipeline type produces a
          different kind of derived result.
        </p>
      </div>

      <div className="pipe-stats">
        <div className="pipe-stat">
          <span className="pipe-stat-v">{total}</span>
          <span className="pipe-stat-k">pipeline{total !== 1 ? 's' : ''}</span>
        </div>
        <div className="pipe-stat">
          <span className="pipe-stat-v">{byType.length}</span>
          <span className="pipe-stat-k">pipeline type{byType.length !== 1 ? 's' : ''}</span>
        </div>
        <div className="pipe-stat">
          <span className="pipe-stat-v">{groupsInUse}</span>
          <span className="pipe-stat-k">group{groupsInUse !== 1 ? 's' : ''} in use</span>
        </div>
      </div>

      <div className="detail-h" style={{ marginTop: 4 }}><IconPipelines size={15} /> Pipeline types</div>
      <div className="pipe-kinds">
        {byType.map((pt) => {
          const Icon = pt.icon;
          return (
            <button key={pt.key} className="pipe-kind" onClick={() => navigate(`/pipelines/${pt.key}`)}>
              <span className="pipe-kind-ico"><Icon size={20} /></span>
              <span className="pipe-kind-body">
                <span className="pipe-kind-name">{pt.label}</span>
                <span className="pipe-kind-desc">{pt.desc}</span>
                <span className="pipe-kind-stat mono">{pt.items.length} {pt.items.length === 1 ? pt.noun : pt.nounPlural}</span>
              </span>
              <span className="pipe-kind-go"><IconArrowRight size={16} /></span>
            </button>
          );
        })}
      </div>

      {featured.length > 0 && (
        <>
          <div className="detail-h" style={{ marginTop: 24 }}><IconPipelines size={15} /> Recent pipelines</div>
          <div className="pipe-recent">
            {featured.map(({ pt, it }) => {
              const Icon = pt.icon;
              return (
                <button key={`${pt.key}/${it.group}/${it.name}`} className="pipe-recent-row" onClick={() => navigate(it.path)}>
                  <span className="idx-ico"><Icon size={14} /></span>
                  <span className="pipe-recent-name mono">{it.name}</span>
                  <span className="topn-group-chip mono"><IconGroup size={11} /> {it.group}</span>
                  {it.meta}
                  <span className="pipe-recent-go"><IconArrowRight size={14} /></span>
                </button>
              );
            })}
            {byType.filter((pt) => pt.items.length > 0).map((pt) => (
              <button key={pt.key} className="pipe-recent-all" onClick={() => navigate(`/pipelines/${pt.key}`)}>
                View all {pt.label} {pt.nounPlural} <IconArrowRight size={14} />
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
