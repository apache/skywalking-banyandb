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

// Overview of the live TopN and trace sampling pipeline registries.

import React, { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router';
import { useQuery } from '@tanstack/react-query';

import type { TopNAggregationSchema } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { IconPipelines, IconTopN, IconSampling, IconArrowRight, IconGroup } from '../components/icons.js';
import { RankBadge } from '../pipelines/topn-shared.js';
import { SamplingStatus } from '../pipelines/SamplingPages.js';

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

function useSamplingPipelineType(): PipelineTypeEntry {
  const { data } = useQuery({ queryKey: ['groups'], queryFn: () => apiDataSource.listGroups() });
  const items = useMemo<PipelineItem[]>(() => (data?.groups ?? [])
    .filter((group) => group.catalog === 'CATALOG_TRACE' && group.pipeline)
    .map((group) => ({
      name: group.name,
      group: group.name,
      path: `/pipelines/sampling/${encodeURIComponent(group.name)}`,
      meta: <SamplingStatus pipeline={group.pipeline} />,
    })), [data]);

  return {
    key: 'sampling',
    label: 'Sampling',
    icon: IconSampling,
    noun: 'pipeline',
    nounPlural: 'pipelines',
    desc: 'Controls trace retention with sampler chains during merge, finalize, and lifecycle stage transitions.',
    items,
  };
}

export function PipelinesPage() {
  const navigate = useNavigate();
  const topn = useTopNPipelineType();
  const sampling = useSamplingPipelineType();
  const byType = useMemo(() => [topn, sampling], [topn, sampling]);

  const all = useMemo(() => byType.flatMap((pt) => pt.items.map((it) => ({ pt, it }))), [byType]);
  const total = all.length;
  const groupsInUse = useMemo(() => new Set(all.map((x) => x.it.group)).size, [all]);
  const featured = all.slice(0, 4);

  return (
    <div className="page-body">
      <header className="page-head">
        <h1 className="page-title">Pipelines</h1>
        <p className="page-meta">Continuous processing for derived results and trace retention.</p>
      </header>

      <div className="pipe-intro">
        <p className="pipe-lead">
          Pipelines process stored data to maintain derived results and control trace retention.
          TopN maintains ranked counters for queries, while sampling selects traces to retain during
          storage events and lifecycle transitions.
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
