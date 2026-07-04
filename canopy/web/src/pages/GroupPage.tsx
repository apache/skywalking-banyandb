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

import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';

import type { StreamSchema, MeasureSchema, TraceSchema, PropertySchema } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import {
  IconMeasures,
  IconPlus, IconEdit, IconTrash, IconSearch, IconPlay,
} from '../components/icons.js';
import { DEFAULT_PAGE_SIZE, Pager, usePagedList, useResetPage } from '../components/Pager.js';
import { CATALOG_MAP, TYPE_TITLES, TYPE_ICONS, formatInterval } from './meta-utils.js';

function tagCount(r: StreamSchema | MeasureSchema | TraceSchema | PropertySchema): number {
  if ('tagFamilies' in r && r.tagFamilies) return r.tagFamilies.reduce((n, f) => n + (f.tags?.length ?? 0), 0);
  if ('tags' in r && r.tags) return r.tags.length;
  return 0;
}

function fieldCount(r: StreamSchema | MeasureSchema | TraceSchema | PropertySchema): number {
  return 'fields' in r && r.fields ? r.fields.length : 0;
}

// One-line "detail" cell per row, matching the handoff:
//   measure: "interval 1m" / "index mode" / "data point"
//   stream:  "1 tag family" / "2 tag families"
//   trace:   "id: trace_id"
//   property:"N entries" / "1 entry"
function detailFor(kind: string, r: StreamSchema | MeasureSchema | TraceSchema | PropertySchema): string {
  if (kind === 'measure') {
    const m = r as MeasureSchema;
    if (m.indexMode) return 'index mode';
    return m.interval ? `interval ${m.interval}` : 'data point';
  }
  if (kind === 'stream') {
    const s = r as StreamSchema;
    const fams = s.tagFamilies?.length ?? 0;
    return `${fams} tag ${fams === 1 ? 'family' : 'families'}`;
  }
  if (kind === 'trace') {
    const t = r as TraceSchema;
    return `id: ${t.traceIdTagName || '—'}`;
  }
  // property — count entries when present, otherwise 0
  const p = r as PropertySchema & { entries?: unknown[] };
  const n = (p.entries ?? []).length;
  return `${n} ${n === 1 ? 'entry' : 'entries'}`;
}

function pluralize(catalogEntry: { singular: string; plural: string }, n: number): string {
  return n === 1 ? catalogEntry.singular : catalogEntry.plural;
}

export function GroupPage({
  type, groupName,
  onNewResource, onEditResource, onDeleteResource,
  onEditGroup, onDeleteGroup,
}: {
  type: string;
  groupName: string;
  onNewResource?: () => void;
  onEditResource?: (resource: StreamSchema | MeasureSchema | TraceSchema | PropertySchema) => void;
  onDeleteResource?: (resource: StreamSchema | MeasureSchema | TraceSchema | PropertySchema) => void;
  onEditGroup?: () => void;
  onDeleteGroup?: () => void;
}) {
  const navigate = useNavigate();
  const { session } = useAuth();
  const isAdmin = session?.role === 'admin';
  const [search, setSearch] = useState('');

  const catalogEntry = CATALOG_MAP[type] ?? CATALOG_MAP['measures'];
  const TypeIcon = TYPE_ICONS[type] ?? IconMeasures;
  const isProperties = type === 'properties';

  const { data: groupsData } = useQuery({
    queryKey: ['groups'],
    queryFn: () => apiDataSource.listGroups(),
  });
  const groups = groupsData?.groups ?? [];
  const group = groups.find((g) => g.name === groupName);

  const { data: resourcesData, isLoading: resourcesLoading } = useQuery({
    queryKey: ['resources', type, groupName],
    queryFn: () => apiDataSource.listResourcesInGroup(type, groupName),
  });
  const allResources = resourcesData ?? [];
  const q = search.trim().toLowerCase();
  const filteredResources = q
    ? allResources.filter((r) => r.metadata.name.toLowerCase().includes(q))
    : allResources;
  const { page, setPage, pageItems } = usePagedList(filteredResources, DEFAULT_PAGE_SIZE);
  // Reset to page 1 whenever the search filter changes so the user isn't
  // stranded on an out-of-range page after narrowing results.
  useResetPage(setPage, q);

  // Surface a "not found" empty state when the group doesn't exist (typo'd URL).
  if (!group && !groupsData) {
    return (
      <div className="page-body">
        <div className="empty">
          <span className="empty-ico spin"><TypeIcon size={32} /></span>
          <div className="empty-title">Loading…</div>
        </div>
      </div>
    );
  }
  if (!group) {
    return (
      <div className="page-body">
        <div className="page-head">
          <div className="crumbs">
            {isProperties ? (
              <>
                <button className="crumb crumb-link" onClick={() => navigate('/properties')}>Properties</button>
                <span className="crumb-sep">/</span>
                <span className="crumb is-last">{groupName}</span>
              </>
            ) : (
              <>
                <span className="crumb">Metadata</span>
                <span className="crumb-sep">/</span>
                <button className="crumb crumb-link" onClick={() => navigate('/metadata/' + type)}>{TYPE_TITLES[type]}</button>
                <span className="crumb-sep">/</span>
                <span className="crumb is-last">{groupName}</span>
              </>
            )}
          </div>
          <h1 className="page-title">{groupName}</h1>
        </div>
        <div className="empty">
          <span className="empty-ico"><TypeIcon size={36} /></span>
          <div className="empty-title">Group not found</div>
          <p className="empty-text">No group named {groupName} exists in this catalog.</p>
        </div>
      </div>
    );
  }

  const rowPath = (name: string) =>
    isProperties ? `/properties/${groupName}/${name}` : `/metadata/${type}/${groupName}/${name}`;

  const stages = group.resourceOpts.stages ?? [];
  // Group-level replicas lives on ResourceOpts (proto field 6), not on each
  // LifecycleStage — BanyanDB's wire response puts it next to shard_num, ttl,
  // and the stages list.
  const replicas = group.resourceOpts.replicas;
  const replicasWarn = typeof replicas === 'number' && replicas === 0;

  return (
    <div className="page-body">
      <header className="page-head">
        <div className="crumbs">
          {isProperties ? (
            <>
              <button className="crumb crumb-link" onClick={() => navigate('/properties')}>Properties</button>
              <span className="crumb-sep">/</span>
              <span className="crumb is-last">{groupName}</span>
            </>
          ) : (
            <>
              <span className="crumb">Metadata</span>
              <span className="crumb-sep">/</span>
              <button className="crumb crumb-link" onClick={() => navigate('/metadata/' + type)}>{TYPE_TITLES[type]}</button>
              <span className="crumb-sep">/</span>
              <span className="crumb is-last">{groupName}</span>
            </>
          )}
        </div>
        <div className="page-title-row">
          <div className="page-title-wrap">
            <h1 className="page-title">
              {groupName}
              <span className="title-badge">{catalogEntry.label}</span>
            </h1>
          </div>
          {isAdmin && (
            <div className="page-actions">
              <button className="btn btn-ghost" onClick={() => onEditGroup?.()}>
                <IconEdit size={15} /> Edit
              </button>
              <button className="btn btn-danger-ghost" onClick={() => onDeleteGroup?.()}>
                <IconTrash size={15} /> Delete
              </button>
            </div>
          )}
        </div>
        <p className="page-meta">
          Group — the minimal physical unit managing shards, segments and retention
        </p>
      </header>

      <div className="grp-meta">
        <div className="meta-chip">
          <span className="meta-k">catalog</span>
          <span className="meta-v">{catalogEntry.label}</span>
        </div>
        <div className="meta-chip">
          <span className="meta-k">shards</span>
          <span className="meta-v">{group.resourceOpts.shardNum}</span>
        </div>
        {typeof replicas === 'number' && (
          <div className="meta-chip">
            <span className="meta-k">replicas</span>
            <span
              className={'meta-v' + (replicasWarn ? ' is-warn' : '')}
              title={replicasWarn ? 'Replicas 0 — data in this group has no redundancy' : undefined}
            >
              {replicasWarn ? '0 — no redundancy' : replicas}
            </span>
          </div>
        )}
        {group.resourceOpts.segmentInterval && (
          <div className="meta-chip">
            <span className="meta-k">segment</span>
            <span className="meta-v">{formatInterval(group.resourceOpts.segmentInterval)}</span>
          </div>
        )}
        <div className="meta-chip">
          <span className="meta-k">ttl</span>
          <span className="meta-v">{formatInterval(group.resourceOpts.ttl)}</span>
        </div>
        {stages.length > 0 && (
          <div className="meta-chip">
            <span className="meta-k">stages</span>
            <span className="meta-v">{stages.map((s) => s.name).join(' → ')}</span>
          </div>
        )}
      </div>

      <div className="res-toolbar">
        <div className="search-box">
          <IconSearch size={15} />
          <input
            type="search"
            placeholder={`Filter ${catalogEntry.plural}…`}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
        <div className="res-toolbar-right">
          <span className="res-count">
            {q
              ? `${filteredResources.length} of ${allResources.length} ${pluralize(catalogEntry, allResources.length)}`
              : `${allResources.length} ${pluralize(catalogEntry, allResources.length)}`}
          </span>
          {isAdmin && (
            <button className="btn btn-primary" onClick={() => onNewResource?.()}>
              <IconPlus size={16} /> New {catalogEntry.singular}
            </button>
          )}
        </div>
      </div>

      {!resourcesLoading && allResources.length === 0 ? (
        <div className="empty">
          <span className="empty-ico"><TypeIcon size={36} /></span>
          <p className="empty-title">No {catalogEntry.plural} in this group</p>
          <p className="empty-text">Define a {catalogEntry.singular} in {groupName} to begin ingesting data.</p>
          {isAdmin && (
            <button className="btn btn-primary" onClick={() => onNewResource?.()}>
              <IconPlus size={15} /> Create {catalogEntry.singular}
            </button>
          )}
        </div>
      ) : !resourcesLoading && filteredResources.length === 0 ? (
        <div className="empty">
          <span className="empty-ico"><IconSearch size={36} /></span>
          <p className="empty-title">No matches</p>
          <p className="empty-text">No {catalogEntry.singular} in {groupName} matches &ldquo;{search}&rdquo;.</p>
        </div>
      ) : (
        <>
        <div className="res-table">
          <div className="res-head">
            <span className="rc-name">Name</span>
            <span className="rc-kind">Type</span>
            <span className="rc-detail">Detail</span>
            <span className="rc-schema">Schema</span>
            <span className="rc-actions rc-actions-h" />
          </div>
          {pageItems.map((r) => {
            const tags = tagCount(r);
            const fields = fieldCount(r);
            const isMeasure = type === 'measures';
            const isMeasureResource = 'fields' in r;
            const isIndexMode = isMeasureResource && (r as MeasureSchema).indexMode;
            const kind = catalogEntry.singular;
            return (
              <div
                key={r.metadata.name}
                className="res-row"
                role="button"
                tabIndex={0}
                onClick={() => navigate(rowPath(r.metadata.name))}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    navigate(rowPath(r.metadata.name));
                  }
                }}
              >
                <span className="rc-name">
                  <span className="rc-ico"><TypeIcon size={15} /></span>
                  <span title={r.metadata.name}>{r.metadata.name}</span>
                </span>
                <span className="rc-kind">
                  <span className={'kind-badge' + (isIndexMode ? ' is-idx' : '')}>
                    {isIndexMode ? 'Index' : (kind.charAt(0).toUpperCase() + kind.slice(1))}
                  </span>
                </span>
                <span className="rc-detail" title={detailFor(kind, r)}>{detailFor(kind, r)}</span>
                <span className="rc-schema">
                  {isProperties ? (
                    <span className="schema-chip">{detailFor(kind, r)}</span>
                  ) : (
                    <>
                      {tags > 0 && (
                        <span className="schema-chip">{tags} tag{tags !== 1 ? 's' : ''}</span>
                      )}
                      {isMeasure && fields > 0 && (
                        <span className="schema-chip">{fields} field{fields !== 1 ? 's' : ''}</span>
                      )}
                    </>
                  )}
                </span>
                <span className="rc-actions" onClick={(e) => e.stopPropagation()}>
                  {!isProperties && (
                    <button
                      className="rc-act"
                      title={`Query this ${catalogEntry.singular}`}
                      onClick={() => navigate('/query', {
                        state: {
                          seed: {
                            catalog: type === 'measures' ? 'measures'
                              : type === 'streams' ? 'streams'
                              : type === 'traces' ? 'traces'
                              : 'measures',
                            group: groupName,
                            resource: r.metadata.name,
                          },
                        },
                      })}
                    >
                      <IconPlay size={14} />
                    </button>
                  )}
                  {isAdmin && (
                    <>
                      <button
                        className="rc-act"
                        title="Edit"
                        onClick={() => onEditResource?.(r)}
                      >
                        <IconEdit size={15} />
                      </button>
                      <button
                        className="rc-act is-danger"
                        title="Delete"
                        onClick={() => onDeleteResource?.(r)}
                      >
                        <IconTrash size={15} />
                      </button>
                    </>
                  )}
                </span>
              </div>
            );
          })}
        </div>
        <Pager
          total={filteredResources.length}
          pageSize={DEFAULT_PAGE_SIZE}
          page={page}
          onPageChange={setPage}
          label={pluralize(catalogEntry, filteredResources.length)}
        />
        </>
      )}
    </div>
  );
}
