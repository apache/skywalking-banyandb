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

import type { StreamSchema, MeasureSchema, TraceSchema, PropertySchema, IndexRuleSchema, IndexRuleBindingSchema } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import {
  IconMeasures,
  IconPlus, IconEdit, IconTrash, IconSearch, IconPlay, IconArrowRight,
} from '../components/icons.js';
import { CATALOG_MAP, TYPE_TITLES, TYPE_ICONS, formatInterval } from './meta-utils.js';

function tagCount(r: StreamSchema | MeasureSchema | TraceSchema | PropertySchema): number {
  if ('tagFamilies' in r && r.tagFamilies) return r.tagFamilies.reduce((n, f) => n + (f.tags?.length ?? 0), 0);
  if ('tags' in r && r.tags) return r.tags.length;
  return 0;
}

function fieldCount(r: StreamSchema | MeasureSchema | TraceSchema | PropertySchema): number {
  return 'fields' in r && r.fields ? r.fields.length : 0;
}

export function GroupPage({
  type, groupName,
  onNewResource, onEditResource, onDeleteResource,
  onEditGroup, onDeleteGroup,
  onNewIndexRule, onEditIndexRule, onDeleteIndexRule,
  onNewIndexRuleBinding, onEditIndexRuleBinding, onDeleteIndexRuleBinding,
}: {
  type: string;
  groupName: string;
  onNewResource?: () => void;
  onEditResource?: (resource: StreamSchema | MeasureSchema | TraceSchema | PropertySchema) => void;
  onDeleteResource?: (resource: StreamSchema | MeasureSchema | TraceSchema | PropertySchema) => void;
  onEditGroup?: () => void;
  onDeleteGroup?: () => void;
  onNewIndexRule?: () => void;
  onEditIndexRule?: (ruleName: string) => void;
  onDeleteIndexRule?: (ruleName: string) => void;
  onNewIndexRuleBinding?: () => void;
  onEditIndexRuleBinding?: (bindingName: string) => void;
  onDeleteIndexRuleBinding?: (bindingName: string) => void;
}) {
  const navigate = useNavigate();
  const { session } = useAuth();
  const isAdmin = session?.role === 'admin';
  const [search, setSearch] = useState('');
  const [page, setPage] = useState(1);

  const PAGE_SIZE = 50;
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
  const filteredResources = search.trim()
    ? allResources.filter((r) => r.metadata.name.toLowerCase().includes(search.trim().toLowerCase()))
    : allResources;
  const totalPages = Math.max(1, Math.ceil(filteredResources.length / PAGE_SIZE));
  const pagedResources = filteredResources.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  const rowPath = (name: string) =>
    isProperties ? `/properties/${groupName}/${name}` : `/metadata/${type}/${groupName}/${name}`;

  return (
    <div className="page-body">
      <header className="page-head">
        <div className="crumbs">
          {isProperties ? (
            <>
              <button className="crumb crumb-link" onClick={() => navigate('/properties')}>Properties</button>
              <span className="crumb-sep"><IconArrowRight size={12} /></span>
              <span className="crumb is-last">{groupName}</span>
            </>
          ) : (
            <>
              <span className="crumb">Metadata</span>
              <span className="crumb-sep"><IconArrowRight size={12} /></span>
              <button className="crumb crumb-link" onClick={() => navigate('/metadata/' + type)}>{TYPE_TITLES[type]}</button>
              <span className="crumb-sep"><IconArrowRight size={12} /></span>
              <span className="crumb is-last">{groupName}</span>
            </>
          )}
        </div>
        <div className="page-title-row">
          <div className="page-title-wrap">
            <h1 className="page-title">{groupName}</h1>
            <span className="title-badge">{catalogEntry.label}</span>
          </div>
          {isAdmin && (
            <div className="page-actions">
              <button className="btn btn-ghost" onClick={() => onEditGroup?.()}>
                <IconEdit size={15} /> Edit group
              </button>
              <button className="btn btn-danger-ghost" onClick={() => onDeleteGroup?.()}>
                <IconTrash size={15} /> Delete
              </button>
            </div>
          )}
        </div>
        <p className="page-meta">Group — the minimal physical unit managing shards, segments and retention</p>
      </header>

      {group && (
        <div className="grp-meta">
          <div className="meta-chip">
            <span className="meta-k">catalog</span>
            <span className="meta-v">{group.catalog}</span>
          </div>
          <div className="meta-chip">
            <span className="meta-k">shards</span>
            <span className="meta-v">{group.resourceOpts.shardNum}</span>
          </div>
          <div className="meta-chip">
            <span className="meta-k">segment</span>
            <span className="meta-v">{formatInterval(group.resourceOpts.segmentInterval)}</span>
          </div>
          <div className="meta-chip">
            <span className="meta-k">ttl</span>
            <span className="meta-v">{formatInterval(group.resourceOpts.ttl)}</span>
          </div>
        </div>
      )}

      <div className="res-toolbar">
        <label className="search-box">
          <IconSearch size={15} />
          <input
            type="search"
            placeholder={`Search ${catalogEntry.plural}…`}
            value={search}
            onChange={(e) => { setSearch(e.target.value); setPage(1); }}
          />
        </label>
        <div className="res-toolbar-right">
          <span className="res-count">{filteredResources.length} {filteredResources.length === 1 ? catalogEntry.singular : catalogEntry.plural}</span>
          {isAdmin && (
            <button className="btn btn-primary" onClick={() => onNewResource?.()}>
              <IconPlus size={15} /> New {catalogEntry.singular}
            </button>
          )}
        </div>
      </div>

      {!resourcesLoading && allResources.length === 0 ? (
        <div className="empty">
          <span className="empty-ico"><TypeIcon size={36} /></span>
          <p className="empty-title">No {catalogEntry.plural} in this group</p>
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
          <p className="empty-text">Try a different search term</p>
        </div>
      ) : (
        <table className="res-table">
          <thead className="res-head">
            <tr>
              <th className="rc-name">Name</th>
              <th className="rc-kind">Type</th>
              <th className="rc-schema">Schema</th>
              <th className="rc-actions rc-actions-h"></th>
            </tr>
          </thead>
          <tbody>
            {pagedResources.map((r) => {
              const tags = tagCount(r);
              const fields = fieldCount(r);
              const isMeasure = type === 'measures';
              const isMeasureResource = 'fields' in r;
              const isIndexMode = isMeasureResource && (r as MeasureSchema).indexMode;
              return (
                <tr
                  key={r.metadata.name}
                  className="res-row"
                  onClick={() => navigate(rowPath(r.metadata.name))}
                  style={{ cursor: 'pointer' }}
                >
                  <td className="rc-name">
                    <span className="rc-ico"><TypeIcon size={15} /></span>
                    <span className="mono">{r.metadata.name}</span>
                  </td>
                  <td className="rc-kind">
                    <span className="kind-badge">{isIndexMode ? 'Index' : catalogEntry.label}</span>
                  </td>
                  <td className="rc-detail rc-schema">
                    {tags > 0 && (
                      <span className="schema-chip">{tags} tag{tags !== 1 ? 's' : ''}</span>
                    )}
                    {isMeasure && fields > 0 && (
                      <span className="schema-chip">{fields} field{fields !== 1 ? 's' : ''}</span>
                    )}
                  </td>
                  <td className="rc-actions" onClick={(e) => e.stopPropagation()}>
                    <button
                      className="rc-act"
                      title="Query"
                      onClick={() => navigate('/query')}
                    >
                      <IconPlay size={15} />
                    </button>
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
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
      {totalPages > 1 && (
        <div className="doc-pager">
          <span>{filteredResources.length} {filteredResources.length === 1 ? catalogEntry.singular : catalogEntry.plural}</span>
          <div className="doc-pager-btns">
            <button className="pg-btn" disabled={page === 1} onClick={() => setPage((p) => p - 1)}>← Prev</button>
            <span className="doc-pager-page mono">{page} / {totalPages}</span>
            <button className="pg-btn" disabled={page === totalPages} onClick={() => setPage((p) => p + 1)}>Next →</button>
          </div>
        </div>
      )}

      <IndexRuleSection
        groupName={groupName}
        isAdmin={isAdmin}
        onNew={onNewIndexRule}
        onEdit={onEditIndexRule}
        onDelete={onDeleteIndexRule}
      />
      <IndexRuleBindingSection
        groupName={groupName}
        isAdmin={isAdmin}
        onNew={onNewIndexRuleBinding}
        onEdit={onEditIndexRuleBinding}
        onDelete={onDeleteIndexRuleBinding}
      />
    </div>
  );
}

function IndexRuleSection({ groupName, isAdmin, onNew, onEdit, onDelete }: {
  groupName: string;
  isAdmin: boolean;
  onNew?: () => void;
  onEdit?: (ruleName: string) => void;
  onDelete?: (ruleName: string) => void;
}) {
  const { data: rules = [], isLoading } = useQuery<IndexRuleSchema[]>({
    queryKey: ['indexRules', groupName],
    queryFn: () => apiDataSource.listIndexRules(groupName),
  });
  return (
    <section className="detail-block">
      <div className="detail-h">
        Index rules <span className="meta-v mono">· {rules.length}</span>
      </div>
      {isAdmin && onNew && (
        <div style={{ marginBottom: 10 }}>
          <button className="btn btn-ghost" onClick={onNew}>
            <IconPlus size={14} /> New index rule
          </button>
        </div>
      )}
      {isLoading ? (
        <p className="page-meta">Loading…</p>
      ) : rules.length === 0 ? (
        <p className="page-meta">No index rules in this group.</p>
      ) : (
        <div className="idx-table">
          <div className="idx-rule-head">
            <span>Name</span>
            <span>Tags</span>
            <span>Type</span>
            <span>Analyzer</span>
            <span className="idx-actions-h">Actions</span>
          </div>
          {rules.map((r) => (
            <div className="idx-rule-row" key={r.metadata.name}>
              <span className="idx-name-cell">
                <span className="idx-name mono">{r.metadata.name}</span>
              </span>
              <span>
                <span className="idx-chiprow">
                  {r.tags.map((t) => <span key={t} className="idx-tag">{t}</span>)}
                </span>
              </span>
              <span>
                <span className={`idx-type-badge ${r.type === 'INDEX_TYPE_TREE' ? 'is-tree' : 'is-inv'}`}>
                  {r.type === 'INDEX_TYPE_TREE' ? 'tree' : 'inv'}
                </span>
              </span>
              <span className="idx-dim">{r.analyzer || '—'}</span>
              <span className="idx-actions" onClick={(e) => e.stopPropagation()}>
                {isAdmin && (
                  <>
                    <button className="idx-act" title="Edit" onClick={() => onEdit?.(r.metadata.name)}>
                      <IconEdit size={14} />
                    </button>
                    <button className="idx-act is-danger" title="Delete" onClick={() => onDelete?.(r.metadata.name)}>
                      <IconTrash size={14} />
                    </button>
                  </>
                )}
              </span>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

function IndexRuleBindingSection({ groupName, isAdmin, onNew, onEdit, onDelete }: {
  groupName: string;
  isAdmin: boolean;
  onNew?: () => void;
  onEdit?: (bindingName: string) => void;
  onDelete?: (bindingName: string) => void;
}) {
  const { data: bindings = [], isLoading } = useQuery<IndexRuleBindingSchema[]>({
    queryKey: ['indexRuleBindings', groupName],
    queryFn: () => apiDataSource.listIndexRuleBindings(groupName),
  });
  return (
    <section className="detail-block">
      <div className="detail-h">
        Index rule bindings <span className="meta-v mono">· {bindings.length}</span>
      </div>
      {isAdmin && onNew && (
        <div style={{ marginBottom: 10 }}>
          <button className="btn btn-ghost" onClick={onNew}>
            <IconPlus size={14} /> New binding
          </button>
        </div>
      )}
      {isLoading ? (
        <p className="page-meta">Loading…</p>
      ) : bindings.length === 0 ? (
        <p className="page-meta">No index rule bindings in this group.</p>
      ) : (
        <div className="idx-table">
          <div className="idx-bind-head">
            <span>Name</span>
            <span>Subject</span>
            <span>Rules</span>
            <span>Window</span>
            <span className="idx-actions-h">Actions</span>
          </div>
          {bindings.map((b) => (
            <div className="idx-bind-row" key={b.metadata.name}>
              <span className="idx-name-cell">
                <span className="idx-name mono">{b.metadata.name}</span>
              </span>
              <span className="idx-subj-cell">
                <span className="idx-subj-cat">{b.subject.catalog.replace('CATALOG_', '')}</span>
                <span className="mono">{b.subject.name}</span>
              </span>
              <span>
                <span className="idx-chiprow">
                  {b.rules.map((r) => <span key={r} className="idx-tag">{r}</span>)}
                </span>
              </span>
              <span className="idx-window">
                <span className="idx-win-row">
                  <span className="idx-win-k">B</span>
                  <span className="mono">{b.beginAt}</span>
                </span>
                <span className="idx-win-row">
                  <span className="idx-win-k">E</span>
                  <span className="mono">{b.expireAt}</span>
                </span>
              </span>
              <span className="idx-actions" onClick={(e) => e.stopPropagation()}>
                {isAdmin && (
                  <>
                    <button className="idx-act" title="Edit" onClick={() => onEdit?.(b.metadata.name)}>
                      <IconEdit size={14} />
                    </button>
                    <button className="idx-act is-danger" title="Delete" onClick={() => onDelete?.(b.metadata.name)}>
                      <IconTrash size={14} />
                    </button>
                  </>
                )}
              </span>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
