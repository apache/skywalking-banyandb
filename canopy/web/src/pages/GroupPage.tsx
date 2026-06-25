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

export function GroupPage({ type, groupName, onNewResource, onEditResource, onDeleteResource, onEditGroup, onDeleteGroup }: {
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
  const filteredResources = search.trim()
    ? allResources.filter((r) => r.metadata.name.toLowerCase().includes(search.trim().toLowerCase()))
    : allResources;

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
            onChange={(e) => setSearch(e.target.value)}
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
            {filteredResources.map((r) => {
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
    </div>
  );
}
