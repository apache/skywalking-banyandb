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

import React from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';

import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import { IconGroup, IconPlus, IconArrowRight } from '../components/icons.js';
import { CATALOG_MAP, TYPE_TITLES, formatInterval } from './meta-utils.js';

export function TypeOverviewPage({ type, onNewGroup }: { type: string; onNewGroup?: () => void }) {
  const navigate = useNavigate();
  const { session } = useAuth();
  const isAdmin = session?.role === 'admin';

  const cat = CATALOG_MAP[type] ?? CATALOG_MAP['measures'];

  const { data } = useQuery({
    queryKey: ['groups'],
    queryFn: () => apiDataSource.listGroups(),
  });

  const groups = (data?.groups ?? []).filter((g) => g.catalog === cat.catalog);

  const groupPath = (groupName: string) =>
    type === 'properties' ? `/properties/${groupName}` : `/metadata/${type}/${groupName}`;

  return (
    <div className="page-body">
      <header className="page-head">
        <div className="crumbs">
          <span className="crumb">Metadata</span>
          <span className="crumb-sep">/</span>
          <span className="crumb is-last">{TYPE_TITLES[type]}</span>
        </div>
        <div className="page-title-row">
          <div className="page-title-wrap">
            <h1 className="page-title">
              {TYPE_TITLES[type]}
              <span className="title-badge">{groups.length}</span>
            </h1>
          </div>
          {isAdmin && (
            <div className="page-actions">
              <button className="btn btn-primary" onClick={() => onNewGroup?.()}>
                <IconPlus size={14} />
                New group
              </button>
            </div>
          )}
        </div>
        <p className="page-meta">Groups that belong to the {cat.singular} catalog</p>
      </header>

      {groups.length === 0 ? (
        <div className="empty">
          <span className="empty-ico"><IconGroup size={32} /></span>
          <div className="empty-title">No {type} groups yet</div>
          <p className="empty-text">Create a group to start defining {cat.plural}.</p>
          {isAdmin && (
            <button className="btn btn-primary" onClick={() => onNewGroup?.()}>
              New group
            </button>
          )}
        </div>
      ) : (
        <div className="grp-cards">
          {groups.map((g) => (
            <button
              key={g.name}
              className="grp-card"
              onClick={() => navigate(groupPath(g.name))}
            >
              <span className="grp-card-ico"><IconGroup size={20} /></span>
              <span className="grp-card-name">{g.name}</span>
              <span className="grp-card-count">{g.resourceOpts.shardNum} shards</span>
              <div className="grp-card-meta">
                <div className="grp-meta">
                  {g.resourceOpts.segmentInterval && (
                    <span className="meta-chip">
                      <span className="meta-k">interval</span>
                      <span className="meta-v">{formatInterval(g.resourceOpts.segmentInterval)}</span>
                    </span>
                  )}
                  {g.resourceOpts.ttl && (
                    <span className="meta-chip">
                      <span className="meta-k">ttl</span>
                      <span className="meta-v">{formatInterval(g.resourceOpts.ttl)}</span>
                    </span>
                  )}
                </div>
              </div>
              <span className="grp-card-arrow"><IconArrowRight size={16} /></span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
