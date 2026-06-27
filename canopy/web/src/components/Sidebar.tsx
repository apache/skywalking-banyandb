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
import { useNavigate, useLocation } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';

import { CanopyMark } from './CanopyMark.js';
import { useAuth } from '../auth/AuthContext.js';
import { apiDataSource } from '../data/api.js';
import {
  IconHome, IconMetadata, IconMeasures, IconStreams, IconTraces,
  IconProperties, IconPipelines, IconQuery, IconChevron, IconCollapse, IconSignOut,
  IconShield, IconViewer,
} from './icons.js';

interface SidebarProps {
  collapsed: boolean;
  onToggleCollapse: () => void;
}

export function Sidebar({ collapsed, onToggleCollapse }: SidebarProps) {
  const { session, setSession } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const [metaOpen, setMetaOpen] = useState(true);

  const { data: groupsData } = useQuery({
    queryKey: ['groups'],
    queryFn: () => apiDataSource.listGroups(),
  });
  const groups = groupsData?.groups ?? [];
  const navCounts = {
    measures: groups.filter(g => g.catalog === 'CATALOG_MEASURE').length,
    streams: groups.filter(g => g.catalog === 'CATALOG_STREAM').length,
  };

  const active = location.pathname;
  const isAdmin = session?.role === 'admin';
  const endpoint = session?.endpoint ?? '';
  const displayHost = endpoint.replace(/^https?:\/\//, '');

  const signOut = async () => {
    try { await fetch('/auth/logout', { method: 'POST' }); } catch { /* ignore */ }
    setSession(null);
    navigate('/');
  };

  return (
    <aside className={'sidebar' + (collapsed ? ' is-collapsed' : '')}>
      <div className="side-head">
        <div className="brand">
          <CanopyMark size={28} className="brand-mark" />
          <div className="brand-text">
            <span className="brand-name">Canopy</span>
            <span className="brand-ver">
              {session?.banyanVersion ? `BanyanDB · v${session.banyanVersion}` : 'BanyanDB'}
            </span>
          </div>
        </div>
      </div>

      <nav className="side-nav" aria-label="Main navigation">
        {/* Home */}
        <div className="nav-node">
          <div className="nav-rowline">
            <button
              className={'nav-row lvl-0' + (active === '/' ? ' is-active' : '')}
              style={{ paddingLeft: 12 }}
              onClick={() => navigate('/')}
              title="Home"
            >
              <span className="nav-ico"><IconHome /></span>
              <span className="nav-label">Home</span>
            </button>
          </div>
        </div>

        {/* Metadata */}
        <div className="nav-node">
          <div className="nav-rowline">
            <button
              className={'nav-row lvl-0' + (active.startsWith('/metadata') ? ' has-active' : '')}
              style={{ paddingLeft: 12, paddingRight: collapsed ? 12 : 32 }}
              onClick={() => { if (!collapsed) setMetaOpen((o) => !o); }}
              aria-expanded={metaOpen}
              title="Metadata"
            >
              <span className="nav-ico"><IconMetadata /></span>
              <span className="nav-label">Metadata</span>
            </button>
            {!collapsed && (
              <button
                type="button"
                className="nav-chev-btn"
                aria-label={(metaOpen ? 'Collapse' : 'Expand') + ' Metadata'}
                aria-expanded={metaOpen}
                onClick={() => setMetaOpen((o) => !o)}
              >
                <span className={'nav-chev' + (metaOpen ? ' is-open' : '')}><IconChevron /></span>
              </button>
            )}
          </div>
          <div className={'nav-sub' + (metaOpen ? ' is-open' : '')}>
            <div className="nav-sub-inner">
              <span className="nav-guide" style={{ left: 21 }} />
              {[
                { path: '/metadata/measures', label: 'Measures', Icon: IconMeasures, count: navCounts.measures },
                { path: '/metadata/streams', label: 'Streams', Icon: IconStreams, count: navCounts.streams },
                { path: '/metadata/traces', label: 'Traces', Icon: IconTraces, count: null },
              ].map(({ path, label, Icon, count }) => (
                <div key={path} className="nav-node">
                  <div className="nav-rowline">
                    <button
                      className={'nav-row lvl-1' + (active.startsWith(path) ? ' is-active' : '')}
                      style={{ paddingLeft: 28 }}
                      onClick={() => navigate(path)}
                      title={label}
                    >
                      <span className="nav-ico"><Icon size={16} /></span>
                      <span className="nav-label">{label}</span>
                      {count != null && <span className="nav-count">{count}</span>}
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Properties */}
        <div className="nav-node">
          <div className="nav-rowline">
            <button
              className={'nav-row lvl-0' + (active.startsWith('/properties') ? ' is-active' : '')}
              style={{ paddingLeft: 12 }}
              onClick={() => navigate('/properties')}
              title="Properties"
            >
              <span className="nav-ico"><IconProperties /></span>
              <span className="nav-label">Properties</span>
            </button>
          </div>
        </div>

        {/* Pipelines */}
        <div className="nav-node">
          <div className="nav-rowline">
            <button
              className={'nav-row lvl-0' + (active.startsWith('/pipelines') ? ' is-active' : '')}
              style={{ paddingLeft: 12 }}
              onClick={() => navigate('/pipelines')}
              title="Pipelines"
            >
              <span className="nav-ico"><IconPipelines /></span>
              <span className="nav-label">Pipelines</span>
            </button>
          </div>
        </div>

        {/* Query */}
        <div className="nav-node">
          <div className="nav-rowline">
            <button
              className={'nav-row lvl-0' + (active === '/query' ? ' is-active' : '')}
              style={{ paddingLeft: 12 }}
              onClick={() => navigate('/query')}
              title="Query"
            >
              <span className="nav-ico"><IconQuery /></span>
              <span className="nav-label">Query</span>
            </button>
          </div>
        </div>
      </nav>

      <div className="side-foot">
        <div className={'acct' + (!isAdmin ? ' is-readonly' : '')} title={isAdmin ? 'Administrator · full control' : 'Read-only · view only'}>
          <span className="acct-ico">
            {isAdmin ? <IconShield /> : <IconViewer />}
          </span>
          <span className="acct-text">
            <span className="acct-role">{isAdmin ? 'Administrator' : 'Read-only'}</span>
            <span className="acct-sub">{isAdmin ? 'Full control' : 'View only'}</span>
          </span>
          <button className="acct-signout" onClick={() => void signOut()} title="Sign out" aria-label="Sign out">
            <IconSignOut />
          </button>
        </div>
        <div className="side-foot-row">
          <div className="conn" title={'Connected to ' + endpoint}>
            <span className="conn-dot" />
            <span className="conn-text">
              <span className="conn-state">Connected</span>
              <span className="conn-host">{displayHost}</span>
            </span>
          </div>
          <button className="collapse-btn" onClick={onToggleCollapse} title="Toggle sidebar (⌘B)" aria-label="Toggle sidebar">
            <IconCollapse />
          </button>
        </div>
      </div>
    </aside>
  );
}
