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

import React, { useEffect, useMemo, useState } from 'react';
import { useNavigate, useLocation } from 'react-router';
import { useQuery } from '@tanstack/react-query';

import { CanopyMark } from './CanopyMark.js';
import { useAuth } from '../auth/AuthContext.js';
import { canopyPath } from '../runtime-config.js';
import { apiDataSource } from '../data/api.js';
import {
  IconHome, IconMetadata, IconMeasures, IconStreams, IconTraces,
  IconProperties, IconPipelines, IconQuery, IconChevron, IconCollapse, IconSignOut,
  IconShield, IconViewer, IconIndex, IconGroup, IconTopN,
} from './icons.js';

/** One property group's row + its lazily-fetched collection list (fetched
 *  only once expanded — see the PropertyNav doc comment below for why
 *  per-collection doc counts are omitted). */
function PropertyGroupRow({
  groupName, open, onToggle, active, navigate, collapsed,
}: {
  groupName: string;
  open: boolean;
  onToggle: () => void;
  active: string;
  navigate: (path: string) => void;
  collapsed: boolean;
}) {
  const path = `/properties/${groupName}`;
  const isActive = active === path;
  const { data: collections = [] } = useQuery({
    queryKey: ['resources', 'properties', groupName],
    queryFn: () => apiDataSource.listResourcesInGroup('properties', groupName),
    enabled: open,
  });
  return (
    <div className="nav-node">
      <div className="nav-rowline">
        <button
          className={'nav-row lvl-2' + (isActive ? ' is-active' : '')}
          style={{ paddingLeft: 44, paddingRight: collapsed ? 12 : 32 }}
          onClick={() => { if (!collapsed) onToggle(); navigate(path); }}
          aria-expanded={open}
          title={groupName}
        >
          <span className="nav-ico grp"><IconGroup size={14} /></span>
          <span className="nav-label">{groupName}</span>
        </button>
        {!collapsed && (
          <button
            type="button"
            className="nav-chev-btn"
            aria-label={(open ? 'Collapse' : 'Expand') + ' ' + groupName}
            aria-expanded={open}
            onClick={onToggle}
          >
            <span className={'nav-chev' + (open ? ' is-open' : '')}><IconChevron /></span>
          </button>
        )}
      </div>
      {open && (
        <div className="nav-sub is-open">
          <div className="nav-sub-inner">
            <span className="nav-guide" style={{ left: 53 }} />
            {collections.length === 0 ? (
              <span className="qb-dim" style={{ paddingLeft: 60, display: 'block' }}>no properties yet</span>
            ) : collections.map((c) => {
              const cPath = `${path}/${c.metadata.name}`;
              return (
                <button
                  key={c.metadata.name}
                  className={'nav-row lvl-3 is-leaf' + (active === cPath ? ' is-active' : '')}
                  style={{ paddingLeft: 60 }}
                  onClick={() => navigate(cPath)}
                  title={c.metadata.name}
                >
                  <span className="nav-label">{c.metadata.name}</span>
                </button>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

/** Properties -> group (expandable) -> collection (leaf) nav tree.
 *
 * Structural nuance vs. CatalogNav (Metadata's section->group-leaf shape):
 * Properties is one level deeper — group rows expand to a per-collection
 * list rather than being leaves themselves (see docs/property-design.md §3).
 * This is the "minimal" option the design calls out, reusing the existing
 * nav-row/nav-count/flyout CSS rather than replacing Sidebar with the
 * handoff's generic recursive NavRow tree (the "fuller port" alternative,
 * deferred — see the PR report).
 *
 * Counts: the "Properties" row's nav-count is the number of property groups
 * (cheap — already fetched for the groups list). Per-collection DOCUMENT
 * counts are deliberately omitted: showing them would need one
 * queryPropertyDocuments call per collection per render, an N+1 query burst
 * every time the sidebar mounts — not worth it for a nav badge. Collection
 * rows show just the name; the detail page is one click away for the actual
 * count. */
function PropertyNav({
  groups, open, onToggle, active, navigate, collapsed,
}: {
  groups: readonly { name: string }[];
  open: boolean;
  onToggle: () => void;
  active: string;
  navigate: (path: string) => void;
  collapsed: boolean;
}) {
  const [openGroups, setOpenGroups] = useState<Set<string>>(new Set());
  const toggleGroup = (name: string) => setOpenGroups((prev) => {
    const next = new Set(prev);
    if (next.has(name)) next.delete(name); else next.add(name);
    return next;
  });
  const onPath = active === '/properties' || active.startsWith('/properties/');
  return (
    <div className="nav-node">
      <div className="nav-rowline">
        <button
          className={'nav-row lvl-0' + (onPath ? ' has-active' : '')}
          style={{ paddingLeft: 12, paddingRight: collapsed ? 12 : 32 }}
          onClick={() => { if (!collapsed) onToggle(); navigate('/properties'); }}
          aria-expanded={open}
          title="Properties"
        >
          <span className="nav-ico"><IconProperties /></span>
          <span className="nav-label">Properties</span>
          <span className="nav-count">{groups.length}</span>
        </button>
        {!collapsed && groups.length > 0 && (
          <button
            type="button"
            className="nav-chev-btn"
            aria-label={(open ? 'Collapse' : 'Expand') + ' Properties'}
            aria-expanded={open}
            onClick={onToggle}
          >
            <span className={'nav-chev' + (open ? ' is-open' : '')}><IconChevron /></span>
          </button>
        )}
      </div>
      {open && groups.length > 0 && (
        <div className="nav-sub is-open">
          <div className="nav-sub-inner">
            <span className="nav-guide" style={{ left: 21 }} />
            {groups.map((g) => (
              <PropertyGroupRow
                key={g.name}
                groupName={g.name}
                open={openGroups.has(g.name)}
                onToggle={() => toggleGroup(g.name)}
                active={active}
                navigate={navigate}
                collapsed={collapsed}
              />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

interface SidebarProps {
  collapsed: boolean;
  onToggleCollapse: () => void;
}

/** A catalog row (Measures/Streams/Traces) with its groups expanded underneath. */
function CatalogNav({
  basePath, label, Icon, groups, open, onToggle, active, navigate, collapsed,
  isIndexOpen, onToggleIndex, onActivateGroupIndex,
}: {
  basePath: string;
  label: string;
  Icon: React.ComponentType<{ size?: number }>;
  groups: Array<{ name: string }>;
  open: boolean;
  onToggle: () => void;
  active: string;
  navigate: (path: string) => void;
  collapsed: boolean;
  /** Returns true if the given group's "Index" sub-row is unfolded. */
  isIndexOpen: (catalog: string, groupName: string) => boolean;
  /** Toggle the per-group Index sub-row. */
  onToggleIndex: (catalog: string, groupName: string) => void;
  /** Select this group as the "currently expanded" Index in its catalog
   *  (folds every other group in the same catalog). */
  onActivateGroupIndex: (catalog: string, groupName: string) => void;
}) {
  // basePath is e.g. "/metadata/measures" — the segment after "/metadata/"
  // is the catalog key we use to scope the open-set so a measure and a
  // stream that share a name don't collide.
  const catalog = basePath.replace(/^\/metadata\//, '');
  const onPath = active === basePath || active.startsWith(basePath + '/');
  return (
    <div className="nav-node">
      <div className="nav-rowline">
        <button
          className={'nav-row lvl-1' + (onPath ? ' has-active' : '')}
          style={{ paddingLeft: 28, paddingRight: collapsed ? 12 : 32 }}
          onClick={() => { if (!collapsed) { onToggle(); navigate(basePath); } else { navigate(basePath); } }}
          aria-expanded={open}
          title={label}
        >
          <span className="nav-ico"><Icon size={16} /></span>
          <span className="nav-label">{label}</span>
          <span className="nav-count">{groups.length}</span>
        </button>
        {!collapsed && groups.length > 0 && (
          <button
            type="button"
            className="nav-chev-btn"
            aria-label={(open ? 'Collapse' : 'Expand') + ' ' + label}
            aria-expanded={open}
            onClick={onToggle}
          >
            <span className={'nav-chev' + (open ? ' is-open' : '')}><IconChevron /></span>
          </button>
        )}
      </div>
      {open && groups.length > 0 && (
        <div className="nav-sub is-open">
          <div className="nav-sub-inner">
            <span className="nav-guide" style={{ left: 37 }} />
            {groups.map((g) => {
              const path = `${basePath}/${g.name}`;
              const indexPath = `${path}/Index`;
              const isActive = active === path;
              const isIndexActive = active === indexPath;
              // Per-group "Index" sub-row: hidden by default; the active group
              // gets auto-opened by a useEffect in the parent, and the user
              // can click the chevron on this row to fold/unfold manually.
              const indexOpen = isIndexOpen(catalog, g.name);
              return (
                <div key={g.name} className="nav-node">
                  <div className="nav-rowline">
                    <button
                      className={'nav-row lvl-2 is-leaf' + (isActive ? ' is-active' : '')}
                      style={{ paddingLeft: 44, paddingRight: collapsed ? 12 : 32 }}
                      onClick={() => { onActivateGroupIndex(catalog, g.name); navigate(path); }}
                      title={g.name}
                    >
                      <span className="nav-ico grp"><IconGroup size={14} /></span>
                      <span className="nav-label">{g.name}</span>
                    </button>
                    {!collapsed && (
                      <button
                        type="button"
                        className="nav-chev-btn"
                        aria-label={(indexOpen ? 'Collapse' : 'Expand') + ' Index for ' + g.name}
                        aria-expanded={indexOpen}
                        onClick={() => onToggleIndex(catalog, g.name)}
                      >
                        <span className={'nav-chev' + (indexOpen ? ' is-open' : '')}><IconChevron /></span>
                      </button>
                    )}
                  </div>
                  {indexOpen && (
                    <div className="nav-sub is-open">
                      <div className="nav-sub-inner">
                        <span className="nav-guide" style={{ left: 53 }} />
                        <button
                          className={'nav-row lvl-3' + (isIndexActive ? ' is-active' : '')}
                          style={{ paddingLeft: 60 }}
                          onClick={() => navigate(indexPath)}
                          title="Index rules and bindings"
                        >
                          <span className="nav-ico"><IconIndex size={18} /></span>
                          <span className="nav-label">Index</span>
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

export function Sidebar({ collapsed, onToggleCollapse }: SidebarProps) {
  const { session, setSession } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const [metaOpen, setMetaOpen] = useState(true);
  const [measuresOpen, setMeasuresOpen] = useState(false);
  const [streamsOpen, setStreamsOpen] = useState(false);
  const [tracesOpen, setTracesOpen] = useState(false);
  const [propertiesOpen, setPropertiesOpen] = useState(false);
  const [pipelinesOpen, setPipelinesOpen] = useState(false);
  // Per-group "Index" sub-row expansion. Empty by default — every Index is
  // folded so the sidebar doesn't grow noisy when there are many groups. The
  // active group's Index is auto-added below; users can fold/unfold manually
  // via the per-group chevron.
  // Per-group "Index" sub-row expansion, keyed by `${catalog}|${group}` so a
  // measure and a stream that happen to share a name are tracked separately.
  // Empty by default — every Index is folded so the sidebar doesn't grow
  // noisy. Clicking a group row activates its Index and folds every other
  // group in the same catalog. The chevron toggles just this group without
  // touching the others. The active group's Index is auto-activated in a
  // useEffect below.
  const [indexOpenGroups, setIndexOpenGroups] = useState<Set<string>>(new Set());
  const indexKey = (catalog: string, group: string) => catalog + '|' + group;
  // Per-catalog state, queried by the CatalogNav for a known catalog.
  const isIndexOpen = (catalog: string, group: string) => indexOpenGroups.has(indexKey(catalog, group));
  // Chevron click: toggle just this group's Index. Multiple groups can be
  // open simultaneously via the chevron — clicking the chevron never closes
  // another group.
  const onToggleIndex = (catalog: string, group: string) => setIndexOpenGroups((prev) => {
    const k = indexKey(catalog, group);
    const next = new Set(prev);
    if (next.has(k)) next.delete(k); else next.add(k);
    return next;
  });
  // Group-row click: select this group as the "currently expanded" Index for
  // its catalog. Every other group in the same catalog gets folded, but
  // groups in OTHER catalogs are left alone (e.g. a Measures group can
  // share a name with a Streams group, and we don't want clicking one to
  // collapse the other).
  const onActivateGroupIndex = (catalog: string, group: string) => setIndexOpenGroups((prev) => {
    const prefix = catalog + '|';
    const next = new Set<string>();
    for (const k of prev) if (!k.startsWith(prefix)) next.add(k);
    next.add(indexKey(catalog, group));
    return next;
  });

  const { data: groupsData } = useQuery({
    queryKey: ['groups'],
    queryFn: () => apiDataSource.listGroups(),
  });
  const groups = useMemo(() => groupsData?.groups ?? [], [groupsData]);
  const measureGroups = useMemo(() => groups.filter((g) => g.catalog === 'CATALOG_MEASURE'), [groups]);
  const streamGroups  = useMemo(() => groups.filter((g) => g.catalog === 'CATALOG_STREAM'),  [groups]);
  const traceGroups   = useMemo(() => groups.filter((g) => g.catalog === 'CATALOG_TRACE'),   [groups]);
  const propertyGroups = useMemo(() => groups.filter((g) => g.catalog === 'CATALOG_PROPERTY'), [groups]);
  const measureGroupNames = useMemo(() => measureGroups.map((g) => g.name), [measureGroups]);

  // Total TopN aggregation count, across every measure group — drives the
  // "Pipelines -> TopN (N)" badge. Pipelines is a flat one-level nav (per
  // docs/pipelines-design.md §3), so this is the only count the sidebar needs
  // (a second pipeline type would add its own count the same way).
  const [topNCount, setTopNCount] = useState(0);
  useEffect(() => {
    if (measureGroupNames.length === 0) { setTopNCount(0); return; }
    let cancelled = false;
    Promise.allSettled(measureGroupNames.map((g) => apiDataSource.listTopNAggregations(g))).then((results) => {
      if (cancelled) return;
      let total = 0;
      for (const r of results) if (r.status === 'fulfilled') total += r.value.length;
      setTopNCount(total);
    });
    return () => { cancelled = true; };
  }, [measureGroupNames]);

  const active = location.pathname;

  // Auto-open the catalog that owns the active path so the user sees
  // context. The handoff calls this "re-clicking the page you're already on
  // toggles its branch open/closed" — clicking the row already navigates.
  useEffect(() => {
    if (active.startsWith('/metadata/measures')) setMeasuresOpen(true);
    if (active.startsWith('/metadata/streams'))  setStreamsOpen(true);
    if (active.startsWith('/metadata/traces'))   setTracesOpen(true);
    if (active.startsWith('/properties/'))       setPropertiesOpen(true);
    if (active.startsWith('/pipelines/'))         setPipelinesOpen(true);
  }, [active]);

  // When the URL lands on a group's Index page, unfold that group's Index
  // sub-row so the user can see it. We only add — the user keeps manual
  // control to fold via the per-group chevron.
  useEffect(() => {
    const m = active.match(/^\/metadata\/(measures|streams|traces)\/([^/]+)\/Index$/);
    if (!m) return;
    const [, catalog, group] = m;
    const k = catalog + '|' + group;
    setIndexOpenGroups((prev) => {
      if (prev.has(k)) return prev;
      const next = new Set(prev);
      next.add(k);
      return next;
    });
  }, [active]);

  const isAdmin = session?.role === 'admin';

  // The BanyanDB upstream is configured once on the BFF (BANYANDB_TARGET).
  // Fetch it from /api/meta on mount so the footer's status row shows the
  // target host and reachability. Re-fetched periodically so the indicator
  // flips back to green once BanyanDB comes back online.
  const [banyandbTarget, setBanyandbTarget] = useState<string>('');
  const [banyandbReachable, setBanyandbReachable] = useState<boolean | null>(null);
  useEffect(() => {
    let cancelled = false;
    const probe = async () => {
      try {
        const res = await fetch(canopyPath('/api/meta'));
        if (!res.ok) return;
        const d = await res.json() as { banyandbTarget?: string; reachable?: boolean };
        if (cancelled) return;
        if (d?.banyandbTarget) setBanyandbTarget(d.banyandbTarget);
        if (typeof d?.reachable === 'boolean') setBanyandbReachable(d.reachable);
      } catch {
        /* keep last known state */
      }
    };
    void probe();
    const handle = setInterval(probe, 10_000);
    return () => { cancelled = true; clearInterval(handle); };
  }, []);
  const displayHost = banyandbTarget.replace(/^https?:\/\//, '');

  const signOut = async () => {
    try { await fetch(canopyPath('/auth/logout'), { method: 'POST' }); } catch { /* ignore */ }
    setSession(null);
    navigate('/');
  };

  return (
    <aside className={'sidebar' + (collapsed ? ' is-collapsed' : '')} aria-label="Sidebar">
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
              <CatalogNav
                basePath="/metadata/measures"
                label="Measures"
                Icon={IconMeasures}
                groups={measureGroups}
                open={measuresOpen}
                onToggle={() => setMeasuresOpen((o) => !o)}
                active={active}
                navigate={navigate}
                collapsed={collapsed}
                isIndexOpen={isIndexOpen}
                onToggleIndex={onToggleIndex}
                onActivateGroupIndex={onActivateGroupIndex}
              />
              <CatalogNav
                basePath="/metadata/streams"
                label="Streams"
                Icon={IconStreams}
                groups={streamGroups}
                open={streamsOpen}
                onToggle={() => setStreamsOpen((o) => !o)}
                active={active}
                navigate={navigate}
                collapsed={collapsed}
                isIndexOpen={isIndexOpen}
                onToggleIndex={onToggleIndex}
                onActivateGroupIndex={onActivateGroupIndex}
              />
              <CatalogNav
                basePath="/metadata/traces"
                label="Traces"
                Icon={IconTraces}
                groups={traceGroups}
                open={tracesOpen}
                onToggle={() => setTracesOpen((o) => !o)}
                active={active}
                navigate={navigate}
                collapsed={collapsed}
                isIndexOpen={isIndexOpen}
                onToggleIndex={onToggleIndex}
                onActivateGroupIndex={onActivateGroupIndex}
              />
            </div>
          </div>
        </div>

        {/* Properties */}
        <PropertyNav
          groups={propertyGroups}
          open={propertiesOpen}
          onToggle={() => setPropertiesOpen((o) => !o)}
          active={active}
          navigate={navigate}
          collapsed={collapsed}
        />

        {/* Pipelines — flat one-level nav: Pipelines -> TopN (count). Simpler
            than Property's group-expanding tree since v1 has exactly one
            pipeline type (docs/pipelines-design.md §3). */}
        <div className="nav-node">
          <div className="nav-rowline">
            <button
              className={'nav-row lvl-0' + (active.startsWith('/pipelines') ? ' has-active' : '')}
              style={{ paddingLeft: 12, paddingRight: collapsed ? 12 : 32 }}
              onClick={() => { if (!collapsed) setPipelinesOpen((o) => !o); navigate('/pipelines'); }}
              aria-expanded={pipelinesOpen}
              title="Pipelines"
            >
              <span className="nav-ico"><IconPipelines /></span>
              <span className="nav-label">Pipelines</span>
            </button>
            {!collapsed && (
              <button
                type="button"
                className="nav-chev-btn"
                aria-label={(pipelinesOpen ? 'Collapse' : 'Expand') + ' Pipelines'}
                aria-expanded={pipelinesOpen}
                onClick={() => setPipelinesOpen((o) => !o)}
              >
                <span className={'nav-chev' + (pipelinesOpen ? ' is-open' : '')}><IconChevron /></span>
              </button>
            )}
          </div>
          {pipelinesOpen && (
            <div className="nav-sub is-open">
              <div className="nav-sub-inner">
                <span className="nav-guide" style={{ left: 21 }} />
                <button
                  className={'nav-row lvl-1' + (active.startsWith('/pipelines/topn') ? ' is-active' : '')}
                  style={{ paddingLeft: 28 }}
                  onClick={() => navigate('/pipelines/topn')}
                  title="TopN"
                >
                  <span className="nav-ico"><IconTopN size={16} /></span>
                  <span className="nav-label">TopN</span>
                  <span className="nav-count">{topNCount}</span>
                </button>
              </div>
            </div>
          )}
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
          <div
            className={'conn' + (banyandbReachable === false ? ' is-unreachable' : '')}
            role="status"
            aria-label="BanyanDB connection"
            title={banyandbReachable === false
              ? `Cannot reach BanyanDB at ${banyandbTarget} — check BANYANDB_TARGET and the BanyanDB process.`
              : 'Connected to ' + banyandbTarget}
          >
            <span className="conn-dot" />
            <span className="conn-text">
              <span className="conn-state">{banyandbReachable === false ? 'Unreachable' : 'Connected'}</span>
              <span className="conn-host">{displayHost}</span>
            </span>
          </div>
          <button className="collapse-btn" onClick={onToggleCollapse} title="Toggle sidebar (⌘B)" aria-label="Toggle sidebar" aria-expanded={!collapsed}>
            <IconCollapse />
          </button>
        </div>
      </div>
    </aside>
  );
}
