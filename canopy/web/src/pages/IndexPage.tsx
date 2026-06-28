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

import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';

import type { IndexRuleSchema, IndexRuleBindingSchema } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import {
  IconMeasures, IconStreams, IconTraces, IconSearch, IconPlus, IconEdit, IconTrash,
  IconArrowLeft, IconAlert, IconEmpty, IconCheck, IconIndex, IconLink,
} from '../components/icons.js';
import { DEFAULT_PAGE_SIZE, Pager, usePagedList, useResetPage } from '../components/Pager.js';
import { SubjectOverflow, type SubjectItem } from '../components/SubjectOverflow.js';
import { CATALOG_MAP, bindingStatus, type BindingStatus } from './meta-utils.js';

const TYPE_ICON: Record<string, React.ComponentType<{ size?: number }>> = {
  measures: IconMeasures,
  streams: IconStreams,
  traces: IconTraces,
};

// ── Status helpers ──────────────────────────────────────────────────────────

const STATUS_META: Record<BindingStatus, { label: string; tone: string }> = {
  active:  { label: 'active',  tone: 'ok' },
  expired: { label: 'expired', tone: 'danger' },
  orphan:  { label: 'orphan',  tone: 'danger' },
  pending: { label: 'pending', tone: 'warn' },
};

function StatusBadge({ status }: { status: BindingStatus }) {
  const m = STATUS_META[status] ?? STATUS_META.active;
  return (
    <span className={'idx-status is-' + m.tone}>
      <span className="idx-status-dot" />{m.label}
    </span>
  );
}

// BanyanDB liaison encodes IndexType as proto-style enum names (TYPE_TREE /
// TYPE_INVERTED). The legacy INDEX_TYPE_* variant is matched here too so rules
// created by older web clients still render correctly after a GET.
function indexTypeLabel(t: string): { tone: string; label: string } {
  if (t === 'TYPE_INVERTED' || t === 'INDEX_TYPE_INVERTED') return { tone: 'is-inv', label: 'inv' };
  if (t === 'TYPE_TREE' || t === 'INDEX_TYPE_TREE') return { tone: 'is-tree', label: 'tree' };
  return { tone: 'is-skip', label: 'skipping' };
}

function TypeBadge({ type, analyzer }: { type: string; analyzer?: string }) {
  const { tone, label } = indexTypeLabel(type);
  return (
    <span className={`idx-type-badge ${tone}`}>
      {label}
      {analyzer && (type === 'TYPE_INVERTED' || type === 'INDEX_TYPE_INVERTED') && (
        <span className="idx-type-analyzer"> · {analyzer}</span>
      )}
    </span>
  );
}

// ── Rule tab ────────────────────────────────────────────────────────────────

function RuleTab({
  type, groupName, focusRule, onNew, onEdit, onDelete, onBind,
}: {
  type: string;
  groupName: string;
  focusRule: string | null;
  onNew: () => void;
  onEdit: (name: string) => void;
  onDelete: (name: string) => void;
  onBind: (ruleName: string) => void;
}) {
  const { session } = useAuth();
  const isAdmin = session?.role === 'admin';
  const [filter, setFilter] = useState('');
  const nowMs = Date.now();

  const { data: rules = [], isLoading } = useQuery<IndexRuleSchema[]>({
    queryKey: ['indexRules', groupName],
    queryFn: () => apiDataSource.listIndexRules(groupName),
  });

  const { data: bindings = [] } = useQuery<IndexRuleBindingSchema[]>({
    queryKey: ['indexRuleBindings', groupName],
    queryFn: () => apiDataSource.listIndexRuleBindings(groupName),
  });

  const { data: resources = [] } = useQuery({
    queryKey: ['resources', type, groupName],
    queryFn: () => apiDataSource.listResourcesInGroup(type, groupName),
  });

  const subjectNames = new Set(
    (resources as Array<{ metadata: { name: string } }>).map((r) => r.metadata.name),
  );

  // For a rule, list every distinct subject that some binding brings it to.
  // When multiple bindings reach the same subject we prefer the "active" status.
  const subjectsFor = (ruleName: string) => {
    const seen = new Map<string, BindingStatus>();
    for (const b of bindings) {
      if (!b.rules.includes(ruleName)) continue;
      const sn = b.subject.name;
      if (!sn) continue;
      const st = bindingStatus(b.beginAt, b.expireAt, subjectNames.has(sn), nowMs);
      const prev = seen.get(sn);
      if (!prev || (prev !== 'active' && st === 'active')) seen.set(sn, st);
    }
    return Array.from(seen, ([name, status]) => ({
      name,
      status,
      exists: subjectNames.has(name),
    }));
  };

  const [glow, setGlow] = useState<string | null>(focusRule);
  useEffect(() => {
    if (!glow) return;
    const t = setTimeout(() => setGlow(null), 2200);
    return () => clearTimeout(t);
  }, [glow]);

  const q = filter.trim().toLowerCase();
  const rows = q
    ? rules.filter((r) => r.metadata.name.toLowerCase().includes(q))
    : rules;
  const { page, setPage, pageItems } = usePagedList(rows, DEFAULT_PAGE_SIZE);
  // Snap back to page 1 when the filter narrows the list.
  useResetPage(setPage, q);

  const basePath = `/metadata/${type}/${groupName}`;

  return (
    <>
      <div className="res-toolbar">
        <label className="search-box">
          <IconSearch size={15} />
          <input type="search" placeholder="Filter rules by name"
            value={filter} onChange={(e) => setFilter(e.target.value)} />
        </label>
        <div className="res-toolbar-right">
          <span className="res-count">
            {q ? `${rows.length} of ${rules.length} rules` : `${rules.length} rule${rules.length === 1 ? '' : 's'}`}
          </span>
          {isAdmin && (
            <button className="btn btn-primary" onClick={onNew}>
              <IconPlus size={16} /> New index rule
            </button>
          )}
        </div>
      </div>

      {isLoading ? (
        <p className="page-meta">Loading…</p>
      ) : rules.length === 0 ? (
        <div className="empty">
          <span className="empty-ico"><IconIndex size={36} /></span>
          <p className="empty-title">No index rules yet</p>
          <p className="empty-text">
            Define an IndexRule to index tags in group {groupName}. Rules take effect once bound to a subject.
          </p>
          {isAdmin && (
            <button className="btn btn-primary" onClick={onNew}>
              <IconPlus size={15} /> New index rule
            </button>
          )}
        </div>
      ) : rows.length === 0 ? (
        <div className="empty">
          <span className="empty-ico"><IconSearch size={36} /></span>
          <p className="empty-title">No matches</p>
          <p className="empty-text">No index rule matches "{filter}".</p>
        </div>
      ) : (
        <div className="idx-table">
          <div className="idx-rule-head">
            <span>Rule</span>
            <span>Indexed tags</span>
            <span>Type</span>
            <span>Bound subjects</span>
            <span className="idx-actions-h">Actions</span>
          </div>
          {pageItems.map((r) => {
            const subjects = subjectsFor(r.metadata.name);
            return (
              <div key={r.metadata.name} className={'idx-rule-row' + (glow === r.metadata.name ? ' is-focus' : '')}>
                <span className="idx-name-cell">
                  <span className="idx-ico"><IconIndex size={14} /></span>
                  <span className="idx-name mono">{r.metadata.name}</span>
                </span>
                <span className="idx-chiprow">
                  {r.tags.map((t) => <span key={t} className="idx-tag mono">{t}</span>)}
                  {r.tags.length > 1 && <span className="idx-multi" title="Multi-tag index">×{r.tags.length}</span>}
                </span>
                <span><TypeBadge type={r.type} analyzer={r.analyzer} /></span>
                <span className="idx-chiprow">
                  {subjects.length === 0 && isAdmin && (
                    <button className="idx-link-cta" onClick={() => onBind(r.metadata.name)} title="Bind to a subject">
                      <IconLink size={12} /> unbound
                    </button>
                  )}
                  {subjects.length === 0 && !isAdmin && (
                    <span className="idx-opt is-flag">unbound</span>
                  )}
                  <SubjectOverflow
                    subjects={subjects.map((s): SubjectItem => ({
                      name: s.name,
                      exists: s.exists,
                      status: (STATUS_META[s.status] ?? STATUS_META.active).tone,
                    }))}
                    basePath={basePath}
                  />
                </span>
                <span className="idx-actions" onClick={(e) => e.stopPropagation()}>
                  {isAdmin && (
                    <>
                      <button className="idx-act" title="Bind to subject" onClick={() => onBind(r.metadata.name)}>
                        <IconLink size={14} />
                      </button>
                      <button className="idx-act" title="Edit" onClick={() => onEdit(r.metadata.name)}>
                        <IconEdit size={14} />
                      </button>
                      <button className="idx-act is-danger" title="Delete" onClick={() => onDelete(r.metadata.name)}>
                        <IconTrash size={14} />
                      </button>
                    </>
                  )}
                </span>
              </div>
            );
          })}
        </div>
      )}
      <Pager
        total={rows.length}
        pageSize={DEFAULT_PAGE_SIZE}
        page={page}
        onPageChange={setPage}
        label="rules"
      />
    </>
  );
}

// ── Binding tab ─────────────────────────────────────────────────────────────

function BindingTab({
  type, groupName, onNew, onEdit, onDelete,
}: {
  type: string;
  groupName: string;
  onNew: () => void;
  onEdit: (name: string) => void;
  onDelete: (name: string) => void;
}) {
  const navigate = useNavigate();
  const { session } = useAuth();
  const isAdmin = session?.role === 'admin';
  const [scope, setScope] = useState<'attention' | 'active' | 'all'>('attention');
  const nowMs = Date.now();

  const { data: bindings = [], isLoading } = useQuery<IndexRuleBindingSchema[]>({
    queryKey: ['indexRuleBindings', groupName],
    queryFn: () => apiDataSource.listIndexRuleBindings(groupName),
  });

  const { data: rules = [] } = useQuery<IndexRuleSchema[]>({
    queryKey: ['indexRules', groupName],
    queryFn: () => apiDataSource.listIndexRules(groupName),
  });

  const { data: resources = [] } = useQuery({
    queryKey: ['resources', type, groupName],
    queryFn: () => apiDataSource.listResourcesInGroup(type, groupName),
  });

  const ruleNames = new Set(rules.map((r) => r.metadata.name));
  const subjectNames = new Set(
    (resources as Array<{ metadata: { name: string } }>).map((r) => r.metadata.name),
  );

  const withStatus = bindings.map((b) => ({
    b,
    status: bindingStatus(b.beginAt, b.expireAt, subjectNames.has(b.subject.name), nowMs),
  }));
  const attentionCount = withStatus.filter((x) => x.status === 'orphan' || x.status === 'expired').length;
  const activeCount = withStatus.filter((x) => x.status === 'active').length;
  const shown = withStatus.filter(({ status }) =>
    scope === 'all' ? true : scope === 'active' ? status === 'active' : (status === 'orphan' || status === 'expired'),
  );
  const { page: bindPage, setPage: setBindPage, pageItems: shownPage } = usePagedList(shown, DEFAULT_PAGE_SIZE);
  // Snap back to page 1 whenever the scope filter changes.
  useResetPage(setBindPage, scope);

  const scopes = [
    { id: 'attention' as const, label: 'Needs attention', n: attentionCount },
    { id: 'active' as const,    label: 'Active',         n: activeCount },
    { id: 'all' as const,       label: 'All',            n: bindings.length },
  ];

  const basePath = `/metadata/${type}/${groupName}`;
  const fmtTs = (ms: number) => new Date(ms).toISOString().replace('T', ' ').slice(0, 16);

  return (
    <>
      <div className="res-toolbar idx-bind-toolbar">
        <div className="idx-filter">
          {scopes.map((s) => (
            <button key={s.id}
              className={'idx-filter-btn' + (scope === s.id ? ' is-on' : '')}
              onClick={() => setScope(s.id)}>
              {s.id === 'attention' && s.n > 0 && <span className="idx-filter-warn" />}
              {s.label}<span className="idx-filter-n">{s.n}</span>
            </button>
          ))}
        </div>
        <div className="res-toolbar-right">
          {isAdmin && (
            <button className="btn btn-primary" onClick={onNew}>
              <IconPlus size={16} /> New binding
            </button>
          )}
        </div>
      </div>

      {scope === 'attention' && attentionCount > 0 && (
        <div className="idx-attention-note">
          <IconAlert size={15} />
          <span>
            <b>{attentionCount}</b> binding{attentionCount !== 1 ? 's' : ''} need attention —
            orphaned (subject removed) or past their <span className="mono">expire_at</span>.
            They no longer produce indices.
          </span>
        </div>
      )}

      {isLoading ? (
        <p className="page-meta">Loading…</p>
      ) : bindings.length === 0 ? (
        <div className="empty">
          <span className="empty-ico"><IconLink size={36} /></span>
          <p className="empty-title">No bindings yet</p>
          <p className="empty-text">
            Bind index rules to a {CATALOG_MAP[type]?.singular ?? 'resource'} in {groupName} to start generating indices.
          </p>
          {isAdmin && (
            <button className="btn btn-primary" onClick={onNew}>
              <IconPlus size={15} /> New binding
            </button>
          )}
        </div>
      ) : shown.length === 0 ? (
        <div className="empty">
          <span className="empty-ico">
            {scope === 'attention' ? <IconCheck size={36} /> : <IconEmpty size={36} />}
          </span>
          <p className="empty-title">
            {scope === 'attention' ? 'Nothing needs attention' : 'No bindings in this view'}
          </p>
          <p className="empty-text">
            {scope === 'attention'
              ? 'Every binding in this group has an existing subject and an active validity window.'
              : 'Switch the filter to see other bindings.'}
          </p>
        </div>
      ) : (
        <div className="idx-table">
          <div className="idx-bind-head">
            <span>Name</span>
            <span>Subject</span>
            <span>Rules</span>
            <span>Window</span>
            <span>Status</span>
            <span className="idx-actions-h">Actions</span>
          </div>
          {shownPage.map(({ b, status }) => {
            const subjName = b.subject.name;
            const subjExists = subjectNames.has(subjName);
            return (
              <div key={b.metadata.name} className={'idx-bind-row' + (status === 'orphan' || status === 'expired' ? ' is-attention' : '')}>
                <span className="idx-name-cell">
                  <span className="idx-ico"><IconLink size={14} /></span>
                  <span className="idx-name mono">{b.metadata.name}</span>
                </span>
                <span className="idx-subj-cell">
                  {subjExists ? (
                    <button className="subj-chip" title={`Open ${subjName}`}
                      onClick={() => navigate(`${basePath}/${subjName}`)}>
                      {subjName}
                    </button>
                  ) : (
                    <span className="subj-chip is-danger" title="Subject no longer exists">
                      <IconAlert size={11} /> {subjName || '—'}
                    </span>
                  )}
                  <span className="idx-subj-cat">{b.subject.catalog.replace('CATALOG_', '').toLowerCase()}</span>
                </span>
                <span>
                  <span className="idx-chiprow">
                    {b.rules.map((rn) =>
                      ruleNames.has(rn)
                        ? <span key={rn} className="idx-tag mono">{rn}</span>
                        : <span key={rn} className="idx-tag mono is-missing" title="Rule no longer exists">{rn}</span>,
                    )}
                    {b.rules.length === 0 && <span className="idx-dim">none</span>}
                  </span>
                </span>
                <span className="idx-window">
                  <span className="idx-win-row">
                    <span className="idx-win-k">B</span>
                    <span className="mono">{fmtTs(b.beginAt)}</span>
                  </span>
                  <span className={'idx-win-row' + (status === 'expired' ? ' is-bad' : '')}>
                    <span className="idx-win-k">E</span>
                    <span className="mono">{fmtTs(b.expireAt)}</span>
                  </span>
                </span>
                <span><StatusBadge status={status} /></span>
                <span className="idx-actions" onClick={(e) => e.stopPropagation()}>
                  {isAdmin && (
                    <>
                      <button className="idx-act" title="Edit" onClick={() => onEdit(b.metadata.name)}>
                        <IconEdit size={14} />
                      </button>
                      <button className="idx-act is-danger" title="Delete" onClick={() => onDelete(b.metadata.name)}>
                        <IconTrash size={14} />
                      </button>
                    </>
                  )}
                </span>
              </div>
            );
          })}
        </div>
      )}
      <Pager
        total={shown.length}
        pageSize={DEFAULT_PAGE_SIZE}
        page={bindPage}
        onPageChange={setBindPage}
        label="bindings"
      />
    </>
  );
}

// ── Page shell ──────────────────────────────────────────────────────────────

export function IndexPage({
  type, groupName, groupExists,
  onNewIndexRule, onEditIndexRule, onDeleteIndexRule,
  onNewIndexRuleBinding, onEditIndexRuleBinding, onDeleteIndexRuleBinding,
}: {
  type: string;
  groupName: string;
  groupExists: boolean;
  onNewIndexRule: () => void;
  onEditIndexRule: (name: string) => void;
  onDeleteIndexRule: (name: string) => void;
  /** Optional rule name opens the binding form pre-selected with that rule. */
  onNewIndexRuleBinding: (presetRuleName?: string) => void;
  onEditIndexRuleBinding: (name: string) => void;
  onDeleteIndexRuleBinding: (name: string) => void;
}) {
  const navigate = useNavigate();
  const [tab, setTab] = useState<'rule' | 'binding'>('rule');

  const { data: rules = [] } = useQuery<IndexRuleSchema[]>({
    queryKey: ['indexRules', groupName],
    queryFn: () => apiDataSource.listIndexRules(groupName),
  });
  const { data: bindings = [] } = useQuery<IndexRuleBindingSchema[]>({
    queryKey: ['indexRuleBindings', groupName],
    queryFn: () => apiDataSource.listIndexRuleBindings(groupName),
  });

  // Deep-link focus: ResourceDetailPage stashes a rule name in window.__idxFocus
  // before navigating here, so the matching row glows briefly and we land on
  // the Rule tab. Read once on mount and clear the slot to avoid re-highlights.
  const focusRule = React.useMemo(() => {
    const w = window as unknown as { __idxFocus?: { rule?: string } };
    const f = w.__idxFocus;
    w.__idxFocus = undefined;
    return f?.rule ?? null;
  }, []);

  const catLabel = (() => {
    const entry = CATALOG_MAP[type];
    if (!entry) return type.charAt(0).toUpperCase() + type.slice(1);
    return entry.singular.charAt(0).toUpperCase() + entry.singular.slice(1);
  })();
  const basePath = `/metadata/${type}/${groupName}`;
  const TypeIcon = TYPE_ICON[type] ?? IconMeasures;

  if (!groupExists) {
    return (
      <div className="page-body">
        <div className="page-head">
          <div className="crumbs">
            <span className="crumb">Metadata</span>
            <span className="crumb-sep">/</span>
            <button className="crumb crumb-link" onClick={() => navigate(`/metadata/${type}`)}>{catLabel}</button>
            <span className="crumb-sep">/</span>
            <span className="crumb">{groupName}</span>
            <span className="crumb-sep">/</span>
            <span className="crumb is-last">Index</span>
          </div>
          <h1 className="page-title">Index</h1>
        </div>
        <div className="empty">
          <span className="empty-ico"><IconEmpty size={32} /></span>
          <div className="empty-title">Group not found</div>
          <p className="empty-text">No group named {groupName} exists in {catLabel}.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="page-body">
      <header className="page-head">
        <div className="crumbs">
          <span className="crumb">Metadata</span>
          <span className="crumb-sep">/</span>
          <button className="crumb crumb-link" onClick={() => navigate(`/metadata/${type}`)}>{catLabel}</button>
          <span className="crumb-sep">/</span>
          <button className="crumb crumb-link" onClick={() => navigate(basePath)}>{groupName}</button>
          <span className="crumb-sep">/</span>
          <span className="crumb is-last">Index</span>
        </div>
        <div className="page-title-row">
          <div className="page-title-wrap">
            <h1 className="page-title">
              Index
              <span className="title-badge">{catLabel}</span>
            </h1>
          </div>
          <div className="page-actions">
            <button className="btn btn-ghost" onClick={() => navigate(basePath)}>
              <IconArrowLeft size={15} /> Back to group
            </button>
          </div>
        </div>
        <p className="page-meta">IndexRules and IndexRuleBindings for group {groupName}</p>
      </header>

      <div className="idx-tabs">
        <button
          className={'idx-tab' + (tab === 'rule' ? ' is-active' : '')}
          onClick={() => setTab('rule')}>
          <TypeIcon size={15} /> Rule <span className="idx-tab-n">{rules.length}</span>
        </button>
        <button
          className={'idx-tab' + (tab === 'binding' ? ' is-active' : '')}
          onClick={() => setTab('binding')}>
          <IconLink size={15} /> Binding <span className="idx-tab-n">{bindings.length}</span>
        </button>
      </div>

      {tab === 'rule' ? (
        <RuleTab type={type} groupName={groupName} focusRule={focusRule}
          onNew={onNewIndexRule}
          onEdit={onEditIndexRule}
          onDelete={onDeleteIndexRule}
          onBind={onNewIndexRuleBinding} />
      ) : (
        <BindingTab type={type} groupName={groupName}
          onNew={() => onNewIndexRuleBinding()}
          onEdit={onEditIndexRuleBinding}
          onDelete={onDeleteIndexRuleBinding} />
      )}
    </div>
  );
}