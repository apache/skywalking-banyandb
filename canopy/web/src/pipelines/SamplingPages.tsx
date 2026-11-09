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
import { Link, useNavigate, useParams } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import type { Group, PipelinePlugin, TracePipelineConfig } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import { IconArrowRight, IconEdit, IconPlus, IconSearch, IconTraces, IconTrash } from '../components/icons.js';
import { SamplingFormModal } from './SamplingForm.js';
import './sampling.css';

export function SamplingStatus({ pipeline }: { pipeline?: TracePipelineConfig }) {
  if (!pipeline) return <span className="sampling-none">Not configured</span>;
  return <span className={'sampling-status' + (pipeline.enabled ? ' is-enabled' : '')}>
    <span />{pipeline.enabled ? 'Enabled' : 'Disabled'}
  </span>;
}

function Chain({ plugins = [], paths = false }: { plugins?: readonly PipelinePlugin[]; paths?: boolean }) {
  return <span className="sampling-chips">{plugins.map((plugin, index) => <React.Fragment key={index}>
    <span className="sampling-chain-chip"><span>{index + 1}.</span>{plugin.name}
      {paths && <span> · {plugin.sampler?.path}</span>}</span>
    {!paths && index < plugins.length - 1 && <IconArrowRight size={12} />}
  </React.Fragment>)}</span>;
}

function Breadcrumbs({ group }: { group?: string }) {
  return <div className="crumbs"><Link className="crumb crumb-link" to="/pipelines">Pipelines</Link>
    <span className="crumb-sep">/</span><Link className="crumb crumb-link" to="/pipelines/sampling">Sampling</Link>
    {group && <><span className="crumb-sep">/</span><span className="crumb is-last">{group}</span></>}
  </div>;
}

function useSamplingGroups() {
  return useQuery({ queryKey: ['groups'], queryFn: () => apiDataSource.listGroups() });
}

function useRemovePipeline() {
  const client = useQueryClient();
  const [error, setError] = useState('');
  const [pending, setPending] = useState(false);
  async function remove(group: Group) {
    if (!window.confirm(`Remove sampling from ${group.name}? The group and its data are untouched.`)) return false;
    setPending(true);
    setError('');
    try {
      // Read the latest complete group to preserve ordinary group edits.
      const latest = (await apiDataSource.listGroups()).groups.find((item) => item.name === group.name);
      if (!latest) throw new Error('Trace group no longer exists.');
      const { pipeline: removed, name, ...rest } = latest;
      void removed;
      if (latest.catalog !== 'CATALOG_TRACE') throw new Error('Sampling requires a trace group.');
      const metadata = 'metadata' in rest && rest.metadata && typeof rest.metadata === 'object' ? rest.metadata : {};
      await apiDataSource.updateGroup(name, { group: { ...rest, metadata: { ...metadata, name } } });
      await client.invalidateQueries({ queryKey: ['groups'] });
      return true;
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
      return false;
    } finally { setPending(false); }
  }
  return { remove, error, pending };
}

export function SamplingList() {
  const { session } = useAuth();
  const canWrite = session?.role === 'admin';
  const navigate = useNavigate();
  const { data, isLoading, error } = useSamplingGroups();
  const groups = (data?.groups ?? []).filter((group) => group.catalog === 'CATALOG_TRACE' && group.pipeline);
  const [search, setSearch] = useState('');
  const [status, setStatus] = useState('All');
  const [editor, setEditor] = useState<{ group?: Group } | null>(null);
  const removal = useRemovePipeline();
  const filtered = groups.filter((group) => group.name.toLowerCase().includes(search.trim().toLowerCase()) &&
    (status === 'All' || (status === 'Enabled' ? group.pipeline?.enabled : !group.pipeline?.enabled)));
  return <div className="page-body sampling-page">
    <header className="sampling-header"><div><Breadcrumbs /><h1 className="page-title">Sampling</h1>
      <p className="page-meta">Tail-sampling retention for trace groups. Each trace group carries at most one sampling pipeline.</p></div>
      {canWrite && <button className="btn btn-primary" onClick={() => setEditor({})}><IconPlus size={16} />New sampling pipeline</button>}
    </header>
    <div className="sampling-toolbar"><div className="sampling-filters">
      <label className="search-box"><IconSearch size={16} /><input aria-label="Filter by group" placeholder="Filter by group…"
        value={search} onChange={(event) => setSearch(event.target.value)} /></label>
      <label className="sampling-filter"><span>Status</span><select value={status} onChange={(event) => setStatus(event.target.value)}>
        {['All', 'Enabled', 'Disabled'].map((value) => <option key={value}>{value}</option>)}
      </select></label></div><span>{groups.length} sampling {groups.length === 1 ? 'pipeline' : 'pipelines'}</span>
    </div>
    {(error || removal.error) && <p role="alert" className="form-error">{removal.error || error?.message}</p>}
    {isLoading ? <p role="status">Loading…</p> : <div className="sampling-table"><div className="sampling-list-head">
      <span>Trace group</span><span>Status</span><span>Events</span><span>Targets</span><span>Sampler chain</span><span>Stage rules</span><span />
    </div>{filtered.map((group) => <div className="sampling-list-row" key={group.name}>
      <Link className="sampling-group" to={'/pipelines/sampling/' + encodeURIComponent(group.name)}>
        <span className="idx-ico"><IconTraces size={14} /></span><span className="mono">{group.name}</span></Link>
      <span><SamplingStatus pipeline={group.pipeline} /></span>
      <span className="sampling-chips">{group.pipeline?.enabledEvents?.map((event) => <span className="sampling-chip" key={event}>
        {event.replace('PIPELINE_EVENT_', '').toLowerCase()}</span>)}</span>
      <span className="mono sampling-target">{group.pipeline ? [...(group.pipeline.schemaNames ?? []), group.pipeline.schemaNameRegex ? 're: ' + group.pipeline.schemaNameRegex : undefined].filter(Boolean).join(', ') || 'All schemas' : ''}</span>
      <Chain plugins={group.pipeline?.plugins} />
      <span className="sampling-chips">{group.pipeline?.stages?.map((stage, index) => <span className="sampling-chip" key={index}>{stage.stage} · {stage.plugins?.length ?? 0}</span>)}
        {group.pipeline && !group.pipeline.stages?.length && '—'}</span>
      <span className="idx-actions">{canWrite && group.pipeline && <>
        <button className="idx-act" aria-label={'Edit ' + group.name} onClick={() => setEditor({ group })}><IconEdit size={15} /></button>
        <button className="idx-act is-danger" disabled={removal.pending} aria-label={'Remove sampling from ' + group.name}
          onClick={() => void removal.remove(group)}><IconTrash size={15} /></button></>}</span>
    </div>)}</div>}
    <p className="sampling-footnote">Only trace groups with a sampling pipeline are listed. Removing a pipeline clears <code>group.pipeline</code>; the group and its data are untouched.</p>
    {editor && <SamplingFormModal group={editor.group} onClose={(saved) => {
      setEditor(null); if (saved) navigate('/pipelines/sampling/' + encodeURIComponent(saved.name));
    }} />}
  </div>;
}

function graceLabel(value: string | undefined, fallback: string) {
  if (!value) return <>{fallback} <small>default</small></>;
  const seconds = Number(value.replace(/s$/, ''));
  return Number.isFinite(seconds) && seconds > 0 && seconds % 3600 === 0 ? `${seconds / 3600}h` :
    Number.isFinite(seconds) && seconds > 0 && seconds % 60 === 0 ? `${seconds / 60}m` : value;
}

export function SamplingDetail() {
  const { group: groupName = '' } = useParams();
  const { session } = useAuth();
  const canWrite = session?.role === 'admin';
  const { data, isLoading, error } = useSamplingGroups();
  const group = data?.groups.find((item) => item.name === groupName && item.catalog === 'CATALOG_TRACE');
  const [editing, setEditing] = useState(false);
  const removal = useRemovePipeline();
  const navigate = useNavigate();
  const pipeline = group?.pipeline;
  if (!group) return <div className="page-body sampling-page"><Breadcrumbs group={groupName} />
    <p role={error ? 'alert' : 'status'}>{error?.message || (isLoading ? 'Loading…' : 'Trace group not found.')}</p></div>;
  const settings = [
    ['Merge event', pipeline?.enabledEvents?.includes('PIPELINE_EVENT_MERGE') ? 'on' : 'off', 'In-merge filter during hot-phase compaction'],
    ['Finalize event', pipeline?.enabledEvents?.includes('PIPELINE_EVENT_FINALIZE') ? 'on' : 'off', 'Tail-sampling at hot segment finalization'],
    ['Merge grace', graceLabel(pipeline?.mergeGrace, '2h'), 'Traces newer than now − grace are kept'],
    ['Finalize grace', graceLabel(pipeline?.finalizeGrace, '5m'), 'Per-segment settling window'],
  ];
  return <div className="page-body sampling-page sampling-detail">
    <header className="sampling-header"><div><Breadcrumbs group={groupName} />
      <div className="sampling-title"><h1 className="page-title mono">{groupName}</h1><SamplingStatus pipeline={pipeline} /></div>
      <p className="page-meta">Sampling pipeline of trace group <Link className="mono" to={'/metadata/traces/' + encodeURIComponent(groupName)}>{groupName}</Link>
        {' · '}{pipeline?.plugins?.length ?? 0} samplers · {pipeline?.stages?.length ?? 0} stage {pipeline?.stages?.length === 1 ? 'rule' : 'rules'}</p></div>
      {canWrite && <div className="page-actions"><button className="btn btn-ghost" onClick={() => setEditing(true)}>
        {pipeline ? <IconEdit size={15} /> : <IconPlus size={15} />}{pipeline ? 'Edit' : 'Configure sampling'}</button>
        {pipeline && <button className="btn btn-danger-ghost" disabled={removal.pending} onClick={async () => {
          if (await removal.remove(group)) navigate('/pipelines/sampling');
        }}><IconTrash size={15} />Remove</button>}</div>}
    </header>
    {removal.error && <p role="alert" className="form-error">{removal.error}</p>}
    {pipeline && <>
      <section><h2>Settings</h2><div className="sampling-settings">{settings.map(([label, value, help], index) =>
        <div className="sampling-setting" key={index}><span>{label}</span><strong className={value === 'on' ? 'is-on' : ''}>{value}</strong><span>{help}</span></div>)}</div></section>
      <section><h2>Targets</h2><div className="sampling-targets"><div><span>Schema names</span><span className="sampling-chips">
        {pipeline.schemaNames?.length ? pipeline.schemaNames.map((name) => <span className="sampling-chip" key={name}>{name}</span>) : 'All schemas'}</span></div>
        <div><span>Schema name regex</span><span className="mono">{pipeline.schemaNameRegex || '—'}</span></div></div></section>
      <section><div className="sampling-section-title"><h2>Sampler chain</h2><span>Hot phase · evaluated in order on enabled events</span></div>
        <div className="sampling-table"><div className="sampling-chain-head"><span>#</span><span>Name</span><span>Plugin</span><span>Symbol</span><span>ABI</span><span>Config</span></div>
          {pipeline.plugins?.map((plugin, index) => <div className="sampling-chain-row" key={index}>
            <span className="sampling-number">{index + 1}</span><strong>{plugin.name}</strong><span>{plugin.sampler?.path || '—'}</span>
            <span>{plugin.sampler?.symbol || 'NewSampler'}</span><span>v{plugin.sampler?.abiVersion ?? 1}</span><code>{JSON.stringify(plugin.sampler?.config ?? {})}</code>
          </div>)}</div></section>
      <section><div className="sampling-section-title"><h2>Stage rules</h2><span>Applied by the lifecycle agent before data migrates out of the source stage</span></div>
        <div className="sampling-stages">{pipeline.stages?.map((stage, index) => {
          const stages = group.resourceOpts.stages ?? [];
          const stageIndex = stages.findIndex((item) => item.name === stage.stage);
          const next = stageIndex >= 0 ? stages[stageIndex + 1]?.name : undefined;
          return <div className="sampling-stage-card" key={index}><div className="sampling-stage-title"><strong>{stage.stage}</strong>
            {next && <><IconArrowRight size={14} /><span>{next}</span></>}</div><Chain plugins={stage.plugins} paths /></div>;
        })}</div></section>
    </>}
    {editing && <SamplingFormModal group={group} onClose={() => setEditing(false)} />}
  </div>;
}
