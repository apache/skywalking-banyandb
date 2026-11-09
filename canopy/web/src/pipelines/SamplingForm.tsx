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

import React, { useEffect, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { dump, load, JSON_SCHEMA } from 'js-yaml';
import type { Group, PipelinePlugin, TracePipelineConfig } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useDirtyGuard, useFocusTrap } from '../components/modal-utils.js';
import './sampling-form.css';

const blankPipeline: TracePipelineConfig = { enabled: true, enabledEvents: ['PIPELINE_EVENT_MERGE', 'PIPELINE_EVENT_FINALIZE'], plugins: [] };
const blankPlugin = (): PipelinePlugin => ({ name: '', sampler: { path: '', abiVersion: 1 } });
const isMapping = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);

/** Convert shorthand durations to protobuf JSON without rounding nanoseconds. */
export function pipelineDuration(value: string): string | undefined {
  if (!value.trim()) return undefined;
  if (/^\d+(?:\.\d{1,9})?s$/.test(value)) return value;
  const match = /^(\d+(?:\.\d{1,9})?)(h|m)$/.exec(value.trim());
  if (!match) throw new Error('Grace must be a duration such as 2h, 5m, or 30s.');
  const [whole, fraction = ''] = match[1].split('.');
  const nanos = (BigInt(whole) * 1000000000n + BigInt(fraction.padEnd(9, '0'))) * BigInt(match[2] === 'h' ? 3600 : 60);
  const remainder = (nanos % 1000000000n).toString().padStart(9, '0').replace(/0+$/, '');
  return `${nanos / 1000000000n}${remainder ? '.' + remainder : ''}s`;
}

function displayGrace(value?: string): string {
  if (!value || !/^\d+s$/.test(value)) return value ?? '';
  const seconds = Number(value.slice(0, -1));
  if (seconds > 0 && seconds % 3600 === 0) return `${seconds / 3600}h`;
  if (seconds > 0 && seconds % 60 === 0) return `${seconds / 60}m`;
  return value;
}

function Field({ title, children, wide }: { title: string; children: React.ReactNode; wide?: boolean }) {
  return <label className={'sampling-field' + (wide ? ' is-wide' : '')}><span>{title}</span>{children}</label>;
}

function SamplerChain({ plugins, onChange, onInvalid, serverError }: {
  plugins: readonly PipelinePlugin[]; onChange: (plugins: PipelinePlugin[]) => void; onInvalid: (invalid: boolean) => void; serverError?: string;
}) {
  const [expanded, setExpanded] = useState<number | null>(null);
  const [configs, setConfigs] = useState<Record<number, string>>({});
  const [invalid, setInvalid] = useState<Set<number>>(new Set());
  const [dragged, setDragged] = useState<number | null>(null);
  const pathErrorIndex = serverError && /SamplerPlugin\.Path/i.test(serverError) ? Number(/Plugins\[(\d+)\]/i.exec(serverError)?.[1] ?? -1) : -1;
  useEffect(() => { if (pathErrorIndex >= 0) setExpanded(pathErrorIndex); }, [pathErrorIndex, serverError]);
  const update = (index: number, patch: Partial<PipelinePlugin>) => onChange(plugins.map((plugin, i) => i === index ? { ...plugin, ...patch } : plugin));
  const move = (from: number, to: number) => {
    if (to < 0 || to >= plugins.length || from === to || invalid.size > 0) return;
    const reordered = [...plugins];
    reordered.splice(to, 0, reordered.splice(from, 1)[0]);
    onChange(reordered); setExpanded(to); setConfigs({}); setInvalid(new Set()); onInvalid(false);
  };
  return <div className="sampling-chain">{plugins.map((plugin, index) => <div key={index} className={'sampling-plugin' + (expanded === index ? ' is-expanded' : '')}
    onDragOver={(event) => event.preventDefault()} onDrop={() => { if (dragged !== null) move(dragged, index); setDragged(null); }}>
    <div className="sampling-plugin-head">
      <span className="sampling-grip" role="button" tabIndex={0} aria-label={`Reorder sampler ${index + 1}`} draggable
        onDragStart={() => setDragged(index)} onKeyDown={(event) => {
          if (event.key === 'ArrowUp' || event.key === 'ArrowDown') { event.preventDefault(); move(index, index + (event.key === 'ArrowUp' ? -1 : 1)); }
        }}>⋮⋮</span>
      <span className="sampling-order">{index + 1}</span><strong className="mono">{plugin.name}</strong>
      <span className="sampling-plugin-path mono">{expanded !== index ? plugin.sampler?.path : ''}</span>
      <button type="button" className="sampling-icon" aria-label={`${expanded === index ? 'Collapse' : 'Expand'} sampler ${index + 1}`}
        onClick={() => setExpanded(expanded === index ? null : index)}><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d={expanded === index ? 'm18 15-6-6-6 6' : 'm6 9 6 6 6-6'} /></svg></button>
      <button type="button" className="sampling-icon is-danger" aria-label={`Remove sampler ${index + 1}`} onClick={() => {
        const remainingInvalid = new Set([...invalid].filter((i) => i !== index).map((i) => i > index ? i - 1 : i));
        const remainingConfigs = Object.fromEntries(Object.entries(configs).filter(([i]) => Number(i) !== index).map(([i, text]) => [Number(i) > index ? Number(i) - 1 : Number(i), text]));
        onChange(plugins.filter((_, i) => i !== index)); setConfigs(remainingConfigs); setInvalid(remainingInvalid); onInvalid(remainingInvalid.size > 0);
        setExpanded(expanded === index ? null : expanded !== null && expanded > index ? expanded - 1 : expanded);
      }}><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round"><path d="M3 6h18M8 6V4h8v2M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6" /></svg></button>
    </div>
    {expanded === index && <div className="sampling-grid">
      <Field title="Name"><input value={plugin.name} onChange={(e) => update(index, { name: e.target.value })} /></Field>
      {plugin.sampler && <>
        <Field title="Plugin file (.so)"><input aria-invalid={pathErrorIndex === index} aria-label="Plugin file (.so)" title="Install this .so file in the trusted plugin directory on the relevant data nodes and lifecycle agent before enabling the pipeline." value={plugin.sampler.path} onChange={(e) => update(index, { sampler: { ...plugin.sampler!, path: e.target.value } })} /><small>Basename only, resolved inside the trusted plugin directory — no slashes.</small></Field>
        <Field title="Symbol"><input placeholder="NewSampler" value={plugin.sampler.symbol ?? ''} onChange={(e) => update(index, { sampler: { ...plugin.sampler!, symbol: e.target.value } })} /></Field>
        <Field title="ABI version"><input type="number" min="1" max="100" value={plugin.sampler.abiVersion} onChange={(e) => update(index, { sampler: { ...plugin.sampler!, abiVersion: Number(e.target.value) } })} /></Field>
        <Field title="Config (JSON)" wide><textarea rows={3} aria-invalid={invalid.has(index)} value={configs[index] ?? JSON.stringify(plugin.sampler.config ?? {}, null, 2)} onChange={(e) => {
          const text = e.target.value; setConfigs({ ...configs, [index]: text }); const next = new Set(invalid);
          try {
            const config: unknown = JSON.parse(text);
            if (!config || typeof config !== 'object' || Array.isArray(config)) throw new Error('Config must be a JSON object');
            update(index, { sampler: { ...plugin.sampler!, config: config as Record<string, unknown> } }); next.delete(index);
          } catch { next.add(index); }
          setInvalid(next); onInvalid(next.size > 0);
        }} />{invalid.has(index) && <span className="sampling-error-text">Config must be a valid JSON object.</span>}</Field>
      </>}
    </div>}
  </div>)}<button type="button" className="sampling-add" onClick={() => { onChange([...plugins, blankPlugin()]); setExpanded(plugins.length); }}>+ Add sampler</button></div>;
}

export interface SamplingFormModalProps { readonly group?: Group; readonly onClose: (saved?: Group) => void }

export function SamplingFormModal({ group, onClose }: SamplingFormModalProps) {
  const qc = useQueryClient();
  const { data, error: groupsError } = useQuery({ queryKey: ['groups'], queryFn: () => apiDataSource.listGroups() });
  const groups = (data?.groups ?? []).filter((item) => item.catalog === 'CATALOG_TRACE' && !item.pipeline);
  const [groupName, setGroupName] = useState(group?.name ?? '');
  const selected = group ?? groups.find((item) => item.name === groupName);
  const [pipeline, setPipeline] = useState<TracePipelineConfig>(() => structuredClone(group?.pipeline ?? blankPipeline));
  const [tab, setTab] = useState<'form' | 'yaml'>('form');
  const [yaml, setYaml] = useState('');
  const [error, setError] = useState('');
  const [rejected, setRejected] = useState(false);
  const [schemaName, setSchemaName] = useState('');
  const [stageEditor, setStageEditor] = useState<number | null>(null);
  const [invalidChains, setInvalidChains] = useState<Record<string, boolean>>({});
  const dirty = JSON.stringify(pipeline) !== JSON.stringify(group?.pipeline ?? blankPipeline) || (tab === 'yaml' && yaml !== dump({ group: groupName, pipeline }, { noRefs: true, lineWidth: 100 })) || !!schemaName || (!group && !!groupName) || Object.values(invalidChains).some(Boolean);
  const { guardedClose, resetDirty } = useDirtyGuard(dirty, () => onClose());
  const patch = (value: Partial<TracePipelineConfig>) => setPipeline((current) => ({ ...current, ...value }));
  const commitSchemaName = (): TracePipelineConfig => {
    const name = schemaName.trim();
    const draft = name ? { ...pipeline, schemaNames: [...new Set([...(pipeline.schemaNames ?? []), name])] } : pipeline;
    setPipeline(draft);
    setSchemaName('');
    return draft;
  };
  const parseYaml = (): TracePipelineConfig => {
    const document = load(yaml, { schema: JSON_SCHEMA }) as { group?: unknown; pipeline?: unknown } | null;
    if (!document || typeof document !== 'object' || Array.isArray(document) || document.group !== groupName || !document.pipeline || typeof document.pipeline !== 'object' || Array.isArray(document.pipeline)) {
      throw new Error('YAML must contain the selected group and a pipeline mapping.');
    }
    try {
      JSON.stringify(document, (_key, value: unknown) => {
        if (typeof value === 'number' && !Number.isFinite(value)) throw new Error('Non-finite number');
        return value;
      });
    } catch {
      throw new Error('YAML must be JSON-compatible without recursive aliases or non-finite numbers.');
    }
    const value = document.pipeline as TracePipelineConfig;
    if (value.enabled !== undefined && typeof value.enabled !== 'boolean') throw new Error('enabled must be a boolean.');
    for (const key of ['mergeGrace', 'finalizeGrace', 'schemaNameRegex'] as const) {
      if (value[key] !== undefined && typeof value[key] !== 'string') throw new Error(`${key} must be a string.`);
    }
    if (value.enabledEvents !== undefined && (!Array.isArray(value.enabledEvents) || value.enabledEvents.some((event) => typeof event !== 'string'))) throw new Error('enabledEvents must be a list of event names.');
    const checkPlugins = (plugins: unknown) => {
      if (plugins === undefined) return;
      if (!Array.isArray(plugins)) throw new Error('plugins must be a list.');
      for (const plugin of plugins) {
        if (!isMapping(plugin) || typeof plugin.name !== 'string' || (plugin.sampler !== undefined && !isMapping(plugin.sampler))) {
          throw new Error('Each plugin must be a mapping with a name and a sampler mapping.');
        }
        const sampler = plugin.sampler;
        if (!isMapping(sampler)) continue;
        if (typeof sampler.path !== 'string') throw new Error('Sampler path must be a string.');
        if (sampler.symbol !== undefined && typeof sampler.symbol !== 'string') throw new Error('Sampler symbol must be a string.');
        if (sampler.abiVersion !== undefined && (typeof sampler.abiVersion !== 'number' || !Number.isInteger(sampler.abiVersion))) {
          throw new Error('Sampler ABI version must be an integer.');
        }
        if (sampler.config !== undefined && !isMapping(sampler.config)) throw new Error('Sampler config must be a JSON object.');
      }
    };
    checkPlugins(value.plugins);
    if (value.stages !== undefined && (!Array.isArray(value.stages) || value.stages.some((stage) => !stage || typeof stage !== 'object' || typeof stage.stage !== 'string'))) throw new Error('Each stage must be a mapping with a stage name.');
    value.stages?.forEach((stage) => checkPlugins(stage.plugins));
    if (value.schemaNames !== undefined && (!Array.isArray(value.schemaNames) || value.schemaNames.some((name) => typeof name !== 'string'))) throw new Error('schemaNames must be a list of names.');
    if (value.plugins !== undefined && !Array.isArray(value.plugins)) throw new Error('plugins must be a list.');
    if (value.stages !== undefined && !Array.isArray(value.stages)) throw new Error('stages must be a list.');
    if (value.enabledEvents !== undefined && !Array.isArray(value.enabledEvents)) throw new Error('enabledEvents must be a list.');
    if (value.schemaNames !== undefined && !Array.isArray(value.schemaNames)) throw new Error('schemaNames must be a list.');
    return value;
  };
  const switchTab = (next: 'form' | 'yaml') => {
    if (next === tab) return;
    try {
      if (next === 'yaml') {
        if (Object.values(invalidChains).some(Boolean)) throw new Error('Config must be a valid JSON object.');
        setYaml(dump({ group: groupName, pipeline: commitSchemaName() }, { noRefs: true, lineWidth: 100 }));
      } else setPipeline(parseYaml());
      setTab(next);
    } catch (cause) { setRejected(false); setError((cause as Error).message); }
  };
  const mutation = useMutation({
    mutationFn: async () => {
      if (!selected) throw new Error('Select a trace group.');
      if (Object.values(invalidChains).some(Boolean)) throw new Error('Config must be a valid JSON object.');
      const draft = tab === 'yaml' ? parseYaml() : commitSchemaName();
      if (draft.enabled && !draft.enabledEvents?.some((event) => event === 'PIPELINE_EVENT_MERGE' || event === 'PIPELINE_EVENT_FINALIZE') &&
        !draft.stages?.some((stage) => stage.plugins?.some((plugin) => plugin.sampler))) {
        throw new Error('Enable MERGE or FINALIZE, or configure a stage sampler chain. Otherwise disable the pipeline.');
      }
      const normalized = { ...draft };
      for (const key of ['mergeGrace', 'finalizeGrace'] as const) {
        if (Object.hasOwn(draft, key)) normalized[key] = pipelineDuration(draft[key] ?? '');
      }
      const latest = (await apiDataSource.listGroups()).groups.find((item) => item.name === selected.name);
      if (!latest) throw new Error('The trace group no longer exists.');
      if (latest.catalog !== 'CATALOG_TRACE') throw new Error('Sampling requires a trace group.');
      if (!group?.pipeline && latest.pipeline) throw new Error('This trace group already has a pipeline. Reopen it to edit.');
      const { name, ...rest } = latest;
      const metadata = 'metadata' in rest && rest.metadata && typeof rest.metadata === 'object' ? rest.metadata : {};
      const saved = await apiDataSource.updateGroup(name, { group: {
        ...rest, metadata: { ...metadata, name }, pipeline: normalized,
      } });
      return saved?.name ? saved : { ...latest, pipeline: normalized };
    },
    onSuccess: (saved) => { void qc.invalidateQueries({ queryKey: ['groups'] }); resetDirty(); onClose(saved); },
    onError: (cause: Error) => { setRejected(true); setError(cause.message); },
  });
  const trapRef = useFocusTrap(!mutation.isPending, guardedClose);
  const stages = pipeline.stages ?? [];
  const title = group?.pipeline ? 'Edit sampling pipeline' : 'Create sampling pipeline';
  return <div className="modal-overlay" onClick={() => { if (!mutation.isPending) guardedClose(); }}>
    <div className="sampling-modal" role="dialog" aria-modal="true" aria-label={title} ref={trapRef} inert={mutation.isPending} onClick={(e) => e.stopPropagation()}>
      <header className="sampling-modal-head"><div><h2>{title}</h2><p>Trace group <span className="mono">{groupName}</span></p></div>
        <button type="button" className="sampling-icon" aria-label="Close" disabled={mutation.isPending} onClick={guardedClose}>×</button></header>
      <div className="sampling-tabs" role="tablist" aria-label="Editor mode">{(['form', 'yaml'] as const).map((mode) => <button type="button" key={mode} role="tab" aria-selected={tab === mode} onClick={() => switchTab(mode)}>{mode === 'form' ? 'Form' : 'YAML'}</button>)}</div>
      {(error || groupsError) && <div className="sampling-error" role="alert"><span aria-hidden="true">ⓘ</span><div><strong>{rejected ? 'BanyanDB rejected the update' : 'Unable to save pipeline'}</strong><code>{error || groupsError?.message}</code></div></div>}
      {tab === 'form' ? <div className="sampling-form-body">
        <div className="sampling-grid"><Field title="Trace group"><select aria-label="Trace group" value={groupName} disabled={!!group} onChange={(e) => setGroupName(e.target.value)}><option value="">— trace group —</option>{group ? <option>{group.name}</option> : groups.map((item) => <option key={item.name}>{item.name}</option>)}</select><small>One pipeline per trace group</small></Field>
          <div className="sampling-field"><span>Status</span><label className="sampling-checkbox"><input type="checkbox" checked={pipeline.enabled ?? false} onChange={(e) => patch({ enabled: e.target.checked })} />Enabled</label></div></div>
        <fieldset><legend>Events</legend><div className="sampling-grid">{(['MERGE', 'FINALIZE'] as const).map((event) => {
          const key = event === 'MERGE' ? 'mergeGrace' : 'finalizeGrace'; const enumValue = `PIPELINE_EVENT_${event}` as const;
          const enabled = (pipeline.enabledEvents ?? []).includes(enumValue);
          return <div key={event} className={'sampling-event' + (enabled ? ' is-on' : '')}><label className="sampling-event-check"><input type="checkbox" aria-label={event} checked={enabled} onChange={(e) => patch({ enabledEvents: e.target.checked ? [...(pipeline.enabledEvents ?? []), enumValue] : pipeline.enabledEvents?.filter((value) => value !== enumValue) })} /><span><strong className="mono">{event}</strong><small>{event === 'MERGE' ? 'In-merge filter during hot-phase compaction' : 'Tail-sampling at hot segment finalization'}</small></span></label>
            <label className="sampling-grace"><span>Grace</span><input aria-label={`${event} grace`} value={displayGrace(pipeline[key])} placeholder={event === 'MERGE' ? '2h (default)' : '5m (default)'} onChange={(e) => patch({ [key]: e.target.value })} /></label></div>;
        })}</div><p>MERGE and FINALIZE run the sampler chain below. With both off, an enabled pipeline requires a stage sampler chain.</p></fieldset>
        <fieldset><legend>Targets</legend><div className="sampling-grid"><Field title="Schema names"><span className="sampling-schema-names">{(pipeline.schemaNames ?? []).map((name, index) => <span className="sampling-schema-chip" key={index}>{name}<button type="button" aria-label={`Remove schema ${name}`} onClick={() => patch({ schemaNames: pipeline.schemaNames?.filter((_, i) => i !== index) })}>×</button></span>)}<input aria-label="Schema names" placeholder="Add schema…" value={schemaName} onChange={(e) => setSchemaName(e.target.value)} onKeyDown={(e) => { if (e.key === 'Enter' && schemaName.trim()) { e.preventDefault(); commitSchemaName(); } }} /></span></Field><Field title="Schema name regex (RE2)"><input placeholder="^segment.*" value={pipeline.schemaNameRegex ?? ''} onChange={(e) => patch({ schemaNameRegex: e.target.value })} /></Field></div></fieldset>
        <fieldset><legend>Sampler chain <span>· hot phase, evaluated in order</span></legend><SamplerChain serverError={rejected ? error : undefined} plugins={pipeline.plugins ?? []} onChange={(plugins) => patch({ plugins })} onInvalid={(invalid) => setInvalidChains((current) => ({ ...current, hot: invalid }))} /></fieldset>
        <fieldset><legend>Stage rules <span>· applied before migrating out of the source stage</span></legend>
          <p>Use the source stage name, or the source node’s type label for the initial tier (for example, hot). The terminal stage has no outgoing migration.</p>
          <datalist id="sampling-stage-names">{[...new Set(['hot', ...(selected?.resourceOpts.stages ?? []).map((rule) => rule.name)])].map((name) => <option key={name} value={name} />)}</datalist>
          <div className="sampling-chain">{stages.map((stage, index) => <React.Fragment key={index}>
            <div className="sampling-stage"><label>Source stage<input aria-label={`Stage ${index + 1}`} list="sampling-stage-names" value={stage.stage}
              onChange={(e) => patch({ stages: stages.map((rule, i) => i === index ? { ...rule, stage: e.target.value } : rule) })} /></label>
              <span className="sampling-stage-plugins">{stage.plugins?.map((plugin, i) => <span key={i}>{i + 1}. {plugin.name}</span>)}</span>
              <button type="button" onClick={() => { if (!Object.values(invalidChains).some(Boolean)) setStageEditor(stageEditor === index ? null : index); }}>Edit chain</button>
            </div>
            {stageEditor === index && <SamplerChain plugins={stage.plugins ?? []}
              onChange={(plugins) => patch({ stages: stages.map((rule, i) => i === index ? { ...rule, plugins } : rule) })}
              onInvalid={(invalid) => setInvalidChains((current) => ({ ...current, [index]: invalid }))} />}
          </React.Fragment>)}<button type="button" className="sampling-add" onClick={() => patch({ stages: [...stages, { stage: 'hot', plugins: [] }] })}>+ Add stage rule</button></div>
        </fieldset>
      </div> : <div className="sampling-yaml-body"><div className="sampling-yaml"><div aria-hidden="true">{yaml.split('\n').map((_, i) => <span key={i}>{i + 1}</span>)}</div><textarea aria-label="Pipeline YAML" spellCheck={false} value={yaml} onChange={(e) => setYaml(e.target.value)} /></div><p>Edits here and in Form stay in sync.</p></div>}
      <footer className="sampling-modal-foot"><button type="button" disabled={mutation.isPending} onClick={guardedClose}>Cancel</button><button type="button" className="is-primary" disabled={mutation.isPending} onClick={() => { setError(''); mutation.mutate(); }}>{mutation.isPending ? 'Saving…' : group?.pipeline ? 'Save changes' : 'Create pipeline'}</button></footer>
    </div>
  </div>;
}
