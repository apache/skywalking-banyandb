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
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import type { Group, LifecycleStage, CreateGroupRequest, UpdateGroupRequest } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useFocusTrap } from './modal-utils.js';

const CATALOG_OPTS = [
  { value: 'CATALOG_MEASURE',  label: 'MEASURE',  hint: 'Numeric time-series measures' },
  { value: 'CATALOG_STREAM',   label: 'STREAM',   hint: 'Append-only stream records' },
  { value: 'CATALOG_TRACE',    label: 'TRACE',    hint: 'Distributed tracing spans' },
  { value: 'CATALOG_PROPERTY', label: 'PROPERTY', hint: 'Schema-free key–value docs' },
] as const;

type Catalog = typeof CATALOG_OPTS[number]['value'];
type IntervalUnit = 'UNIT_DAY' | 'UNIT_HOUR';
interface Interval { num: number | ''; unit: IntervalUnit; }
interface Stage {
  name: string;
  shardNum: number | '';
  segmentInterval: Interval;
  ttl: Interval;
  nodeSelector: string;
  close: boolean;
  replicas: number | '';
}
interface StageErrors {
  name?: string;
  shardNum?: string;
  segmentInterval?: string;
  ttl?: string;
  nodeSelector?: string;
}

interface FormErrors {
  _?: string;
  name?: string;
  shardNum?: string;
  segmentInterval?: string;
  ttl?: string;
  stages?: StageErrors[];
}

function IntervalField({ value, onChange }: {
  value: Interval;
  onChange: (v: Interval) => void;
}) {
  return (
    <div className="iv-row">
      <input
        className="f-input mono iv-num"
        type="number" min="1"
        value={value.num}
        onChange={(e) => onChange({ ...value, num: e.target.value === '' ? '' : Number(e.target.value) })}
      />
      <div className="f-seg">
        {(['UNIT_HOUR', 'UNIT_DAY'] as const).map((u) => (
          <button type="button" key={u}
            className={`seg-btn${value.unit === u ? ' is-on' : ''}`}
            onClick={() => onChange({ ...value, unit: u })}>
            {u === 'UNIT_HOUR' ? 'Hour' : 'Day'}
          </button>
        ))}
      </div>
    </div>
  );
}

function Field({ label, hint, error, required, locked, children }: {
  label: React.ReactNode;
  hint?: string;
  error?: string;
  required?: boolean;
  locked?: boolean;
  children: React.ReactNode;
}) {
  return (
    <div className={`f-field${error ? ' has-error' : ''}`}>
      <label className="f-label">
        {label}
        {required && <span className="f-req">*</span>}
        {locked && <span className="f-lock">read-only</span>}
      </label>
      {children}
      {error ? <div className="f-error">{error}</div> : hint ? <div className="f-hint">{hint}</div> : null}
    </div>
  );
}

function NumField({ value, onChange, min = 0 }: {
  value: number | '';
  onChange: (v: number | '') => void;
  min?: number;
}) {
  return (
    <input
      className="f-input mono"
      type="number"
      min={min}
      value={value}
      onChange={(e) => onChange(e.target.value === '' ? '' : Number(e.target.value))}
    />
  );
}

const DEFAULT_STAGE: Stage = {
  name: '', shardNum: 2,
  segmentInterval: { num: 1, unit: 'UNIT_DAY' },
  ttl: { num: 7, unit: 'UNIT_DAY' },
  nodeSelector: '', close: true, replicas: 0,
};

function StagesEditor({ stages, errors, onChange }: {
  stages: Stage[];
  errors?: StageErrors[];
  onChange: (s: Stage[]) => void;
}) {
  const upd = (i: number, patch: Partial<Stage>) =>
    onChange(stages.map((s, idx) => (idx === i ? { ...s, ...patch } : s)));
  const del = (i: number) => onChange(stages.filter((_, idx) => idx !== i));

  return (
    <div className="stages">
      {stages.map((s, i) => {
        const er: StageErrors = (errors && errors[i]) || {};
        return (
          <div key={i} className="stage-card">
            <div className="stage-head">
              <span className="stage-idx">Stage {i + 1}</span>
              <button type="button" className="stage-del" onClick={() => del(i)}>Remove</button>
            </div>
            <div className="f-grid">
              <Field label="Name" required error={er.name}>
                <input type="text" className="f-input mono" value={s.name} placeholder="warm"
                  onChange={(e) => upd(i, { name: e.target.value })} />
              </Field>
              <Field label="Shards" required error={er.shardNum}>
                <NumField value={s.shardNum} min={1} onChange={(val) => upd(i, { shardNum: val })} />
              </Field>
              <Field label="Segment interval" required error={er.segmentInterval}>
                <IntervalField value={s.segmentInterval} onChange={(val) => upd(i, { segmentInterval: val })} />
              </Field>
              <Field label="TTL" required error={er.ttl}>
                <IntervalField value={s.ttl} onChange={(val) => upd(i, { ttl: val })} />
              </Field>
              <Field label="Node selector" required hint="Target node label (e.g. tier=warm)" error={er.nodeSelector}>
                <input type="text" className="f-input mono" value={s.nodeSelector} placeholder="tier=warm"
                  onChange={(e) => upd(i, { nodeSelector: e.target.value })} />
              </Field>
              <Field label="Replicas">
                <NumField value={s.replicas} min={0} onChange={(val) => upd(i, { replicas: val })} />
              </Field>
            </div>
            <label className="f-check">
              <input type="checkbox" checked={s.close}
                onChange={(e) => upd(i, { close: e.target.checked })} />
              Close non-live segments in this stage
            </label>
          </div>
        );
      })}
      <button type="button" className="btn btn-ghost stage-add"
        onClick={() => onChange([...stages, { ...DEFAULT_STAGE }])}>
        + Add lifecycle stage
      </button>
    </div>
  );
}

/** GroupForm renders a create-group modal, an edit-group modal, or a delete-group confirmation dialog. */
export function GroupForm({ mode, initialName, initialCatalog, onClose }: {
  mode: 'create' | 'edit' | 'delete';
  initialName?: string;
  initialCatalog?: Catalog;
  onClose: (created?: Group) => void;
}) {
  const qc = useQueryClient();

  const [name, setName] = useState('');
  const [catalog, setCatalog] = useState<Catalog>(initialCatalog ?? 'CATALOG_MEASURE');
  const [shardNum, setShardNum] = useState<number | ''>(2);
  const [segmentInterval, setSegmentInterval] = useState<Interval>({ num: 1, unit: 'UNIT_DAY' });
  const [ttl, setTtl] = useState<Interval>({ num: 7, unit: 'UNIT_DAY' });
  const [stages, setStages] = useState<Stage[]>([]);
  const [errors, setErrors] = useState<FormErrors>({});
  const [initialized, setInitialized] = useState(false);
  const [deleteConfirm, setDeleteConfirm] = useState('');

  const { data: groupsData } = useQuery({
    queryKey: ['groups'],
    queryFn: () => apiDataSource.listGroups(),
    enabled: mode === 'edit',
  });
  const editGroup = groupsData?.groups.find((g) => g.name === initialName);

  useEffect(() => {
    if (mode === 'edit' && editGroup && !initialized) {
      setShardNum(editGroup.resourceOpts.shardNum);
      const seg = editGroup.resourceOpts.segmentInterval;
      if (seg) setSegmentInterval({ num: seg.num, unit: seg.unit as IntervalUnit });
      const t = editGroup.resourceOpts.ttl;
      if (t) setTtl({ num: t.num, unit: t.unit as IntervalUnit });
      const loadedStages = (editGroup.resourceOpts.stages ?? []).map((s: LifecycleStage): Stage => ({
        name: s.name,
        shardNum: s.shardNum,
        segmentInterval: { num: s.segmentInterval.num, unit: s.segmentInterval.unit as IntervalUnit },
        ttl: { num: s.ttl.num, unit: s.ttl.unit as IntervalUnit },
        nodeSelector: s.nodeSelector ?? '',
        close: s.close ?? false,
        replicas: s.replicas ?? 0,
      }));
      setStages(loadedStages);
      setInitialized(true);
    }
  }, [mode, editGroup, initialized]);

  const createMut = useMutation({
    mutationFn: (req: CreateGroupRequest) => apiDataSource.createGroup(req),
    onSuccess: (group) => {
      qc.invalidateQueries({ queryKey: ['groups'] });
      onClose(group);
    },
    onError: (e: Error) => setErrors({ _: e.message }),
  });

  const updateMut = useMutation({
    mutationFn: (req: UpdateGroupRequest) => apiDataSource.updateGroup(initialName!, req),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['groups'] });
      onClose();
    },
    onError: (e: Error) => setErrors({ _: e.message }),
  });

  const deleteMut = useMutation({
    mutationFn: () => apiDataSource.deleteGroup(initialName!),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['groups'] });
      onClose();
    },
    onError: (e: Error) => setErrors({ _: e.message }),
  });

  const isEdit = mode === 'edit';
  const activeCatalog: Catalog = isEdit ? (editGroup?.catalog ?? initialCatalog ?? 'CATALOG_MEASURE') as Catalog : catalog;
  const isPropertyCatalog = activeCatalog === 'CATALOG_PROPERTY';

  const trapRef = useFocusTrap(true, () => onClose());

  function validate(): FormErrors {
    const e: Record<string, string | StageErrors[]> = {};
    if (!isEdit && !name.trim()) e.name = 'Group name is required.';
    if (shardNum === '' || Number(shardNum) <= 0) e.shardNum = 'Must be greater than 0.';
    if (!isPropertyCatalog) {
      if (segmentInterval.num === '' || Number(segmentInterval.num) <= 0) e.segmentInterval = 'Required.';
      if (ttl.num === '' || Number(ttl.num) <= 0) e.ttl = 'Required.';
      const stageErrs: StageErrors[] = [];
      stages.forEach((s, i) => {
        const se: StageErrors = {};
        if (!s.name.trim()) se.name = 'Stage name is required';
        if (s.shardNum === '' || Number(s.shardNum) <= 0) se.shardNum = 'Must be > 0';
        if (s.segmentInterval.num === '' || Number(s.segmentInterval.num) <= 0) se.segmentInterval = 'Required';
        if (s.ttl.num === '' || Number(s.ttl.num) <= 0) se.ttl = 'Required';
        if (!s.nodeSelector.trim()) se.nodeSelector = 'Required';
        if (Object.keys(se).length) stageErrs[i] = se;
      });
      if (stageErrs.length) e.stages = stageErrs;
    }
    return e;
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const errs = validate();
    setErrors(errs);
    if (Object.keys(errs).length) return;

    const seg = { num: Number(segmentInterval.num), unit: segmentInterval.unit };
    const t = { num: Number(ttl.num), unit: ttl.unit };
    const mappedStages = stages.map((s) => ({
      name: s.name.trim(),
      shardNum: Number(s.shardNum),
      segmentInterval: { num: Number(s.segmentInterval.num), unit: s.segmentInterval.unit },
      ttl: { num: Number(s.ttl.num), unit: s.ttl.unit },
      nodeSelector: s.nodeSelector.trim() || undefined,
      close: s.close,
      replicas: Number(s.replicas) || 0,
    }));

    if (isEdit) {
      updateMut.mutate({
        group: {
          metadata: { name: initialName! },
          catalog: editGroup?.catalog,
          resourceOpts: isPropertyCatalog
            ? { shardNum: Number(shardNum) }
            : { shardNum: Number(shardNum), segmentInterval: seg, ttl: t, stages: mappedStages },
        },
      });
    } else {
      createMut.mutate({
        group: {
          metadata: { name: name.trim() },
          catalog,
          resourceOpts: isPropertyCatalog
            ? { shardNum: Number(shardNum) }
            : { shardNum: Number(shardNum), segmentInterval: seg, ttl: t, stages: mappedStages },
        },
      });
    }
  }

  const isPending = createMut.isPending || updateMut.isPending || deleteMut.isPending;

  if (mode === 'delete') {
    const deleteMatch = deleteConfirm === initialName;
    return (
      <div className="modal-overlay" onClick={() => onClose()}>
        <div className="modal is-danger" ref={trapRef} onClick={(e) => e.stopPropagation()}>
          <div className="modal-head">
            <span className="modal-title">Delete group</span>
            <button className="modal-x" onClick={() => onClose()} />
          </div>
          <div className="modal-body">
            <p className="del-warn">
              You are about to permanently delete group{' '}
              <span className="mono">{initialName}</span> and all its resources.
              All shards, segments and stored data will be removed.
            </p>
            <div className="f-field" style={{ marginTop: 16 }}>
              <label className="f-label">
                Type <span className="mono">{initialName}</span> to confirm
              </label>
              <input
                type="text"
                className="f-input mono"
                autoFocus
                value={deleteConfirm}
                placeholder={initialName}
                onChange={(e) => setDeleteConfirm(e.target.value)}
                onKeyDown={(e) => { if (e.key === 'Enter' && deleteMatch && !isPending) deleteMut.mutate(); }}
              />
            </div>
            {errors._ && <div className="f-error" style={{ marginTop: 8 }}>{errors._}</div>}
          </div>
          <div className="modal-foot">
            <button className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>Cancel</button>
            <button
              className="btn btn-danger"
              onClick={() => deleteMut.mutate()}
              disabled={isPending || !deleteMatch}
            >
              {isPending ? 'Deleting…' : 'Delete group'}
            </button>
          </div>
        </div>
      </div>
    );
  }

  const catalogHint = CATALOG_OPTS.find((c) => c.value === activeCatalog)?.hint;

  return (
    <div className="modal-overlay" onClick={() => onClose()}>
      <form
        className="modal is-wide"
        ref={trapRef}
        onSubmit={handleSubmit}
        onClick={(e) => e.stopPropagation()}
        data-initialized={String(initialized)}
      >
        <div className="modal-head">
          <div>
            <span className="modal-title">{isEdit ? 'Edit group' : 'New group'}</span>
            <p className="modal-sub">
              {isEdit
                ? 'Name and catalog are immutable. Update the storage options below.'
                : 'Define a group — the minimal physical unit managing shards, segments and retention.'}
            </p>
          </div>
          <button type="button" className="modal-x" onClick={() => onClose()} />
        </div>

        <div className="modal-body">
          <section className="f-section">
            <div className="f-section-title">Identity</div>
            <div className="f-grid">
              <Field
                label="Name"
                required={!isEdit}
                locked={isEdit}
                error={errors.name}
                hint={isEdit ? undefined : "Unique within the catalog · letters, digits, '_' and '-'"}
              >
                <input
                  className="f-input mono"
                  type="text"
                  placeholder="sw_metric"
                  value={isEdit ? (initialName ?? '') : name}
                  onChange={(e) => { if (!isEdit) setName(e.target.value); }}
                  readOnly={isEdit}
                  autoFocus={!isEdit}
                />
              </Field>
              <Field label="Catalog" required locked={isEdit} hint={isEdit ? undefined : catalogHint}>
                <div className="f-seg cat-seg">
                  {CATALOG_OPTS.map((c) => (
                    <button
                      type="button"
                      key={c.value}
                      disabled={isEdit}
                      className={`seg-btn${activeCatalog === c.value ? ' is-on' : ''}`}
                      onClick={() => setCatalog(c.value)}
                    >
                      {c.label}
                    </button>
                  ))}
                </div>
              </Field>
            </div>
          </section>

          <section className="f-section">
            <div className="f-section-title">Resource options</div>
            <div className="f-grid">
              <Field
                label="Shard number"
                required
                error={errors.shardNum}
                hint="Number of shards distributed in the group"
              >
                <input
                  className="f-input mono"
                  type="number"
                  min={1}
                  value={shardNum}
                  onChange={(e) => setShardNum(e.target.value === '' ? '' : Number(e.target.value))}
                />
              </Field>
              {!isPropertyCatalog && (
                <>
                  <Field label="Segment interval" required error={errors.segmentInterval} hint="Length of each storage segment">
                    <IntervalField value={segmentInterval} onChange={setSegmentInterval} />
                  </Field>
                  <Field label="TTL" required error={errors.ttl} hint="How long data is retained">
                    <IntervalField value={ttl} onChange={setTtl} />
                  </Field>
                </>
              )}
            </div>
          </section>

          {!isPropertyCatalog && (
            <section className="f-section">
              <div className="f-section-title">
                Lifecycle stages <span className="f-optional">optional</span>
              </div>
              <p className="f-section-desc">Data progresses through these stages sequentially (e.g. warm → cold).</p>
              <StagesEditor
                stages={stages}
                errors={errors.stages}
                onChange={setStages}
              />
            </section>
          )}

          {errors._ && <div className="f-error">{errors._}</div>}
        </div>

        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>
            Cancel
          </button>
          <button type="submit" className="btn btn-primary" disabled={isPending}>
            {isPending ? (isEdit ? 'Saving…' : 'Creating…') : (isEdit ? 'Save changes' : 'Create group')}
          </button>
        </div>
      </form>
    </div>
  );
}
