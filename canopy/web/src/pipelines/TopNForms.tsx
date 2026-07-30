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

// TopNForms.tsx — TopNAggregation CRUD: create/edit modal + delete confirm.
// Ported from .handoff-import/banyandb/project/topn-form.jsx (window-global
// JSX -> ES module TSX). Validation mirrors schema.proto TopNAggregation
// (metadata, source_measure, field_name min_len 1, field_value_sort,
// group_by_tag_names, criteria, counters_number, lru_size).
//
// ADAPTATIONS FROM THE HANDOFF:
//  - Mock `groups` prop -> live queries: listGroups (source group options),
//    listResourcesInGroup('measures', ...) (source measure options),
//    getResource (source measure's fields/tags), listTopNAggregations
//    (per-group existing names, for the create-time uniqueness check).
//  - The handoff's bespoke TopNSelect/TopNChipPicker are replaced by canopy's
//    existing Combobox/MultiCombobox (Combobox.tsx) for the group/measure/
//    field/group-by pickers — same fuzzy-filter UX already used by
//    IndexRuleBindingForm, without introducing a second select widget.
//  - CriteriaEditor's tag(name)/op/value rows build/parse the real
//    model.v1.Criteria tree via topn-shared.ts's buildTopNCriteria /
//    flattenTopNCriteria instead of carrying a flat array over the wire.
//  - onSubmit -> createTopNAggregation / updateTopNAggregation; reuses
//    useFocusTrap/useDirtyGuard from components/modal-utils.ts, matching
//    every other *Form.tsx in this app (there is no shared exported
//    Field/Modal component to reuse — each form locally duplicates the
//    modal-overlay/modal markup, per GroupForm.tsx/PropertyForms.tsx).

import React, { useEffect, useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import type {
  MeasureSchema, TopNAggregationSchema,
  CreateTopNAggregationRequest, UpdateTopNAggregationRequest,
} from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useFocusTrap, useDirtyGuard } from '../components/modal-utils.js';
import { Combobox, MultiCombobox } from '../components/Combobox.js';
import { IconChevron, IconPlus, IconCheck } from '../components/icons.js';
import {
  SORT_OPTS, TOPN_OPS, DEFAULT_COUNTERS, topNTone, topNRank,
  buildTopNCriteria, flattenTopNCriteria, topNAggregationsQueryKey, type TopNCondition,
} from './topn-shared.js';

const TOPN_NAME_RE = /^[a-zA-Z0-9_-]+$/;

const IconClose = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M6 6l12 12M18 6 6 18" />
  </svg>
);

// Mirrors the Field wrapper every *Form.tsx in this app duplicates locally.
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

/* ============ criteria editor — flat ANDed {tag, op, value} rows ============ */

function CriteriaEditor({ criteria, tagOptions, errors, onChange }: {
  criteria: readonly TopNCondition[];
  tagOptions: readonly string[];
  errors?: ReadonlyArray<{ tag?: string; value?: string } | undefined>;
  onChange: (v: TopNCondition[]) => void;
}) {
  const upd = (i: number, patch: Partial<TopNCondition>) =>
    onChange(criteria.map((c, idx) => (idx === i ? { ...c, ...patch } : c)));
  const del = (i: number) => onChange(criteria.filter((_, idx) => idx !== i));
  const add = () => onChange([...criteria, { tag: tagOptions[0] ?? '', op: 'BINARY_OP_EQ', value: '' }]);

  return (
    <div className="spec-list">
      {criteria.map((c, i) => {
        const er = errors?.[i] ?? {};
        return (
          <div key={i} className="kv-row">
            <div className={'spec-cell' + (er.tag ? ' has-error' : '')}>
              <Combobox
                value={c.tag}
                options={tagOptions}
                onChange={(val) => upd(i, { tag: val })}
                placeholder="— tag —"
                noOptionsHint="Source measure has no tags"
                ariaLabel={`Criteria tag ${i + 1}`}
              />
              {er.tag && <div className="f-error">{er.tag}</div>}
            </div>
            <div className="spec-cell type">
              <div className="f-select-wrap">
                <select className="f-input f-select mono" aria-label={`Criteria operator ${i + 1}`} value={c.op}
                  onChange={(e) => upd(i, { op: e.target.value })}>
                  {TOPN_OPS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
                </select>
                <span className="f-select-chev"><IconChevron size={13} /></span>
              </div>
            </div>
            <div className={'spec-cell grow' + (er.value ? ' has-error' : '')}>
              <input className="f-input mono" value={c.value} placeholder="value" aria-label={`Criteria value ${i + 1}`}
                onChange={(e) => upd(i, { value: e.target.value })} />
              {er.value && <div className="f-error">{er.value}</div>}
            </div>
            <button type="button" className="spec-del" onClick={() => del(i)} aria-label={`Remove condition ${i + 1}`}>
              <IconClose width={14} height={14} />
            </button>
          </div>
        );
      })}
      <button type="button" className="spec-add" onClick={add}><IconPlus size={14} /> Add condition</button>
    </div>
  );
}

/* ============ validation ============ */

export interface TopNDraft {
  readonly name: string;
  readonly sourceGroup: string;
  readonly sourceName: string;
  readonly fieldName: string;
  readonly fieldValueSort: 'SORT_DESC' | 'SORT_ASC' | 'SORT_UNSPECIFIED';
  readonly groupByTagNames: readonly string[];
  readonly criteria: readonly TopNCondition[];
  readonly countersNumber: number | '';
  readonly lruSize: number | '';
}

export interface TopNValidationErrors {
  _?: string;
  name?: string;
  sourceGroup?: string;
  sourceMeasure?: string;
  fieldName?: string;
  countersNumber?: string;
  lruSize?: string;
  criteria?: ReadonlyArray<{ tag?: string; value?: string } | undefined>;
}

export interface TopNValidationCtx {
  readonly mode: 'create' | 'edit';
  readonly existingNames?: ReadonlySet<string>;
  readonly fieldOptions?: readonly string[];
}

/** Pure validation — advisory only; the registry is authoritative (mirrors
 *  web/src/validation.ts's Principle 5 for the M3 forms). */
export function validateTopN(v: TopNDraft, ctx: TopNValidationCtx): TopNValidationErrors {
  const e: TopNValidationErrors = {};
  if (ctx.mode === 'create') {
    const name = v.name.trim();
    if (!name) e.name = 'Name is required';
    else if (name.length > 255) e.name = 'Must be 255 characters or fewer';
    else if (!TOPN_NAME_RE.test(name)) e.name = "Only letters, digits, '_' and '-' are allowed";
    else if (ctx.existingNames?.has(name.toLowerCase())) e.name = `An aggregation named "${name}" already exists in this group`;
  }

  if (!v.sourceGroup) e.sourceGroup = 'Select a source group';
  if (!v.sourceName) e.sourceMeasure = 'Select a source measure';

  const fieldName = v.fieldName.trim();
  if (!fieldName) e.fieldName = 'Required';
  else if (ctx.fieldOptions && ctx.fieldOptions.length > 0 && !ctx.fieldOptions.includes(fieldName)) {
    e.fieldName = 'Must be a field of the source measure';
  }

  if (v.countersNumber !== '') {
    const n = Number(v.countersNumber);
    if (!Number.isInteger(n) || n <= 0) e.countersNumber = 'Must be a whole number greater than 0';
  }
  if (v.lruSize !== '') {
    const n = Number(v.lruSize);
    if (!Number.isInteger(n) || n < 0) e.lruSize = 'Must be 0 or a positive whole number';
  }

  const condErrs: Array<{ tag?: string; value?: string } | undefined> = [];
  v.criteria.forEach((c, i) => {
    const ce: { tag?: string; value?: string } = {};
    if (!c.tag.trim()) ce.tag = 'Required';
    if (!c.value.trim()) ce.value = 'Required';
    if (Object.keys(ce).length) condErrs[i] = ce;
  });
  if (condErrs.length) e.criteria = condErrs;

  return e;
}

function topNHasErrors(e: TopNValidationErrors): boolean {
  return (Object.keys(e) as Array<keyof TopNValidationErrors>).some((k) => {
    const val = e[k];
    if (Array.isArray(val)) return val.some((x) => x && Object.keys(x).length > 0);
    return !!val;
  });
}

function blankTopN(sourceGroup: string): TopNDraft {
  return {
    name: '',
    sourceGroup,
    sourceName: '',
    fieldName: '',
    fieldValueSort: 'SORT_DESC',
    groupByTagNames: [],
    criteria: [],
    countersNumber: DEFAULT_COUNTERS,
    lruSize: 10,
  };
}

function draftFromSchema(agg: TopNAggregationSchema): TopNDraft {
  return {
    name: agg.metadata.name,
    sourceGroup: agg.sourceMeasure?.group ?? agg.metadata.group,
    sourceName: agg.sourceMeasure?.name ?? '',
    fieldName: agg.fieldName ?? '',
    fieldValueSort: agg.fieldValueSort ?? 'SORT_DESC',
    groupByTagNames: agg.groupByTagNames ?? [],
    criteria: flattenTopNCriteria(agg.criteria),
    countersNumber: agg.countersNumber ?? DEFAULT_COUNTERS,
    lruSize: agg.lruSize ?? '',
  };
}

/* ============ create / edit modal ============ */

export interface TopNFormModalProps {
  readonly mode: 'create' | 'edit';
  /** Locks the target measure group. Required for edit; optional for create
   *  (when omitted the user picks a measure group in the Identity section —
   *  mirrors the handoff's "New aggregation" from the all-groups TopN list). */
  readonly groupName?: string;
  readonly aggregation?: TopNAggregationSchema;
  readonly onClose: (created?: TopNAggregationSchema) => void;
}

export function TopNFormModal({ mode, groupName, aggregation, onClose }: TopNFormModalProps) {
  const qc = useQueryClient();
  const isEdit = mode === 'edit';
  const fixedGroup = isEdit ? (aggregation?.metadata.group ?? groupName ?? null) : (groupName ?? null);

  const { data: groupsData } = useQuery({ queryKey: ['groups'], queryFn: () => apiDataSource.listGroups() });
  const measureGroupNames = useMemo(
    () => (groupsData?.groups ?? []).filter((g) => g.catalog === 'CATALOG_MEASURE').map((g) => g.name).sort(),
    [groupsData],
  );

  const [targetGroup, setTargetGroup] = useState(fixedGroup ?? '');
  useEffect(() => {
    if (!fixedGroup && !targetGroup && measureGroupNames.length > 0) setTargetGroup(measureGroupNames[0]);
  }, [fixedGroup, targetGroup, measureGroupNames]);

  const init = useMemo<TopNDraft>(
    () => (isEdit && aggregation ? draftFromSchema(aggregation) : blankTopN(fixedGroup ?? '')),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- snapshot on mount only
    [],
  );
  const [v, setV] = useState<TopNDraft>(init);
  const set = (patch: Partial<TopNDraft>) => setV((c) => ({ ...c, ...patch }));

  // Existing aggregation names in the target group, for the create-time
  // uniqueness check (the registry itself is authoritative either way).
  const { data: existingAggs = [] } = useQuery({
    queryKey: topNAggregationsQueryKey(targetGroup),
    queryFn: () => apiDataSource.listTopNAggregations(targetGroup),
    enabled: !!targetGroup,
  });
  const existingNames = useMemo(
    () => new Set(
      existingAggs
        .filter((a) => !isEdit || a.metadata.name !== aggregation?.metadata.name)
        .map((a) => a.metadata.name.toLowerCase()),
    ),
    [existingAggs, isEdit, aggregation],
  );

  const { data: sourceMeasures = [] } = useQuery({
    queryKey: ['resources', 'measures', v.sourceGroup],
    queryFn: () => apiDataSource.listResourcesInGroup('measures', v.sourceGroup),
    enabled: !!v.sourceGroup,
  });
  const measureOptions = useMemo(() => sourceMeasures.map((m) => m.metadata.name).sort(), [sourceMeasures]);

  const { data: srcMeasure } = useQuery({
    queryKey: ['resource', 'measures', v.sourceGroup, v.sourceName],
    queryFn: () => apiDataSource.getResource('measures', v.sourceGroup, v.sourceName) as Promise<MeasureSchema>,
    enabled: !!v.sourceGroup && !!v.sourceName,
  });
  const fieldOptions = useMemo(() => srcMeasure?.fields.map((f) => f.name) ?? [], [srcMeasure]);
  const tagOptions = useMemo(
    () => srcMeasure?.tagFamilies.flatMap((f) => f.tags.map((t) => t.name)) ?? [],
    [srcMeasure],
  );

  // When the source measure changes, drop a field/group-by selection that no
  // longer applies to the new measure.
  const changeSourceMeasure = (name: string) => {
    set({ sourceName: name, fieldName: '', groupByTagNames: [] });
  };
  const changeSourceGroup = (group: string) => {
    set({ sourceGroup: group, sourceName: '', fieldName: '', groupByTagNames: [] });
  };

  const [errors, setErrors] = useState<TopNValidationErrors>({});
  const [submitted, setSubmitted] = useState(false);
  useEffect(() => {
    if (submitted) setErrors(validateTopN(v, { mode, existingNames, fieldOptions }));
    // eslint-disable-next-line react-hooks/exhaustive-deps -- re-validate on relevant state, not on the ctx object identity
  }, [v, submitted, existingNames, fieldOptions, mode]);

  const dirty = useMemo(
    () => JSON.stringify(v) !== JSON.stringify(init) || targetGroup !== (fixedGroup ?? init.sourceGroup),
    [v, init, targetGroup, fixedGroup],
  );
  const { guardedClose, resetDirty } = useDirtyGuard(dirty, () => onClose());
  const trapRef = useFocusTrap(true, guardedClose);

  const createMut = useMutation({
    mutationFn: (req: CreateTopNAggregationRequest) => apiDataSource.createTopNAggregation(req),
    onSuccess: (agg) => {
      void qc.invalidateQueries({ queryKey: ['topnAggregations'] });
      resetDirty();
      onClose(agg);
    },
    onError: (e: Error) => setErrors({ _: e.message }),
  });
  const updateMut = useMutation({
    mutationFn: (req: UpdateTopNAggregationRequest) => apiDataSource.updateTopNAggregation(targetGroup, aggregation!.metadata.name, req),
    onSuccess: (agg) => {
      void qc.invalidateQueries({ queryKey: ['topnAggregations'] });
      void qc.invalidateQueries({ queryKey: ['topNAggregation', targetGroup, aggregation?.metadata.name] });
      resetDirty();
      onClose(agg);
    },
    onError: (e: Error) => setErrors({ _: e.message }),
  });
  const isPending = createMut.isPending || updateMut.isPending;

  const submit = () => {
    const e = validateTopN(v, { mode, existingNames, fieldOptions });
    setSubmitted(true);
    setErrors(e);
    if (topNHasErrors(e)) {
      requestAnimationFrame(() => {
        const first = document.querySelector<HTMLElement>('.modal .has-error .f-input, .modal .has-error input');
        first?.focus();
      });
      return;
    }
    // A TopNAggregation ranks measures within its own registry group: the
    // precomputed result measure is written into metadata.group while the
    // counters stream from sourceMeasure.group, so a mismatch produces
    // results the user can't trace. Block it here with a clear message
    // instead of letting the request fail server-side.
    if (v.sourceGroup !== targetGroup) {
      setErrors({ _: `Source measure group "${v.sourceGroup}" must match the aggregation's group "${targetGroup}" — a TopN aggregation ranks measures within its own group.` });
      return;
    }
    // Guard against zeroing group-by tags before the source measure's schema
    // has finished loading (tagOptions starts empty until that query settles).
    const validGroupBy = tagOptions.length > 0 ? v.groupByTagNames.filter((t) => tagOptions.includes(t)) : v.groupByTagNames;
    const payload = {
      metadata: { name: (isEdit ? aggregation!.metadata.name : v.name.trim()), group: targetGroup },
      sourceMeasure: { group: v.sourceGroup, name: v.sourceName },
      fieldName: v.fieldName.trim(),
      fieldValueSort: v.fieldValueSort,
      groupByTagNames: validGroupBy,
      criteria: buildTopNCriteria(v.criteria),
      countersNumber: v.countersNumber === '' ? DEFAULT_COUNTERS : Number(v.countersNumber),
      lruSize: v.lruSize === '' ? undefined : Number(v.lruSize),
    };
    if (isEdit) updateMut.mutate({ topNAggregation: payload });
    else createMut.mutate({ topNAggregation: payload });
  };

  return (
    <div className="modal-overlay" onClick={guardedClose}>
      <div className="modal is-wide" role="dialog" aria-modal="true"
        aria-label={isEdit ? 'Edit TopN aggregation' : 'Create TopN aggregation'}
        ref={trapRef} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <div>
            <span className="modal-title">{isEdit ? 'Edit TopN aggregation' : 'Create TopN aggregation'}</span>
            <p className="modal-sub">
              {isEdit
                ? 'Name is immutable. Update the source, ranking and criteria below.'
                : (fixedGroup ? `Define a TopNAggregation in measure group "${fixedGroup}".` : 'Define a TopNAggregation. Choose the measure group it belongs to below.')}
            </p>
          </div>
          <button type="button" className="modal-x" onClick={guardedClose} aria-label="Close" />
        </div>

        <div className="modal-body">
          <section className="f-section">
            <div className="f-section-title">Identity</div>
            <div className="f-grid">
              <Field label="Name" required={!isEdit} locked={isEdit} error={errors.name}
                hint={isEdit ? undefined : "Unique within the group · letters, digits, '_' and '-'"}>
                <input className="f-input mono" value={isEdit ? aggregation!.metadata.name : v.name} disabled={isEdit} autoFocus={!isEdit}
                  placeholder="service_cpm_minute_topn" onChange={(e) => set({ name: e.target.value })} />
              </Field>
              <Field label="Group" required={!fixedGroup} locked={!!fixedGroup}
                hint={fixedGroup ? 'Aggregations are scoped to their measure group' : 'Measure group this aggregation is registered in'}>
                {fixedGroup ? (
                  <input className="f-input mono" value={fixedGroup} disabled />
                ) : (
                  <Combobox value={targetGroup} options={measureGroupNames} onChange={setTargetGroup}
                    placeholder="— group —" ariaLabel="Group" noOptionsHint="No measure groups yet" />
                )}
              </Field>
            </div>
          </section>

          <section className="f-section">
            <div className="f-section-title">Source measure <span className="f-req">*</span></div>
            <p className="f-section-desc">The measure whose data points this aggregation ranks, and the field used for ranking.</p>
            <div className="f-grid">
              <Field label="Source group" required error={errors.sourceGroup}>
                <Combobox value={v.sourceGroup} options={measureGroupNames} onChange={changeSourceGroup}
                  placeholder="— group —" ariaLabel="Source group" noOptionsHint="No measure groups yet" />
              </Field>
              <Field label="Source measure" required error={errors.sourceMeasure}>
                {measureOptions.length === 0 ? (
                  <div className="picker-empty">No measures in this group.</div>
                ) : (
                  <Combobox value={v.sourceName} options={measureOptions} onChange={changeSourceMeasure}
                    placeholder="— measure —" ariaLabel="Source measure" noOptionsHint="No measures in this group" />
                )}
              </Field>
              <Field label="Ranked field" required error={errors.fieldName}
                hint={srcMeasure && fieldOptions.length === 0 ? 'Source measure has no fields' : 'field_name used for ranking'}>
                {fieldOptions.length === 0 ? (
                  <input className="f-input mono" value={v.fieldName} placeholder="field_name" onChange={(e) => set({ fieldName: e.target.value })} />
                ) : (
                  <Combobox value={v.fieldName} options={fieldOptions} onChange={(val) => set({ fieldName: val })}
                    placeholder="— field —" ariaLabel="Ranked field" noOptionsHint="No fields" />
                )}
              </Field>
            </div>
          </section>

          <section className="f-section">
            <div className="f-section-title">Ranking direction</div>
            <p className="f-section-desc">
              <span className="mono">field_value_sort</span> — DESC ranks the largest values (topN), ASC the smallest (bottomN).
            </p>
            <div className="idx-type-choices">
              {SORT_OPTS.map((s) => (
                <button type="button" key={s.value}
                  className={'idx-type-card' + (v.fieldValueSort === s.value ? ' is-on' : '')}
                  onClick={() => set({ fieldValueSort: s.value })}>
                  <span className="idx-type-card-h">
                    <span className={'topn-rank is-' + topNTone(s.value)}>{topNRank(s.value)}</span>
                    {v.fieldValueSort === s.value && <IconCheck size={15} />}
                  </span>
                  <span className="idx-type-card-hint"><span className="mono">{s.label}</span> · {s.hint}</span>
                </button>
              ))}
            </div>
            <div className="f-grid" style={{ marginTop: 16 }}>
              <Field label="Counters number" error={errors.countersNumber} hint="Number of counters tracked · default 1000">
                <input className="f-input mono" type="number" min={1} aria-label="Counters number" value={v.countersNumber}
                  onChange={(e) => set({ countersNumber: e.target.value === '' ? '' : Number(e.target.value) })} />
              </Field>
              <Field label="LRU size" error={errors.lruSize} hint="In-memory entries retained · optional">
                <input className="f-input mono" type="number" min={0} aria-label="LRU size" value={v.lruSize}
                  onChange={(e) => set({ lruSize: e.target.value === '' ? '' : Number(e.target.value) })} />
              </Field>
            </div>
          </section>

          <section className="f-section">
            <div className="f-section-title">Group by tags <span className="f-optional">optional</span></div>
            <p className="f-section-desc">Data points are grouped into separate ranked counters per distinct combination of these tags.</p>
            {tagOptions.length === 0 ? (
              <div className="picker-empty">{srcMeasure ? 'Source measure has no tags.' : 'Choose a source measure first.'}</div>
            ) : (
              <MultiCombobox value={[...v.groupByTagNames]} options={tagOptions} onChange={(groupByTagNames) => set({ groupByTagNames })}
                placeholder="— select group-by tags —" ariaLabel="Group by tags" noOptionsHint="Source measure has no tags" />
            )}
          </section>

          <section className="f-section">
            <div className="f-section-title">Criteria <span className="f-optional">optional</span></div>
            <p className="f-section-desc">Select a partial set of data points to aggregate. Conditions are combined with AND.</p>
            {tagOptions.length === 0 && v.criteria.length === 0 ? (
              <div className="picker-empty">Choose a source measure with tags to add criteria.</div>
            ) : (
              <CriteriaEditor criteria={v.criteria} tagOptions={tagOptions} errors={errors.criteria}
                onChange={(criteria) => set({ criteria })} />
            )}
          </section>

          {errors._ && <div className="f-error" role="alert">{errors._}</div>}
        </div>

        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={guardedClose} disabled={isPending}>Cancel</button>
          <button type="button" className="btn btn-primary" onClick={submit} disabled={isPending}>
            {isPending ? (isEdit ? 'Saving…' : 'Creating…') : (isEdit ? 'Save changes' : 'Create aggregation')}
          </button>
        </div>
      </div>
    </div>
  );
}

/* ============ delete confirmation ============ */

export interface DeleteTopNModalProps {
  readonly groupName: string;
  readonly aggregation: TopNAggregationSchema;
  readonly onClose: () => void;
  readonly onDeleted?: () => void;
}

export function DeleteTopNModal({ groupName, aggregation, onClose, onDeleted }: DeleteTopNModalProps) {
  const qc = useQueryClient();
  const [text, setText] = useState('');
  const trapRef = useFocusTrap(true, onClose);
  const match = text === aggregation.metadata.name;

  const deleteMut = useMutation({
    mutationFn: () => apiDataSource.deleteTopNAggregation(groupName, aggregation.metadata.name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['topnAggregations'] });
      onClose();
      onDeleted?.();
    },
  });

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal is-danger" role="dialog" aria-modal="true" aria-label="Delete TopN aggregation" ref={trapRef} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <span className="modal-title">Delete TopN aggregation</span>
          <button type="button" className="modal-x" onClick={onClose} aria-label="Close" />
        </div>
        <div className="modal-body">
          <p className="del-warn">
            You are about to permanently delete the TopNAggregation <b className="mono">{aggregation.metadata.name}</b> from
            group <b className="mono">{groupName}</b>. Its pre-computed ranked statistics will be dropped.
            The source measure <b className="mono">{aggregation.sourceMeasure?.name}</b> is not affected.
          </p>
          <div className="f-field" style={{ marginTop: 16 }}>
            <label className="f-label">Type <span className="mono">{aggregation.metadata.name}</span> to confirm</label>
            <input type="text" className="f-input mono" autoFocus value={text} placeholder={aggregation.metadata.name}
              onChange={(e) => setText(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter' && match && !deleteMut.isPending) deleteMut.mutate(); }} />
          </div>
          {deleteMut.isError && <div className="f-error" style={{ marginTop: 8 }}>{deleteMut.error.message}</div>}
        </div>
        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={onClose} disabled={deleteMut.isPending}>Cancel</button>
          <button type="button" className="btn btn-danger" disabled={!match || deleteMut.isPending} onClick={() => deleteMut.mutate()}>
            {deleteMut.isPending ? 'Deleting…' : 'Delete aggregation'}
          </button>
        </div>
      </div>
    </div>
  );
}
