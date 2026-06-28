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

import React, { useState, useEffect, useMemo } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import type {
  IndexRuleSchema, IndexRuleBindingSchema, IndexType,
  CreateIndexRuleRequest, UpdateIndexRuleRequest,
} from 'canopy-shared';
import { IndexType as IndexTypeEnum } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { validateIndexRule } from '../validation.js';
import { IconCheck, IconPlus } from './icons.js';
import { useFocusTrap, useDirtyGuard } from './modal-utils.js';

// Mirrors the handoff Field wrapper: label + required indicator + hint/error.
function Field({
  label, hint, error, required, locked, children,
}: {
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

// Styled <select> matching the form's other selects (IndexRuleBindingForm).
function IdxSelect<T extends string>({ value, onChange, options, disabled }: {
  value: T;
  onChange: (v: T) => void;
  options: ReadonlyArray<{ value: T; label: string }>;
  disabled?: boolean;
}) {
  return (
    <div className="f-select-wrap">
      <select className="f-input f-select mono" value={value} disabled={disabled}
        onChange={(e) => onChange(e.target.value as T)}>
        {options.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
      </select>
      <span className="f-select-chev">▾</span>
    </div>
  );
}

const INDEX_TYPES: ReadonlyArray<{
  value: IndexType; label: string; tone: 'inv' | 'skip' | 'tree'; hint: string;
}> = [
  { value: IndexTypeEnum.INVERTED, label: 'inverted', tone: 'inv',
    hint: 'Full-text / equality search; supports sorting' },
  { value: IndexTypeEnum.SKIPPING, label: 'skipping', tone: 'skip',
    hint: 'Block-level skip index for numeric range scans' },
  { value: IndexTypeEnum.TREE,     label: 'tree',     tone: 'tree',
    hint: 'Hierarchical tree index' },
];

const INDEX_ANALYZERS = [
  { value: '',         label: '— none —' },
  { value: 'standard', label: 'standard' },
  { value: 'simple',   label: 'simple' },
  { value: 'keyword',  label: 'keyword' },
  { value: 'url',      label: 'url' },
] as const;

type AnalyzerValue = typeof INDEX_ANALYZERS[number]['value'];

/** IndexRuleForm renders a create/edit/delete modal for an IndexRule. */
export function IndexRuleForm({
  mode, groupName, initialName, existingNames, onClose,
}: {
  mode: 'create' | 'edit' | 'delete';
  groupName: string;
  initialName?: string;
  existingNames?: ReadonlySet<string>;
  onClose: (created?: IndexRuleSchema) => void;
}) {
  const qc = useQueryClient();

  const [name, setName] = useState('');
  const [tags, setTags] = useState<string[]>([]);
  const [customTag, setCustomTag] = useState('');
  const [ruleType, setRuleType] = useState<IndexType>(IndexTypeEnum.INVERTED);
  const [analyzer, setAnalyzer] = useState<AnalyzerValue>('');
  const [noSort, setNoSort] = useState(false);
  const [error, setError] = useState('');
  const [errors, setErrors] = useState<{ name?: string; tags?: string }>({});
  const [submitted, setSubmitted] = useState(false);
  const [initialized, setInitialized] = useState(false);

  const { data: editResource } = useQuery({
    queryKey: ['indexRule', groupName, initialName ?? ''],
    queryFn: () => apiDataSource.getIndexRule(groupName, initialName!),
    enabled: mode === 'edit' && !!initialName,
  });

  // Look up any bindings that reference this rule so the delete confirmation
  // can warn the user about the cascade impact.
  const { data: bindings = [] } = useQuery<IndexRuleBindingSchema[]>({
    queryKey: ['indexRuleBindings', groupName],
    queryFn: () => apiDataSource.listIndexRuleBindings(groupName),
    enabled: mode === 'delete' && !!initialName,
  });
  const referencingBindings = initialName
    ? bindings.filter((b) => b.rules.includes(initialName))
    : [];

  useEffect(() => {
    if (mode === 'edit' && editResource && !initialized) {
      setName(editResource.metadata.name);
      setTags(editResource.tags ?? []);
      setRuleType(editResource.type);
      setAnalyzer((editResource.analyzer ?? '') as AnalyzerValue);
      setNoSort(!!editResource.noSort);
      setInitialized(true);
    }
  }, [mode, editResource, initialized]);

  // Track edits so the modal can warn before discarding unsaved changes.
  // In create mode the form is dirty the moment any field carries a value; in
  // edit mode dirty = current state differs from the loaded rule.
  const dirty = useMemo(() => {
    if (mode === 'edit' && editResource) {
      return ruleType !== editResource.type
        || analyzer !== (editResource.analyzer ?? '')
        || noSort !== !!editResource.noSort
        || JSON.stringify([...tags].sort())
          !== JSON.stringify([...(editResource.tags ?? [])].sort());
    }
    return name.trim().length > 0
      || tags.length > 0
      || analyzer !== ''
      || noSort !== false;
  }, [mode, editResource, name, tags, analyzer, noSort, ruleType]);

  const { guardedClose, resetDirty } = useDirtyGuard(dirty, onClose);

  const createMut = useMutation({
    mutationFn: (req: CreateIndexRuleRequest) => apiDataSource.createIndexRule(req),
    onSuccess: (rule) => {
      qc.invalidateQueries({ queryKey: ['indexRules', groupName] });
      resetDirty();
      onClose(rule);
    },
    onError: (e: Error) => setError(e.message),
  });

  const updateMut = useMutation({
    mutationFn: (req: UpdateIndexRuleRequest) => apiDataSource.updateIndexRule(groupName, initialName!, req),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['indexRules', groupName] });
      qc.invalidateQueries({ queryKey: ['indexRule', groupName, initialName] });
      resetDirty();
      onClose();
    },
    onError: (e: Error) => setError(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: () => apiDataSource.deleteIndexRule(groupName, initialName!),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['indexRules', groupName] });
      qc.invalidateQueries({ queryKey: ['indexRuleBindings', groupName] });
      onClose();
    },
    onError: (e: Error) => setError(e.message),
  });

  function removeTag(t: string) {
    setTags((prev) => prev.filter((x) => x !== t));
  }

  function addCustomTag() {
    const t = customTag.trim();
    if (!t) return;
    // Handoff behavior: silently no-op on duplicate so the user can move on
    // without a confusing error band while typing.
    if (tags.includes(t)) {
      setCustomTag('');
      return;
    }
    setTags((prev) => [...prev, t]);
    setCustomTag('');
  }

  // Map the validator's single error string onto a per-field slot. Any error
  // that doesn't match a known field surfaces as a top-level band.
  function mapValidationError(err: string): { name?: string; tags?: string } {
    if (/^Name\b/.test(err)
        || /^Must be 255/.test(err)
        || /^An index rule named/.test(err)
        || /^Name may contain only/.test(err)) {
      return { name: err };
    }
    if (/tag/i.test(err)) return { tags: err };
    return {};
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const submittedName = mode === 'edit' ? initialName! : name.trim();
    const isInverted = ruleType === IndexTypeEnum.INVERTED;
    const err = validateIndexRule({
      isEdit: mode === 'edit',
      name: submittedName,
      tags,
      type: ruleType,
      analyzer: analyzer || undefined,
      existingNames,
    });
    setSubmitted(true);
    if (err) {
      const mapped = mapValidationError(err);
      setErrors(mapped);
      setError(mapped.name || mapped.tags ? '' : err);
      return;
    }
    setErrors({});
    setError('');

    const payload = {
      metadata: { name: submittedName, group: groupName },
      tags,
      type: ruleType,
      analyzer: isInverted ? (analyzer || '') : '',
      noSort,
    };

    if (mode === 'edit') {
      updateMut.mutate({ indexRule: payload });
    } else {
      createMut.mutate({ indexRule: payload });
    }
  }

  const isPending = createMut.isPending || updateMut.isPending || deleteMut.isPending;
  const isEdit = mode === 'edit';
  const isInverted = ruleType === IndexTypeEnum.INVERTED;
  const trapRef = useFocusTrap(true, guardedClose);

  if (mode === 'delete') {
    return (
      <div className="modal-overlay" onClick={() => onClose()}>
        <div className="modal is-danger" onClick={(e) => e.stopPropagation()}>
          <div className="modal-head">
            <span className="modal-title">Delete index rule</span>
            <button className="modal-x" onClick={() => onClose()} aria-label="Close" />
          </div>
          <div className="modal-body">
            <p className="del-warn">
              You are about to permanently delete the index rule{' '}
              <b className="mono">{initialName}</b> from group{' '}
              <b className="mono">{groupName}</b>.
            </p>
            <p className="f-section-desc">Any binding that references this rule may become invalid.</p>
            {referencingBindings.length > 0 && (
              <div className="idx-inline-warn">
                <span>
                  It is referenced by <b>{referencingBindings.length}</b> binding
                  {referencingBindings.length !== 1 ? 's' : ''} (
                  {referencingBindings.map((b) => b.metadata.name).join(', ')}).
                  The rule will be removed from them.
                </span>
              </div>
            )}
            {error && <div className="f-error">{error}</div>}
          </div>
          <div className="modal-foot">
            <button className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>Cancel</button>
            <button className="btn btn-danger" onClick={() => deleteMut.mutate()} disabled={isPending}>
              {isPending ? 'Deleting…' : 'Delete index rule'}
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="modal-overlay" onClick={guardedClose}>
      <form className="modal" ref={trapRef} onSubmit={handleSubmit} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <div>
            <span className="modal-title">{isEdit ? 'Edit index rule' : 'Create index rule'}</span>
            <p className="modal-sub">
              {isEdit
                ? 'Name is immutable. Update the rule below — bindings revalidate on save.'
                : `Define an IndexRule for tags in group “${groupName}”.`}
            </p>
          </div>
          <button type="button" className="modal-x" onClick={guardedClose} aria-label="Close" />
        </div>

        <div className="modal-body">
          {/* Identity */}
          <section className="f-section">
            <div className="f-section-title">Identity</div>
            <div className="f-grid">
              <Field
                label="Name"
                required={!isEdit}
                locked={isEdit}
                hint={isEdit ? undefined : "Unique within the group · letters, digits, '_' and '-'"}
                error={submitted ? errors.name : undefined}
              >
                <input className="f-input mono" type="text" placeholder="by_service"
                  value={isEdit ? (initialName ?? '') : name}
                  onChange={(e) => { if (!isEdit) setName(e.target.value); }}
                  readOnly={isEdit} autoFocus={!isEdit} />
              </Field>
              <Field label="Group" locked hint="Index rules are scoped to their group">
                <input className="f-input mono" type="text" value={groupName} disabled />
              </Field>
            </div>
          </section>

          {/* Indexed tags — custom-tag-only flow (recommended tags picker removed). */}
          <section className="f-section">
            <div className="f-section-title">Indexed tags <span className="f-req">*</span></div>
            <p className="f-section-desc">
              Tags this rule builds an index over. Multiple tags create a composite index, evaluated together.
            </p>
            {submitted && errors.tags && (
              <div className="f-error" style={{ marginBottom: 10 }}>{errors.tags}</div>
            )}
            {tags.length > 0 && (
              <div className="picker">
                <div className="picker-selected">
                  {tags.map((n) => (
                    <button type="button" key={n}
                      className="picker-chip is-on"
                      onClick={() => removeTag(n)}>
                      <span className="mono">{n}</span>
                      <span className="chip-x" aria-label={`Remove ${n}`}>×</span>
                    </button>
                  ))}
                </div>
              </div>
            )}
            <div className="f-hint" style={{ marginTop: 8 }}>
              Type a tag name and press Enter to add it:
              <span className="inline-add">
                <input className="f-input mono" type="text" placeholder="tag_name"
                  value={customTag}
                  onChange={(e) => setCustomTag(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') { e.preventDefault(); addCustomTag(); }
                  }} />
                <button type="button" className="btn btn-ghost btn-sm" onClick={addCustomTag}>
                  <IconPlus size={13} /> Add
                </button>
              </span>
            </div>
          </section>

          {/* Index type — card grid (handoff design). */}
          <section className="f-section">
            <div className="f-section-title">Index type <span className="f-req">*</span></div>
            <div className="idx-type-choices">
              {INDEX_TYPES.map((t) => (
                <button type="button" key={t.value}
                  className={'idx-type-card' + (ruleType === t.value ? ' is-on' : '')}
                  onClick={() => setRuleType(t.value)}>
                  <span className="idx-type-card-h">
                    <span className={'idx-type-badge is-' + t.tone}>{t.label}</span>
                    {ruleType === t.value && <IconCheck size={15} />}
                  </span>
                  <span className="idx-type-card-hint">{t.hint}</span>
                </button>
              ))}
            </div>
          </section>

          {/* Options — analyzer (locked when not INVERTED) + sorting in f-grid. */}
          <section className="f-section">
            <div className="f-section-title">Options</div>
            <div className="f-grid">
              <Field label="Analyzer" locked={!isInverted}
                hint={isInverted
                  ? 'Tokenizer for full-text matching'
                  : 'Only applies to inverted indices'}>
                <IdxSelect value={analyzer} disabled={!isInverted}
                  options={INDEX_ANALYZERS}
                  onChange={(v) => setAnalyzer(v)} />
              </Field>
              <Field label="Sorting" hint="Disable to skip building the sortable doc-values">
                <label className="f-check" style={{ marginTop: 6 }}>
                  <input type="checkbox" checked={noSort}
                    onChange={(e) => setNoSort(e.target.checked)} />
                  No sort (<span className="mono">no_sort</span>)
                </label>
              </Field>
            </div>
          </section>

          {error && <div className="f-error">{error}</div>}
        </div>

        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={guardedClose} disabled={isPending}>Cancel</button>
          <button type="submit" className="btn btn-primary" disabled={isPending}>
            {isPending ? (isEdit ? 'Saving…' : 'Creating…') : (isEdit ? 'Save changes' : 'Create index rule')}
          </button>
        </div>
      </form>
    </div>
  );
}
