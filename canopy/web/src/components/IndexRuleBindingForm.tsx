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
  IndexRuleBindingSchema, IndexRuleSchema,
  CreateIndexRuleBindingRequest, UpdateIndexRuleBindingRequest,
} from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { validateIndexRuleBinding } from '../validation.js';
import { FAR_FUTURE, floorToMinute, localInputToMs, msToLocalInput } from '../pages/meta-utils.js';
import { Combobox, MultiCombobox } from './Combobox.js';
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

/** IndexRuleBindingForm renders a create/edit/delete modal for an IndexRuleBinding. */
export function IndexRuleBindingForm({
  mode, groupName, type, catalog, initialName, presetRuleName, existingNames, onClose,
}: {
  mode: 'create' | 'edit' | 'delete';
  groupName: string;
  /** Catalog type used to fetch the group's resources (e.g. 'measures'). */
  type?: string;
  /** Catalog to lock the subject selector to (e.g. "CATALOG_STREAM"). */
  catalog?: string;
  initialName?: string;
  /** When provided, the form pre-selects this rule in the rules picker. */
  presetRuleName?: string;
  existingNames?: ReadonlySet<string>;
  onClose: (created?: IndexRuleBindingSchema) => void;
}) {
  const qc = useQueryClient();

  const [name, setName] = useState('');
  const [subjectName, setSubjectName] = useState('');
  const [rules, setRules] = useState<string[]>(presetRuleName ? [presetRuleName] : []);
  const [beginAt, setBeginAt] = useState<number>(floorToMinute(Date.now()));
  const [expireAt, setExpireAt] = useState<number>(FAR_FUTURE);
  const [neverExpire, setNeverExpire] = useState(true);
  const [error, setError] = useState('');
  const [initialized, setInitialized] = useState(false);

  const { data: editResource } = useQuery({
    queryKey: ['indexRuleBinding', groupName, initialName ?? ''],
    queryFn: () => apiDataSource.getIndexRuleBinding(groupName, initialName!),
    enabled: mode === 'edit' && !!initialName,
  });

  const { data: availableRules = [] } = useQuery<IndexRuleSchema[]>({
    queryKey: ['indexRules', groupName],
    queryFn: () => apiDataSource.listIndexRules(groupName),
  });

  // Look up the group's resources so the subject picker can offer real names
  // instead of forcing the user to type them blind.
  const { data: resources = [] } = useQuery({
    queryKey: ['resources', type, groupName],
    queryFn: () => type ? apiDataSource.listResourcesInGroup(type, groupName) : Promise.resolve([]),
    enabled: !!type,
  });
  const subjectOptions = useMemo(
    () => (resources as Array<{ metadata: { name: string } }>).map((r) => r.metadata.name).sort(),
    [resources],
  );

  const availableRuleNames = useMemo(
    () => availableRules.map((r) => r.metadata.name).sort(),
    [availableRules],
  );

  // Dirty tracking: compare current state to the loaded binding (edit) or
  // blank defaults (create). Wires the modal close-warning via useDirtyGuard.
  const dirty = useMemo(() => {
    if (mode === 'edit' && editResource) {
      return subjectName !== (editResource.subject.name ?? '')
        || JSON.stringify([...rules].sort())
          !== JSON.stringify([...(editResource.rules ?? [])].sort())
        || beginAt !== (editResource.beginAt ?? beginAt)
        || (neverExpire
              ? (editResource.expireAt ?? 0) < FAR_FUTURE
              : expireAt !== (editResource.expireAt ?? expireAt));
    }
    // Create mode: dirty if the user has typed anything beyond the defaults.
    return name.trim().length > 0
      || subjectName.length > 0
      || (rules.length > (presetRuleName ? 1 : 0));
  }, [mode, editResource, name, subjectName, rules, beginAt, expireAt, neverExpire, presetRuleName]);

  const { guardedClose, resetDirty } = useDirtyGuard(dirty, onClose);

  // Catalog is locked to whatever the parent page is on (handoff decision: you
  // can't bind a stream's index rule to a measure). Prefer the explicit `catalog`
  // prop the parent passes (derived from the route). Fall back to inspecting
  // the group's resources if the parent didn't provide it.
  const lockedCatalog = useMemo(() => {
    if (catalog) return catalog;
    if (resources.length > 0) {
      return (resources[0] as { metadata: { catalog?: string } }).metadata.catalog ?? '';
    }
    return '';
  }, [catalog, resources]);

  const subjectMissing = !!subjectName && !subjectOptions.includes(subjectName);

  useEffect(() => {
    if (mode === 'edit' && editResource && !initialized) {
      setName(editResource.metadata.name);
      setSubjectName(editResource.subject.name);
      setRules(editResource.rules ?? []);
      setBeginAt(editResource.beginAt);
      const isFarFuture = editResource.expireAt >= FAR_FUTURE;
      setExpireAt(isFarFuture ? FAR_FUTURE : editResource.expireAt);
      setNeverExpire(isFarFuture);
      setInitialized(true);
    }
  }, [mode, editResource, initialized]);

  const createMut = useMutation({
    mutationFn: (req: CreateIndexRuleBindingRequest) => apiDataSource.createIndexRuleBinding(req),
    onSuccess: (binding) => {
      qc.invalidateQueries({ queryKey: ['indexRuleBindings', groupName] });
      resetDirty();
      onClose(binding);
    },
    onError: (e: Error) => setError(e.message),
  });

  const updateMut = useMutation({
    mutationFn: (req: UpdateIndexRuleBindingRequest) => apiDataSource.updateIndexRuleBinding(groupName, initialName!, req),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['indexRuleBindings', groupName] });
      qc.invalidateQueries({ queryKey: ['indexRuleBinding', groupName, initialName] });
      resetDirty();
      onClose();
    },
    onError: (e: Error) => setError(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: () => apiDataSource.deleteIndexRuleBinding(groupName, initialName!),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['indexRuleBindings', groupName] });
      onClose();
    },
    onError: (e: Error) => setError(e.message),
  });

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const submittedName = mode === 'edit' ? initialName! : name.trim();
    const err = validateIndexRuleBinding({
      isEdit: mode === 'edit',
      name: submittedName,
      rules,
      subject: { name: subjectName.trim(), catalog: lockedCatalog },
      beginAt,
      expireAt: neverExpire ? FAR_FUTURE : expireAt,
      existingNames,
    });
    if (err) { setError(err); return; }
    setError('');

    const payload = {
      metadata: { name: submittedName, group: groupName },
      rules,
      subject: { name: subjectName.trim(), catalog: lockedCatalog },
      beginAt,
      expireAt: neverExpire ? FAR_FUTURE : expireAt,
    };

    if (mode === 'edit') {
      updateMut.mutate({ indexRuleBinding: payload });
    } else {
      createMut.mutate({ indexRuleBinding: payload });
    }
  }

  const isPending = createMut.isPending || updateMut.isPending || deleteMut.isPending;
  const isEdit = mode === 'edit';
  const trapRef = useFocusTrap(true, guardedClose);

  if (mode === 'delete') {
    return (
      <div className="modal-overlay" onClick={() => onClose()}>
        <div className="modal is-danger" onClick={(e) => e.stopPropagation()}>
          <div className="modal-head">
            <span className="modal-title">Delete binding</span>
            <button className="modal-x" onClick={() => onClose()} aria-label="Close" />
          </div>
          <div className="modal-body">
            <p className="del-warn">
              You are about to permanently delete the binding{' '}
              <b className="mono">{initialName}</b> from group{' '}
              <b className="mono">{groupName}</b>.
            </p>
            <p className="f-section-desc">
              The subject will stop generating indices for its {rules.length} rule{rules.length !== 1 ? 's' : ''}.
              The index rules themselves are not deleted.
            </p>
            {error && <div className="f-error">{error}</div>}
          </div>
          <div className="modal-foot">
            <button className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>Cancel</button>
            <button className="btn btn-danger" onClick={() => deleteMut.mutate()} disabled={isPending}>
              {isPending ? 'Deleting…' : 'Delete binding'}
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
            <span className="modal-title">{isEdit ? 'Edit binding' : 'Create binding'}</span>
            <p className="modal-sub">
              {isEdit
                ? 'Name is immutable. Update the rules, subject or validity window.'
                : `Bind index rules to a resource in group “${groupName}”.`}
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
              >
                <input className="f-input mono" type="text" placeholder="binding_name"
                  value={isEdit ? (initialName ?? '') : name}
                  onChange={(e) => { if (!isEdit) setName(e.target.value); }}
                  readOnly={isEdit} autoFocus={!isEdit} />
              </Field>
              <Field label="Group" locked hint="Resources are scoped to their group">
                <input className="f-input mono" type="text" value={groupName} disabled />
              </Field>
            </div>
          </section>

          {/* Subject — catalog locked, name picked from group resources */}
          <section className="f-section">
            <div className="f-section-title">Subject <span className="f-req">*</span></div>
            <p className="f-section-desc">
              The resource whose data these rules index. Catalog is fixed to the current page's catalog.
            </p>
            {presetRuleName && (
              <div className="idx-inline-warn">
                <span>
                  Created from rule <b className="mono">{presetRuleName}</b> — it has been pre-selected in the rules section below.
                </span>
              </div>
            )}
            <div className="f-grid">
              <Field label="Catalog" locked hint="Bound to the current page's catalog">
                <input className="f-input mono" type="text" value={lockedCatalog || '—'} disabled />
              </Field>
              <Field label="Resource name" required
                hint="Must match an existing resource in this group"
                error={subjectMissing ? `Subject "${subjectName}" no longer exists — binding would be orphaned.` : undefined}>
                <Combobox
                  value={subjectName}
                  options={subjectOptions}
                  onChange={setSubjectName}
                  placeholder="— select a resource —"
                  emptyHint="No matching resources"
                  noOptionsHint="No resources in this group yet"
                  ariaLabel="Resource name"
                />
              </Field>
            </div>
          </section>

          {/* Rules */}
          <section className="f-section">
            <div className="f-section-title">Index rules <span className="f-req">*</span></div>
            <p className="f-section-desc">One or more IndexRules to apply to the subject.</p>
            {availableRuleNames.length === 0 ? (
              <div className="picker-empty">
                No index rules defined in this group yet. Create one in the Rule tab first.
              </div>
            ) : (
              <MultiCombobox
                value={rules}
                options={availableRuleNames}
                onChange={setRules}
                placeholder="— select index rules —"
                emptyHint="No matching rules"
                noOptionsHint="No index rules defined in this group yet"
                ariaLabel="Index rules"
              />
            )}
          </section>

          {/* Validity window — datetime-local + never-expires toggle */}
          <section className="f-section">
            <div className="f-section-title">Validity window <span className="f-req">*</span></div>
            <p className="f-section-desc">
              The binding only generates indices between <span className="mono">begin_at</span> and
              <span className="mono"> expire_at</span>.
            </p>
            <div className="f-grid">
              <Field label="Begin at" required>
                <input className="f-input mono" type="datetime-local"
                  value={msToLocalInput(beginAt)}
                  onChange={(e) => {
                    const ms = localInputToMs(e.target.value);
                    if (ms != null) setBeginAt(ms);
                  }} />
              </Field>
              <Field label="Expire at" required={!neverExpire}>
                {neverExpire ? (
                  // The disabled input that lived here used to show today's date
                  // (the browser's empty-value placeholder for datetime-local), which
                  // looked like a real value but meant nothing. Render an explicit
                  // "Never expires" indicator instead.
                  <div className="f-input f-never-expires mono" aria-label="Never expires">
                    <span className="never-icon" aria-hidden="true">∞</span>
                    <span>Never expires</span>
                  </div>
                ) : (
                  <input className="f-input mono" type="datetime-local"
                    value={msToLocalInput(expireAt)}
                    onChange={(e) => {
                      const ms = localInputToMs(e.target.value);
                      if (ms != null) setExpireAt(ms);
                    }} />
                )}
              </Field>
            </div>
            <label className="f-check" style={{ marginTop: 4 }}>
              <input type="checkbox" checked={neverExpire}
                onChange={(e) => {
                  const checked = e.target.checked;
                  setNeverExpire(checked);
                  if (checked) {
                    // Switching ON: collapse expireAt to the FAR_FUTURE sentinel.
                    setExpireAt(FAR_FUTURE);
                  } else if (expireAt >= FAR_FUTURE) {
                    // Switching OFF from a fresh form: replace the sentinel with a
                    // sensible default (begin + 30 days) instead of showing
                    // "2099-12-31" in the date picker.
                    const thirtyDays = 30 * 24 * 60 * 60 * 1000;
                    setExpireAt(beginAt + thirtyDays);
                  }
                }} />
              Never expires
            </label>
          </section>

          {error && <div className="f-error">{error}</div>}
        </div>

        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={guardedClose} disabled={isPending}>Cancel</button>
          <button type="submit" className="btn btn-primary" disabled={isPending}>
            {isPending ? (isEdit ? 'Saving…' : 'Creating…') : (isEdit ? 'Save changes' : 'Create binding')}
          </button>
        </div>
      </form>
    </div>
  );
}