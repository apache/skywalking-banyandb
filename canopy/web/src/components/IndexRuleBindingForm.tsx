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

const CATALOGS: ReadonlyArray<{ value: string; label: string }> = [
  { value: 'CATALOG_MEASURE', label: 'MEASURE' },
  { value: 'CATALOG_STREAM', label: 'STREAM' },
  { value: 'CATALOG_TRACE', label: 'TRACE' },
  { value: 'CATALOG_PROPERTY', label: 'PROPERTY' },
];

/** IndexRuleBindingForm renders a create/edit/delete modal for an IndexRuleBinding. */
export function IndexRuleBindingForm({ mode, groupName, initialName, onClose }: {
  mode: 'create' | 'edit' | 'delete';
  groupName: string;
  initialName?: string;
  onClose: (created?: IndexRuleBindingSchema) => void;
}) {
  const qc = useQueryClient();

  const [name, setName] = useState('');
  const [subjectCatalog, setSubjectCatalog] = useState<string>('CATALOG_MEASURE');
  const [subjectName, setSubjectName] = useState('');
  const [rules, setRules] = useState<string[]>([]);
  const [beginAt, setBeginAt] = useState('');
  const [expireAt, setExpireAt] = useState('');
  const [error, setError] = useState('');
  const [initialized, setInitialized] = useState(false);

  const { data: editResource } = useQuery({
    queryKey: ['indexRuleBinding', groupName, initialName ?? ''],
    queryFn: () => apiDataSource.getIndexRuleBinding(groupName, initialName!),
    enabled: mode === 'edit' && !!initialName,
  });

  // List available rules in this group so the picker only shows real options.
  const { data: availableRules = [] } = useQuery<IndexRuleSchema[]>({
    queryKey: ['indexRules', groupName],
    queryFn: () => apiDataSource.listIndexRules(groupName),
  });

  const availableRuleNames = useMemo(() => availableRules.map((r) => r.metadata.name), [availableRules]);

  useEffect(() => {
    if (mode === 'edit' && editResource && !initialized) {
      setName(editResource.metadata.name);
      setSubjectCatalog(editResource.subject.catalog);
      setSubjectName(editResource.subject.name);
      setRules(editResource.rules ?? []);
      setBeginAt(editResource.beginAt ?? '');
      setExpireAt(editResource.expireAt ?? '');
      setInitialized(true);
    }
  }, [mode, editResource, initialized]);

  const createMut = useMutation({
    mutationFn: (req: CreateIndexRuleBindingRequest) => apiDataSource.createIndexRuleBinding(req),
    onSuccess: (binding) => {
      qc.invalidateQueries({ queryKey: ['indexRuleBindings', groupName] });
      onClose(binding);
    },
    onError: (e: Error) => setError(e.message),
  });

  const updateMut = useMutation({
    mutationFn: (req: UpdateIndexRuleBindingRequest) => apiDataSource.updateIndexRuleBinding(groupName, initialName!, req),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['indexRuleBindings', groupName] });
      qc.invalidateQueries({ queryKey: ['indexRuleBinding', groupName, initialName] });
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

  function toggleRule(ruleName: string) {
    setRules((prev) => prev.includes(ruleName)
      ? prev.filter((r) => r !== ruleName)
      : [...prev, ruleName]);
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const submittedName = mode === 'edit' ? initialName! : name.trim();
    const err = validateIndexRuleBinding({
      isEdit: mode === 'edit',
      name: submittedName,
      rules,
      subject: { name: subjectName.trim(), catalog: subjectCatalog },
      beginAt,
      expireAt,
    });
    if (err) { setError(err); return; }
    setError('');

    const payload = {
      metadata: { name: submittedName, group: groupName },
      rules,
      subject: { name: subjectName.trim(), catalog: subjectCatalog },
      beginAt,
      expireAt,
    };

    if (mode === 'edit') {
      updateMut.mutate({ indexRuleBinding: payload });
    } else {
      createMut.mutate({ indexRuleBinding: payload });
    }
  }

  const isPending = createMut.isPending || updateMut.isPending || deleteMut.isPending;
  const isEdit = mode === 'edit';

  if (mode === 'delete') {
    return (
      <div className="modal-overlay" onClick={() => onClose()}>
        <div className="modal is-danger" onClick={(e) => e.stopPropagation()}>
          <div className="modal-head">
            <span className="modal-title">Delete index rule binding</span>
            <button className="modal-x" onClick={() => onClose()} />
          </div>
          <div className="modal-body">
            <p>This will permanently delete binding <span className="mono">{initialName}</span> from group <span className="mono">{groupName}</span>.</p>
            {error && <div className="f-error">{error}</div>}
          </div>
          <div className="modal-foot">
            <button className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>Cancel</button>
            <button className="btn btn-danger" onClick={() => deleteMut.mutate()} disabled={isPending}>
              {isPending ? 'Deleting…' : 'Delete'}
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="modal-overlay" onClick={() => onClose()}>
      <form className="modal" onSubmit={handleSubmit} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <span className="modal-title">{isEdit ? 'Edit index rule binding' : 'New index rule binding'}</span>
          <button type="button" className="modal-x" onClick={() => onClose()} />
        </div>

        <div className="modal-body">
          <div className="f-section">
            <label className="f-field">
              <span className="f-label">Name {!isEdit && <span className="f-req">*</span>}</span>
              <input className="f-input mono" type="text"
                value={isEdit ? (initialName ?? '') : name}
                onChange={(e) => { if (!isEdit) setName(e.target.value); }}
                readOnly={isEdit} autoFocus={!isEdit} />
            </label>
          </div>

          <div className="f-section">
            <span className="f-section-title">Subject <span className="f-req">*</span></span>
            <span className="f-section-desc">The resource this binding applies indices to.</span>
            <div className="f-grid" style={{ gridTemplateColumns: '200px 1fr' }}>
              <div className="f-seg">
                {CATALOGS.map((c) => (
                  <button type="button" key={c.value}
                    className={`seg-btn${subjectCatalog === c.value ? ' is-on' : ''}`}
                    onClick={() => setSubjectCatalog(c.value)}>
                    {c.label}
                  </button>
                ))}
              </div>
              <input className="f-input mono" type="text" placeholder="resource_name"
                value={subjectName}
                onChange={(e) => setSubjectName(e.target.value)} />
            </div>
          </div>

          <div className="f-section">
            <span className="f-section-title">Rules <span className="f-req">*</span></span>
            <span className="f-section-desc">Index rules from this group to apply to the subject.</span>
            {availableRuleNames.length === 0 ? (
              <p className="page-meta">No index rules exist in this group yet.</p>
            ) : (
              <div className="picker">
                <div className="picker-selected">
                  {rules.map((ruleName, ord) => (
                    <button key={ruleName} type="button" className="picker-chip is-on"
                      onClick={() => toggleRule(ruleName)}>
                      <span className="picker-ord">{ord + 1}</span>{ruleName}
                    </button>
                  ))}
                </div>
                <div className="picker-avail">
                  {availableRuleNames.filter((n) => !rules.includes(n)).map((ruleName) => (
                    <button key={ruleName} type="button" className="picker-chip"
                      onClick={() => toggleRule(ruleName)}>
                      {ruleName}
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>

          <div className="f-section">
            <span className="f-section-title">Validity window <span className="f-req">*</span></span>
            <div className="f-grid" style={{ gridTemplateColumns: '1fr 1fr' }}>
              <label className="f-field">
                <span className="f-label">Begin at (RFC3339)</span>
                <input className="f-input mono" type="text" placeholder="2026-01-01T00:00:00Z"
                  value={beginAt}
                  onChange={(e) => setBeginAt(e.target.value)} />
              </label>
              <label className="f-field">
                <span className="f-label">Expire at (RFC3339)</span>
                <input className="f-input mono" type="text" placeholder="2026-02-01T00:00:00Z"
                  value={expireAt}
                  onChange={(e) => setExpireAt(e.target.value)} />
              </label>
            </div>
          </div>

          {error && <div className="f-error">{error}</div>}
        </div>

        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>Cancel</button>
          <button type="submit" className="btn btn-primary" disabled={isPending}>
            {isPending ? (isEdit ? 'Saving…' : 'Creating…') : (isEdit ? 'Save' : 'Create')}
          </button>
        </div>
      </form>
    </div>
  );
}
