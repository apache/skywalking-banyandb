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

import type {
  IndexRuleSchema, IndexType, CreateIndexRuleRequest, UpdateIndexRuleRequest,
} from 'canopy-shared';
import { IndexType as IndexTypeEnum } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { validateIndexRule } from '../validation.js';

const INDEX_TYPES: ReadonlyArray<{ value: IndexType; label: string; hint: string }> = [
  { value: IndexTypeEnum.TREE, label: 'TREE', hint: 'B+tree — range and equality lookups' },
  { value: IndexTypeEnum.INVERTED, label: 'INVERTED', hint: 'Inverted index — text/token search' },
];

/** IndexRuleForm renders a create/edit/delete modal for an IndexRule. */
export function IndexRuleForm({ mode, groupName, initialName, onClose }: {
  mode: 'create' | 'edit' | 'delete';
  groupName: string;
  initialName?: string;
  onClose: (created?: IndexRuleSchema) => void;
}) {
  const qc = useQueryClient();

  const [name, setName] = useState('');
  const [tagInput, setTagInput] = useState('');
  const [tags, setTags] = useState<string[]>([]);
  const [type, setType] = useState<IndexType>(IndexTypeEnum.TREE);
  const [analyzer, setAnalyzer] = useState('');
  const [error, setError] = useState('');
  const [initialized, setInitialized] = useState(false);

  const { data: editResource } = useQuery({
    queryKey: ['indexRule', groupName, initialName ?? ''],
    queryFn: () => apiDataSource.getIndexRule(groupName, initialName!),
    enabled: mode === 'edit' && !!initialName,
  });

  useEffect(() => {
    if (mode === 'edit' && editResource && !initialized) {
      setName(editResource.metadata.name);
      setTags(editResource.tags ?? []);
      setType(editResource.type);
      setAnalyzer(editResource.analyzer ?? '');
      setInitialized(true);
    }
  }, [mode, editResource, initialized]);

  const createMut = useMutation({
    mutationFn: (req: CreateIndexRuleRequest) => apiDataSource.createIndexRule(req),
    onSuccess: (rule) => {
      qc.invalidateQueries({ queryKey: ['indexRules', groupName] });
      onClose(rule);
    },
    onError: (e: Error) => setError(e.message),
  });

  const updateMut = useMutation({
    mutationFn: (req: UpdateIndexRuleRequest) => apiDataSource.updateIndexRule(groupName, initialName!, req),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['indexRules', groupName] });
      qc.invalidateQueries({ queryKey: ['indexRule', groupName, initialName] });
      onClose();
    },
    onError: (e: Error) => setError(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: () => apiDataSource.deleteIndexRule(groupName, initialName!),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['indexRules', groupName] });
      onClose();
    },
    onError: (e: Error) => setError(e.message),
  });

  function addTag() {
    const t = tagInput.trim();
    if (!t) return;
    if (tags.includes(t)) {
      setError(`Tag "${t}" is already added.`);
      return;
    }
    setTags((prev) => [...prev, t]);
    setTagInput('');
    setError('');
  }

  function removeTag(t: string) {
    setTags((prev) => prev.filter((x) => x !== t));
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const submittedName = mode === 'edit' ? initialName! : name.trim();
    const err = validateIndexRule({
      isEdit: mode === 'edit',
      name: submittedName,
      tags,
      type,
      analyzer: analyzer.trim() || undefined,
    });
    if (err) { setError(err); return; }
    setError('');

    const payload = {
      metadata: { name: submittedName, group: groupName },
      tags,
      type,
      analyzer: analyzer.trim() || undefined,
    };

    if (mode === 'edit') {
      updateMut.mutate({ indexRule: payload });
    } else {
      createMut.mutate({ indexRule: payload });
    }
  }

  const isPending = createMut.isPending || updateMut.isPending || deleteMut.isPending;
  const isEdit = mode === 'edit';

  if (mode === 'delete') {
    return (
      <div className="modal-overlay" onClick={() => onClose()}>
        <div className="modal is-danger" onClick={(e) => e.stopPropagation()}>
          <div className="modal-head">
            <span className="modal-title">Delete index rule</span>
            <button className="modal-x" onClick={() => onClose()} />
          </div>
          <div className="modal-body">
            <p>This will permanently delete index rule <span className="mono">{initialName}</span> from group <span className="mono">{groupName}</span>.</p>
            <p className="page-meta" style={{ marginTop: 8 }}>Any binding that references this rule may become invalid.</p>
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
          <span className="modal-title">{isEdit ? 'Edit index rule' : 'New index rule'}</span>
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
            <span className="f-section-title">Type <span className="f-req">*</span></span>
            <div className="f-seg">
              {INDEX_TYPES.map((t) => (
                <button type="button" key={t.value}
                  className={`seg-btn${type === t.value ? ' is-on' : ''}`}
                  onClick={() => setType(t.value)}>
                  {t.label}
                </button>
              ))}
            </div>
            <p className="f-section-desc">{INDEX_TYPES.find((t) => t.value === type)?.hint}</p>
          </div>

          <div className="f-section">
            <span className="f-section-title">Tags <span className="f-req">*</span></span>
            <span className="f-section-desc">Names of resource tags this rule indexes. Must match tag names defined on the target resource.</span>
            <div className="fam-card">
              {tags.length > 0 && (
                <div className="chip-row" style={{ marginBottom: 8 }}>
                  {tags.map((t) => (
                    <span key={t} className="ord-chip">
                      {t}
                      <button type="button" className="chip-x" onClick={() => removeTag(t)} aria-label={`Remove ${t}`}>×</button>
                    </span>
                  ))}
                </div>
              )}
              <div className="spec-row">
                <div className="spec-cell" style={{ flex: 1 }}>
                  <input className="f-input mono" type="text" placeholder="tag_name"
                    value={tagInput}
                    onChange={(e) => setTagInput(e.target.value)}
                    onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); addTag(); } }} />
                </div>
                <button type="button" className="btn btn-ghost" onClick={addTag}>Add tag</button>
              </div>
            </div>
          </div>

          {type === 'INDEX_TYPE_INVERTED' && (
            <div className="f-section">
              <label className="f-field">
                <span className="f-label">Analyzer <span className="f-optional">optional</span></span>
                <span className="f-section-desc">Analyzer name (e.g. keyword, simple). Leave empty for default.</span>
                <input className="f-input mono" type="text" placeholder="keyword"
                  value={analyzer}
                  onChange={(e) => setAnalyzer(e.target.value)} />
              </label>
            </div>
          )}

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
