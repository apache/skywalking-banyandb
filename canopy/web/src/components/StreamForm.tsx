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
import { useMutation, useQueryClient } from '@tanstack/react-query';

import type { StreamSchema, CreateStreamRequest } from 'canopy-shared';
import { TagType } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { IconChevron } from './icons.js';

const TAG_TYPES = [
  'TAG_TYPE_STRING',
  'TAG_TYPE_INT64',
  'TAG_TYPE_FLOAT64',
  'TAG_TYPE_STRING_ARRAY',
  'TAG_TYPE_INT64_ARRAY',
  'TAG_TYPE_DATA_BINARY',
] as const;

interface TagRow {
  name: string;
  type: string;
}

interface FamilyRow {
  name: string;
  tags: TagRow[];
}

/** StreamForm renders either a create-stream modal or a delete-stream confirmation dialog. */
export function StreamForm({ mode, groupName, initialName, onClose }: {
  mode: 'create' | 'delete';
  groupName: string;
  initialName?: string;
  onClose: (created?: StreamSchema) => void;
}) {
  const qc = useQueryClient();

  const [name, setName] = useState('');
  const [families, setFamilies] = useState<FamilyRow[]>([
    { name: 'default', tags: [{ name: '', type: 'TAG_TYPE_STRING' }] },
  ]);
  const [entityTags, setEntityTags] = useState<string[]>([]);
  const [error, setError] = useState('');

  const createMut = useMutation({
    mutationFn: (req: CreateStreamRequest) => apiDataSource.createStream(req),
    onSuccess: (stream) => {
      qc.invalidateQueries({ queryKey: ['resources', 'streams', groupName] });
      onClose(stream);
    },
    onError: (e: Error) => setError(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: () => apiDataSource.deleteResource('streams', groupName, initialName!),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['resources', 'streams', groupName] });
      onClose();
    },
    onError: (e: Error) => setError(e.message),
  });

  const allTagNames = families.flatMap(f => f.tags.map(t => t.name)).filter(Boolean);
  const availTags = allTagNames.filter(n => !entityTags.includes(n));

  function addFamily() {
    setFamilies(prev => [...prev, { name: '', tags: [{ name: '', type: 'TAG_TYPE_STRING' }] }]);
  }

  function removeFamily(famIdx: number) {
    setFamilies(prev => prev.filter((_, i) => i !== famIdx));
  }

  function updateFamilyName(famIdx: number, value: string) {
    setFamilies(prev => prev.map((fam, i) => i === famIdx ? { ...fam, name: value } : fam));
  }

  function addTag(famIdx: number) {
    setFamilies(prev => prev.map((fam, i) =>
      i === famIdx ? { ...fam, tags: [...fam.tags, { name: '', type: 'TAG_TYPE_STRING' }] } : fam,
    ));
  }

  function removeTag(famIdx: number, tagIdx: number) {
    setFamilies(prev => prev.map((fam, i) =>
      i === famIdx ? { ...fam, tags: fam.tags.filter((_, j) => j !== tagIdx) } : fam,
    ));
  }

  function updateTagName(famIdx: number, tagIdx: number, value: string) {
    setFamilies(prev => prev.map((fam, i) =>
      i === famIdx
        ? { ...fam, tags: fam.tags.map((tag, j) => j === tagIdx ? { ...tag, name: value } : tag) }
        : fam,
    ));
  }

  function updateTagType(famIdx: number, tagIdx: number, value: string) {
    setFamilies(prev => prev.map((fam, i) =>
      i === famIdx
        ? { ...fam, tags: fam.tags.map((tag, j) => j === tagIdx ? { ...tag, type: value } : tag) }
        : fam,
    ));
  }

  function toggleEntityTag(tagName: string) {
    setEntityTags(prev =>
      prev.includes(tagName) ? prev.filter(t => t !== tagName) : [...prev, tagName],
    );
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim()) {
      setError('Name is required.');
      return;
    }
    const validFamilies = families.filter(f => f.name.trim() && f.tags.some(t => t.name.trim()));
    if (validFamilies.length === 0) {
      setError('At least one tag family with a name and one tag is required.');
      return;
    }
    for (const fam of families) {
      for (const tag of fam.tags) {
        if (!tag.name.trim()) {
          setError('All tags must have a name.');
          return;
        }
      }
    }
    if (entityTags.length === 0) {
      setError('At least one entity tag is required.');
      return;
    }
    setError('');
    createMut.mutate({
      stream: {
        metadata: { name: name.trim(), group: groupName },
        tagFamilies: families.map(f => ({
          name: f.name,
          tags: f.tags.map(t => ({ name: t.name, type: t.type as TagType })),
        })),
        entity: { tagNames: entityTags },
      },
    });
  }

  const isPending = createMut.isPending || deleteMut.isPending;

  if (mode === 'delete') {
    return (
      <div className="modal-overlay" onClick={() => onClose()}>
        <div className="modal is-danger" onClick={(e) => e.stopPropagation()}>
          <div className="modal-head">
            <span className="modal-title">Delete stream</span>
            <button className="modal-x" onClick={() => onClose()} />
          </div>
          <div className="modal-body">
            <p>
              This will permanently delete stream{' '}
              <span className="mono">{initialName}</span>.
            </p>
            {error && <div className="f-error">{error}</div>}
          </div>
          <div className="modal-foot">
            <button className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>
              Cancel
            </button>
            <button
              className="btn btn-danger"
              onClick={() => deleteMut.mutate()}
              disabled={isPending}
            >
              {isPending ? 'Deleting…' : 'Delete'}
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="modal-overlay" onClick={() => onClose()}>
      <form className="modal is-wide" onSubmit={handleSubmit} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <span className="modal-title">New stream</span>
          <button type="button" className="modal-x" onClick={() => onClose()} />
        </div>

        <div className="modal-body">
          {/* Basic info */}
          <div className="f-section">
            <label className="f-field">
              <span className="f-label">
                Name <span className="f-req">*</span>
              </span>
              <input
                className="f-input mono"
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                autoFocus
              />
            </label>
          </div>

          {/* Tag families */}
          <div className="f-section">
            <span className="f-section-title">
              Tag families <span className="f-req">*</span>
            </span>
            <div className="fam-list">
              {families.map((fam, famIdx) => (
                <div className="fam-card" key={famIdx}>
                  <div className="fam-head">
                    <input
                      className="f-input fam-name"
                      type="text"
                      placeholder="Family name"
                      value={fam.name}
                      onChange={(e) => updateFamilyName(famIdx, e.target.value)}
                    />
                    {families.length > 1 && (
                      <button
                        type="button"
                        className="btn fam-del"
                        onClick={() => removeFamily(famIdx)}
                      >
                        Remove
                      </button>
                    )}
                  </div>
                  {fam.tags.map((tag, tagIdx) => (
                    <div className="spec-row" key={tagIdx}>
                      <div className="spec-cell">
                        <input
                          className="f-input"
                          type="text"
                          placeholder="Tag name"
                          value={tag.name}
                          onChange={(e) => updateTagName(famIdx, tagIdx, e.target.value)}
                        />
                      </div>
                      <div className="spec-cell">
                        <div className="f-select-wrap">
                          <select
                            className="f-select"
                            value={tag.type}
                            onChange={(e) => updateTagType(famIdx, tagIdx, e.target.value)}
                          >
                            {TAG_TYPES.map(tt => (
                              <option key={tt} value={tt}>{tt}</option>
                            ))}
                          </select>
                          <span className="f-select-chev">
                            <IconChevron />
                          </span>
                        </div>
                      </div>
                      <button
                        type="button"
                        className="btn"
                        onClick={() => removeTag(famIdx, tagIdx)}
                      >
                        ×
                      </button>
                    </div>
                  ))}
                  <button type="button" className="btn btn-ghost" onClick={() => addTag(famIdx)}>
                    Add tag
                  </button>
                </div>
              ))}
            </div>
            <button type="button" className="btn btn-ghost" onClick={addFamily}>
              Add family
            </button>
          </div>

          {/* Entity */}
          <div className="f-section">
            <span className="f-section-title">Entity</span>
            <span className="f-section-desc">Select tag names as entity identifiers</span>
            <div className="picker">
              {allTagNames.length === 0 ? (
                <span className="picker-empty">No tags defined yet</span>
              ) : (
                <>
                  <div className="picker-selected">
                    {entityTags.map((tagName, ord) => (
                      <button
                        key={tagName}
                        type="button"
                        className="picker-chip is-on"
                        onClick={() => toggleEntityTag(tagName)}
                      >
                        <span className="picker-ord">{ord + 1}</span>
                        {tagName}
                      </button>
                    ))}
                  </div>
                  <div className="picker-avail">
                    {availTags.map(tagName => (
                      <button
                        key={tagName}
                        type="button"
                        className="picker-chip"
                        onClick={() => toggleEntityTag(tagName)}
                      >
                        {tagName}
                      </button>
                    ))}
                  </div>
                </>
              )}
            </div>
          </div>

          {error && <div className="f-error">{error}</div>}
        </div>

        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>
            Cancel
          </button>
          <button type="submit" className="btn btn-primary" disabled={isPending}>
            {isPending ? 'Creating…' : 'Create'}
          </button>
        </div>
      </form>
    </div>
  );
}
