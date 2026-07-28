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

// PropertyDetailPage.tsx — the `property-documents` screen (NEW for MP; see
// docs/property-design.md §2/§3): breadcrumb, primary-key banner, New
// document, the embedded PropertyQuery (builder + code) console, and DocList
// results. Document CRUD (Apply/Edit/Delete) + collection delete are wired
// here via PropertyForms.tsx's modals.

import React, { useState } from 'react';
import { useNavigate } from 'react-router';
import { useQuery } from '@tanstack/react-query';

import type { PropertyDocument } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import { IconProperties, IconPlus, IconTrash, IconKey } from '../components/icons.js';
import { PropertyQuery, type PropertyQueryResult } from '../query/PropertyQuery.js';
import { DocList } from '../query/results/DocList.js';
import {
  PropertyEntryModal, DeletePropertyEntryModal, DeletePropertyCollectionModal,
} from '../query/PropertyForms.js';

type ModalState =
  | { readonly kind: 'entry-create' }
  | { readonly kind: 'entry-edit'; readonly entry: PropertyDocument }
  | { readonly kind: 'entry-delete'; readonly entry: PropertyDocument }
  | { readonly kind: 'collection-delete' }
  | null;

export function PropertyDetailPage({ groupName, propName }: { readonly groupName: string; readonly propName: string }) {
  const navigate = useNavigate();
  const { session } = useAuth();
  const isAdmin = session?.role === 'admin';
  const [modal, setModal] = useState<ModalState>(null);
  const [result, setResult] = useState<PropertyQueryResult>({ documents: [], error: null });
  const [refreshToken, setRefreshToken] = useState(0);

  // Confirm the collection exists (surfaces a friendly empty state on a typo'd URL).
  const { data: schemaList, isLoading } = useQuery({
    queryKey: ['resources', 'properties', groupName],
    queryFn: () => apiDataSource.listResourcesInGroup('properties', groupName),
  });
  const exists = (schemaList ?? []).some((r) => r.metadata.name === propName);

  // Schema-free — the tag list offered in the query builder / tag editor is
  // the union of tag keys observed across the last-fetched documents (mirrors
  // the handoff's propertyTagKeys(), which unioned r.entries instead of a
  // live query result).
  const tagKeys = React.useMemo(() => {
    const set: string[] = [];
    for (const doc of result.documents) {
      for (const t of doc.tags) if (!set.includes(t.key)) set.push(t.key);
    }
    return set.sort();
  }, [result.documents]);

  const afterMutate = () => setRefreshToken((n) => n + 1);

  if (!isLoading && !exists) {
    return (
      <div className="page-body">
        <header className="page-head">
          <div className="crumbs">
            <button className="crumb crumb-link" onClick={() => navigate('/properties')}>Properties</button>
            <span className="crumb-sep">/</span>
            <button className="crumb crumb-link" onClick={() => navigate(`/properties/${groupName}`)}>{groupName}</button>
            <span className="crumb-sep">/</span>
            <span className="crumb is-last">{propName}</span>
          </div>
          <h1 className="page-title">{propName}</h1>
        </header>
        <div className="empty">
          <span className="empty-ico"><IconProperties size={36} /></span>
          <div className="empty-title">Property not found</div>
          <p className="empty-text">No property collection named {propName} exists in group {groupName}.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="page-body">
      <header className="page-head">
        <div className="crumbs">
          <button className="crumb crumb-link" onClick={() => navigate('/properties')}>Properties</button>
          <span className="crumb-sep">/</span>
          <button className="crumb crumb-link" onClick={() => navigate(`/properties/${groupName}`)}>{groupName}</button>
          <span className="crumb-sep">/</span>
          <span className="crumb is-last">{propName}</span>
        </div>
        <div className="page-title-row">
          <h1 className="page-title">{propName}</h1>
          <div className="page-actions">
            <button type="button" className="btn btn-ghost" onClick={() => navigate(`/properties/${groupName}`)}>Back</button>
            {isAdmin && (
              <>
                <button type="button" className="btn btn-primary" onClick={() => setModal({ kind: 'entry-create' })}>
                  <IconPlus size={16} /> New document
                </button>
                <button type="button" className="btn btn-danger-ghost" onClick={() => setModal({ kind: 'collection-delete' })}>
                  <IconTrash size={15} /> Delete property
                </button>
              </>
            )}
          </div>
        </div>
        <p className="page-meta">Property collection — a schema-free set of documents, each keyed by id.</p>
      </header>

      <div className="key-banner">
        <IconKey size={15} />
        <span className="key-banner-text">
          Documents are keyed <b className="mono key-id">{groupName}</b><span className="key-sep">/</span><b className="mono key-id">{propName}</b><span className="key-sep">/</span><b className="mono key-id">&lt;id&gt;</b>
        </span>
        <span className="key-banner-note">immutable identity</span>
      </div>

      <PropertyQuery
        groupName={groupName}
        propName={propName}
        tags={tagKeys}
        onResult={setResult}
        refreshToken={refreshToken}
      />

      {result.error ? (
        <div className="qb-error" role="alert">{result.error}</div>
      ) : (
        <DocList
          rows={result.documents}
          groupName={groupName}
          propName={propName}
          onEditEntry={(entry) => setModal({ kind: 'entry-edit', entry })}
          onDeleteEntry={(entry) => setModal({ kind: 'entry-delete', entry })}
        />
      )}

      {modal?.kind === 'entry-create' && (
        <PropertyEntryModal
          mode="create"
          groupName={groupName}
          propName={propName}
          existingIds={new Set(result.documents.map((d) => d.id.toLowerCase()))}
          onClose={() => setModal(null)}
          onApplied={afterMutate}
        />
      )}
      {modal?.kind === 'entry-edit' && (
        <PropertyEntryModal
          mode="edit"
          groupName={groupName}
          propName={propName}
          entry={modal.entry}
          onClose={() => setModal(null)}
          onApplied={afterMutate}
        />
      )}
      {modal?.kind === 'entry-delete' && (
        <DeletePropertyEntryModal
          groupName={groupName}
          propName={propName}
          entry={modal.entry}
          onClose={() => setModal(null)}
          onDeleted={afterMutate}
        />
      )}
      {modal?.kind === 'collection-delete' && (
        <DeletePropertyCollectionModal
          groupName={groupName}
          propName={propName}
          onClose={() => setModal(null)}
          onDeleted={() => navigate(`/properties/${groupName}`)}
        />
      )}
    </div>
  );
}
