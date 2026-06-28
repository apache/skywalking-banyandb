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
import { BrowserRouter, Routes, Route, useParams, useNavigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { AuthProvider, useAuth } from './auth/AuthContext.js';
import { LoginPage } from './pages/LoginPage.js';
import { Shell } from './components/Shell.js';
import { HomePage } from './pages/HomePage.js';
import { TypeOverviewPage } from './pages/TypeOverviewPage.js';
import { GroupPage } from './pages/GroupPage.js';
import { ResourceDetailPage } from './pages/ResourceDetailPage.js';
import { GroupForm } from './components/GroupForm.js';
import { MeasureForm } from './components/MeasureForm.js';
import { StreamForm } from './components/StreamForm.js';
import { TraceForm } from './components/TraceForm.js';
import { IndexRuleForm } from './components/IndexRuleForm.js';
import { IndexRuleBindingForm } from './components/IndexRuleBindingForm.js';
import { IndexPage } from './pages/IndexPage.js';
import { apiDataSource } from './data/api.js';
import { useQuery } from '@tanstack/react-query';
import type {
  Group, MeasureSchema, StreamSchema, TraceSchema,
  IndexRuleSchema, IndexRuleBindingSchema,
} from 'canopy-shared';

const queryClient = new QueryClient({
  defaultOptions: { queries: { staleTime: 30_000, retry: 1 } },
});

type ModalState =
  | { kind: 'group-create' }
  | { kind: 'group-edit'; groupName: string }
  | { kind: 'group-delete'; groupName: string }
  | { kind: 'measure-create'; groupName: string }
  | { kind: 'measure-edit'; groupName: string; resourceName: string }
  | { kind: 'measure-delete'; groupName: string; resourceName: string }
  | { kind: 'stream-create'; groupName: string }
  | { kind: 'stream-edit'; groupName: string; resourceName: string }
  | { kind: 'stream-delete'; groupName: string; resourceName: string }
  | { kind: 'trace-create'; groupName: string }
  | { kind: 'trace-edit'; groupName: string; resourceName: string }
  | { kind: 'trace-delete'; groupName: string; resourceName: string }
  | { kind: 'indexrule-create'; groupName: string; catalog: string }
  | { kind: 'indexrule-edit'; groupName: string; ruleName: string; catalog: string }
  | { kind: 'indexrule-delete'; groupName: string; ruleName: string; catalog: string }
  | { kind: 'indexrulebinding-create'; groupName: string; catalog: string; presetRuleName?: string }
  | { kind: 'indexrulebinding-edit'; groupName: string; bindingName: string; catalog: string }
  | { kind: 'indexrulebinding-delete'; groupName: string; bindingName: string; catalog: string }
  | null;

const TYPE_CATALOG: Record<string, 'CATALOG_MEASURE' | 'CATALOG_STREAM' | 'CATALOG_TRACE' | 'CATALOG_PROPERTY'> = {
  measures: 'CATALOG_MEASURE',
  streams: 'CATALOG_STREAM',
  traces: 'CATALOG_TRACE',
  properties: 'CATALOG_PROPERTY',
};

function MetadataTypeRoute() {
  const { type = 'measures' } = useParams<{ type: string }>();
  const navigate = useNavigate();
  const [modal, setModal] = useState<ModalState>(null);

  return (
    <>
      <TypeOverviewPage
        type={type}
        onNewGroup={() => setModal({ kind: 'group-create' })}
      />
      {modal?.kind === 'group-create' && (
        <GroupForm
          mode="create"
          initialCatalog={TYPE_CATALOG[type]}
          onClose={(created?: Group) => {
            setModal(null);
            if (created) navigate(`/metadata/${type}/${created.name}`);
          }}
        />
      )}
    </>
  );
}

function MetadataGroupRoute() {
  const { type = 'measures', group = '' } = useParams<{ type: string; group: string }>();
  const navigate = useNavigate();
  const [modal, setModal] = useState<ModalState>(null);

  return (
    <>
      <GroupPage
        type={type}
        groupName={group}
        onNewResource={() => {
          if (type === 'measures') setModal({ kind: 'measure-create', groupName: group });
          else if (type === 'streams') setModal({ kind: 'stream-create', groupName: group });
          else if (type === 'traces') setModal({ kind: 'trace-create', groupName: group });
        }}
        onEditResource={(r) => {
          if (type === 'measures') setModal({ kind: 'measure-edit', groupName: group, resourceName: r.metadata.name });
          else if (type === 'streams') setModal({ kind: 'stream-edit', groupName: group, resourceName: r.metadata.name });
          else if (type === 'traces') setModal({ kind: 'trace-edit', groupName: group, resourceName: r.metadata.name });
        }}
        onDeleteResource={(r) => {
          if (type === 'measures') setModal({ kind: 'measure-delete', groupName: group, resourceName: r.metadata.name });
          else if (type === 'streams') setModal({ kind: 'stream-delete', groupName: group, resourceName: r.metadata.name });
          else if (type === 'traces') setModal({ kind: 'trace-delete', groupName: group, resourceName: r.metadata.name });
        }}
        onEditGroup={() => setModal({ kind: 'group-edit', groupName: group })}
        onDeleteGroup={() => setModal({ kind: 'group-delete', groupName: group })}
      />
      {modal?.kind === 'group-edit' && (
        <GroupForm
          mode="edit"
          initialName={modal.groupName}
          onClose={() => setModal(null)}
        />
      )}
      {modal?.kind === 'group-delete' && (
        <GroupForm
          mode="delete"
          initialName={modal.groupName}
          onClose={() => {
            setModal(null);
          }}
          onDeleted={() => navigate(`/metadata/${type}`)}
        />
      )}
      {modal?.kind === 'measure-create' && (
        <MeasureForm
          mode="create"
          groupName={modal.groupName}
          onClose={(created?: MeasureSchema) => {
            setModal(null);
            if (created) navigate(`/metadata/${type}/${group}/${created.metadata.name}`);
          }}
        />
      )}
      {modal?.kind === 'stream-create' && (
        <StreamForm
          mode="create"
          groupName={modal.groupName}
          onClose={(created?: StreamSchema) => {
            setModal(null);
            if (created) navigate(`/metadata/${type}/${group}/${created.metadata.name}`);
          }}
        />
      )}
      {modal?.kind === 'trace-create' && (
        <TraceForm
          mode="create"
          groupName={modal.groupName}
          onClose={(created?: TraceSchema) => {
            setModal(null);
            if (created) navigate(`/metadata/${type}/${group}/${created.metadata.name}`);
          }}
        />
      )}
      {modal?.kind === 'measure-edit' && (
        <MeasureForm
          mode="edit"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => setModal(null)}
        />
      )}
      {modal?.kind === 'measure-delete' && (
        <MeasureForm
          mode="delete"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => setModal(null)}
          onDeleted={() => {
            setModal(null);
            // The list is refetched by React Query automatically; the
            // current row vanishes. No navigation needed since the user
            // is staying on the group page (where the pager still works).
          }}
        />
      )}
      {modal?.kind === 'stream-edit' && (
        <StreamForm
          mode="edit"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => setModal(null)}
        />
      )}
      {modal?.kind === 'stream-delete' && (
        <StreamForm
          mode="delete"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => setModal(null)}
          onDeleted={() => setModal(null)}
        />
      )}
      {modal?.kind === 'trace-edit' && (
        <TraceForm
          mode="edit"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => setModal(null)}
        />
      )}
      {modal?.kind === 'trace-delete' && (
        <TraceForm
          mode="delete"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => setModal(null)}
          onDeleted={() => setModal(null)}
        />
      )}
    </>
  );
}

function MetadataIndexRoute() {
  const { type = 'measures', group = '' } = useParams<{ type: string; group: string }>();
  const navigate = useNavigate();
  const [modal, setModal] = useState<ModalState>(null);

  // Determine whether the group exists so the page can render a friendly
  // empty state instead of crashing on a typo'd URL.
  const { data: groupsData } = useQuery({
    queryKey: ['groups'],
    queryFn: () => apiDataSource.listGroups(),
  });
  const allGroups = groupsData?.groups ?? [];
  const catalogForType: Record<string, string> = {
    measures: 'CATALOG_MEASURE',
    streams:  'CATALOG_STREAM',
    traces:   'CATALOG_TRACE',
  };
  const targetCatalog = catalogForType[type];
  const groupExists = allGroups.some((g) => g.name === group && g.catalog === targetCatalog);

  // Lookup the existing rule/binding names so the forms can flag duplicates
  // before submission.
  const { data: existingRules = [] } = useQuery({
    queryKey: ['indexRules', group],
    queryFn: () => apiDataSource.listIndexRules(group),
    enabled: groupExists,
  });
  const { data: existingBindings = [] } = useQuery({
    queryKey: ['indexRuleBindings', group],
    queryFn: () => apiDataSource.listIndexRuleBindings(group),
    enabled: groupExists,
  });
  const existingRuleNames = new Set(existingRules.map((r) => r.metadata.name.toLowerCase()));
  const existingBindingNames = new Set(existingBindings.map((b) => b.metadata.name.toLowerCase()));

  return (
    <>
      <IndexPage
        type={type}
        groupName={group}
        groupExists={groupExists}
        onNewIndexRule={() => setModal({ kind: 'indexrule-create', groupName: group, catalog: targetCatalog })}
        onEditIndexRule={(ruleName) => setModal({ kind: 'indexrule-edit', groupName: group, ruleName, catalog: targetCatalog })}
        onDeleteIndexRule={(ruleName) => setModal({ kind: 'indexrule-delete', groupName: group, ruleName, catalog: targetCatalog })}
        onNewIndexRuleBinding={(presetRuleName) =>
          setModal({ kind: 'indexrulebinding-create', groupName: group, catalog: targetCatalog, presetRuleName })}
        onEditIndexRuleBinding={(bindingName) => setModal({ kind: 'indexrulebinding-edit', groupName: group, bindingName, catalog: targetCatalog })}
        onDeleteIndexRuleBinding={(bindingName) => setModal({ kind: 'indexrulebinding-delete', groupName: group, bindingName, catalog: targetCatalog })}
      />
      {modal?.kind === 'indexrule-create' && (
        <IndexRuleForm
          mode="create"
          groupName={modal.groupName}
          existingNames={existingRuleNames}
          onClose={() => setModal(null)}
        />
      )}
      {modal?.kind === 'indexrule-edit' && (
        <IndexRuleForm
          mode="edit"
          groupName={modal.groupName}
          initialName={modal.ruleName}
          onClose={() => setModal(null)}
        />
      )}
      {modal?.kind === 'indexrule-delete' && (
        <IndexRuleForm
          mode="delete"
          groupName={modal.groupName}
          initialName={modal.ruleName}
          onClose={() => setModal(null)}
        />
      )}
      {modal?.kind === 'indexrulebinding-create' && (
        <IndexRuleBindingForm
          mode="create"
          groupName={modal.groupName}
          type={type}
          catalog={targetCatalog}
          presetRuleName={modal.presetRuleName}
          existingNames={existingBindingNames}
          onClose={() => setModal(null)}
        />
      )}
      {modal?.kind === 'indexrulebinding-edit' && (
        <IndexRuleBindingForm
          mode="edit"
          groupName={modal.groupName}
          initialName={modal.bindingName}
          type={type}
          catalog={targetCatalog}
          onClose={() => setModal(null)}
        />
      )}
      {modal?.kind === 'indexrulebinding-delete' && (
        <IndexRuleBindingForm
          mode="delete"
          groupName={modal.groupName}
          initialName={modal.bindingName}
          type={type}
          catalog={targetCatalog}
          onClose={() => setModal(null)}
        />
      )}
    </>
  );
}

function MetadataResourceRoute() {
  const { type = 'measures', group = '', name = '' } = useParams<{ type: string; group: string; name: string }>();
  const navigate = useNavigate();
  const [modal, setModal] = useState<ModalState>(null);

  return (
    <>
      <ResourceDetailPage
        type={type}
        groupName={group}
        resourceName={name}
        onEdit={() => {
          if (type === 'measures') setModal({ kind: 'measure-edit', groupName: group, resourceName: name });
          else if (type === 'streams') setModal({ kind: 'stream-edit', groupName: group, resourceName: name });
          else if (type === 'traces') setModal({ kind: 'trace-edit', groupName: group, resourceName: name });
        }}
        onDelete={() => {
          if (type === 'measures') setModal({ kind: 'measure-delete', groupName: group, resourceName: name });
          else if (type === 'streams') setModal({ kind: 'stream-delete', groupName: group, resourceName: name });
          else if (type === 'traces') setModal({ kind: 'trace-delete', groupName: group, resourceName: name });
        }}
      />
      {modal?.kind === 'measure-edit' && (
        <MeasureForm
          mode="edit"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => setModal(null)}
        />
      )}
      {modal?.kind === 'measure-delete' && (
        <MeasureForm
          mode="delete"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => setModal(null)}
          onDeleted={() => navigate(`/metadata/${type}/${group}`)}
        />
      )}
      {modal?.kind === 'stream-edit' && (
        <StreamForm
          mode="edit"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => setModal(null)}
        />
      )}
      {modal?.kind === 'stream-delete' && (
        <StreamForm
          mode="delete"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => setModal(null)}
          onDeleted={() => navigate(`/metadata/${type}/${group}`)}
        />
      )}
      {modal?.kind === 'trace-edit' && (
        <TraceForm
          mode="edit"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => setModal(null)}
        />
      )}
      {modal?.kind === 'trace-delete' && (
        <TraceForm
          mode="delete"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => setModal(null)}
          onDeleted={() => navigate(`/metadata/${type}/${group}`)}
        />
      )}
    </>
  );
}

function AppContent() {
  const { session, loading } = useAuth();

  if (loading) {
    return (
      <div className="loading-shell">
        <span className="spin" />
      </div>
    );
  }

  if (!session) {
    return <LoginPage />;
  }

  return (
    <Shell>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/metadata/:type/:group/Index" element={<MetadataIndexRoute />} />
        <Route path="/metadata/:type/:group/:name" element={<MetadataResourceRoute />} />
        <Route path="/metadata/:type/:group" element={<MetadataGroupRoute />} />
        <Route path="/metadata/:type" element={<MetadataTypeRoute />} />
        <Route path="/pipelines/*" element={<div className="page-body"><h1 className="page-title">Pipelines</h1><p className="page-meta">Coming soon.</p></div>} />
        <Route path="/query" element={<div className="page-body"><h1 className="page-title">Query</h1><p className="page-meta">Coming in M4.</p></div>} />
        <Route path="*" element={<div className="page-body"><h1 className="page-title">Not found</h1></div>} />
      </Routes>
    </Shell>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AuthProvider>
          <AppContent />
        </AuthProvider>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
