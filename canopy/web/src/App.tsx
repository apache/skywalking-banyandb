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
import type { Group, MeasureSchema, StreamSchema } from 'canopy-shared';

const queryClient = new QueryClient({
  defaultOptions: { queries: { staleTime: 30_000, retry: 1 } },
});

type ModalState =
  | { kind: 'group-create' }
  | { kind: 'group-delete'; groupName: string }
  | { kind: 'measure-create'; groupName: string }
  | { kind: 'measure-delete'; groupName: string; resourceName: string }
  | { kind: 'stream-create'; groupName: string }
  | { kind: 'stream-delete'; groupName: string; resourceName: string }
  | null;

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
          onClose={(created?: Group) => {
            setModal(null);
            if (created) navigate(`/metadata/${type}/${created.name}`);
          }}
        />
      )}
    </>
  );
}

function PropertiesRoute() {
  const navigate = useNavigate();
  const [modal, setModal] = useState<ModalState>(null);

  return (
    <>
      <TypeOverviewPage
        type="properties"
        onNewGroup={() => setModal({ kind: 'group-create' })}
      />
      {modal?.kind === 'group-create' && (
        <GroupForm
          mode="create"
          onClose={(created?: Group) => {
            setModal(null);
            if (created) navigate(`/properties/${created.name}`);
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
        }}
        onEditGroup={() => { /* edit in a future iteration */ }}
        onDeleteGroup={() => setModal({ kind: 'group-delete', groupName: group })}
      />
      {modal?.kind === 'group-delete' && (
        <GroupForm
          mode="delete"
          initialName={modal.groupName}
          onClose={() => {
            setModal(null);
            navigate(`/metadata/${type}`);
          }}
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
    </>
  );
}

function PropertiesGroupRoute() {
  const { group = '' } = useParams<{ group: string }>();
  const navigate = useNavigate();
  const [modal, setModal] = useState<ModalState>(null);

  return (
    <>
      <GroupPage
        type="properties"
        groupName={group}
        onEditGroup={() => { /* future */ }}
        onDeleteGroup={() => setModal({ kind: 'group-delete', groupName: group })}
      />
      {modal?.kind === 'group-delete' && (
        <GroupForm
          mode="delete"
          initialName={modal.groupName}
          onClose={() => {
            setModal(null);
            navigate('/properties');
          }}
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
        onEdit={() => { /* future */ }}
        onDelete={() => {
          if (type === 'measures') setModal({ kind: 'measure-delete', groupName: group, resourceName: name });
          else if (type === 'streams') setModal({ kind: 'stream-delete', groupName: group, resourceName: name });
        }}
      />
      {modal?.kind === 'measure-delete' && (
        <MeasureForm
          mode="delete"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => {
            setModal(null);
            navigate(`/metadata/${type}/${group}`);
          }}
        />
      )}
      {modal?.kind === 'stream-delete' && (
        <StreamForm
          mode="delete"
          groupName={modal.groupName}
          initialName={modal.resourceName}
          onClose={() => {
            setModal(null);
            navigate(`/metadata/${type}/${group}`);
          }}
        />
      )}
    </>
  );
}

function PropertiesResourceRoute() {
  const { group = '', name = '' } = useParams<{ group: string; name: string }>();
  return (
    <ResourceDetailPage
      type="properties"
      groupName={group}
      resourceName={name}
    />
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
        <Route path="/metadata/measures" element={<MetadataTypeRoute />} />
        <Route path="/metadata/streams" element={<MetadataTypeRoute />} />
        <Route path="/metadata/traces" element={<MetadataTypeRoute />} />
        <Route path="/metadata/:type/:group/:name" element={<MetadataResourceRoute />} />
        <Route path="/metadata/:type/:group" element={<MetadataGroupRoute />} />
        <Route path="/metadata/:type" element={<MetadataTypeRoute />} />
        <Route path="/properties" element={<PropertiesRoute />} />
        <Route path="/properties/:group/:name" element={<PropertiesResourceRoute />} />
        <Route path="/properties/:group" element={<PropertiesGroupRoute />} />
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
