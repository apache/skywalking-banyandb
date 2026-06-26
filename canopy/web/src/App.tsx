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
import React from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { AuthProvider, useAuth } from './auth/AuthContext.js';
import { LoginPage } from './pages/LoginPage.js';
import { Shell } from './components/Shell.js';
import { HomePage } from './pages/HomePage.js';

const queryClient = new QueryClient({
  defaultOptions: { queries: { staleTime: 30_000, retry: 1 } },
});

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
        <Route path="/metadata/*" element={<div className="page-body"><h1 className="page-title">Metadata</h1><p className="page-meta">Coming in M3.</p></div>} />
        <Route path="/properties/*" element={<div className="page-body"><h1 className="page-title">Properties</h1><p className="page-meta">Coming in M3.</p></div>} />
        <Route path="/pipelines/*" element={<div className="page-body"><h1 className="page-title">Pipelines</h1><p className="page-meta">Coming in M3.</p></div>} />
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
