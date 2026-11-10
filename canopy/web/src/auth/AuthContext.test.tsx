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
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { AuthProvider, useAuth } from './AuthContext.js';
import { apiDataSource } from '../data/api.js';

const session = { user: 'admin', role: 'admin', banyanVersion: '0.11' };
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status });

function Probe() {
  const { session: current, loading } = useAuth();
  return <><span>{loading ? 'Loading' : current?.user ?? 'Signed out'}</span>
    <button onClick={() => { void apiDataSource.listGroups().catch(() => undefined); }}>Load groups</button></>;
}

function mount() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  client.setQueryData(['groups'], { groups: [{ name: 'cached-private-group' }] });
  render(<QueryClientProvider client={client}><AuthProvider><Probe /></AuthProvider></QueryClientProvider>);
  return client;
}

afterEach(() => vi.unstubAllGlobals());

it('invalidates the UI session and cached data when the API reports unauthenticated', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(json(session))
    .mockResolvedValue(json({ error: 'unauthenticated', message: 'Login required' }, 401)));
  const client = mount();
  await screen.findByText('admin');
  await act(async () => { fireEvent.click(screen.getByRole('button', { name: 'Load groups' })); });
  await screen.findByText('Signed out');
  expect(client.getQueryData(['groups'])).toBeUndefined();
});

it.each([
  [401, 'upstream_auth_error'],
  [403, 'forbidden'],
  [502, 'upstream_unavailable'],
])('keeps the session for HTTP %s %s', async (status, error) => {
  const fetchMock = vi.fn().mockResolvedValueOnce(json(session)).mockResolvedValue(json({ error, message: error }, status));
  vi.stubGlobal('fetch', fetchMock);
  const client = mount();
  await screen.findByText('admin');
  await act(async () => { fireEvent.click(screen.getByRole('button', { name: 'Load groups' })); });
  await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
  expect(screen.getByText('admin')).toBeInTheDocument();
  expect(client.getQueryData(['groups'])).toBeDefined();
});

it('does not restore an expired session from an older session probe', async () => {
  let finishProbe!: (response: Response) => void;
  const pending = new Promise<Response>(resolve => { finishProbe = resolve; });
  vi.stubGlobal('fetch', vi.fn().mockReturnValueOnce(pending)
    .mockResolvedValue(json({ error: 'unauthenticated', message: 'Login required' }, 401)));
  mount();
  await act(async () => { fireEvent.click(screen.getByRole('button', { name: 'Load groups' })); });
  await screen.findByText('Signed out');
  await act(async () => { finishProbe(json(session)); });
  expect(screen.getByText('Signed out')).toBeInTheDocument();
});
