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
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { GroupForm } from './GroupForm.js';

describe('GroupForm — pipeline wire round-trip', () => {
  const pipeline = {
    enabled: true,
    enabledEvents: ['PIPELINE_EVENT_MERGE', 'PIPELINE_EVENT_FINALIZE'],
    mergeGrace: '7200s',
    finalizeGrace: '300s',
    schemaNames: ['traces'],
    schemaNameRegex: 'traces.*',
    plugins: [{ name: 'hot-sampler', sampler: {
      path: 'sampling.so', symbol: 'NewSampler', abiVersion: 1,
      config: { ratio: 0.25, nested: { enabled: true } },
    } }],
    stages: [{ stage: 'warm', plugins: [{ name: 'warm-sampler', sampler: {
      path: 'warm.so', abiVersion: 1, config: { ratio: 0.1 },
    } }] }],
  };

  afterEach(() => vi.unstubAllGlobals());

  it.each([true, false])('preserves pipeline presence (%s) through GET and PUT', async (hasPipeline) => {
    let submitted: { group: Record<string, unknown> } | undefined;
    const group = {
      metadata: { name: 'traces' },
      catalog: 'CATALOG_TRACE',
      resourceOpts: {
        shardNum: 2,
        segmentInterval: { num: 1, unit: 'UNIT_DAY' },
        ttl: { num: 7, unit: 'UNIT_DAY' },
      },
      ...(hasPipeline ? { pipeline } : {}),
    };
    const fetchMock = vi.fn(async (url: string, init?: RequestInit) => {
      if (init?.method === 'PUT') {
        expect(url).toBe('/api/v1/group/schema/traces');
        submitted = JSON.parse(String(init.body));
        return new Response(JSON.stringify({ modRevision: '12' }), { status: 200 });
      }
      expect(url).toBe('/api/v1/group/schema/lists');
      return new Response(JSON.stringify({ group: [group] }), { status: 200 });
    });
    vi.stubGlobal('fetch', fetchMock);
    const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const user = userEvent.setup();
    const onClose = vi.fn();
    render(<QueryClientProvider client={client}>
      <GroupForm mode="edit" initialName="traces" onClose={onClose} />
    </QueryClientProvider>);
    await waitFor(() => expect(screen.getByRole('dialog')).toHaveAttribute('data-initialized', 'true'));
    const ttl = screen.getByRole('spinbutton', { name: 'TTL value' });
    await user.clear(ttl);
    await user.type(ttl, '14');
    await user.click(screen.getByRole('button', { name: 'Save changes' }));
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
    expect(submitted?.group.resourceOpts).toMatchObject({ ttl: { num: 14, unit: 'UNIT_DAY' } });
    if (hasPipeline) expect(submitted?.group.pipeline).toEqual(pipeline);
    else expect(submitted?.group).not.toHaveProperty('pipeline');
  });
});
