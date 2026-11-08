/**
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. Apache Software
 * Foundation (ASF) licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { BanyanDBClient } from '../client/index.js';
import { httpFetch } from './http.js';

const requestTimeoutMs = 2 * 60 * 1000;

// A fetch that never answers and, like the real one, rejects with the signal's abort reason.
function hangingFetch(_url: string | URL | Request, init?: RequestInit): Promise<Response> {
  return new Promise((_resolve, reject) => {
    init?.signal?.addEventListener('abort', () => reject(init.signal?.reason));
  });
}

describe('httpFetch timeout', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.stubGlobal('fetch', vi.fn(hangingFetch));
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('rejects with a TimeoutError when the request takes too long', async () => {
    const request = httpFetch({
      url: 'http://localhost:17913/api/v1/group/schema/lists',
      method: 'GET',
      json: undefined,
    });
    const assertion = expect(request).rejects.toMatchObject({ name: 'TimeoutError' });
    await vi.advanceTimersByTimeAsync(requestTimeoutMs);
    await assertion;
    await expect(request).rejects.toBeInstanceOf(Error);
  });

  it('reports a query timeout from the client', async () => {
    const client = new BanyanDBClient('localhost:17900');
    const request = client.query("SELECT * FROM STREAM sw IN default TIME > '-15m'");
    const assertion = expect(request).rejects.toThrow(/^Query timeout\./);
    await vi.advanceTimersByTimeAsync(requestTimeoutMs);
    await assertion;
  });

  it('reports a list groups timeout from the client', async () => {
    const client = new BanyanDBClient('localhost:17900');
    const request = client.listGroups();
    const assertion = expect(request).rejects.toThrow(/^List groups timeout\./);
    await vi.advanceTimersByTimeAsync(requestTimeoutMs);
    await assertion;
  });
});
