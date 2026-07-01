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

import { describe, it, expect, vi } from 'vitest';
import { checkEndpointSafe, SsrfError } from '../src/lib/ssrf.js';

vi.mock('node:dns/promises', () => ({
  lookup: vi.fn(async (hostname: string) => {
    const map: Record<string, string> = {
      '169.254.169.254': '169.254.169.254',
      'metadata.google.internal': '169.254.169.254',
      'safe.host': '93.184.216.34',
      'rfc1918.host': '192.168.1.1',
      'localhost': '127.0.0.1',
    };
    const addr = map[hostname] || hostname;
    return { address: addr, family: 4 };
  }),
}));

describe('SSRF deny-narrow', () => {
  it('rejects 169.254.169.254 (link-local metadata)', async () => {
    await expect(checkEndpointSafe('http://169.254.169.254/latest/meta-data')).rejects.toBeInstanceOf(SsrfError);
  });

  it('rejects metadata.google.internal', async () => {
    await expect(checkEndpointSafe('http://metadata.google.internal')).rejects.toBeInstanceOf(SsrfError);
  });

  it('rejects non-http(s) scheme', async () => {
    await expect(checkEndpointSafe('ftp://safe.host')).rejects.toBeInstanceOf(SsrfError);
  });

  it('allows safe public host', async () => {
    await expect(checkEndpointSafe('http://safe.host:17913')).resolves.toBeUndefined();
  });

  it('allows localhost when RFC-1918 deny is OFF (default)', async () => {
    await expect(checkEndpointSafe('http://localhost:17913', false)).resolves.toBeUndefined();
  });

  it('rejects RFC-1918 address when blockRfc1918=true', async () => {
    await expect(checkEndpointSafe('http://rfc1918.host', true)).rejects.toBeInstanceOf(SsrfError);
  });
});
