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

import { lookup } from 'node:dns/promises';

// Addresses that are always denied (link-local, cloud-metadata)
const DENY_EXACT = new Set([
  '169.254.169.254',
  '::ffff:169.254.169.254',
  'fd00:ec2::254', // AWS IPv6 metadata
]);

const DENY_HOSTNAMES = new Set([
  'metadata.google.internal',
  'metadata.google',
]);

// RFC-1918 ranges: 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
function isRfc1918(ip: string): boolean {
  const parts = ip.split('.').map(Number);
  if (parts.length !== 4) return false;
  const [a, b] = parts;
  return a === 10 ||
    (a === 172 && b >= 16 && b <= 31) ||
    (a === 192 && b === 168);
}

function isLinkLocal(ip: string): boolean {
  if (DENY_EXACT.has(ip)) return true;
  // 169.254.x.x
  const parts = ip.split('.');
  if (parts.length === 4 && parts[0] === '169' && parts[1] === '254') return true;
  // fe80::/10 IPv6 link-local
  if (ip.toLowerCase().startsWith('fe80:')) return true;
  return false;
}

export class SsrfError extends Error {
  readonly statusCode = 403;
  constructor(message: string) {
    super(message);
    this.name = 'SsrfError';
  }
}

export async function checkEndpointSafe(url: string, blockRfc1918 = false): Promise<void> {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new SsrfError(`Invalid endpoint URL: ${url}`);
  }

  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new SsrfError(`Endpoint must use http or https protocol, got: ${parsed.protocol}`);
  }

  const hostname = parsed.hostname;

  if (DENY_HOSTNAMES.has(hostname.toLowerCase())) {
    throw new SsrfError(`Endpoint hostname is denied: ${hostname}`);
  }

  // Resolve DNS per-request (NOT cached/pinned) to defeat DNS rebinding.
  // If DNS resolution fails and blockRfc1918 is false, allow the request
  // (an unresolvable host cannot be a link-local/metadata address).
  // If blockRfc1918 is true, strict mode: block on resolution failure.
  let resolvedIp: string | undefined;
  try {
    const result = await lookup(hostname);
    resolvedIp = result.address;
  } catch {
    if (blockRfc1918) {
      throw new SsrfError(`Cannot resolve endpoint hostname: ${hostname}`);
    }
    // Non-strict mode: allow unresolvable hostnames (will fail at connect time).
    return;
  }

  if (isLinkLocal(resolvedIp)) {
    throw new SsrfError(`Endpoint resolves to a denied address (link-local/metadata): ${resolvedIp}`);
  }

  if (blockRfc1918 && isRfc1918(resolvedIp)) {
    throw new SsrfError(`Endpoint resolves to a private network address: ${resolvedIp}`);
  }
}
