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

import type { MonitorSnapshot } from 'canopy-shared';
import { canopyPath } from '../runtime-config.js';
import { SESSION_EXPIRED_EVENT } from '../auth/session-events.js';

// Keeps the status, unlike apiFetch: the Monitor page shows a 403 and an unreachable endpoint differently.
export class MonitorError extends Error {
  constructor(message: string, readonly status: number, readonly path?: string) {
    super(message);
  }
}

export async function fetchMonitorSnapshot(): Promise<MonitorSnapshot> {
  const res = await fetch(canopyPath('/monitor/snapshot'));
  if (res.ok) return res.json() as Promise<MonitorSnapshot>;
  let body: { error?: string; message?: string; path?: string } = {};
  try {
    body = await res.json();
  } catch { /* not JSON */ }
  if (res.status === 401 && body.error === 'unauthenticated') {
    window.dispatchEvent(new Event(SESSION_EXPIRED_EVENT));
  }
  throw new MonitorError(body.message ?? `${res.status} ${res.statusText}`, res.status, body.path);
}
