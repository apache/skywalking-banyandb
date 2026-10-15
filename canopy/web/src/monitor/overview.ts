/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership. The ASF licenses this file to You under
 * the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

import type { MonitorOverview } from 'canopy-shared';
import { canopyPath } from '../runtime-config.js';

export class MonitorAccessError extends Error {}

export function refreshInterval(value: number | undefined): number {
  return typeof value === 'number' && Number.isFinite(value)
    ? Math.min(300_000, Math.max(5_000, value)) : 10_000;
}

export async function fetchOverview(signal: AbortSignal): Promise<MonitorOverview> {
  const response = await fetch(canopyPath('/api/monitor/overview'), { signal });
  if (response.status === 401) throw new MonitorAccessError('Your session has expired. Sign in again to view monitoring.');
  if (response.status === 403) throw new MonitorAccessError('Monitoring access is restricted. Ask your administrator for access.');
  if (!response.ok) throw new Error(`Monitoring request failed (HTTP ${response.status}).`);
  return response.json() as Promise<MonitorOverview>;
}

export function formatBytes(value: number | null): string {
  if (value === null || !Number.isFinite(value) || value < 0) return 'Unavailable';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  const exponent = value === 0 ? 0 : Math.max(0, Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1));
  return `${(value / 1024 ** exponent).toLocaleString(undefined, { maximumFractionDigits: 2 })} ${units[exponent]}`;
}

export function diskUsage(used: number | null, total: number | null): string {
  return used !== null && total !== null && Number.isFinite(used) && Number.isFinite(total)
    && used >= 0 && total > 0 && used <= total ? `${(used / total * 100).toFixed(1)}%` : 'Unavailable';
}

export function formatUptime(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return 'Unavailable';
  let remaining = Math.floor(seconds);
  const parts: string[] = [];
  for (const [unit, size] of [['d', 86_400], ['h', 3_600], ['m', 60], ['s', 1]] as const) {
    const value = Math.floor(remaining / size);
    remaining %= size;
    if (value > 0) parts.push(`${value}${unit}`);
  }
  return parts.join(' ') || '0s';
}
