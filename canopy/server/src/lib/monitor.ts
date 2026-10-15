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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import type { MonitorDisk, MonitorMetric, MonitorNode, MonitorUptime } from 'canopy-shared/monitor';

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) throw new Error('Invalid cluster response');
  return value as Record<string, unknown>;
}

function strings(value: unknown): string[] {
  if (value === undefined) return [];
  if (!Array.isArray(value) || value.some(item => typeof item !== 'string')) throw new Error('Invalid cluster response');
  return value as string[];
}

export function parseClusterState(value: unknown): { nodes: MonitorNode[] } {
  const response = record(value);
  if (!Object.hasOwn(response, 'routeTables') && Object.keys(response).length !== 0) throw new Error('Invalid cluster response');
  const tables = record(response.routeTables ?? {});
  const nodes: MonitorNode[] = [];
  for (const [route, rawTable] of Object.entries(tables)) {
    const table = record(rawTable);
    const active = new Set(strings(table.active));
    const evictable = new Set(strings(table.evictable));
    const registered = table.registered ?? [];
    if (!Array.isArray(registered)) throw new Error('Invalid cluster response');
    for (const rawNode of registered) {
      const node = record(rawNode);
      const metadata = record(node.metadata);
      const version = node.version == null ? {} : record(node.version);
      if (typeof metadata.name !== 'string' || !metadata.name) throw new Error('Invalid node name');
      if (node.grpcAddress !== undefined && typeof node.grpcAddress !== 'string') throw new Error('Invalid node address');
      nodes.push({
        name: metadata.name,
        route,
        roles: strings(node.roles),
        address: typeof node.grpcAddress === 'string' ? node.grpcAddress : '',
        state: evictable.has(metadata.name) ? 'evictable' : active.has(metadata.name) ? 'active' : 'registered',
        ...(typeof version.apiVersion === 'string' ? { apiVersion: version.apiVersion } : {}),
        ...(typeof version.fileFormatVersion === 'string' ? { fileFormatVersion: version.fileFormatVersion } : {}),
      });
      if (nodes.length > 10000) throw new Error('Too many cluster nodes');
    }
  }
  return { nodes };
}

function parseLabels(input: string): Record<string, string> {
  const labels: Record<string, string> = Object.create(null) as Record<string, string>;
  let rest = input;
  while (rest.trim()) {
    const match = /^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"((?:[^"\\\n]|\\[\\n"])*)"\s*(,|$)/.exec(rest);
    if (!match || Object.hasOwn(labels, match[1])) throw new Error('Invalid metric labels');
    labels[match[1]] = match[2].replace(/\\([\\n"])/g, (_, escaped: string) => escaped === 'n' ? '\n' : escaped);
    rest = rest.slice(match[0].length);
  }
  return labels;
}

function metricNode(labels: Record<string, string>): string {
  return labels.pod_name
    ? [labels.pod_name, labels.container_name].filter(Boolean).join('/')
    : labels.instance || 'Configured monitor target (direct scrape)';
}

export function parseStorageMetrics(text: string): { disks: MonitorDisk[]; metrics: MonitorMetric[]; uptimes: MonitorUptime[] } {
  // Prometheus text exposition ends each sample with a newline. Missing it can indicate truncation.
  if (!text.endsWith('\n')) throw new Error('Incomplete metrics response');
  const metrics: MonitorMetric[] = [];
  const disks = new Map<string, MonitorDisk>();
  const uptimes = new Map<string, MonitorUptime>();
  const seen = new Set<string>();
  let samples = 0;
  for (const line of text.split('\n')) {
    if (!line.trim() || line.trimStart().startsWith('#')) continue;
    const match = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+([+-]?(?:(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?|Inf)|NaN)(?:\s+-?\d+)?\s*$/.exec(line);
    if (!match || ++samples > 10000) throw new Error('Invalid or oversized metrics response');
    const labels = parseLabels(match[2] ?? '');
    const name = match[1];
    const value = Number(match[3]);
    const identity = JSON.stringify([name, Object.entries(labels).sort(([a], [b]) => a.localeCompare(b))]);
    if (seen.has(identity)) throw new Error('Duplicate metric series');
    seen.add(identity);
    if (name === 'banyandb_system_up_time') {
      const node = metricNode(labels);
      if (!Number.isFinite(value) || value < 0 || uptimes.has(node)) throw new Error('Invalid or ambiguous uptime sample');
      uptimes.set(node, { node, seconds: value });
      continue;
    }
    // Keep storage diagnostics bounded and avoid exporting unrelated process metrics.
    if (!/^banyandb_(?:system_disk|(?:measure|stream_tst|trace_tst)_total_(?:mem_part|mem_elements|mem_blocks|mem_part_bytes|mem_part_uncompressed_bytes|file_parts|file_elements|file_blocks|file_part_bytes|file_part_uncompressed_bytes))$/.test(name)) continue;
    if (!Number.isFinite(value)) throw new Error('Non-finite storage sample');
    metrics.push({ name, labels, value });
    if (name !== 'banyandb_system_disk' || !['used', 'total'].includes(labels.kind)) continue;
    if (!labels.path || value < 0) throw new Error('Invalid disk sample');
    const node = metricNode(labels);
    const key = JSON.stringify([node, labels.path]);
    const previous = disks.get(key) ?? { node, path: labels.path, totalBytes: null, usedBytes: null };
    const field = labels.kind === 'total' ? 'totalBytes' : 'usedBytes';
    if (previous[field] !== null) throw new Error('Ambiguous disk series');
    disks.set(key, { ...previous, [field]: value });
  }
  if (samples === 0) throw new Error('No metric samples');
  return { disks: [...disks.values()], metrics, uptimes: [...uptimes.values()] };
}
