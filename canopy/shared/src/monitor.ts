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

// GET /monitor/snapshot. Ratios are 0..1 and rates are per second; a rate is
// undefined until the BFF has two samples to compute it from.

export type MonitorMode = 'standalone';

export interface MonitorDiskPath {
  readonly path: string;
  readonly usedBytes: number;
  readonly totalBytes: number;
}

export interface MonitorRate {
  readonly perSecond?: number;
  // Average latency over the last sample window; BanyanDB exports latency as a counter, not a histogram.
  readonly avgLatencyMs?: number;
}

export interface MonitorNode {
  readonly id: string;
  readonly role: 'standalone';
  readonly uptimeSeconds?: number;
  readonly cpuCores?: number;
  readonly cpuUsedCores?: number;
  readonly rssBytes?: number;
  readonly memoryUsedRatio?: number;
  readonly goroutines?: number;
  readonly heapInuseBytes?: number;
  readonly disk: readonly MonitorDiskPath[];
  readonly write: MonitorRate;
  readonly query: MonitorRate;
  readonly errorsPerMinute?: number;
  readonly cpuHistory: readonly MonitorPoint[];
}

export interface MonitorPoint {
  readonly t: number;
  readonly v: number;
}

export interface MonitorSnapshot {
  readonly mode: MonitorMode;
  readonly source: string;
  readonly scrapedAt: number;
  readonly nodes: readonly MonitorNode[];
}
