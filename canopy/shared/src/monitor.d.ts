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

export type MonitorResult<T> =
  | { readonly status: 'ok'; readonly data: T }
  | { readonly status: 'error'; readonly message: string };

export interface MonitorNode {
  readonly name: string;
  readonly route: string;
  readonly roles: readonly string[];
  readonly address: string;
  readonly apiVersion?: string;
  readonly fileFormatVersion?: string;
  readonly state: 'active' | 'evictable' | 'registered';
}

export interface MonitorDisk {
  readonly node: string;
  readonly path: string;
  readonly totalBytes: number | null;
  readonly usedBytes: number | null;
}

export interface MonitorMetric {
  readonly name: string;
  readonly labels: Readonly<Record<string, string>>;
  readonly value: number;
}

export interface MonitorUptime {
  readonly node: string;
  readonly seconds: number;
}

export interface MonitorOverview {
  readonly observedAt: string;
  readonly refreshIntervalMs: number;
  readonly experimentalSections: boolean;
  readonly cluster: MonitorResult<{ readonly nodes: readonly MonitorNode[] }>;
  readonly storage: MonitorResult<{
    readonly disks: readonly MonitorDisk[];
    readonly metrics: readonly MonitorMetric[];
    readonly uptimes?: readonly MonitorUptime[];
  }>;
}
