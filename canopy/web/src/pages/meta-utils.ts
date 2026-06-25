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

import type React from 'react';
import type { IntervalRule } from 'canopy-shared';

import { IconMeasures, IconStreams, IconTraces, IconProperties } from '../components/icons.js';

export interface CatalogEntry {
  catalog: string;
  label: string;
  singular: string;
  plural: string;
}

export const CATALOG_MAP: Record<string, CatalogEntry> = {
  measures:   { catalog: 'CATALOG_MEASURE',  label: 'MEASURE',  singular: 'measure',  plural: 'measures' },
  streams:    { catalog: 'CATALOG_STREAM',   label: 'STREAM',   singular: 'stream',   plural: 'streams' },
  traces:     { catalog: 'CATALOG_TRACE',    label: 'TRACE',    singular: 'trace',    plural: 'traces' },
  properties: { catalog: 'CATALOG_PROPERTY', label: 'PROPERTY', singular: 'property', plural: 'properties' },
};

export const TYPE_TITLES: Record<string, string> = {
  measures:   'Measures',
  streams:    'Streams',
  traces:     'Traces',
  properties: 'Properties',
};

export const TYPE_ICONS: Record<string, React.ComponentType<{ size?: number }>> = {
  measures:   IconMeasures,
  streams:    IconStreams,
  traces:     IconTraces,
  properties: IconProperties,
};

/** Converts an IntervalRule to a human-readable string e.g. "1d", "24h". */
export function formatInterval(r: IntervalRule | undefined | null): string {
  if (!r) return '';
  const suffix = r.unit === 'UNIT_HOUR' ? 'h' : 'd';
  return `${r.num}${suffix}`;
}

/** Parses a user-typed interval string like "1d" or "24h" into an IntervalRule. */
export function parseInterval(s: string): IntervalRule | null {
  const m = /^(\d+)\s*([dDhH]?)$/.exec(s.trim());
  if (!m || !m[1]) return null;
  const num = parseInt(m[1], 10);
  if (num <= 0) return null;
  const unit = (m[2] ?? 'd').toLowerCase() === 'h' ? 'UNIT_HOUR' : 'UNIT_DAY';
  return { unit, num };
}
