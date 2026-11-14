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

export interface Sample {
  readonly name: string;
  readonly labels: Readonly<Record<string, string>>;
  readonly value: number;
}

const SAMPLE = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)/;
const LABEL = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"/g;
const UNESCAPE: Readonly<Record<string, string>> = { n: '\n', '\\': '\\', '"': '"' };

/** Parses the Prometheus text exposition format, skipping comments and lines it cannot read. */
export function parsePrometheusText(text: string): Sample[] {
  const samples: Sample[] = [];
  for (const raw of text.split('\n')) {
    const line = raw.trim();
    if (line === '' || line.startsWith('#')) continue;
    const match = SAMPLE.exec(line);
    if (!match) continue;
    const value = Number(match[3].replace(/^\+?Inf$/, 'Infinity').replace(/^-Inf$/, '-Infinity'));
    if (Number.isNaN(value) && match[3] !== 'NaN') continue;
    const labels: Record<string, string> = {};
    for (const [, key, val] of (match[2] ?? '').matchAll(LABEL)) {
      labels[key] = val.replace(/\\(.)/g, (_, c: string) => UNESCAPE[c] ?? c);
    }
    samples.push({ name: match[1], labels, value });
  }
  return samples;
}

/** Sums every sample of a metric whose labels include all of `match`; undefined when none exist. */
export function sumOf(samples: readonly Sample[], name: string, match: Readonly<Record<string, string>> = {}): number | undefined {
  let total: number | undefined;
  for (const s of samples) {
    if (s.name !== name || !Object.entries(match).every(([k, v]) => s.labels[k] === v)) continue;
    total = (total ?? 0) + s.value;
  }
  return total;
}
