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

// Pure validation helpers for M3 forms.
//
// These mirror the BanyanDB server's proto rules so the UI can give fast
// feedback while typing. They are **advisory** (per M3 plan, Principle 5):
// the server is authoritative. A client check that passes is NOT a
// guarantee the server will accept; the BFF/server-authority path must
// still surface server rejections (covered by the MF2 e2e test).
//
// Each validator is a pure function: it takes the form state as input and
// returns a partial record of field-name → error-message. Empty record
// means the input is locally-valid.

import type { IndexType } from 'canopy-shared';

// ── shared shapes ────────────────────────────────────────────────────────────

export interface IntervalInput {
  num: number | '' | null | undefined;
  unit: 'UNIT_DAY' | 'UNIT_HOUR' | string;
}

export interface StageInput {
  name: string;
  shardNum: number | '' | null | undefined;
  segmentInterval: IntervalInput;
  ttl: IntervalInput;
  nodeSelector?: string;
  close?: boolean;
  replicas?: number | '' | null | undefined;
}

export interface StageErrors {
  name?: string;
  shardNum?: string;
  segmentInterval?: string;
  ttl?: string;
  nodeSelector?: string;
  replicas?: string;
}

export interface GroupInput {
  // Edit mode disables name + catalog; the form passes `isEdit` so the
  // validator can skip immutable-field checks.
  isEdit: boolean;
  name: string;
  catalog: 'CATALOG_MEASURE' | 'CATALOG_STREAM' | 'CATALOG_TRACE' | 'CATALOG_PROPERTY' | string;
  shardNum: number | '' | null | undefined;
  segmentInterval?: IntervalInput;
  ttl?: IntervalInput;
  stages?: readonly StageInput[];
}

export interface GroupErrors {
  name?: string;
  shardNum?: string;
  segmentInterval?: string;
  ttl?: string;
  stages?: StageErrors[];
}

export interface TagInput {
  name: string;
  type: string;
}

export interface StreamInput {
  isEdit: boolean;
  name: string;
  families: ReadonlyArray<{ name: string; tags: readonly TagInput[] }>;
  entityTagNames: readonly string[];
}

export interface MeasureInput {
  isEdit: boolean;
  name: string;
  families: ReadonlyArray<{ name: string; tags: readonly TagInput[] }>;
  fields: ReadonlyArray<{ name: string; fieldType: string }>;
  entityTagNames: readonly string[];
  indexMode: boolean;
  interval: string;
}

export interface TraceInput {
  isEdit: boolean;
  name: string;
  tags: readonly TagInput[];
  traceIdTagName: string;
  spanIdTagName: string;
  timestampTagName: string;
}

export interface IndexRuleInput {
  isEdit: boolean;
  name: string;
  tags: readonly string[];
  type: IndexType | string;
  analyzer?: string;
  /**
   * Lower-case set of existing rule/binding names. When provided, create-mode
   * validation rejects a name that is already taken in the group.
   */
  existingNames?: ReadonlySet<string>;
}

export interface IndexRuleBindingInput {
  isEdit: boolean;
  name: string;
  rules: readonly string[];
  subject: { name: string; catalog: string };
  /** Validity window start, expressed as epoch milliseconds. */
  beginAt: number | null;
  /** Validity window end, expressed as epoch milliseconds. */
  expireAt: number | null;
  existingNames?: ReadonlySet<string>;
}

// ── shared helpers ──────────────────────────────────────────────────────────

const NAME_RE = /^[A-Za-z0-9_-]+$/;
const RESERVED_TAG_PREFIX = '#';

function numIsPositive(n: number | '' | null | undefined): boolean {
  return typeof n === 'number' && Number.isFinite(n) && n > 0;
}

function numIsNonNegative(n: number | '' | null | undefined): boolean {
  return n === '' || n == null || (typeof n === 'number' && Number.isFinite(n) && n >= 0);
}

// ── 1. validateInterval ─────────────────────────────────────────────────────

/** validateInterval checks a single {num, unit} shape. */
export function validateInterval(iv: IntervalInput | undefined, label = 'value'): string | undefined {
  if (!iv) return `${label} is required.`;
  if (!numIsPositive(iv.num)) return `${label} must be greater than 0.`;
  if (iv.unit !== 'UNIT_DAY' && iv.unit !== 'UNIT_HOUR') return `${label} unit must be UNIT_DAY or UNIT_HOUR.`;
  return undefined;
}

// ── 2. validateStages ───────────────────────────────────────────────────────

/** validateStages returns one StageErrors per stage, indexed by position. */
export function validateStages(stages: readonly StageInput[] | undefined): StageErrors[] {
  if (!stages || stages.length === 0) return [];
  const out: StageErrors[] = [];
  stages.forEach((s, i) => {
    const e: StageErrors = {};
    if (!s.name || !s.name.trim()) e.name = 'Stage name is required.';
    else if (!NAME_RE.test(s.name)) e.name = 'Stage name may contain only letters, digits, "_" and "-".';
    if (!numIsPositive(s.shardNum)) e.shardNum = 'Must be greater than 0.';
    const seg = validateInterval(s.segmentInterval, 'Segment interval');
    if (seg) e.segmentInterval = seg;
    const ttl = validateInterval(s.ttl, 'TTL');
    if (ttl) e.ttl = ttl;
    if (s.nodeSelector !== undefined && !s.nodeSelector.trim()) {
      e.nodeSelector = 'Node selector is required.';
    }
    if (!numIsNonNegative(s.replicas)) e.replicas = 'Replicas must be ≥ 0.';
    if (Object.keys(e).length > 0) out[i] = e;
  });
  return out;
}

// ── 3. validateGroup ────────────────────────────────────────────────────────

/** validateGroup mirrors the Group proto rules (name regex, shardNum>0, optional stages). */
export function validateGroup(input: GroupInput): GroupErrors {
  const e: GroupErrors = {};
  if (!input.isEdit) {
    if (!input.name || !input.name.trim()) e.name = 'Group name is required.';
    else if (!NAME_RE.test(input.name)) e.name = 'May contain only letters, digits, "_" and "-".';
  }
  if (!numIsPositive(input.shardNum)) e.shardNum = 'Shard number must be greater than 0.';
  if (input.catalog !== 'CATALOG_PROPERTY') {
    const seg = validateInterval(input.segmentInterval, 'Segment interval');
    if (seg) e.segmentInterval = seg;
    const ttl = validateInterval(input.ttl, 'TTL');
    if (ttl) e.ttl = ttl;
    const stageErrs = validateStages(input.stages);
    if (stageErrs.length > 0) e.stages = stageErrs;
  }
  return e;
}

export function isGroupValid(e: GroupErrors): boolean {
  return Object.keys(e).length === 0;
}

// ── 4. validateStream ───────────────────────────────────────────────────────

/** validateStream: name regex, tag families, no `#` in tag names, ≥1 entity tag, entity ⊆ defined tags. */
export function validateStream(input: StreamInput): string | undefined {
  const submitted = (input.name ?? '').trim();
  if (!submitted) return 'Name is required.';
  if (!NAME_RE.test(submitted)) return 'Name may contain only letters, digits, "_" and "-".';
  if (input.families.length === 0) return 'At least one tag family is required.';
  // Tag uniqueness across families (checked first so duplicates don't get
  // masked by later entity-tag checks)
  const seen = new Set<string>();
  for (const f of input.families) {
    for (const t of f.tags) {
      if (seen.has(t.name)) return `Duplicate tag name "${t.name}".`;
      seen.add(t.name);
    }
  }
  for (const fam of input.families) {
    if (!fam.name || !fam.name.trim()) return 'Each tag family must have a name.';
    for (const tag of fam.tags) {
      if (!tag.name || !tag.name.trim()) return `All tags in family "${fam.name}" must have names.`;
      if (tag.name.includes(RESERVED_TAG_PREFIX)) return `Tag name "${tag.name}" must not contain "#".`;
    }
  }
  if (input.entityTagNames.length === 0) return 'At least one entity tag is required.';
  const allTagNames = input.families.flatMap((f) => f.tags.map((t) => t.name));
  for (const ent of input.entityTagNames) {
    if (!allTagNames.includes(ent)) return `Entity tag "${ent}" is not defined in any family.`;
  }
  return undefined;
}

// ── 5. validateMeasure ──────────────────────────────────────────────────────

/** validateMeasure: stream-like + fields (skipped when indexMode=true), interval non-empty. */
export function validateMeasure(input: MeasureInput): string | undefined {
  const submitted = (input.name ?? '').trim();
  if (!submitted) return 'Name is required.';
  if (!NAME_RE.test(submitted)) return 'Name may contain only letters, digits, "_" and "-".';
  if (input.families.length === 0) return 'At least one tag family is required.';
  // Tag uniqueness across families (checked first)
  const seen = new Set<string>();
  for (const f of input.families) {
    for (const t of f.tags) {
      if (seen.has(t.name)) return `Duplicate tag name "${t.name}".`;
      seen.add(t.name);
    }
  }
  for (const fam of input.families) {
    if (!fam.name || !fam.name.trim()) return 'Each tag family must have a name.';
    for (const tag of fam.tags) {
      if (!tag.name || !tag.name.trim()) return `All tags in family "${fam.name}" must have names.`;
      if (tag.name.includes(RESERVED_TAG_PREFIX)) return `Tag name "${tag.name}" must not contain "#".`;
    }
  }
  if (input.entityTagNames.length === 0) return 'At least one entity tag is required.';
  const allTagNames = input.families.flatMap((f) => f.tags.map((t) => t.name));
  for (const ent of input.entityTagNames) {
    if (!allTagNames.includes(ent)) return `Entity tag "${ent}" is not defined in any family.`;
  }
  if (!input.indexMode) {
    if (input.fields.length === 0) return 'At least one field is required (or enable index mode).';
    for (const f of input.fields) {
      if (!f.name || !f.name.trim()) return 'All fields must have names.';
    }
  } else {
    if (input.fields.length > 0) {
      return 'Index mode is enabled: fields must be empty.';
    }
  }
  if (!input.interval || !input.interval.trim()) return 'Interval is required (e.g. 1d, 1h).';
  return undefined;
}

// ── 6. validateTrace ────────────────────────────────────────────────────────

/** validateTrace: name, tags, traceId/spanId/ts tags must reference defined tags (all distinct). */
export function validateTrace(input: TraceInput): string | undefined {
  const submitted = (input.name ?? '').trim();
  if (!submitted) return 'Name is required.';
  if (!NAME_RE.test(submitted)) return 'Name may contain only letters, digits, "_" and "-".';
  if (input.tags.length === 0) return 'At least one tag is required.';
  const names = new Set<string>();
  for (const tag of input.tags) {
    if (!tag.name || !tag.name.trim()) return 'All tags must have names.';
    if (tag.name.includes(RESERVED_TAG_PREFIX)) return `Tag name "${tag.name}" must not contain "#".`;
    if (names.has(tag.name)) return `Duplicate tag name "${tag.name}".`;
    names.add(tag.name);
  }
  if (!input.traceIdTagName) return 'Trace ID tag name is required.';
  if (!input.spanIdTagName) return 'Span ID tag name is required.';
  if (!input.timestampTagName) return 'Timestamp tag name is required.';
  if (!names.has(input.traceIdTagName)) return `Trace ID tag "${input.traceIdTagName}" is not defined.`;
  if (!names.has(input.spanIdTagName)) return `Span ID tag "${input.spanIdTagName}" is not defined.`;
  if (!names.has(input.timestampTagName)) return `Timestamp tag "${input.timestampTagName}" is not defined.`;
  if (input.traceIdTagName === input.spanIdTagName) return 'Trace ID and Span ID must reference different tags.';
  if (input.traceIdTagName === input.timestampTagName) return 'Trace ID and Timestamp must reference different tags.';
  if (input.spanIdTagName === input.timestampTagName) return 'Span ID and Timestamp must reference different tags.';
  return undefined;
}

// ── 7. validateIndexRule ────────────────────────────────────────────────────

/** validateIndexRule: name regex, length cap, ≥1 tag, type ∈ {TREE, INVERTED}. */
export function validateIndexRule(input: IndexRuleInput): string | undefined {
  const submitted = (input.name ?? '').trim();
  if (!submitted) return 'Name is required.';
  if (submitted.length > 255) return 'Must be 255 characters or fewer.';
  if (!NAME_RE.test(submitted)) return 'Name may contain only letters, digits, "_" and "-".';
  if (!input.isEdit && input.existingNames?.has(submitted.toLowerCase())) {
    return `An index rule named "${submitted}" already exists.`;
  }
  if (input.tags.length === 0) return 'At least one tag is required.';
  const seen = new Set<string>();
  for (const t of input.tags) {
    if (!t || !t.trim()) return 'All tag names must be non-empty.';
    if (seen.has(t)) return `Duplicate tag "${t}".`;
    seen.add(t);
  }
  if (input.type !== 'TYPE_TREE' && input.type !== 'TYPE_INVERTED'
      && input.type !== 'TYPE_SKIPPING') {
    return 'Index type must be TREE, INVERTED, or SKIPPING.';
  }
  return undefined;
}

// ── 8. validateIndexRuleBinding ─────────────────────────────────────────────

/** validateIndexRuleBinding: name, ≥1 rule, subject set, expireAt > beginAt. */
export function validateIndexRuleBinding(input: IndexRuleBindingInput): string | undefined {
  const submitted = (input.name ?? '').trim();
  if (!submitted) return 'Name is required.';
  if (submitted.length > 255) return 'Must be 255 characters or fewer.';
  if (!NAME_RE.test(submitted)) return 'Name may contain only letters, digits, "_" and "-".';
  if (!input.isEdit && input.existingNames?.has(submitted.toLowerCase())) {
    return `A binding named "${submitted}" already exists.`;
  }
  if (input.rules.length === 0) return 'At least one rule is required.';
  if (!input.subject || !input.subject.name || !input.subject.name.trim()) {
    return 'Subject resource is required.';
  }
  if (!input.subject.catalog) return 'Subject catalog is required.';
  if (input.beginAt == null) return 'Begin time is required.';
  if (input.expireAt == null) return 'Expire time is required.';
  if (input.expireAt <= input.beginAt) return 'Expire time must be after begin time.';
  return undefined;
}
