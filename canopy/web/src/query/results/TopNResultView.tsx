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

// TopNResultView.tsx — Top-N leaderboard view.
//
// Models the handoff's topn-results.jsx layout: a Ranking/Trace tab pair on
// the result card, an `executed in X ms · N lists · M items` status line,
// a "Top N of <resource> by value" toolbar with a list-count pill and a
// `topN` rank badge, a time-bucket picker (← pills → current ts) when the
// response contains more than one TopNList, and a per-list
// # | ENTITY_ID | TOPN bar | VALUE leaderboard with podium styling for
// the top three rows and comma-formatted values.
//
// The view consumes `response.topn_result.lists` directly (the BanyanDB
// wire shape with per-list timestamps) instead of the flattened
// `response.elements` — that's what lets the time-bucket picker group
// rows by their list-level timestamp.
//
// When AGGREGATE BY is set on the query, lists.size == 1 and the time
// picker is hidden.

import React, { useEffect, useMemo, useState } from 'react';
import type { QueryResponse, TopNList, TopNItem } from 'canopy-shared';
import type { QBBuilderState } from '../bydbql.js';
import { ResultPanel } from './ResultPanel.js';
import { ResultEmpty } from './ResultEmpty.js';
import { TopNTracePanel } from './TopNTracePanel.js';

interface Props {
  readonly response: QueryResponse;
  readonly showTrace: boolean;
  readonly setShowTrace: (v: boolean) => void;
  readonly execMs?: number;
  /** Builder state — used to render the "Top N of <resource> by value" title
   *  and to know whether the user picked ASC vs DESC vs an aggregation. */
  readonly state?: QBBuilderState;
  /** First entry of the topn-agg's `groupByTagNames`. Drives the
   *  ENTITY_ID column header. Falls back to "ENTITY_ID" when unknown. */
  readonly entityTag?: string;
}

type Tag = { key: string; value: string | number | boolean | null };

const IconTopN = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3.5" y="5" width="4.2" height="15" rx="1" />
    <rect x="9.9" y="9.5" width="4.2" height="10.5" rx="1" />
    <rect x="16.3" y="14" width="4.2" height="6" rx="1" />
  </svg>
);
const IconArrowLeft = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="m15 6-6 6 6 6" />
  </svg>
);
const IconArrowRight = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="m9 6 6 6-6 6" />
  </svg>
);

// Read a FieldValue oneof (`{float: n}` / `{int: n}` / `{str: s}`) into a JS
// number. The BanyanDB wire format occasionally wraps ints in `{int:{value:...}}`
// for 64-bit precision — accept both shapes.
function readItemValue(v: unknown): number {
  if (v == null || typeof v !== 'object') return 0;
  const o = v as { float?: unknown; int?: unknown; str?: unknown };
  if (typeof o.float === 'number') return o.float;
  const innerInt = o.int as { value?: unknown } | number | undefined;
  if (innerInt !== undefined) {
    const raw = typeof innerInt === 'object' && innerInt !== null ? (innerInt as { value?: unknown }).value : innerInt;
    if (typeof raw === 'string') return Number(raw);
    if (typeof raw === 'number') return raw;
  }
  const innerStr = o.str as { value?: unknown } | string | undefined;
  if (innerStr !== undefined) {
    const raw = typeof innerStr === 'object' && innerStr !== null ? (innerStr as { value?: unknown }).value : innerStr;
    if (typeof raw === 'string') {
      const n = Number(raw);
      return Number.isFinite(n) ? n : 0;
    }
  }
  return 0;
}

// "14:30" / "14:30:00" formatter for the time-bucket picker.
function fmtBucket(ts: string | undefined): string {
  if (!ts) return '';
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts;
  const p = (x: number) => String(x).padStart(2, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}`;
}
function fmtBucketFull(ts: string | undefined): string {
  if (!ts) return '';
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts;
  const p = (x: number) => String(x).padStart(2, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}
const fmtNum = (n: number): string => Number(Math.round(n)).toLocaleString('en-US');

// BanyanDB sets the list-level `timestamp` to null for aggregated responses
// (where AGGREGATE BY rolled every bucket into one list) — there isn't a
// single meaningful timestamp for "all time". Per-item timestamps are still
// populated. Fall back to the most-recent item timestamp so the picker
// always has something to render; return undefined when neither is set
// (e.g. a degenerate response with no items).
function resolveListTs(list: TopNList | undefined): string | undefined {
  if (!list) return undefined;
  if (list.timestamp) return list.timestamp;
  const items = list.items ?? [];
  let latest: string | undefined;
  for (const it of items) {
    const t = (it as { timestamp?: string }).timestamp;
    if (!t) continue;
    if (!latest || t > latest) latest = t;
  }
  return latest;
}

/** Resolve the list shape from `response.topn_result.lists` (preferred) or
 *  the flattened `response.elements` (fallback for tests / hand-shaped
 *  fixtures that only carry the flat shape). */
function resolveLists(response: QueryResponse): readonly TopNList[] {
  const lists = response.topn_result?.lists;
  if (lists && lists.length > 0) return lists;
  // Fallback: group flat elements by their `timestamp` field. Each unique
  // timestamp becomes one TopNList — preserves the time-bucket UX even when
  // the caller only hands us the flattened shape.
  const byTs = new Map<string, TopNItem[]>();
  for (const e of (response.elements ?? []) as readonly Record<string, unknown>[]) {
    const ts = String(e.timestamp ?? '');
    if (!byTs.has(ts)) byTs.set(ts, []);
    const arr = byTs.get(ts)!;
    const entity: Tag[] = Object.keys(e)
      .filter((k) => k !== 'value' && k !== 'timestamp')
      .map((k) => ({ key: k, value: e[k] as Tag['value'] }));
    arr.push({ entity, value: { float: Number(e.value ?? 0) } });
  }
  return [...byTs.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([timestamp, items]) => ({ timestamp, items }));
}

export function TopNResultView({ response, showTrace, setShowTrace, execMs, state, entityTag }: Props) {
  const lists = useMemo(() => resolveLists(response), [response]);
  // Default to the most-recent (last) list — matches the handoff default and
  // is the most useful slice when the user lands on the result view.
  const [li, setLi] = useState(Math.max(0, lists.length - 1));
  // Reset the active list whenever a new response comes in (e.g. on Run).
  useEffect(() => { setLi(Math.max(0, lists.length - 1)); }, [lists]);

  const multi = lists.length > 1;
  const idx = Math.min(li, lists.length - 1);
  const list = lists[idx];
  const asc = state?.orderDir === 'ASC';
  const topN = state?.topN ?? 10;
  const aggFn = state?.aggFn ?? '';
  const resource = state?.resource ?? '';
  const totalItems = lists.reduce((a, l) => (l.items?.length ?? 0) + a, 0);

  // The right-side status line: "executed in X ms · N lists · M items".
  // For Top-N, "lists" is more meaningful than "rows" — we surface both
  // the time-bucket count and the total item count across all lists.
  const statusLine =
    execMs !== undefined
      ? `executed in ${execMs < 1 ? execMs.toFixed(2) : execMs.toFixed(1)} ms`
      : '';

  return (
    <ResultPanel
      catalog="topn"
      response={response}
      execMs={execMs}
      traceEnabled={!!state?.trace}
      showTrace={showTrace}
      setShowTrace={setShowTrace}
      tabs={{ result: 'Ranking', trace: 'Trace' }}
      className="mr-card"
    >
      {showTrace ? (
        <TopNTracePanel response={response} />
      ) : lists.length === 0 ? (
        <ResultEmpty title="No leaderboard data" text="The Top-N query returned no ranked series for this measure and window." />
      ) : (
        <>
          <div className="mr-toolbar">
            <span className="mr-tool-label">
              {asc ? 'Bottom ' : 'Top '}{topN} of <span className="mono">{resource || '—'}</span> by {aggFn ? `${aggFn.toLowerCase()}(value)` : 'value'}
            </span>
            <div className="mr-tool-right">
              {multi
                ? <span className="mr-auto-tag">{lists.length} lists by time</span>
                : <span className="mr-auto-tag">aggregated · 1 list</span>}
              <span className={'topn-rank is-' + (asc ? 'bottomn' : 'topn')}>
                <IconTopN width={11} height={11} /> {asc ? 'bottomN' : 'topN'}
              </span>
            </div>
          </div>

          {multi && list && (
            <div className="tnlb-ts-bar">
              <button
                type="button"
                className="tnlb-ts-nav"
                disabled={idx === 0}
                onClick={() => setLi(Math.max(0, idx - 1))}
                title="Earlier list"
              >
                <IconArrowLeft width={14} height={14} />
              </button>
              <div className="tnlb-ts-pills">
                {lists.map((l, i) => (
                  <button
                    key={i}
                    type="button"
                    className={'tnlb-ts-pill mono' + (i === idx ? ' is-on' : '')}
                    onClick={() => setLi(i)}
                    title={'List at ' + fmtBucketFull(resolveListTs(l))}
                  >
                    {fmtBucket(resolveListTs(l))}
                  </button>
                ))}
              </div>
              <button
                type="button"
                className="tnlb-ts-nav"
                disabled={idx === lists.length - 1}
                onClick={() => setLi(Math.min(lists.length - 1, idx + 1))}
                title="Later list"
              >
                <IconArrowRight width={14} height={14} />
              </button>
              <span className="tnlb-ts-meta mono">{fmtBucketFull(resolveListTs(list))}</span>
            </div>
          )}

          <TopNListView list={list} entityTag={entityTag} />
          {multi && (
            <div className="rv-trace-meta">
              <span className="rv-meta">{lists.length} lists · {totalItems} items · {statusLine}</span>
            </div>
          )}
        </>
      )}
    </ResultPanel>
  );
}

function TopNListView({ list, entityTag }: { list: TopNList | undefined; entityTag?: string }) {
  if (!list) return null;
  const items = (list.items ?? []) as readonly TopNItem[];
  const max = Math.max(...items.map((it) => readItemValue(it.value)), 0);
  // The BanyanDB TopN wire format always encodes the entity under the key
  // `entity_id` regardless of which group-by tags the topn-agg declares. We
  // surface that literal key as the column header so the UI matches what the
  // user sees in the proto schema. The `entityTag` prop is reserved for
  // future overrides (e.g. when the user picks a custom column label) but
  // defaults to `entity_id`.
  const headerLabel = entityTag ?? 'entity_id';
  return (
    <div className="tnlb">
      <div className="tnlb-head">
        <span className="tnlb-h-rank">#</span>
        <span className="tnlb-h-label">{headerLabel}</span>
        <span className="tnlb-h-bar">topN</span>
        <span className="tnlb-h-val num">value</span>
      </div>
      {items.map((it, i) => {
        const rank = i + 1;
        const v = readItemValue(it.value);
        const pct = max ? Math.max(2, (v / max) * 100) : 0;
        return (
          <div key={rank} className={'tnlb-row' + (rank <= 3 ? ' is-podium' : '')}>
            <span className={'tnlb-rank rank-' + (rank <= 3 ? rank : 'n')}>{rank}</span>
            <span className="tnlb-label"><TopNEntity entity={it.entity ?? []} /></span>
            <span className="tnlb-bar-wrap">
              <span className="tnlb-bar" style={{ width: pct.toFixed(1) + '%' }} />
            </span>
            <span className="tnlb-val num mono">{fmtNum(v)}</span>
          </div>
        );
      })}
    </div>
  );
}

// Decode a wire-format entity value into the human-readable string shown in
// the leaderboard. BanyanDB emits entity tags with several on-the-wire
// encodings depending on the schema version and the underlying group-by tag
// type, so we try a sequence of strategies and pick the most readable one:
//
//   1. Plain string (e.g. top_service's `service` groupBy returns plain
//      strings like "checkout", "orders") — no decoding needed.
//   2. `<base64-name>.<base64-description>` — the standard format for newer
//      topn-aggs (service_id / attr0 groupBy). Decode the first half.
//   3. Whole-string base64 decoding, then split on the last space and take
//      the trailing identifier. Some precomputed topn-aggs (older
//      instance_id topn-aggs) encode the whole value as base64, with the
//      decoded form being `<hash> <pod-XXX>` where the hash is opaque binary
//      and the trailing token is the meaningful instance name. Surfaces as
//      `ɕ^^ pod-006` until decoded.
//   4. Fallback: return the wire value verbatim so the user can still see
//      something rather than `[object Object]`.
//
// The trailing-identifier strategy is what makes `pod-006` appear instead
// of `ɕ^^ pod-006` in the leaderboard cell.
function decodeEntityValue(raw: unknown): string {
  if (raw == null) return '';
  const o = raw as { str?: { value?: unknown } };
  const wire = o.str && typeof o.str.value === 'string' ? o.str.value : '';
  if (!wire) return String(raw);

  // Strategy 1: plain string (no base64, no dot).
  if (!wire.includes('=') && !wire.includes('.')) {
    // Heuristic: if it looks like plain text already (printable ASCII
    // without high-byte characters), use it as-is. atob is a no-op here
    // because there's no padding.
    if (/^[\x20-\x7e]+$/.test(wire)) return wire;
  }

  if (typeof atob !== 'function') return wire;

  // Strategy 2: `<base64-name>.<base64-description>` — decode the head.
  const dot = wire.indexOf('.');
  if (dot > 0) {
    const head = wire.slice(0, dot);
    try {
      const decoded = atob(head);
      // The decoded head should be a printable identifier.
      if (/^[\x20-\x7e]+$/.test(decoded)) return decoded;
    } catch { /* fall through to whole-string strategy */ }
  }

  // Strategy 3: whole-string base64; the decoded form is `<hash> <name>` —
  // take the trailing identifier after the last space.
  try {
    const decoded = atob(wire);
    if (decoded && /^[\x20-\x7e]+$/.test(decoded)) {
      const lastSpace = decoded.lastIndexOf(' ');
      if (lastSpace >= 0 && lastSpace < decoded.length - 1) {
        return decoded.slice(lastSpace + 1);
      }
      return decoded;
    }
  } catch { /* not base64 — fall through */ }

  // Strategy 4: best-effort fallback — return the wire as-is so the cell
  // still has SOMETHING readable.
  return wire;
}

function TopNEntity({ entity }: { entity: readonly Tag[] }) {
  if (!entity.length) return <span className="tnlb-ent"><span className="tnlb-ent-v dim">—</span></span>;
  return (
    <span className="tnlb-ent">
      {entity.map((t, i) => {
        // Render the entity as a single readable identifier — the wire
        // format wraps it as {key:"entity_id", value:{str:{value:"<base64>"}}}
        // and we surface only the decoded name (not the key + raw object).
        const display = decodeEntityValue(t.value);
        return (
          <span key={i} className="tnlb-ent-v mono" title={`${t.key} = ${String(t.value)}`}>
            {display || '—'}
          </span>
        );
      })}
    </span>
  );
}