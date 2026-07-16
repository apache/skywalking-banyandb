/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

// MeasureResultView.tsx — measure query result view (Table + Chart).
//
// Chart is multi-series (one line per distinct idTags tuple) with Y-axis
// ticks, X-axis time labels, last-point dot markers, and a per-series
// legend showing avg / last / change%. Table is the time-major flat view.
// Trace tab surfaces the raw query trace when WITH QUERY_TRACE is enabled.
//
// idTags derivation: state.groupBy if non-empty, else the first projection
// tag (which approximates the entity identifier for measures). The handoff
// falls back to the schema's entity.tagNames when projection is empty;
// we follow a simpler rule (first projection) because QueryBuilder does
// not expose entity tag names.

import React, { useMemo, useRef, useState } from 'react';
import type { QueryResponse } from 'canopy-shared';
import type { QBBuilderState } from '../bydbql.js';
import { parseTs, parseIntervalMs, formatTs } from '../time.js';
import { TimestampCell } from '../time.js';
import { ResultPanel } from './ResultPanel.js';
import { ResultEmpty } from './ResultEmpty.js';
import { TraceView, TraceDisabled } from './TraceView.js';

interface Props {
  readonly response: QueryResponse;
  readonly state: QBBuilderState;
  readonly showTrace: boolean;
  readonly setShowTrace: (v: boolean) => void;
  /** True when the last page returned a full LIMIT — i.e. more rows likely exist. */
  readonly hasMore: boolean;
  /** Called when the user clicks "Load more" — should re-run with offset+=limit. */
  readonly onLoadMore: () => void;
  /** Disables the button + shows a spinner label while the next page is in flight. */
  readonly isLoadingMore: boolean;
  readonly execMs?: number;
  /** Tag names for the current resource. Used to expand "all tags" in the table. */
  readonly tags?: readonly string[];
  /** Measure schema interval (e.g. "1m", "30s", "1h"). Controls timestamp granularity. */
  readonly interval?: string;
}

type Elem = Record<string, unknown>;

// Per-series color palette (mirrors .handoff-import/banyandb/project/measure-results.jsx MR_COLORS).
const MR_COLORS = ['#3d82f6', '#3cc8b4', '#d98a3c', '#9b8cf0', '#e0707a', '#56c2d6'];

function fmtNum(n: number): string {
  if (!Number.isFinite(n)) return '–';
  return Math.round(n).toLocaleString('en-US');
}

function fmtPct(dpct: number): string {
  return `${dpct >= 0 ? '▲' : '▼'} ${Math.abs(dpct).toFixed(1)}%`;
}

const IconRows = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M3 6h18M3 12h18M3 18h18" />
  </svg>
);
const IconChart = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M3 3v18h18" />
    <path d="M19 9l-4 4-3-3-4 4" />
  </svg>
);
const IconEye = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M2 12s3.6-7 10-7 10 7 10 7-3.6 7-10 7-10-7-10-7z" />
    <circle cx="12" cy="12" r="3" />
  </svg>
);
const IconEyeOff = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M10.6 6.2A9.7 9.7 0 0 1 12 5c6.4 0 10 7 10 7a17 17 0 0 1-3.2 4M6.3 7.4A17 17 0 0 0 2 12s3.6 7 10 7a9.7 9.7 0 0 0 4-.9M9.9 9.9a3 3 0 0 0 4.2 4.2" />
    <path d="M3 3l18 18" />
  </svg>
);

interface SeriesPoint { readonly t: number; readonly v: number; }
interface Series {
  readonly key: string;
  readonly idLabel: string;
  readonly color: string;
  readonly points: readonly SeriesPoint[];
}

function deriveIdTags(state: QBBuilderState, tags?: readonly string[]): string[] {
  const groupBy = state.groupBy ?? [];
  if (groupBy.length > 0) return groupBy;
  const projection = state.projection ?? [];
  if (projection.length > 0) return [projection[0]];
  // "all tags" is selected; use the first known tag as the series identifier.
  return tags?.length ? [tags[0]] : [];
}

interface GroupResult {
  readonly series: readonly Series[];
  /** True iff at least one data point has a real timestamp. */
  readonly hasTimeAxis: boolean;
}

function groupElements(elements: readonly Elem[], idTags: readonly string[], numField: string): GroupResult {
  const groups = new Map<string, { color: string; tagVals: Record<string, unknown>; points: SeriesPoint[] }>();
  let hasTimeAxis = false;
  for (let i = 0; i < elements.length; i++) {
    const e = elements[i];
    const tagVals: Record<string, unknown> = {};
    for (const t of idTags) tagVals[t] = e[t];
    const key = idTags.length === 0
      ? 'all'
      : idTags.map((t) => String(tagVals[t] ?? '')).join(' / ');
    if (!groups.has(key)) {
      groups.set(key, {
        color: MR_COLORS[groups.size % MR_COLORS.length],
        tagVals,
        points: [],
      });
    }
    const ts = parseTs(e.timestamp);
    // BanyanDB returns null timestamps for some aggregated rows (e.g. a
    // single MEAN bucket has no per-row timestamp). Fall back to the
    // element index so the chart still has a usable x-axis; flag the chart
    // so it can render the x-axis as a row index instead of HH:MM.
    let t: number;
    if (Number.isFinite(ts)) {
      t = ts;
      hasTimeAxis = true;
    } else {
      t = i + 1;
    }
    const raw = e[numField];
    const v = typeof raw === 'number' ? raw : Number(raw);
    if (Number.isFinite(t) && Number.isFinite(v)) {
      groups.get(key)!.points.push({ t, v });
    }
  }
  const series: Series[] = [];
  for (const [key, s] of groups.entries()) {
    const sortedPoints = s.points.slice().sort((a, b) => a.t - b.t);
    series.push({
      key,
      idLabel: idTags.length === 0 ? 'all data points' : idTags.map((t) => `${t}=${String(s.tagVals[t] ?? '')}`).join(' · '),
      color: s.color,
      points: sortedPoints,
    });
  }
  series.sort((a, b) => a.key.localeCompare(b.key));
  return { series, hasTimeAxis };
}

export function MeasureResultView({ response, state, showTrace, setShowTrace, hasMore, onLoadMore, isLoadingMore, execMs, tags, interval }: Props) {
  const elements = (response.elements ?? []) as readonly Elem[];
  const fields = (state.select ?? []).filter((r) => r.field);
  const idTags = useMemo(() => deriveIdTags(state, tags), [state.groupBy, state.projection, tags]);
  // Table columns: explicit projection, or all known tags when "all tags" is selected.
  const tableTags = useMemo(() =>
    (state.projection ?? []).length > 0 ? (state.projection ?? []) : (tags ?? []),
  [state.projection, tags]);
  // The numeric column the chart plots: first field with a function (agggregated);
  // fall back to the first projection tag for raw (non-aggregated) queries.
  const numField = useMemo(() => {
    // First field's name (raw or aggregated) is what we plot; fall back to
    // the first projection tag when no field is selected at all.
    const f = fields[0];
    if (f && f.field) return f.field;
    return (state.projection ?? [])[0] ?? '';
  }, [state.select, state.projection]);
  const [activeField, setActiveField] = React.useState<string>('');
  React.useEffect(() => { setActiveField(numField); }, [numField]);
  const chartField = activeField || numField;
  const { series, hasTimeAxis } = useMemo(() => groupElements(elements, idTags, chartField), [elements, idTags, chartField]);
  const intervalMs = useMemo(() => parseIntervalMs(interval), [interval]);

  const hasAgg = fields.some((r) => r.fn);
  // auto: chart when aggregated, else table.
  const [style, setStyle] = useState<'auto' | 'table' | 'chart'>('auto');
  const [showMeta, setShowMeta] = useState(false);
  const chartAvailable = fields.length > 0;
  const eff = !chartAvailable ? 'table' : (style === 'auto' ? (hasAgg ? 'chart' : 'table') : style);

  const toolbar = (
    <div className="mr-toolbar">
      <span className="mr-tool-label">
        {series.length} series · {elements.length.toLocaleString('en-US')} data points
      </span>
      <div className="mr-tool-right">
        {eff === 'table' && (
          <button
            type="button"
            className={'mr-meta-btn' + (showMeta ? ' is-on' : '')}
            onClick={() => setShowMeta((v) => !v)}
            title={showMeta ? 'Hide series id & version columns' : 'Show series id & version columns'}
          >
            {showMeta ? <IconEyeOff width={13} height={13} /> : <IconEye width={13} height={13} />} sid / version
          </button>
        )}
        <div className="mr-view-seg" role="tablist" aria-label="Result style">
          <button
            type="button"
            className={'mr-view-btn' + (eff === 'table' ? ' is-on' : '')}
            onClick={() => setStyle('table')}
          >
            <IconRows width={13} height={13} /> Table
          </button>
          <button
            type="button"
            className={'mr-view-btn' + (eff === 'chart' ? ' is-on' : '')}
            disabled={!chartAvailable}
            onClick={() => setStyle('chart')}
            title={chartAvailable ? '' : 'Add a field to plot a chart'}
          >
            <IconChart width={13} height={13} /> Chart
          </button>
        </div>
      </div>
    </div>
  );

  return (
    <ResultPanel catalog="measures" response={response} execMs={execMs} traceEnabled={state.trace} showTrace={showTrace} setShowTrace={setShowTrace} subBar={toolbar}>
      {showTrace ? (
        state.trace ? <TraceView response={response} /> : <TraceDisabled />
      ) : eff === 'table' ? (
        <TableView elements={elements} fields={fields} idTags={tableTags} showMeta={showMeta} intervalMs={intervalMs} hasMore={hasMore} onLoadMore={onLoadMore} isLoadingMore={isLoadingMore} />
      ) : (
        <ChartView series={series} fields={fields} activeField={chartField} setActiveField={setActiveField} hasTimeAxis={hasTimeAxis} intervalMs={intervalMs} hasMore={hasMore} onLoadMore={onLoadMore} isLoadingMore={isLoadingMore} />
      )}
    </ResultPanel>
  );
}

function TableView({ elements, fields, idTags, showMeta, intervalMs, hasMore, onLoadMore, isLoadingMore }: {
  elements: readonly Elem[];
  fields: readonly { field: string; fn?: string }[];
  idTags: readonly string[];
  showMeta: boolean;
  intervalMs?: number;
  hasMore: boolean;
  onLoadMore: () => void;
  isLoadingMore: boolean;
}) {
  if (elements.length === 0) return <ResultEmpty title="No rows" text="The query executed successfully but matched no data points in this window." />;
  const fieldLabels = fields.map((f) => f.fn ? `${f.fn}(${f.field})` : f.field);
  return (
    <div className="mtbl-scroll">
      <table className="mtbl">
        <thead>
          <tr className="mtbl-tier">
            <th className="tier-time">time</th>
            {idTags.length > 0 && <th className="tier-grp" colSpan={idTags.length}>tags</th>}
            {fields.length > 0 && <th className="tier-grp tier-fields" colSpan={fields.length}>fields</th>}
            {showMeta && <th className="tier-grp tier-meta" colSpan={2}>metadata</th>}
          </tr>
          <tr className="mtbl-cols">
            <th>timestamp</th>
            {idTags.map((t) => <th key={t}>{t}</th>)}
            {fieldLabels.map((label) => <th key={label} className="num">{label}</th>)}
            {showMeta && <th className="meta-col">sid</th>}
            {showMeta && <th className="meta-col">version</th>}
          </tr>
        </thead>
        <tbody>
          {elements.map((e, i) => (
            <tr key={i}>
              <TimestampCell ts={parseTs(e.timestamp)} opts={{ intervalMs }} />
              {idTags.map((t, ti) => (
                <td key={t} className="mono">
                  {ti === 0 ? <span className="tagpill">{String(e[t] ?? '')}</span> : String(e[t] ?? '')}
                </td>
              ))}
              {fields.map((f, fi) => {
                const raw = e[f.field];
                const v = typeof raw === 'number' ? raw : Number(raw);
                return (
                  <td key={fieldLabels[fi]} className="num mono strong">
                    {Number.isFinite(v) ? fmtNum(v) : String(raw ?? '')}
                  </td>
                );
              })}
              {showMeta && <td className="mono faint meta-col">{String(e.sid ?? '')}</td>}
              {showMeta && <td className="mono faint meta-col num">{String(e.version ?? '')}</td>}
            </tr>
          ))}
        </tbody>
      </table>
      {hasMore && (
        <div className="rv-loadmore">
          <button type="button" className="rv-loadmore-btn" onClick={onLoadMore} disabled={isLoadingMore}>
            {isLoadingMore ? 'Loading…' : `Load more (${elements.length} shown)`}
          </button>
        </div>
      )}
    </div>
  );
}

function ChartView({ series, fields, activeField, setActiveField, hasTimeAxis, intervalMs, hasMore: _hasMore, onLoadMore: _onLoadMore, isLoadingMore: _isLoadingMore }: {
  series: readonly Series[];
  fields: readonly { field: string; fn?: string }[];
  activeField: string;
  setActiveField: (v: string) => void;
  hasTimeAxis: boolean;
  intervalMs?: number;
  hasMore: boolean;
  onLoadMore: () => void;
  isLoadingMore: boolean;
}) {
  const W = 900, H = 300;
  const padL = 54, padR = 16, padT = 16, padB = 28;
  const innerW = W - padL - padR, innerH = H - padT - padB;

  // Pick the active numeric field (already filtered in MeasureResultView)
  // Compute time + value bounds across all points
  const allPoints = series.flatMap((s) => s.points);
  if (allPoints.length === 0) return <ResultEmpty title="No data" text="There are no data points to plot for the selected field and time window." />;
  const times = allPoints.map((p) => p.t);
  const vals = allPoints.map((p) => p.v);
  const tMin = Math.min(...times), tMax = Math.max(...times);
  let vMin = Math.min(...vals), vMax = Math.max(...vals);
  if (vMin === vMax) { vMin -= 1; vMax += 1; }
  const vpad = (vMax - vMin) * 0.14 || 1;
  vMin = Math.max(0, Math.floor((vMin - vpad) / 50) * 50);
  vMax = Math.ceil((vMax + vpad) / 50) * 50;

  const X = (t: number) => padL + ((t - tMin) / (tMax - tMin || 1)) * innerW;
  const Y = (v: number) => padT + (1 - (v - vMin) / (vMax - vMin || 1)) * innerH;

  const yTicks = 4;
  const yLines: Array<{ v: number; y: number }> = [];
  for (let i = 0; i <= yTicks; i++) {
    const v = vMin + ((vMax - vMin) * i) / yTicks;
    yLines.push({ v, y: Y(v) });
  }
  const xLabels: Array<{ t: number; x: number; align: 'start' | 'middle' | 'end' }> = [];
  if (series.length > 0 && series[0].points.length > 0) {
    const n = series[0].points.length;
    xLabels.push({ t: tMin, x: X(tMin), align: 'start' });
    if (n > 2) xLabels.push({ t: series[0].points[Math.floor((n - 1) / 2)].t, x: X(series[0].points[Math.floor((n - 1) / 2)].t), align: 'middle' });
    xLabels.push({ t: tMax, x: X(tMax), align: 'end' });
  }
  // When timestamps are missing (e.g. MEAN aggregation returns no per-row ts),
  // label the x-axis with row indices instead of HH:MM.
  const fmtX = (t: number) => hasTimeAxis ? formatTs(t, { intervalMs }) : `#${Math.round(t)}`;

  const activeFieldLabel = (() => {
    const spec = fields.find((f) => f.field === activeField);
    if (!spec) return activeField;
    return spec.fn ? `${spec.fn}(${spec.field})` : spec.field;
  })();

  return (
    <div className="mchart-wrap">
      {fields.length > 1 && (
        <div className="mchart-fields">
          {fields.map((f) => (
            <button
              key={f.field}
              type="button"
              className={'mr-chip' + (activeField === f.field ? ' is-on' : '')}
              onClick={() => setActiveField(f.field)}
            >
              {f.fn ? `${f.fn}(${f.field})` : f.field}
            </button>
          ))}
        </div>
      )}
      <div className="mchart">
        <svg className="mc-svg" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="xMidYMid meet" role="img" aria-label={`${activeFieldLabel} over time`}>
          {yLines.map((g, i) => (
            <g key={`y${i}`}>
              <line x1={padL} y1={g.y} x2={W - padR} y2={g.y} stroke="rgba(255,255,255,.06)" />
              <text x={padL - 10} y={g.y + 4} textAnchor="end" className="mc-axis">{fmtNum(g.v)}</text>
            </g>
          ))}
          {xLabels.map((l, i) => (
            <text key={`x${i}`} x={l.x} y={H - 9} textAnchor={l.align} className="mc-axis">{fmtX(l.t)}</text>
          ))}
          {series.map((s) => {
            if (s.points.length === 0) return null;
            const d = s.points.map((p, i) => (i ? 'L' : 'M') + X(p.t).toFixed(1) + ' ' + Y(p.v).toFixed(1)).join(' ');
            const last = s.points[s.points.length - 1];
            return (
              <g key={s.key}>
                <path d={d} fill="none" stroke={s.color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
                <circle cx={X(last.t)} cy={Y(last.v)} r={3.1} fill={s.color} stroke="var(--panel)" strokeWidth={1.5} />
              </g>
            );
          })}
        </svg>
      </div>
      <div className="mlegend">
        {series.map((s) => {
          const vs = s.points.map((p) => p.v);
          const last = vs[vs.length - 1] ?? 0;
          const avg = vs.length > 0 ? vs.reduce((a, b) => a + b, 0) / vs.length : 0;
          const prev = vs.length >= 2 ? vs[vs.length - 2] : last;
          const dpct = prev ? ((last - prev) / prev) * 100 : 0;
          return (
            <div key={s.key} className="mlg-row">
              <span className="mlg-dot" style={{ background: s.color }} />
              <span className="mlg-name">{s.idLabel}</span>
              <span className="mlg-gap" />
              <span className="mlg-metric"><i>avg</i><span className="mono">{fmtNum(avg)}</span></span>
              <span className="mlg-metric"><i>last</i><span className="mono strong">{fmtNum(last)}</span></span>
              <span className={'mlg-delta ' + (dpct >= 0 ? 'up' : 'down')}>{fmtPct(dpct)}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}


