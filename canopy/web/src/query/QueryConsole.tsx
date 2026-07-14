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

// QueryConsole.tsx — dual-mode query console: visual builder ⇄ raw BydbQL.
// Ported from .handoff-import/banyandb/project/query-console.jsx.
// 348px builder rail + resizer, result-panel host, Builder/Code segmented
// toggle, Eject/Resync, dirty-warning modal on Resync, Run, deep-link seed.

import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useLocation } from 'react-router-dom';
import { QueryBuilder } from './QueryBuilder.js';
import { CodeEditor } from './CodeEditor.js';
import { MeasureResultView } from './results/MeasureResultView.js';
import { StreamResultView } from './results/StreamResultView.js';
import { TraceResultView } from './results/TraceResultView.js';
import { TopNResultView } from './results/TopNResultView.js';
import { ResultEmpty } from './results/ResultEmpty.js';
import { ResultError } from './results/ResultError.js';
import {
  buildBydbQL, qbDataCatalog, qbEmptyWhere, qbPruneWhere, qbNewCond,
  qbProtoCatalog, qbHasTraceIdFilter, qbHasTraceIdCondition,
  type QBBuilderState, type QB_CATALOG_VALUE,
} from './bydbql.js';
import { apiDataSource } from '../data/api.js';
import { useRunQuery } from '../data/hooks.js';
import type { Group, MeasureSchema, PropertySchema, QueryResponse, QueryRequest, StreamSchema, TraceSchema } from 'canopy-shared';

const QB_STORE = 'canopy.query.v3';
const QB_RAIL_DEFAULT = 348;
const QB_RAIL_MIN = 280;
const QB_RAIL_MAX = 720;

interface QuerySeed {
  readonly catalog: 'measures' | 'streams' | 'traces' | 'topn';
  readonly group: string;
  readonly resource: string;
}

function defaultState(groups: Group[]): QBBuilderState {
  const firstGroup = groups.find((g) => g.catalog === 'CATALOG_MEASURE') ?? groups[0];
  return {
    catalog: 'measures',
    group: firstGroup?.name ?? '',
    resource: '',
    select: [],
    projection: [],
    where: qbEmptyWhere(),
    groupBy: [],
    time: { mode: 'relative', rel: '-24h', from: '', to: '' },
    orderField: 'time',
    orderDir: 'DESC',
    limit: 100,
    offset: 0,
    trace: false,
    topN: 10,
    aggFn: '',
    fromAgg: null,
    fromResource: null,
  };
}

export function QueryConsole() {
  const location = useLocation();
  const seed = (location.state as { seed?: QuerySeed } | null)?.seed;

  const [groups, setGroups] = useState<Group[]>([]);
  const [groupsLoaded, setGroupsLoaded] = useState(false);
  const [groupsError, setGroupsError] = useState<string | null>(null);

  const [state, setState] = useState<QBBuilderState>(() => {
    try {
      const raw = localStorage.getItem(QB_STORE);
      if (raw) {
        const parsed = JSON.parse(raw) as { builder?: QBBuilderState };
        if (parsed?.builder) return parsed.builder;
      }
    } catch { /* ignore */ }
    return defaultState([]);
  });
  const [mode, setMode] = useState<'builder' | 'code'>('builder');
  const [code, setCode] = useState('');
  const [codeDirty, setCodeDirty] = useState(false);
  const [status, setStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle');
  // Paging state: accumulate elements across pages; reset when the query changes.
  const [paged, setPaged] = useState<{ elements: readonly Record<string, unknown>[]; offset: number; hasMore: boolean }>({ elements: [], offset: 0, hasMore: false });
  // Keep the latest raw response (for trace, groupStatuses, etc.) and derive the
  // view-ready response by overlaying the accumulated `paged.elements`.
  const [lastResp, setLastResp] = useState<QueryResponse | null>(null);
  const response = useMemo<QueryResponse | null>(() => {
    if (!lastResp) return null;
    return { ...lastResp, elements: paged.elements };
  }, [lastResp, paged.elements]);
  // Reset paging when the query inputs change. The state object is the
  // dependency — every setState(...) that mutates it (auto-pick, user
  // edits, builder <-> code toggle) triggers this once.
  useEffect(() => {
    setPaged({ elements: [], offset: 0, hasMore: false });
    setLastResp(null);
    setExecMs(undefined);
    setStatus('idle');
  }, [state, code, mode]);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [showTrace, setShowTrace] = useState(false);
  const [execMs, setExecMs] = useState<number | undefined>(undefined);
  const [railW, setRailW] = useState<number>(() => {
    try {
      const v = parseInt(localStorage.getItem('canopy.qb-rail-w') ?? '', 10);
      if (Number.isFinite(v)) return Math.min(QB_RAIL_MAX, Math.max(QB_RAIL_MIN, v));
    } catch { /* ignore */ }
    return QB_RAIL_DEFAULT;
  });
  const [confirmDiscard, setConfirmDiscard] = useState<'builder' | 'resync' | null>(null);
  // Builder post-run accordion: collapse clauses into summaries after the first run.
  const [builderCompact, setBuilderCompact] = useState(true);
  const [builderOpenSection, setBuilderOpenSection] = useState<string | null>(null);
  const [isResizing, setIsResizing] = useState(false);
  const railDragRef = useRef(false);

  // Load groups
  useEffect(() => {
    let cancelled = false;
    apiDataSource.listGroups().then(
      (resp) => { if (!cancelled) { setGroups(resp.groups); setGroupsLoaded(true); } },
      (err: unknown) => { if (!cancelled) { setGroupsError(err instanceof Error ? err.message : String(err)); setGroupsLoaded(true); } },
    );
    return () => { cancelled = true; };
  }, []);

  // Apply deep-link seed once groups are loaded
  useEffect(() => {
    if (!groupsLoaded || !seed) return;
    setState((cur) => ({
      ...cur,
      catalog: seed.catalog,
      group: seed.group,
      resource: seed.resource,
      fromResource: `${seed.group}/${seed.resource}`,
      fromAgg: null,
    }));
  }, [groupsLoaded, seed]);

  // Persist builder state
  useEffect(() => {
    try {
      localStorage.setItem(QB_STORE, JSON.stringify({ mode, builder: state, code, codeDirty }));
    } catch { /* quota */ }
  }, [mode, state, code, codeDirty]);

  const patch = (p: Partial<QBBuilderState>) => setState((s) => ({ ...s, ...p }));

  // Persist rail width
  useEffect(() => {
    try { localStorage.setItem('canopy.qb-rail-w', String(railW)); } catch { /* ignore */ }
  }, [railW]);

  // Trace queries require a trace_id equality filter. If the user picks a trace
  // resource and the WHERE tree has no trace_id condition at all, seed an empty
  // one so the builder never generates a query that BanyanDB will reject with 500.
  // We check for the condition (not the value) so a single seed doesn't fight
  // the user while they type in the value field.
  useEffect(() => {
    if (state.catalog !== 'traces' || !state.resource || qbHasTraceIdCondition(state.where)) return;
    patch({
      where: {
        combinator: 'AND',
        children: [{ tag: 'trace_id', op: 'BINARY_OP_EQ', value: '' }],
      },
    });
  }, [state.catalog, state.resource, state.where, patch]);

  // Handler for the From-row fuzzy search: pick any catalog/group/resource
  // combo, cascade-reset SELECT/WHERE/GROUP BY, and stamp fromResource so the
  // pre-filled banner shows. Mirrors the handoff's pickResource in
  // .handoff-import/banyandb/project/query-console.jsx.
  const onPickResource = (catalog: QB_CATALOG_VALUE, group: string, resource: string) => {
    const isMeasure = catalog === 'measures';
    // Trace queries require a trace_id equality filter: the default m4-traces
    // schema only indexes trace_id, so BanyanDB rejects queries without one.
    const defaultWhere = catalog === 'traces'
      ? { combinator: 'AND' as const, children: [{ tag: 'trace_id', op: 'BINARY_OP_EQ' as const, value: '' }] }
      : qbEmptyWhere();
    patch({
      catalog,
      group,
      resource,
      select: isMeasure ? [] : [],
      projection: isMeasure ? [] : [],
      where: defaultWhere,
      groupBy: [],
      // Trace queries filter by trace_id and the default m4 schema has no
      // order-able index, so leave orderField empty to avoid server-side errors.
      orderField: catalog === 'traces' ? '' : 'time',
      orderDir: 'DESC',
      fromResource: `${group}/${resource}`,
      fromAgg: null,
      offset: 0,
    });
  };

  const srcCat = qbDataCatalog(state.catalog);
  // srcCat is the data catalog ('measures' | 'streams' | 'traces'); map to
  // the BanyanDB proto enum used by group filtering. qbProtoCatalog returns
  // undefined for 'topn', but srcCat is already normalized away from that.
  const protoCatalog = qbProtoCatalog(srcCat);
  const filteredGroups = groups.filter((g) => g.catalog === protoCatalog);
  const groupNames = filteredGroups.map((g) => g.name);
  // Fetch resources for the current group
  // Keep the full resource objects — the SELECT row reads tagFamilies +
  // fields off currentResource to render the per-resource tag chips and
  // field options. Stripping to `{name}` only would leave the SELECT row
  // empty even though the live cluster had tag/field metadata.
  type ResourceWithSchema = (MeasureSchema | StreamSchema | TraceSchema | PropertySchema) & { name: string };
  const [resourceList, setResourceList] = useState<readonly ResourceWithSchema[]>([]);
  // Cached resource names keyed by `${dataCatalog}/${groupName}` so the From-row
  // fuzzy search can span every catalog at once. The per-group fetch below
  // primes the current entry; the prefetch effect after this block fills the
  // rest in the background.
  const [groupResources, setGroupResources] = useState<Map<string, readonly string[]>>(new Map());
  useEffect(() => {
    let cancelled = false;
    if (!state.group) { setResourceList([]); return; }
    const cat = srcCat;
    apiDataSource.listResourcesInGroup(cat, state.group).then(
      (rs) => {
        if (!cancelled) {
          // Normalize once: listResourcesInGroup returns the typed union
          // but for measures we know it's MeasureSchema-shaped.
          setResourceList(
            rs.map((r) => ({ ...(r as object), name: (r as { metadata: { name: string } }).metadata.name })) as unknown as readonly ResourceWithSchema[],
          );
          setGroupResources((prev) => {
            const next = new Map(prev);
            next.set(
              `${cat}/${state.group}`,
              rs.map((r) => (r as { metadata: { name: string } }).metadata.name),
            );
            return next;
          });
        }
      },
      () => { if (!cancelled) setResourceList([]); },
    );
    return () => { cancelled = true; };
  }, [state.group, srcCat]);

  // Pre-fetch resources for every group so the From-row fuzzy search has the
  // full index as soon as possible. Runs once per `groups` change.
  useEffect(() => {
    if (!groupsLoaded || groups.length === 0) return;
    let cancelled = false;
    const fetches = groups.map(async (g) => {
      let dataCat: 'measures' | 'streams' | 'traces' | null = null;
      switch (g.catalog) {
        case 'CATALOG_MEASURE': dataCat = 'measures'; break;
        case 'CATALOG_STREAM': dataCat = 'streams'; break;
        case 'CATALOG_TRACE': dataCat = 'traces'; break;
        default: return null; // PROPERTY / UNSPECIFIED — skip
      }
      const rs = await apiDataSource.listResourcesInGroup(dataCat, g.name);
      return {
        key: `${dataCat}/${g.name}`,
        names: rs.map((r) => (r as { metadata: { name: string } }).metadata.name).sort(),
      };
    });
    Promise.allSettled(fetches).then((results) => {
      if (cancelled) return;
      setGroupResources((prev) => {
        const next = new Map(prev);
        for (const r of results) {
          if (r.status === 'fulfilled' && r.value) next.set(r.value.key, r.value.names);
        }
        return next;
      });
    });
    return () => { cancelled = true; };
  }, [groupsLoaded, groups]);

  const currentResource = resourceList.find((r) => r.name === state.resource);
  // Field / tag lists — stream/measure carry tagFamilies; trace/property carry a flat tags array.
  const tags: string[] = useMemo(() => {
    if (!currentResource) return [];
    if ('tagFamilies' in currentResource && currentResource.tagFamilies) {
      return currentResource.tagFamilies.flatMap((f) => (f.tags ?? []).map((t) => t.name));
    }
    if ('tags' in currentResource && currentResource.tags) {
      return currentResource.tags.map((t) => t.name);
    }
    return [];
  }, [currentResource]);
  const fields: string[] = useMemo(() => {
    const r = currentResource as { fields?: { name: string }[] } | undefined;
    return r?.fields?.map((f) => f.name) ?? [];
  }, [currentResource]);
  // Tag specs carry the schema type so the stream result view can infer roles
  // from storage type (TAG_TYPE_STRING/INT/DATA_BINARY/...) instead of guessing.
  const tagSpecs = useMemo(() => {
    if (!currentResource) return [];
    if ('tagFamilies' in currentResource && currentResource.tagFamilies) {
      return currentResource.tagFamilies.flatMap((f) => (f.tags ?? []).map((t) => ({ name: t.name, type: t.type })));
    }
    if ('tags' in currentResource && currentResource.tags) {
      return currentResource.tags.map((t) => ({ name: t.name, type: t.type }));
    }
    return [];
  }, [currentResource]);

  // Generated BydbQL: must be declared after `tags`/`fields` because measure
  // queries expand "all tags" to the explicit tag list.
  const generated = useMemo(() => buildBydbQL(state, tags), [state, tags]);
  const codeEdited = codeDirty && code.trim() !== '' && code.trim() !== generated.trim();

  // Re-prune WHERE tags when resource changes
  useEffect(() => {
    setState((s) => ({ ...s, where: qbPruneWhere(s.where, tags) }));
  }, [state.resource]); // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-pick a group once groups load so the rail renders real content
  // on initial load instead of "— none —". Prefers the canonical handoff
  // group `sw_metric` (with `service_cpm_minute` selected below) so the
  // rail matches the handoff's /#/query baseline; otherwise falls back to
  // the alphabetically-first measure group. Skipped if the user (or a
  // deep-link seed) already picked a group.
  useEffect(() => {
    if (!groupsLoaded) return;
    if (state.group) return;
    const measureGroups = groups.filter((g) => g.catalog === 'CATALOG_MEASURE');
    const canonical = measureGroups.find((g) => g.name === 'sw_metric');
    const first = canonical ?? [...measureGroups].sort((a, b) => a.name.localeCompare(b.name))[0];
    if (!first) return;
    patch({ group: first.name });
  }, [groupsLoaded, groups, state.group]);

  // Auto-select the canonical resource of the chosen group so the rail
  // renders real content instead of "— none —" on initial load. Prefers
  // `service_cpm_minute` (the handoff-canonical measure) when present so
  // the rail matches the handoff's `sw_metric / service_cpm_minute`
  // baseline. Skips the auto-generated `_top_n_result` table the topn
  // registry creates alongside the user's measures. Skipped if the user
  // (or a deep-link seed) already picked a resource.
  useEffect(() => {
    if (!state.resource && resourceList.length > 0) {
      const canonical = resourceList.find((r) => r.name === 'service_cpm_minute');
      if (canonical) { patch({ resource: canonical.name }); return; }
      const candidates = resourceList
        .filter((r) => r.name !== '_top_n_result')
        .map((r) => r.name)
        .sort();
      const picked = candidates[0] ?? resourceList[0]?.name ?? '';
      if (picked) patch({ resource: picked });
    }
  }, [resourceList, state.resource]);

  // Auto-pick a field with MEAN when a measure is selected and no field is
  // set yet. Mirrors the handoff's qbDefaultSelect so the chart view is
  // available immediately on resource pick. Skipped for Top-N (which uses
  // its own topN/aggFn fields, not select[]) and for users who have
  // already added a field.
  useEffect(() => {
    if (state.catalog !== 'measures') return;
    if (!state.resource) return;
    if ((state.select ?? []).length > 0) return;
    const f = fields[0];
    if (!f) return;
    patch({ select: [{ field: f, fn: 'MEAN' }] });
  }, [state.catalog, state.resource, fields, state.select]);

  const runMutation = useRunQuery();

  // Surface invalid absolute time ranges (TO <= FROM) as an inline error
  // and disable Run so the user can't fire a query that will return no
  // rows or fail server-side. The validation only fires when both FROM
  // and TO are set; an open-ended range (only one bound) is valid.
  const timeRangeError: string | null = (() => {
    const { mode, from, to } = state.time;
    if (mode !== 'absolute' || !from || !to) return null;
    if (from >= to) return 'TO must be later than FROM.';
    return null;
  })();

  const run = async (loadMore = false) => {
    if (timeRangeError) {
      setErrorMsg(timeRangeError);
      setStatus('error');
      return;
    }
    // Trace queries require a trace_id equality filter. The default m4-traces
    // schema only indexes trace_id; without it BanyanDB returns 500.
    if (mode === 'builder' && state.catalog === 'traces' && !qbHasTraceIdFilter(state.where)) {
      setErrorMsg('Trace queries require a trace_id filter because trace_id is the only indexed tag.');
      setStatus('error');
      return;
    }
    if (runMutation.isPending) return;
    // For paging, the query string needs to carry the right OFFSET. In
    // builder mode we rebuild it with an override; in code mode the user
    // owns the query string and we add an OFFSET suffix if not present.
    const effectiveOffset = loadMore ? paged.offset : 0;
    let activeQuery = mode === 'code' ? code : generated;
    if (loadMore && effectiveOffset > 0) {
      // Append an OFFSET if the user's code query doesn't already have one.
      if (!/\bOFFSET\s+\d+\b/i.test(activeQuery)) {
        activeQuery = `${activeQuery.replace(/;\s*$/, '')}\nOFFSET ${effectiveOffset}`;
      }
    }
    if (!activeQuery.trim()) {
      setErrorMsg('Empty query.');
      setStatus('error');
      return;
    }
    setStatus('running');
    setErrorMsg(null);
    // BanyanDB's BydbQL gateway accepts a single { query: string } and parses
    // it server-side. For TopN, dispatch to /v1/measure/topn with the
    // structured TopNRequest instead.
    const req: QueryRequest = state.catalog === 'topn'
      ? {
          query: activeQuery,
          topN: {
            groups: state.group ? [state.group] : [],
            name: state.resource,
            top_n: state.topN,
            agg: state.aggFn ? { function: state.aggFn, field_name: '' } : undefined,
            field_value_sort: state.orderDir === 'ASC' ? 'SORT_ASC' : 'SORT_DESC',
            time_range: state.time.mode === 'absolute' && (state.time.from || state.time.to)
              ? { begin: state.time.from, end: state.time.to }
              : undefined,
            trace: state.trace,
          },
        }
      : {
          query: activeQuery,
        };
    try {
      const t0 = performance.now();
      const resp = await runMutation.mutateAsync(req);
      const t1 = performance.now();
      const newElements = (resp.elements ?? []) as readonly Record<string, unknown>[];
      setLastResp(resp);
      setExecMs(t1 - t0);
      // Track accumulated elements + next offset; show "Load more" only when
      // the last paged returned a full paged (suggesting more rows exist).
      setPaged((prev) => {
        const merged = loadMore ? [...prev.elements, ...newElements] : newElements;
        return {
          elements: merged,
          offset: effectiveOffset + (state.limit > 0 ? state.limit : newElements.length),
          hasMore: newElements.length > 0 && newElements.length >= (state.limit > 0 ? state.limit : 1),
        };
      });
      setShowTrace(!!state.trace);
      setStatus('done');
    } catch (err) {
      setErrorMsg(err instanceof Error ? err.message : String(err));
      setStatus('error');
    }
  };

  // Cmd/Ctrl+Enter to run
  const runRef = useRef(run);
  runRef.current = run;
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); runRef.current(); }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  const startRailDrag = (e: React.PointerEvent) => {
    e.preventDefault();
    const startX = e.clientX;
    const startW = railW;
    setIsResizing(true);
    railDragRef.current = true;
    document.body.classList.add('qb-resizing');
    const move = (ev: PointerEvent) => {
      const w = Math.min(QB_RAIL_MAX, Math.max(QB_RAIL_MIN, startW + (ev.clientX - startX)));
      setRailW(w);
    };
    const up = () => {
      setIsResizing(false);
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
      document.body.classList.remove('qb-resizing');
    };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
  };

  const ejectToCode = () => { setCode(generated); setCodeDirty(false); setMode('code'); };
  const backToBuilder = () => {
    if (codeEdited) { setConfirmDiscard('builder'); return; }
    setCodeDirty(false); setMode('builder');
  };
  const resync = () => {
    if (codeEdited) { setConfirmDiscard('resync'); return; }
    setCode(generated); setCodeDirty(false);
  };
  const doDiscard = () => {
    if (confirmDiscard === 'builder') { setCodeDirty(false); setMode('builder'); }
    else { setCode(generated); setCodeDirty(false); }
    setConfirmDiscard(null);
  };

  return (
    <div className="query-body">
      <div className="page-head qb-page-head">
        <div className="page-title-row">
          <div>
            <h1 className="page-title">Query</h1>
            <p className="page-meta">Compose BydbQL visually, or eject to raw code for advanced refinement</p>
          </div>
          <div className="page-actions">
            <div className="qb-mode-seg" role="tablist" aria-label="Query mode">
              <button role="tab" className={'qb-mode-btn' + (mode === 'builder' ? ' is-on' : '')} onClick={() => (mode === 'builder' ? undefined : backToBuilder())}>
                Builder
              </button>
              <button role="tab" className={'qb-mode-btn' + (mode === 'code' ? ' is-on' : '')} onClick={() => (mode === 'code' ? undefined : ejectToCode())}>
                Code
              </button>
            </div>
          </div>
        </div>
      </div>

      <div className="qb-split" style={{ ['--qb-rail-w' as never]: `${railW}px` }}>
        <div className="qb-rail">
          {mode === 'builder' ? (
            <>
              <QueryBuilder
                state={state}
                onChange={patch}
                tags={tags}
                fields={fields}
                groupNames={groupNames}
                resourceNames={resourceList.map((r) => r.name)}
                groups={groups}
                groupResources={groupResources}
                onPickResource={onPickResource}
                isRunning={runMutation.isPending}
                onEjectToCode={() => { setCode(generated); setCodeDirty(false); setMode('code'); }}
                onRun={run}
                hasRun={status === 'done'}
                compact={builderCompact}
                setCompact={setBuilderCompact}
                openSection={builderOpenSection}
                setOpenSection={setBuilderOpenSection}
              />
              {codeEdited && (
                <div className="qb-rail-foot">
                  <span className="qb-rail-note">Code edited · use Resync to pull builder changes in</span>
                </div>
              )}
            </>
          ) : (
            <>
              <CodeEditor
                value={code}
                onChange={(v) => { setCode(v); setCodeDirty(true); }}
                hint={codeDirty ? 'edited' : 'from builder'}
                toolbarRight={(
                  <button type="button" className="btn btn-ghost" onClick={backToBuilder} title="Back to builder">
                    <span aria-hidden="true">←</span> Builder
                  </button>
                )}
              />
              <div className="qb-foot">
                <div className="qb-foot-row">
                  <button
                    type="button"
                    className="btn btn-ghost"
                    onClick={resync}
                    title={codeEdited ? 'Regenerate from the builder — asks before overwriting your edits' : 'Regenerate from the builder'}
                  >
                    <span aria-hidden="true">≡</span> Re-sync
                  </button>
                  <span className="qb-gap" />
                  <button
                    type="button"
                    className="btn btn-primary"
                    disabled={status === 'running'}
                    onClick={() => run(false)}
                  >
                    <span aria-hidden="true">▶</span>
                    {status === 'running' ? 'Running…' : 'Run'}
                    <kbd className="kbd">{typeof navigator !== 'undefined' && /Mac/i.test(navigator.platform) ? '⌘↵' : 'Ctrl↵'}</kbd>
                  </button>
                </div>
              </div>
            </>
          )}
          {groupsError && <div className="qb-error">Could not load groups: {groupsError}</div>}
        </div>

        <div
          className={'qb-resizer' + (isResizing ? ' is-dragging' : '')}
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize builder rail"
          tabIndex={0}
          onPointerDown={startRailDrag}
          onKeyDown={(e) => {
            if (e.key === 'ArrowLeft') { e.preventDefault(); setRailW((w) => Math.max(QB_RAIL_MIN, w - 16)); }
            else if (e.key === 'ArrowRight') { e.preventDefault(); setRailW((w) => Math.min(QB_RAIL_MAX, w + 16)); }
          }}
        >
          <div className="qb-resizer-grip" />
        </div>

        <div className="qb-results">
          {status === 'idle' && (
            <div className="result-card">
              <div className="result-bar">
                <span className="result-tab is-active">Result</span>
                <span className="result-tab">Trace</span>
                <span className="result-status"><span className="rs idle">not run</span></span>
              </div>
              <div className="result-pane">
                <ResultEmpty
                  title="No results yet"
                  text="Build or write a query above, then run it to inspect rows and the execution trace."
                />
              </div>
            </div>
          )}
          {status === 'running' && !response && (
            <div className="result-card">
              <div className="result-bar">
                <span className="result-tab is-active">Result</span>
                <span className="result-tab">Trace</span>
                <span className="result-status"><span className="rs run">executing…</span></span>
              </div>
              <div className="result-pane">
                <ResultEmpty
                  title="Running query…"
                  text="Executing against the cluster."
                />
              </div>
            </div>
          )}
          {status === 'error' && errorMsg && (
            <div className="result-card">
              <div className="result-bar">
                <span className="result-tab is-active">Result</span>
                <span className="result-tab">Trace</span>
                <span className="result-status"><span className="rs fail">failed</span></span>
              </div>
              <div className="result-pane">
                <ResultError message={errorMsg} onRetry={() => run(false)} />
              </div>
            </div>
          )}
          {response && (
            <>
              <ResultViewRouter
                state={state}
                response={response}
                showTrace={showTrace}
                setShowTrace={setShowTrace}
                hasMore={paged.hasMore}
                onLoadMore={() => run(true)}
                isLoadingMore={runMutation.isPending}
                execMs={execMs}
                tags={tags}
                tagSpecs={tagSpecs}
                interval={currentResource?.interval}
              />
              {response.truncated && (
                <div className="qb-trunc">
                  showing first {response.elements?.length ?? 0} of {response.totalRowCount ?? '?'} rows
                </div>
              )}
            </>
          )}
        </div>
      </div>

      {confirmDiscard && (
        <div className="modal-backdrop" role="dialog" aria-modal="true" aria-labelledby="qb-discard-title">
          <div className="modal">
            <h2 id="qb-discard-title" className="modal-title">
              {confirmDiscard === 'builder' ? 'Discard code edits?' : 'Discard code edits and resync?'}
            </h2>
            <p className="modal-body">The code has been edited manually. {confirmDiscard === 'builder' ? 'Returning to the builder' : 'Resyncing'} will discard those edits.</p>
            <div className="modal-actions">
              <button type="button" className="qb-btn qb-btn-ghost" onClick={() => setConfirmDiscard(null)}>Keep edits</button>
              <button type="button" className="qb-btn qb-btn-danger" onClick={doDiscard}>Discard</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function ResultViewRouter({ state, response, showTrace, setShowTrace, hasMore, onLoadMore, isLoadingMore, execMs, tags, tagSpecs, interval }: {
  state: QBBuilderState;
  response: QueryResponse;
  showTrace: boolean;
  setShowTrace: (v: boolean) => void;
  hasMore: boolean;
  onLoadMore: () => void;
  isLoadingMore: boolean;
  execMs?: number;
  tags?: readonly string[];
  tagSpecs?: readonly { readonly name: string; readonly type: string }[];
  interval?: string;
}) {
  switch (state.catalog) {
    case 'topn':
      return <TopNResultView response={response} showTrace={showTrace} setShowTrace={setShowTrace} execMs={execMs} />;
    case 'measures':
      return <MeasureResultView
        response={response}
        state={state}
        showTrace={showTrace}
        setShowTrace={setShowTrace}
        hasMore={hasMore}
        onLoadMore={onLoadMore}
        isLoadingMore={isLoadingMore}
        execMs={execMs}
        tags={tags}
        interval={interval}
      />;
    case 'streams':
      return <StreamResultView response={response} state={state} showTrace={showTrace} setShowTrace={setShowTrace} execMs={execMs} tagSpecs={tagSpecs} hasMore={hasMore} onLoadMore={onLoadMore} isLoadingMore={isLoadingMore} />;
    case 'traces':
      return <TraceResultView response={response} state={state} showTrace={showTrace} setShowTrace={setShowTrace} execMs={execMs} tagSpecs={tagSpecs} hasMore={hasMore} onLoadMore={onLoadMore} isLoadingMore={isLoadingMore} />;
    default:
      return <div className="qb-empty"><p>Unsupported catalog: {state.catalog}</p></div>;
  }
}

// Re-export to keep tree-shaking honest
export { qbNewCond };