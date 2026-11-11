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

import React, { useEffect, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import type { Group, MonitorNode, MonitorSnapshot } from 'canopy-shared';

import { apiDataSource } from '../data/api.js';
import { fetchMonitorSnapshot, MonitorError } from '../data/monitor.js';

// Warn and danger thresholds from the Monitor design; disk danger matches STATUS_DISK_FULL at 95%.
const CPU = [70, 90] as const;
const MEMORY = [75, 90] as const;
const DISK = [85, 95] as const;
const SLOW_MS = 150;

const REFRESH = [
  { label: '10s', ms: 10_000 },
  { label: '30s', ms: 30_000 },
  { label: '1m', ms: 60_000 },
  { label: 'Off', ms: 0 },
] as const;

type Level = 'ok' | 'warn' | 'danger';

const level = (pct: number | undefined, [warn, danger]: readonly [number, number]): Level =>
  pct === undefined ? 'ok' : pct >= danger ? 'danger' : pct >= warn ? 'warn' : 'ok';

const pct = (ratio: number | undefined) => (ratio === undefined ? undefined : Math.round(ratio * 100));

export function formatBytes(bytes: number | undefined): string {
  if (bytes === undefined) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let v = bytes;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v >= 100 || i === 0 ? Math.round(v) : v.toFixed(1)} ${units[i]}`;
}

export function formatRate(perSecond: number | undefined): string {
  if (perSecond === undefined) return '—';
  return perSecond >= 1000 ? `${(perSecond / 1000).toFixed(1)}k/s` : `${perSecond >= 10 ? Math.round(perSecond) : perSecond.toFixed(1)}/s`;
}

export function formatUptime(seconds: number | undefined): string {
  if (seconds === undefined) return '—';
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return d ? `${d}d ${h}h` : h ? `${h}h ${m}m` : `${m}m`;
}

const formatMs = (ms: number | undefined) => (ms === undefined ? '—' : `${ms >= 10 ? Math.round(ms) : ms.toFixed(1)}ms`);

// Each path reports its filesystem's usage, so paths on one filesystem report the same
// numbers; count those once instead of adding the same disk up per path.
function diskTotals(node: MonitorNode) {
  const filesystems = new Map(node.disk.map((d) => [`${d.usedBytes}/${d.totalBytes}`, d]));
  const used = [...filesystems.values()].reduce((a, d) => a + d.usedBytes, 0);
  const total = [...filesystems.values()].reduce((a, d) => a + d.totalBytes, 0);
  return { used, total, pct: total ? Math.round((used / total) * 100) : undefined };
}

function nodeHealth(node: MonitorNode) {
  const cpu = node.cpuCores && node.cpuUsedCores !== undefined ? Math.round((node.cpuUsedCores / node.cpuCores) * 100) : undefined;
  const memory = pct(node.memoryUsedRatio);
  const disk = diskTotals(node).pct;
  const warn = (disk ?? 0) >= DISK[0] || (memory ?? 0) >= MEMORY[0];
  return { cpu, memory, disk, warn };
}

function Bar({ label, value }: { label: string; value: number | undefined; }) {
  return (
    <>
      <span className="mon-bar-k">{label}</span>
      <span className="mon-bar" aria-hidden="true">
        <span className="mon-bar-fill" data-level={level(value, label === 'cpu' ? CPU : label === 'disk' ? DISK : MEMORY)} style={{ width: `${Math.min(value ?? 0, 100)}%` }} />
      </span>
      <span className="mon-bar-v">{value === undefined ? '—' : `${value}%`}</span>
    </>
  );
}

function Sparkline({ points, max }: { points: MonitorNode['cpuHistory']; max: number }) {
  if (points.length < 2) return <p className="mon-faint">Collecting samples…</p>;
  const w = 300;
  const h = 56;
  const t0 = points[0].t;
  const span = points[points.length - 1].t - t0 || 1;
  const xy = points.map((p) => `${(((p.t - t0) / span) * w).toFixed(1)} ${(h - 2 - (Math.min(p.v, max) / max) * (h - 4)).toFixed(1)}`);
  const line = `M${xy.join(' L')}`;
  return (
    <svg className="mon-spark" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" role="img" aria-label="CPU cores used, last 15 minutes">
      <path d={`${line} L${w} ${h} L0 ${h} Z`} className="mon-spark-area" />
      <path d={line} className="mon-spark-line" />
    </svg>
  );
}

function NodeDrawer({ node, onClose }: { node: MonitorNode; onClose: () => void }) {
  const health = nodeHealth(node);
  const disk = diskTotals(node);
  const closeRef = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    closeRef.current?.focus();
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);
  return (
    <aside className="mon-drawer" role="dialog" aria-label={`Node ${node.id}`}>
      <div className="mon-drawer-head">
        <div>
          <div className="mon-drawer-title"><span className="mon-dot" data-level={health.warn ? 'warn' : 'ok'} />{node.id}</div>
          <div className="mon-faint">standalone · {health.warn ? 'Degraded' : 'Healthy'}</div>
        </div>
        <button ref={closeRef} className="btn btn-ghost" onClick={onClose} aria-label="Close node details">✕</button>
      </div>
      <dl className="mon-stats">
        <div><dt>Uptime</dt><dd>{formatUptime(node.uptimeSeconds)}</dd></div>
        <div><dt>CPU</dt><dd>{node.cpuUsedCores === undefined ? '—' : node.cpuUsedCores.toFixed(1)} / {node.cpuCores ?? '—'} cores</dd></div>
        <div><dt>Memory</dt><dd>{formatBytes(node.rssBytes)} RSS · {health.memory ?? '—'}%</dd></div>
        <div><dt>Disk</dt><dd>{formatBytes(disk.used)} · {disk.pct ?? '—'}%</dd></div>
        <div><dt>Goroutines</dt><dd>{node.goroutines?.toLocaleString('en-US') ?? '—'}</dd></div>
        <div><dt>Heap in use</dt><dd>{formatBytes(node.heapInuseBytes)}</dd></div>
      </dl>
      <h3 className="mon-h3">CPU cores · last 15m</h3>
      <Sparkline points={node.cpuHistory} max={node.cpuCores ?? 1} />
      <h3 className="mon-h3">Disk by path</h3>
      {node.disk.map((d) => {
        const p = d.totalBytes ? Math.round((d.usedBytes / d.totalBytes) * 100) : undefined;
        return (
          <div key={d.path} className="mon-path">
            <span className="mon-path-name" title={d.path}>{shortPath(d.path)}</span>
            <span className="mon-bar" aria-hidden="true"><span className="mon-bar-fill" data-level={level(p, DISK)} style={{ width: `${p ?? 0}%` }} /></span>
            <span className="mon-path-v">{formatBytes(d.usedBytes)} / {formatBytes(d.totalBytes)}</span>
          </div>
        );
      })}
    </aside>
  );
}

// Data paths share long prefixes, so keep the end, which is where they differ.
export function shortPath(path: string): string {
  const parts = path.split('/').filter(Boolean);
  return parts.length > 2 ? `…/${parts.slice(-2).join('/')}` : path;
}

function groupRows(snapshot: MonitorSnapshot, groups: readonly Group[]) {
  const sizes = new Map(snapshot.groups.map((g) => [g.name, g.bytes]));
  return groups
    .filter((g) => g.catalog !== 'CATALOG_PROPERTY')
    .map((g) => ({
      name: g.name,
      catalog: g.catalog.replace('CATALOG_', '').toLowerCase(),
      shards: g.resourceOpts.shardNum,
      size: sizes.get(g.name),
    }));
}

export function MonitorPage() {
  const [refresh, setRefresh] = useState<(typeof REFRESH)[number]>(REFRESH[0]);
  const [selected, setSelected] = useState<string | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const snapshot = useQuery<MonitorSnapshot, MonitorError>({
    queryKey: ['monitor-snapshot'],
    queryFn: fetchMonitorSnapshot,
    refetchInterval: (query) => (query.state.error?.status === 403 || !refresh.ms ? false : refresh.ms),
    retry: false,
  });
  const groups = useQuery({ queryKey: ['groups'], queryFn: () => apiDataSource.listGroups(), enabled: snapshot.isSuccess });
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  const error = snapshot.error;
  const data = snapshot.data;
  const blocked = error?.status === 403;
  const unreachable = !!error && !blocked;
  const node = data?.nodes[0];
  const health = node && nodeHealth(node);
  const badge = unreachable ? 'unknown' : health?.warn ? 'degraded' : data ? 'healthy' : undefined;
  const disk = node && diskTotals(node);
  const selectedNode = data?.nodes.find((n) => n.id === selected);
  const ago = data ? Math.max(0, Math.round((now - snapshot.dataUpdatedAt) / 1000)) : undefined;

  return (
    <div className="page-body mon-page">
      <header className="page-head">
        <div className="crumbs">
          <span className="crumb">Monitor</span>
          <span className="crumb-sep">/</span>
          <span className="crumb is-last">Standalone</span>
        </div>
        <div className="page-title-row">
          <h1 className="page-title">
            Node monitor
            {badge && <span className="mon-badge" data-level={badge === 'healthy' ? 'ok' : badge === 'degraded' ? 'warn' : 'faint'}>{badge}</span>}
          </h1>
          {!blocked && (
            <div className="mon-refresh">
              {ago !== undefined && <span className="mon-faint">updated {ago}s ago</span>}
              <div className="mon-seg" role="group" aria-label="Auto refresh">
                {REFRESH.map((r) => (
                  <button key={r.label} className={r === refresh ? 'is-active' : ''} aria-pressed={r === refresh} onClick={() => setRefresh(r)}>{r.label}</button>
                ))}
              </div>
              <button className="btn btn-ghost" onClick={() => void snapshot.refetch()} disabled={snapshot.isFetching}>Refresh</button>
            </div>
          )}
        </div>
        <p className="page-meta">Node health and storage, scraped from the node&apos;s metrics endpoint{data && <> <span className="mon-mono">{data.source}</span></>}</p>
      </header>

      {snapshot.isPending && <p className="mon-faint" role="status">Loading node metrics…</p>}

      {blocked && (
        <div className="mon-notice" role="alert">
          <div className="mon-notice-title">Monitor is limited to administrators</div>
          <p>Your account is read-only. An operator can open this page to read-only users by setting <span className="mon-mono">CANOPY_MONITOR_ROLES=admin,readonly</span> on the Canopy server.</p>
        </div>
      )}

      {unreachable && (
        <div className="mon-notice" data-level="danger" role="alert">
          <div>
            <div className="mon-notice-title">Cannot reach the node&apos;s metrics endpoint</div>
            <p>{error.message}. Schema and query pages are unaffected. Check <span className="mon-mono">MONITOR_TARGET</span> and the BanyanDB process.{refresh.ms ? ` Retrying every ${refresh.label}.` : ''}</p>
            {data && <p className="mon-faint">Last successful scrape {new Date(snapshot.dataUpdatedAt).toLocaleTimeString()}</p>}
          </div>
          <button className="btn" onClick={() => void snapshot.refetch()}>Retry now</button>
        </div>
      )}

      {node && health && disk && !blocked && (
        <>
          <div className="mon-fleet">
            <div><span className="mon-k">Reporting</span><span className="mon-v">{unreachable ? '0' : '1'} / 1</span></div>
            <div><span className="mon-k">Mode</span><span className="mon-v">standalone</span></div>
            <div><span className="mon-k">CPU</span><span className="mon-v">{node.cpuUsedCores?.toFixed(1) ?? '—'}<small> / {node.cpuCores ?? '—'} cores</small></span></div>
            <div><span className="mon-k">Memory (RSS)</span><span className="mon-v">{formatBytes(node.rssBytes)}</span></div>
            <div><span className="mon-k">Disk</span><span className="mon-v">{formatBytes(disk.used)}<small> / {formatBytes(disk.total)}</small></span></div>
          </div>

          <section className="mon-section">
            <div className="mon-section-head">
              <span className="mon-section-title">Topology <span className="mon-faint">· standalone</span></span>
              <span className="mon-faint">rate · avg latency · click the node</span>
            </div>
            <button className="mon-node" data-level={health.warn ? 'warn' : 'ok'} onClick={() => setSelected(node.id)}>
              <span className="mon-node-id"><span className="mon-dot" data-level={health.warn ? 'warn' : 'ok'} />{node.id}<span className="mon-role">standalone</span></span>
              <span className="mon-bars">
                <Bar label="cpu" value={health.cpu} />
                <Bar label="memory" value={health.memory} />
                <Bar label="disk" value={health.disk} />
              </span>
              <span className="mon-work">
                <span className="mon-k">write</span><span>{formatRate(node.write.perSecond)}</span><span />
                <span className="mon-k">query</span><span>{formatRate(node.query.perSecond)}</span>
                <span data-level={(node.query.avgLatencyMs ?? 0) >= SLOW_MS ? 'warn' : undefined}>{formatMs(node.query.avgLatencyMs)}</span>
                <span className="mon-k">error</span><span data-level={node.errorsPerMinute ? 'danger' : undefined}>{node.errorsPerMinute === undefined ? '—' : `${node.errorsPerMinute.toFixed(node.errorsPerMinute >= 10 ? 0 : 1)}/min`}</span><span />
              </span>
            </button>
            <p className="mon-faint">
              Liaison and data roles run in one process. There is no tier-2 publish and no shard distribution to watch; storage and workload come from this node&apos;s own <span className="mon-mono">/metrics</span>. Uptime {formatUptime(node.uptimeSeconds)}.
            </p>
          </section>

          <section className="mon-section">
            <div className="mon-section-head">
              <span className="mon-section-title">Storage <span className="mon-faint">· single node</span></span>
              <span className="mon-faint">writes are rejected with STATUS_DISK_FULL at 95%</span>
            </div>
            <div className="mon-storage">
              <span>standalone · 1 node</span>
              <span className="mon-bar" aria-hidden="true"><span className="mon-bar-fill" data-level={level(disk.pct, DISK)} style={{ width: `${disk.pct ?? 0}%` }} /></span>
              <span>{formatBytes(disk.used)} / {formatBytes(disk.total)} · {disk.pct ?? '—'}%</span>
            </div>
            {groups.data && (
              <table className="mon-table">
                <thead><tr><th>Group</th><th>Catalog</th><th>Shards</th><th className="num">Size</th></tr></thead>
                <tbody>
                  {groupRows(data, groups.data.groups).map((g) => (
                    <tr key={g.name}><td>{g.name}</td><td>{g.catalog}</td><td>{g.shards}</td><td className="num">{formatBytes(g.size)}</td></tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>
        </>
      )}

      {selectedNode && <NodeDrawer node={selectedNode} onClose={() => setSelected(null)} />}
    </div>
  );
}
