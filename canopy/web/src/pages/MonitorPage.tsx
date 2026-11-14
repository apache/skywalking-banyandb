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

import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import type { Group, MonitorNode, MonitorSnapshot } from 'canopy-shared';

import { apiDataSource } from '../data/api.js';
import { fetchMonitorSnapshot, MonitorError } from '../data/monitor.js';
import { useFocusTrap } from '../components/modal-utils.js';

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
type Status = Level | 'unknown' | 'stale';

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

// Paths can share a filesystem and nothing in the metrics says which do, so sums over paths
// may count one disk more than once. Pressure comes from the fullest path, which is exact.
function diskUsage(node: MonitorNode) {
  const used = node.disk.reduce((a, d) => a + d.usedBytes, 0);
  const total = node.disk.reduce((a, d) => a + d.totalBytes, 0);
  const fullest = node.disk.length
    ? Math.max(...node.disk.map((d) => (d.totalBytes ? Math.round((d.usedBytes / d.totalBytes) * 100) : 0)))
    : undefined;
  return { used, total, fullest };
}

const STATUS_LABELS: Record<Status, string> = {
  ok: 'Healthy', warn: 'Degraded', danger: 'Critical', unknown: 'Unknown', stale: 'Stale',
};

export function nodeHealth(node: MonitorNode) {
  const cpu = node.cpuCores && node.cpuUsedCores !== undefined ? Math.round((node.cpuUsedCores / node.cpuCores) * 100) : undefined;
  const memory = pct(node.memoryUsedRatio);
  const disk = diskUsage(node).fullest;
  const levels = [level(cpu, CPU), level(memory, MEMORY), level(disk, DISK), node.errorsPerMinute ? 'warn' : 'ok'];
  // Without CPU, memory and disk readings there is nothing to call healthy yet.
  const status: Status = cpu === undefined || memory === undefined || disk === undefined
    ? 'unknown'
    : levels.includes('danger') ? 'danger' : levels.includes('warn') ? 'warn' : 'ok';
  return { cpu, memory, disk, status };
}

const dotLevel = (status: Status) => (status === 'unknown' || status === 'stale' ? 'faint' : status);

function Bar({ label, value, title }: { label: string; value: number | undefined; title?: string }) {
  return (
    <>
      <span className="mon-bar-k" title={title}>{label}</span>
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

// Data from the last successful scrape, kept on screen after a failed refresh.
interface Observed {
  readonly status: Status;
  readonly label: string;
}

function observed(node: MonitorNode, staleSince: number | undefined): Observed {
  if (staleSince !== undefined) return { status: 'stale', label: `Stale · as of ${new Date(staleSince).toLocaleTimeString()}` };
  const { status } = nodeHealth(node);
  return { status, label: STATUS_LABELS[status] };
}

function NodeDrawer({ node, seen, onClose }: { node: MonitorNode; seen: Observed; onClose: () => void }) {
  const health = nodeHealth(node);
  const disk = diskUsage(node);
  const trapRef = useFocusTrap(true, onClose);
  return (
    <aside ref={trapRef} className="mon-drawer" role="dialog" aria-modal="true" aria-label={`Node ${node.id}`}>
      <div className="mon-drawer-head">
        <div>
          <div className="mon-drawer-title"><span className="mon-dot" data-level={dotLevel(seen.status)} />{node.id}</div>
          <div className="mon-faint">standalone · {seen.label}</div>
        </div>
        <button className="btn btn-ghost" onClick={onClose} aria-label="Close node details">✕</button>
      </div>
      <dl className="mon-stats">
        <div><dt>Uptime</dt><dd>{formatUptime(node.uptimeSeconds)}</dd></div>
        <div><dt>CPU</dt><dd>{node.cpuUsedCores === undefined ? '—' : node.cpuUsedCores.toFixed(1)} / {node.cpuCores ?? '—'} cores</dd></div>
        <div><dt>Memory</dt><dd>{formatBytes(node.rssBytes)} RSS · {health.memory ?? '—'}%</dd></div>
        <div><dt>Fullest path</dt><dd>{disk.fullest ?? '—'}%</dd></div>
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
      <p className="mon-faint">Each path reports the filesystem it is on, so paths on one disk show the same usage.</p>
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

const BADGE: Record<Status, string> = { ok: 'healthy', warn: 'degraded', danger: 'critical', unknown: 'unknown', stale: 'unknown' };

export function MonitorPage() {
  const [refresh, setRefresh] = useState<(typeof REFRESH)[number]>(REFRESH[0]);
  const [selected, setSelected] = useState<string | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const opener = useRef<HTMLElement | null>(null);
  const snapshot = useQuery<MonitorSnapshot, MonitorError>({
    queryKey: ['monitor-snapshot'],
    queryFn: fetchMonitorSnapshot,
    refetchInterval: (query) => (query.state.error?.status === 403 || !refresh.ms ? false : refresh.ms),
    retry: false,
  });
  const groups = useQuery({ queryKey: ['groups'], queryFn: () => apiDataSource.listGroups(), enabled: snapshot.isSuccess || !!snapshot.data });
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);
  // Stable, so the page's one-second clock does not re-run the drawer's focus handling.
  const closeDrawer = useCallback(() => {
    setSelected(null);
    opener.current?.focus();
  }, []);
  const openDrawer = (id: string, from: HTMLElement) => {
    opener.current = from;
    setSelected(id);
  };

  const error = snapshot.error;
  const data = snapshot.data;
  const blocked = error?.status === 403;
  const unreachable = !!error && !blocked;
  const staleSince = unreachable && data ? snapshot.dataUpdatedAt : undefined;
  const node = data?.nodes[0];
  const seen = node && observed(node, staleSince);
  const health = node && nodeHealth(node);
  const badge = unreachable ? 'unknown' : seen ? BADGE[seen.status] : undefined;
  const disk = node && diskUsage(node);
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
            {badge && <span className="mon-badge" data-level={badge === 'healthy' ? 'ok' : badge === 'degraded' ? 'warn' : badge === 'critical' ? 'danger' : 'faint'}>{badge}</span>}
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
            {data && <p className="mon-faint">The values below are from the last successful scrape at {new Date(snapshot.dataUpdatedAt).toLocaleTimeString()}.</p>}
          </div>
          <button className="btn" onClick={() => void snapshot.refetch()}>Retry now</button>
        </div>
      )}

      {node && health && disk && seen && !blocked && (
        <>
          <div className="mon-fleet" data-stale={staleSince !== undefined ? '' : undefined}>
            <div><span className="mon-k">Reporting</span><span className="mon-v">{unreachable ? '0' : '1'} / 1</span></div>
            <div><span className="mon-k">Mode</span><span className="mon-v">standalone</span></div>
            <div><span className="mon-k">CPU</span><span className="mon-v">{node.cpuUsedCores?.toFixed(1) ?? '—'}<small> / {node.cpuCores ?? '—'} cores</small></span></div>
            <div><span className="mon-k">Memory (RSS)</span><span className="mon-v">{formatBytes(node.rssBytes)}</span></div>
            <div>
              <span className="mon-k" title="Paths on one filesystem are each counted, so this can overlap">Disk, sum of paths</span>
              <span className="mon-v">{formatBytes(disk.used)}<small> / {formatBytes(disk.total)}</small></span>
            </div>
          </div>

          <section className="mon-section">
            <div className="mon-section-head">
              <span className="mon-section-title">Topology <span className="mon-faint">· standalone</span></span>
              <span className="mon-faint">click the node for details</span>
            </div>
            <button className="mon-node" data-level={dotLevel(seen.status)} data-stale={staleSince !== undefined ? '' : undefined} onClick={(e) => openDrawer(node.id, e.currentTarget)}>
              <span className="mon-node-id">
                <span className="mon-dot" data-level={dotLevel(seen.status)} />{node.id}<span className="mon-role">standalone</span>
                <span className="mon-faint">{seen.label}</span>
              </span>
              <span className="mon-bars">
                <Bar label="cpu" value={health.cpu} />
                <Bar label="memory" value={health.memory} />
                <Bar label="disk" value={health.disk} title="Fullest path" />
              </span>
              <span className="mon-work">
                <span className="mon-k">write</span><span>{formatRate(node.write.perSecond)}</span>
                <span className="mon-k" title="Measure, stream, trace and property query executions">query</span>
                <span>{formatRate(node.query.perSecond)} · <span data-level={(node.query.avgLatencyMs ?? 0) >= SLOW_MS ? 'warn' : undefined}>avg {node.query.avgLatencyMs === undefined ? 'n/a' : formatMs(node.query.avgLatencyMs)}</span></span>
                <span className="mon-k">error</span><span data-level={node.errorsPerMinute ? 'danger' : undefined}>{node.errorsPerMinute === undefined ? '—' : `${node.errorsPerMinute.toFixed(node.errorsPerMinute >= 10 ? 0 : 1)}/min`}</span>
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
              <span>fullest path</span>
              <span className="mon-bar" aria-hidden="true"><span className="mon-bar-fill" data-level={level(disk.fullest, DISK)} style={{ width: `${disk.fullest ?? 0}%` }} /></span>
              <span>{disk.fullest ?? '—'}%</span>
            </div>
            <p className="mon-faint">{node.disk.length} {node.disk.length === 1 ? 'path' : 'paths'}, {formatBytes(disk.used)} of {formatBytes(disk.total)} summed; paths on one filesystem are each counted.</p>
            {groups.error && (
              <p className="mon-notice" data-level="warn" role="status">Group details are unavailable: {groups.error.message}. Node metrics above are unaffected.</p>
            )}
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

      {selectedNode && seen && <NodeDrawer node={selectedNode} seen={seen} onClose={closeDrawer} />}
    </div>
  );
}
