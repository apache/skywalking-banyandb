/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership. The ASF licenses this file to You under
 * the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { diskUsage, fetchOverview, formatBytes, formatUptime, MonitorAccessError, refreshInterval } from '../monitor/overview.js';
import '../monitor/monitor.css';

export function MonitorPage() {
  const query = useQuery({
    queryKey: ['monitor', 'overview'],
    queryFn: ({ signal }) => fetchOverview(signal),
    retry: false,
    staleTime: 0,
    gcTime: 0,
    refetchInterval: (current) => current.state.error instanceof MonitorAccessError
      ? false : refreshInterval(current.state.data?.refreshIntervalMs),
    refetchIntervalInBackground: false,
  });
  // Do not retain privileged data on screen if the server denies a later refresh.
  const denied = query.error instanceof MonitorAccessError;
  const overview = denied ? undefined : query.data;
  return (
    <div className="page-body monitor-page">
      <header className="page-head">
        <div className="page-title-row">
          <h1 className="page-title">Monitor</h1>
          <button type="button" className="btn" disabled={query.isFetching} onClick={() => void query.refetch()}>
            {query.isFetching ? 'Refreshing…' : 'Refresh'}
          </button>
        </div>
        <p className="page-meta">Cluster routing and storage usage.</p>
        {overview && <p className="page-meta">Observed at <time dateTime={overview.observedAt}>{overview.observedAt}</time>
          {' · '}Refreshes every {refreshInterval(overview.refreshIntervalMs) / 1000} seconds while this page is visible.</p>}
      </header>
      {query.isPending && <p role="status">Loading monitoring data…</p>}
      {query.error && <p role="alert" className="monitor-notice">
        {overview ? 'Refresh failed. The data below is stale. ' : ''}{query.error.message}
      </p>}
      {overview && <>
        <section className="monitor-section" aria-labelledby="monitor-cluster-title">
          <h2 id="monitor-cluster-title">Cluster</h2>
          <p className="page-meta">Nodes and routing state reported by the connected BanyanDB instance.</p>
          {overview.cluster.status === 'error' ? <p role="alert">Cluster unavailable: {overview.cluster.message}</p>
            : overview.cluster.data.nodes.length === 0 ? <p>No cluster nodes reported.</p> : (
              <div className="monitor-table-wrap"><table className="monitor-table monitor-cluster-table">
                <caption className="monitor-caption">Cluster nodes</caption>
                <thead><tr><th scope="col">Node</th><th scope="col">Roles</th><th scope="col">Address</th><th scope="col">API version</th><th scope="col">File format</th><th scope="col">Route</th><th scope="col">State</th></tr></thead>
                <tbody>{overview.cluster.data.nodes.map((node, index) => <tr key={`${node.name}:${node.route}:${index}`}>
                  <th scope="row">{node.name}</th><td>{node.roles.join(', ') || 'Unavailable'}</td>
                  <td>{node.address || 'Unavailable'}</td><td>{node.apiVersion || 'Unavailable'}</td>
                  <td>{node.fileFormatVersion || 'Unavailable'}</td><td>{node.route || 'No route'}</td>
                  <td>{node.state === 'registered' ? 'Registered (no active route)' : node.state === 'evictable' ? 'Evictable' : 'Active route'}</td>
                </tr>)}</tbody>
              </table></div>
            )}
          <h3>Reported uptime</h3>
          <p className="page-meta">Uptime from the metrics source. Metrics identities may differ from cluster routing names.</p>
          {overview.storage.status === 'error' ? <p>Uptime unavailable: {overview.storage.message}</p>
            : !overview.storage.data.uptimes?.length ? <p>No uptime metrics reported.</p> : (
              <div className="monitor-table-wrap"><table className="monitor-table">
                <caption className="monitor-caption">Uptime by metrics identity</caption>
                <thead><tr><th scope="col">Metrics identity</th><th scope="col">Uptime</th></tr></thead>
                <tbody>{overview.storage.data.uptimes.map((uptime, index) => <tr key={`${uptime.node}:${index}`}>
                  <th scope="row">{uptime.node || 'Unavailable'}</th><td>{formatUptime(uptime.seconds)}</td>
                </tr>)}</tbody>
              </table></div>
            )}
        </section>
        <section className="monitor-section" aria-labelledby="monitor-storage-title">
          <h2 id="monitor-storage-title">Storage</h2>
          {overview.storage.status === 'error' ? <p role="alert">Storage unavailable: {overview.storage.message}</p> : <>
            <h3>Disk usage</h3>
            <p className="page-meta">Usage per reported path. Paths may share a filesystem.</p>
            {overview.storage.data.disks.length === 0 ? <p>No disk usage metrics reported.</p> : (
              <div className="monitor-table-wrap"><table className="monitor-table">
                <caption className="monitor-caption">Disk usage by node and path</caption>
                <thead><tr><th scope="col">Node</th><th scope="col">Path</th><th scope="col">Used</th><th scope="col">Total</th><th scope="col">Usage</th></tr></thead>
                <tbody>{overview.storage.data.disks.map((disk, index) => <tr key={`${disk.node}:${disk.path}:${index}`}>
                  <th scope="row">{disk.node || 'Unavailable'}</th><td>{disk.path || 'Unavailable'}</td>
                  <td>{formatBytes(disk.usedBytes)}</td><td>{formatBytes(disk.totalBytes)}</td><td>{diskUsage(disk.usedBytes, disk.totalBytes)}</td>
                </tr>)}</tbody>
              </table></div>
            )}
            <h3>Storage metrics</h3>
            {overview.storage.data.metrics.length === 0 ? <p>No storage size or parts metrics reported.</p> : (
              <div className="monitor-table-wrap"><table className="monitor-table">
                <caption className="monitor-caption">Storage metric samples</caption>
                <thead><tr><th scope="col">Metric</th><th scope="col">Labels</th><th scope="col">Value</th></tr></thead>
                <tbody>{overview.storage.data.metrics.map((metric, index) => <tr key={`${metric.name}:${index}`}>
                  <th scope="row"><code>{metric.name}</code></th>
                  <td>{Object.entries(metric.labels).map(([key, value]) => `${key}=${value}`).join(', ') || 'None'}</td>
                  <td>{metric.name.endsWith('_bytes') ? formatBytes(metric.value) : metric.value.toLocaleString()}</td>
                </tr>)}</tbody>
              </table></div>
            )}
          </>}
        </section>
        {overview.experimentalSections && ['Query', 'Events', 'Trace'].map((name) => (
          <section key={name} className="monitor-section" aria-label={`${name} monitoring`}>
            <h2>{name} <span className="monitor-experimental">Experimental</span></h2>
            <p>Unavailable: {name.toLowerCase()} monitoring is not connected to a data source.</p>
          </section>
        ))}
      </>}
    </div>
  );
}
