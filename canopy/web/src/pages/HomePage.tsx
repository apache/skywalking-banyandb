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

import React from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';

import { apiDataSource } from '../data/api.js';
import { CanopyMark } from '../components/CanopyMark.js';
import {
  IconMeasures, IconStreams, IconTraces, IconProperties, IconPipelines, IconQuery, IconChevron,
} from '../components/icons.js';

const QUICK_ITEMS = [
  { path: '/metadata/measures', Icon: IconMeasures, label: 'Measures', desc: 'Numeric time-series, grouped by entity' },
  { path: '/metadata/streams', Icon: IconStreams, label: 'Streams', desc: 'Append-only event & log records' },
  { path: '/metadata/traces', Icon: IconTraces, label: 'Traces', desc: 'Distributed spans grouped by trace ID' },
  { path: '/properties', Icon: IconProperties, label: 'Properties', desc: 'Schema-free key–value documents' },
  { path: '/pipelines', Icon: IconPipelines, label: 'Pipelines', desc: 'Continuous processing that derives results from stored data' },
  { path: '/query', Icon: IconQuery, label: 'Run a query', desc: 'Explore data with BydbQL' },
];

export function HomePage() {
  const navigate = useNavigate();
  const { data } = useQuery({
    queryKey: ['groups'],
    queryFn: () => apiDataSource.listGroups(),
  });

  const groups = data?.groups ?? [];
  const snapshot = [
    { k: 'groups', v: groups.length },
    { k: 'measures', v: groups.filter((g) => g.catalog === 'CATALOG_MEASURE').length },
    { k: 'streams', v: groups.filter((g) => g.catalog === 'CATALOG_STREAM').length },
    { k: 'traces', v: groups.filter((g) => g.catalog === 'CATALOG_TRACE').length },
    { k: 'properties', v: groups.filter((g) => g.catalog === 'CATALOG_PROPERTY').length },
  ];

  return (
    <div className="page-body">
      <header className="page-head">
        <div className="crumbs">
          <span className="crumb is-last">Home</span>
        </div>
        <div className="page-title-row">
          <h1 className="page-title">Home</h1>
        </div>
        <p className="page-meta">Apache SkyWalking BanyanDB — observability data store</p>
      </header>

      <div className="welcome">
        <CanopyMark size={56} className="welcome-mark" />
        <div className="welcome-copy">
          <div className="welcome-eyebrow">CANOPY</div>
          <p className="welcome-lead">
            Data is organized into <b>streams</b>, <b>measures</b>, <b>traces</b> and{' '}
            <b>properties</b> within groups. Browse the metadata catalog or run BydbQL
            queries against your cluster.
          </p>
        </div>
      </div>

      <div className="grp-meta" style={{ marginTop: 22, marginBottom: 26 }}>
        {snapshot.map((s) => (
          <div key={s.k} className="meta-chip">
            <span className="meta-k">{s.k}</span>
            <span className="meta-v">{s.v}</span>
          </div>
        ))}
      </div>

      <div className="quick-grid">
        {QUICK_ITEMS.map(({ path, Icon, label, desc }) => (
          <button key={path} className="quick-card" onClick={() => navigate(path)}>
            <span className="quick-ico"><Icon size={18} /></span>
            <span className="quick-label">{label}</span>
            <span className="quick-desc">{desc}</span>
            <span className="quick-arrow"><IconChevron size={15} /></span>
          </button>
        ))}
      </div>
    </div>
  );
}
