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
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Group } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { SamplingFormModal, pipelineDuration } from './SamplingForm.js';

vi.mock('../data/api.js', () => ({ apiDataSource: { listGroups: vi.fn(), updateGroup: vi.fn() } }));
const plugin = { name: 'keep-errors', sampler: { path: 'errors.so', abiVersion: 1, config: { statuses: ['ERROR'] } } };
const group: Group = { name: 'traces', catalog: 'CATALOG_TRACE', resourceOpts: {
  shardNum: 3, replicas: 2, ttl: { unit: 'UNIT_DAY', num: 7 },
  stages: [{ name: 'warm', shardNum: 2, segmentInterval: { unit: 'UNIT_DAY', num: 1 }, ttl: { unit: 'UNIT_DAY', num: 4 } }],
}, pipeline: { enabled: true, plugins: [plugin], enabledEvents: ['PIPELINE_EVENT_MERGE'], mergeGrace: '7200.000000001s' } };
function mount(value?: Group) {
  const onClose = vi.fn();
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  render(<QueryClientProvider client={qc}><SamplingFormModal group={value} onClose={onClose} /></QueryClientProvider>);
  return onClose;
}
beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(apiDataSource.listGroups).mockResolvedValue({ groups: [group, { ...group, name: 'new-traces', pipeline: undefined }] });
  vi.mocked(apiDataSource.updateGroup).mockResolvedValue(group);
});

it.each([
  ['merge only', ['PIPELINE_EVENT_MERGE']],
  ['finalize only', ['PIPELINE_EVENT_FINALIZE']],
  ['both events', ['PIPELINE_EVENT_MERGE', 'PIPELINE_EVENT_FINALIZE']],
] as const)('saves %s without replacing unrelated group settings or exact durations', async (_, events) => {
  const current = { ...group, pipeline: { ...group.pipeline, enabledEvents: events } };
  mount(current);
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await waitFor(() => expect(apiDataSource.updateGroup).toHaveBeenCalledWith('traces', { group: {
    metadata: { name: 'traces' }, catalog: group.catalog, resourceOpts: group.resourceOpts, pipeline: current.pipeline,
  } }));
});
it('disables a pipeline without deleting its sampler configuration', async () => {
  mount(group);
  fireEvent.click(screen.getByLabelText('Enabled'));
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await waitFor(() => expect(apiDataSource.updateGroup).toHaveBeenCalled());
  expect(vi.mocked(apiDataSource.updateGroup).mock.calls[0][1].group.pipeline).toEqual({ ...group.pipeline, enabled: false });
});
it('retains edits and server rejection while allowing a retry', async () => {
  vi.mocked(apiDataSource.updateGroup).mockRejectedValueOnce(new Error('invalid SamplerPlugin.Path'));
  const onClose = mount(group);
  fireEvent.change(screen.getByLabelText('MERGE grace'), { target: { value: '3h' } });
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  expect(await screen.findByText('invalid SamplerPlugin.Path')).toBeInTheDocument();
  expect(screen.getByLabelText('MERGE grace')).toHaveValue('3h');
  expect(onClose).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await waitFor(() => expect(onClose).toHaveBeenCalledWith(group));
  expect(vi.mocked(apiDataSource.updateGroup).mock.calls[1][1].group.pipeline?.mergeGrace).toBe('10800s');
});
it('synchronizes YAML and Form and retains unrecognized configuration', async () => {
  const future = { ...group.pipeline, futureField: { retained: true }, plugins: [{ ...plugin, futurePlugin: 42 }] };
  mount({ ...group, pipeline: future });
  fireEvent.click(screen.getByRole('tab', { name: 'YAML' }));
  const editor = screen.getByLabelText('Pipeline YAML');
  fireEvent.change(editor, { target: { value: (editor as HTMLTextAreaElement).value.replace('enabled: true', 'enabled: false') } });
  fireEvent.click(screen.getByRole('tab', { name: 'Form' }));
  expect(screen.getByLabelText('Enabled')).not.toBeChecked();
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await waitFor(() => expect(apiDataSource.updateGroup).toHaveBeenCalled());
  expect(vi.mocked(apiDataSource.updateGroup).mock.calls[0][1].group.pipeline).toEqual({ ...future, enabled: false });
});
it('blocks malformed YAML without destroying the editor text', async () => {
  mount(group);
  fireEvent.click(screen.getByRole('tab', { name: 'YAML' }));
  fireEvent.change(screen.getByLabelText('Pipeline YAML'), { target: { value: 'group: traces\npipeline: {plugins: [null]}' } });
  fireEvent.click(screen.getByRole('tab', { name: 'Form' }));
  expect(screen.getByLabelText('Pipeline YAML')).toHaveValue('group: traces\npipeline: {plugins: [null]}');
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await screen.findByRole('alert');
  expect(apiDataSource.updateGroup).not.toHaveBeenCalled();
});
it('reorders samplers with the prototype drag handle keyboard controls', async () => {
  mount({ ...group, pipeline: { ...group.pipeline, plugins: [plugin, { ...plugin, name: 'second' }] } });
  fireEvent.keyDown(screen.getByRole('button', { name: 'Reorder sampler 2' }), { key: 'ArrowUp' });
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await waitFor(() => expect(apiDataSource.updateGroup).toHaveBeenCalled());
  expect(vi.mocked(apiDataSource.updateGroup).mock.calls[0][1].group.pipeline?.plugins?.map((item) => item.name)).toEqual(['second', 'keep-errors']);
});
it('requires valid JSON without losing the invalid draft when switching tabs', () => {
  mount(group);
  fireEvent.click(screen.getByRole('button', { name: 'Expand sampler 1' }));
  fireEvent.change(screen.getByLabelText('Config (JSON)'), { target: { value: '{broken' } });
  fireEvent.click(screen.getByRole('tab', { name: 'YAML' }));
  expect(screen.getByLabelText(/Config \(JSON\)/)).toHaveValue('{broken');
  expect(apiDataSource.updateGroup).not.toHaveBeenCalled();
});
it('creates only in a trace group without an existing pipeline', async () => {
  mount();
  const select = await screen.findByRole('option', { name: 'new-traces' });
  expect(select).toBeInTheDocument();
  expect(screen.queryByRole('option', { name: 'traces' })).not.toBeInTheDocument();
  fireEvent.change(screen.getByLabelText('Trace group'), { target: { value: 'new-traces' } });
  fireEvent.click(screen.getByRole('button', { name: 'Create pipeline' }));
  await waitFor(() => expect(apiDataSource.updateGroup).toHaveBeenCalled());
  expect(vi.mocked(apiDataSource.updateGroup).mock.calls[0][0]).toBe('new-traces');
});
it('normalizes shorthand without rounding nanosecond durations', () => {
  expect(pipelineDuration('0.000000001m')).toBe('0.00000006s');
  expect(pipelineDuration('7200.000000001s')).toBe('7200.000000001s');
  expect(pipelineDuration('')).toBeUndefined();
});

it('uses fresh group settings and handles a revision-only update response', async () => {
  const fresh = { ...group, resourceOpts: { ...group.resourceOpts, shardNum: 9 }, metadata: { name: group.name, modRevision: '42' }, futureGroup: { retained: true } };
  vi.mocked(apiDataSource.listGroups).mockResolvedValue({ groups: [fresh] });
  vi.mocked(apiDataSource.updateGroup).mockResolvedValue(undefined as unknown as Group);
  const onClose = mount(group);
  fireEvent.click(screen.getByLabelText('Enabled'));
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await waitFor(() => expect(onClose).toHaveBeenCalledWith({ ...fresh, pipeline: { ...group.pipeline, enabled: false } }));
  expect(vi.mocked(apiDataSource.updateGroup).mock.calls[0][1].group).toMatchObject({ metadata: fresh.metadata, futureGroup: fresh.futureGroup, resourceOpts: fresh.resourceOpts });
});
it.each(['enabled: yes', 'mergeGrace: 15', 'enabledEvents: [{}]', 'schemaNames: [{}]'])('rejects unsafe YAML scalar shape %s', (yaml) => {
  mount(group);
  fireEvent.click(screen.getByRole('tab', { name: 'YAML' }));
  fireEvent.change(screen.getByLabelText('Pipeline YAML'), { target: { value: `group: traces\npipeline:\n  ${yaml}` } });
  fireEvent.click(screen.getByRole('tab', { name: 'Form' }));
  expect(screen.getByLabelText('Pipeline YAML')).toBeInTheDocument();
  expect(screen.getByRole('alert')).toBeInTheDocument();
});
it('retains an invalid config draft when another sampler is removed', () => {
  mount({ ...group, pipeline: { ...group.pipeline, plugins: [plugin, { ...plugin, name: 'second' }] } });
  fireEvent.click(screen.getByRole('button', { name: 'Expand sampler 2' }));
  fireEvent.change(screen.getByLabelText('Config (JSON)'), { target: { value: '{broken' } });
  fireEvent.click(screen.getByRole('button', { name: 'Remove sampler 1' }));
  expect(screen.getByLabelText(/Config \(JSON\)/)).toHaveValue('{broken');
  fireEvent.click(screen.getByRole('tab', { name: 'YAML' }));
  expect(screen.getByLabelText(/Config \(JSON\)/)).toHaveValue('{broken');
});

it.each([
  [false, false, true],
  [true, false, false],
  [false, true, false],
  [true, true, false],
  [true, false, true],
  [false, true, true],
  [true, true, true],
] as const)('saves independent selections: merge=%s finalize=%s stages=%s', async (merge, finalize, staged) => {
  const stages = staged ? [{ stage: 'warm', plugins: [plugin] }] : [];
  mount({ ...group, pipeline: { ...group.pipeline, enabledEvents: ['PIPELINE_EVENT_MERGE', 'PIPELINE_EVENT_FINALIZE'], stages } });
  if (!merge) fireEvent.click(screen.getByLabelText('MERGE', { exact: true }));
  if (!finalize) fireEvent.click(screen.getByLabelText('FINALIZE', { exact: true }));
  fireEvent.click(screen.getByRole('tab', { name: 'YAML' }));
  fireEvent.click(screen.getByRole('tab', { name: 'Form' }));
  expect(screen.getByLabelText('MERGE', { exact: true })).toHaveProperty('checked', merge);
  expect(screen.getByLabelText('FINALIZE', { exact: true })).toHaveProperty('checked', finalize);
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await waitFor(() => expect(apiDataSource.updateGroup).toHaveBeenCalled());
  const saved = vi.mocked(apiDataSource.updateGroup).mock.calls[0][1].group.pipeline;
  expect(saved?.enabledEvents).toEqual([
    ...(merge ? ['PIPELINE_EVENT_MERGE'] : []),
    ...(finalize ? ['PIPELINE_EVENT_FINALIZE'] : []),
  ]);
  expect(saved?.stages).toEqual(stages);
  expect(saved?.plugins).toEqual([plugin]);
  expect(saved?.enabled).toBe(true);
});

it.each([undefined, []] as const)('opens stage-only pipelines with events %s disabled', (enabledEvents) => {
  mount({ ...group, pipeline: { ...group.pipeline, enabledEvents, stages: [{ stage: 'warm', plugins: [plugin] }] } });
  expect(screen.getByLabelText('MERGE', { exact: true })).not.toBeChecked();
  expect(screen.getByLabelText('FINALIZE', { exact: true })).not.toBeChecked();
  expect(screen.getByLabelText('Enabled')).toBeChecked();
  expect(screen.getByRole('button', { name: 'Edit chain' })).toBeInTheDocument();
});

it('edits a sampler chain on an implicit hot source stage', async () => {
  mount({ ...group, pipeline: { enabled: true, enabledEvents: [], plugins: [] } });
  fireEvent.click(screen.getByRole('button', { name: '+ Add stage rule' }));
  expect(screen.getByLabelText('Stage 1')).toHaveValue('hot');
  fireEvent.click(screen.getByRole('button', { name: 'Edit chain' }));
  fireEvent.click(screen.getAllByRole('button', { name: '+ Add sampler' })[1]);
  fireEvent.change(screen.getByLabelText('Name', { exact: true }), { target: { value: 'stage-keep' } });
  fireEvent.change(screen.getByLabelText('Plugin file (.so)'), { target: { value: 'keep.so' } });
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await waitFor(() => expect(apiDataSource.updateGroup).toHaveBeenCalled());
  expect(vi.mocked(apiDataSource.updateGroup).mock.calls[0][1].group.pipeline).toMatchObject({
    enabled: true, enabledEvents: [], stages: [{ stage: 'hot', plugins: [{ name: 'stage-keep', sampler: { path: 'keep.so' } }] }],
  });
});

it.each(['form', 'yaml'])('rejects enabled pipelines with no events or stage sampler in %s', async (mode) => {
  mount(group);
  fireEvent.click(screen.getByLabelText('MERGE', { exact: true }));
  if (mode === 'yaml') fireEvent.click(screen.getByRole('tab', { name: 'YAML' }));
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  expect(await screen.findByRole('alert')).toHaveTextContent('Enable MERGE or FINALIZE, or configure a stage sampler chain');
  expect(apiDataSource.updateGroup).not.toHaveBeenCalled();
});

it('allows an inactive pipeline with neither events nor stages', async () => {
  mount({ ...group, pipeline: { ...group.pipeline, enabled: false, enabledEvents: [] } });
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await waitFor(() => expect(apiDataSource.updateGroup).toHaveBeenCalled());
});

it.each(['save', 'YAML'] as const)('includes a pending schema target on %s without requiring Enter', async (action) => {
  mount({ ...group, pipeline: { ...group.pipeline, schemaNames: ['existing'] } });
  expect(screen.getByRole('group', { name: 'Migration targets' })).toHaveTextContent(
    'Schema selectors apply only to stage sampling during lifecycle migration. MERGE and FINALIZE apply to all trace schemas in the group.',
  );
  fireEvent.change(screen.getByLabelText('Schema names', { exact: true }), { target: { value: ' pending ' } });
  if (action === 'YAML') {
    fireEvent.click(screen.getByRole('tab', { name: 'YAML' }));
    expect(screen.getByLabelText('Pipeline YAML')).toHaveProperty('value', expect.stringContaining('- pending'));
    fireEvent.click(screen.getByRole('tab', { name: 'Form' }));
    expect(screen.getByLabelText('Schema names', { exact: true })).toHaveValue('');
    expect(screen.getByRole('button', { name: 'Remove schema pending' })).toBeInTheDocument();
  }
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await waitFor(() => expect(apiDataSource.updateGroup).toHaveBeenCalled());
  expect(vi.mocked(apiDataSource.updateGroup).mock.calls[0][1].group.pipeline?.schemaNames).toEqual(['existing', 'pending']);
});

it('does not duplicate a pending schema target when saving again after rejection', async () => {
  vi.mocked(apiDataSource.updateGroup).mockRejectedValueOnce(new Error('Try again'));
  mount(group);
  fireEvent.change(screen.getByLabelText('Schema names', { exact: true }), { target: { value: 'pending' } });
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await screen.findByText('Try again');
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await waitFor(() => expect(apiDataSource.updateGroup).toHaveBeenCalledTimes(2));
  expect(vi.mocked(apiDataSource.updateGroup).mock.calls[1][1].group.pipeline?.schemaNames).toEqual(['pending']);
});

it.each([
  ['path: {bad: value}', 'path must be a string'],
  ['path: errors.so, symbol: {}', 'symbol must be a string'],
  ['path: errors.so, abiVersion: "1"', 'ABI version must be an integer'],
  ['path: errors.so, abiVersion: 1.5', 'ABI version must be an integer'],
  ['path: errors.so, config: []', 'config must be a JSON object'],
  ['path: errors.so, config: invalid', 'config must be a JSON object'],
  ['path: errors.so, config: &config {self: *config}', 'without recursive aliases'],
  ['path: errors.so, config: {threshold: .inf}', 'non-finite numbers'],
])('rejects unsafe nested sampler YAML %s for both event and stage chains', async (sampler, message) => {
  mount(group);
  fireEvent.click(screen.getByRole('tab', { name: 'YAML' }));
  for (const chain of [`plugins: [{name: keep, sampler: {${sampler}}}]`, `stages: [{stage: hot, plugins: [{name: keep, sampler: {${sampler}}}]}]`]) {
    const yaml = `group: traces\npipeline:\n  enabled: false\n  ${chain}`;
    fireEvent.change(screen.getByLabelText('Pipeline YAML'), { target: { value: yaml } });
    fireEvent.click(screen.getByRole('tab', { name: 'Form' }));
    expect(screen.getByLabelText('Pipeline YAML')).toHaveValue(yaml);
    expect(screen.getByRole('alert')).toHaveTextContent(message);
    fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('BanyanDB rejected the update'));
    expect(apiDataSource.updateGroup).not.toHaveBeenCalled();
  }
});

it('allows nonrecursive YAML aliases and preserves nested JSON config', async () => {
  mount(group);
  fireEvent.click(screen.getByRole('tab', { name: 'YAML' }));
  fireEvent.change(screen.getByLabelText('Pipeline YAML'), { target: { value: `group: traces
pipeline:
  enabled: false
  plugins:
    - name: keep
      sampler:
        path: errors.so
        symbol: NewSampler
        abiVersion: 1
        config:
          first: &values [null, true, 2, {status: ERROR}]
          second: *values
` } });
  fireEvent.click(screen.getByRole('tab', { name: 'Form' }));
  fireEvent.click(screen.getByRole('button', { name: 'Expand sampler 1' }));
  expect(screen.getByLabelText('Config (JSON)')).toHaveValue(JSON.stringify({ first: [null, true, 2, { status: 'ERROR' }], second: [null, true, 2, { status: 'ERROR' }] }, null, 2));
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await waitFor(() => expect(apiDataSource.updateGroup).toHaveBeenCalled());
});

it.each(['form', 'yaml'])('rejects duplicate source stage rules in %s before saving', async (mode) => {
  const onClose = mount(group);
  if (mode === 'form') {
    fireEvent.click(screen.getByRole('button', { name: '+ Add stage rule' }));
    fireEvent.click(screen.getByRole('button', { name: '+ Add stage rule' }));
  } else {
    fireEvent.click(screen.getByRole('tab', { name: 'YAML' }));
    fireEvent.change(screen.getByLabelText('Pipeline YAML'), { target: { value: `group: traces
pipeline:
  enabled: true
  enabledEvents: [PIPELINE_EVENT_MERGE]
  stages:
    - stage: hot
      plugins: []
    - stage: hot
      plugins: []
` } });
  }
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  expect(await screen.findByRole('alert')).toHaveTextContent('Only one sampler chain is allowed for source stage "hot".');
  expect(apiDataSource.updateGroup).not.toHaveBeenCalled();
  expect(onClose).not.toHaveBeenCalled();

  if (mode === 'yaml') fireEvent.click(screen.getByRole('tab', { name: 'Form' }));
  expect(screen.getByLabelText('Stage 1')).toHaveValue('hot');
  expect(screen.getByLabelText('Stage 2')).toHaveValue('hot');
  fireEvent.change(screen.getByLabelText('Stage 2'), { target: { value: 'warm' } });
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await waitFor(() => expect(apiDataSource.updateGroup).toHaveBeenCalledTimes(1));
  expect(vi.mocked(apiDataSource.updateGroup).mock.calls[0][1].group.pipeline?.stages).toEqual([
    { stage: 'hot', plugins: [] }, { stage: 'warm', plugins: [] },
  ]);
});
