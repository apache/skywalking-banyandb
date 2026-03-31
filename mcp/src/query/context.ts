/**
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. Apache Software
 * Foundation (ASF) licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { BanyanDBClient } from '../client/index.js';
import { ResourcesByGroup } from './types.js';
import { log } from '../utils/logger.js';

export async function loadQueryContext(
  banyandbClient: BanyanDBClient,
): Promise<{ groups: string[]; resourcesByGroup: ResourcesByGroup }> {
  let groups: string[] = [];
  try {
    const groupsList = await banyandbClient.listGroups();
    groups = groupsList
      .map((groupResource) => groupResource.metadata?.name || '')
      .filter((groupName) => groupName !== '');
  } catch (error) {
    log.warn(
      'Failed to fetch groups, continuing without group information:',
      error instanceof Error ? error.message : String(error),
    );
  }

  const resourcesByGroup: ResourcesByGroup = {};
  for (const group of groups) {
    try {
      const [streams, measures, traces, properties, topNItems, indexRule] = await Promise.all([
        banyandbClient.listStreams(group).catch(() => []),
        banyandbClient.listMeasures(group).catch(() => []),
        banyandbClient.listTraces(group).catch(() => []),
        banyandbClient.listProperties(group).catch(() => []),
        banyandbClient.listTopN(group).catch(() => []),
        banyandbClient.listIndexRule(group).catch(() => []),
      ]);

      resourcesByGroup[group] = {
        streams: streams.map((resource) => resource.metadata?.name || '').filter((resourceName) => resourceName !== ''),
        measures: measures
          .map((resource) => resource.metadata?.name || '')
          .filter((resourceName) => resourceName !== ''),
        traces: traces.map((resource) => resource.metadata?.name || '').filter((resourceName) => resourceName !== ''),
        properties: properties
          .map((resource) => resource.metadata?.name || '')
          .filter((resourceName) => resourceName !== ''),
        topNItems: topNItems
          .map((resource) => resource.metadata?.name || '')
          .filter((resourceName) => resourceName !== ''),
        indexRule: indexRule
          .filter((resource) => !resource.noSort && resource.metadata?.name)
          .map((resource) => resource.metadata?.name || ''),
      };
    } catch (error) {
      log.warn(
        `Failed to fetch resources for group "${group}", continuing:`,
        error instanceof Error ? error.message : String(error),
      );
      resourcesByGroup[group] = { streams: [], measures: [], traces: [], properties: [], topNItems: [], indexRule: [] };
    }
  }

  return { groups, resourcesByGroup };
}
