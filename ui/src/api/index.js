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

import { queryClient } from '@/plugins/vue-query';
import { httpQuery } from './base';

function fetchWithQuery(queryKey, request, options = {}) {
  return queryClient.fetchQuery({
    queryKey,
    queryFn: () => httpQuery(request),
    ...options,
  });
}

async function invalidateQueries(keys = []) {
  if (!Array.isArray(keys) || keys.length === 0) {
    return;
  }

  await Promise.all(
    keys.map((key) => {
      if (typeof key === 'function') {
        return queryClient.invalidateQueries({ predicate: key });
      }
      if (key && typeof key === 'object' && key.queryKey) {
        return queryClient.invalidateQueries(key);
      }
      const queryKey = Array.isArray(key) ? key : [key];
      return queryClient.invalidateQueries({ queryKey });
    }),
  );
}

async function mutateWithInvalidation(request, invalidate = []) {
  const result = await httpQuery(request);
  if (!result?.error && invalidate.length) {
    await invalidateQueries(invalidate);
  }
  return result;
}

export function getGroupList() {
  return fetchWithQuery(['groups'], {
    url: '/api/v1/group/schema/lists',
    method: 'GET',
  });
}

export function getAllTypesOfResourceList(type, name) {
  return fetchWithQuery(['groupResources', type, name], {
    url: `/api/v1/${type}/schema/lists/${name}`,
    method: 'GET',
  });
}

export function getResourceOfAllType(type, group, name) {
  return fetchWithQuery(['resource', type, group, name], {
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'GET',
  });
}

export function getTableList(data, type) {
  const queryKey = ['tableData', type, JSON.stringify(data)];
  return fetchWithQuery(
    queryKey,
    {
      url: `/api/v1/${type}/data`,
      json: data,
      method: 'POST',
    },
    { staleTime: 0 },
  );
}

export function deleteAllTypesOfResource(type, group, name) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/${type}/schema/${group}/${name}`,
      method: 'DELETE',
    },
    [
      ['groupResources', type, group],
      ['resource', type, group, name],
    ],
  );
}

export function deleteGroup(group) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/group/schema/${group}`,
      method: 'DELETE',
    },
    [['groups'], ['groupResources']],
  );
}

export function createGroup(data) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/group/schema`,
      method: 'POST',
      json: data,
    },
    [['groups'], ['groupResources']],
  );
}

export function editGroup(group, data) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/group/schema/${group}`,
      method: 'PUT',
      json: data,
    },
    [['groups'], ['groupResources']],
  );
}

export function createResources(type, data) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/${type}/schema`,
      method: 'POST',
      json: data,
    },
    [['groupResources'], ['resource']],
  );
}

export function editResources(type, group, name, data) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/${type}/schema/${group}/${name}`,
      method: 'PUT',
      json: data,
    },
    [
      ['groupResources', type, group],
      ['resource', type, group, name],
    ],
  );
}

export function getindexRuleList(name) {
  return fetchWithQuery(['indexRuleList', name], {
    url: `/api/v1/index-rule/schema/lists/${name}`,
    method: 'GET',
  });
}

export function getindexRuleBindingList(name) {
  return fetchWithQuery(['indexRuleBindingList', name], {
    url: `/api/v1/index-rule-binding/schema/lists/${name}`,
    method: 'GET',
  });
}

export function getTopNAggregationList(name) {
  return fetchWithQuery(['topNAggregationList', name], {
    url: `/api/v1/topn-agg/schema/lists/${name}`,
    method: 'GET',
  });
}

export function getTopNAggregationData(data) {
  const queryKey = ['topNAggregationData', JSON.stringify(data)];
  return fetchWithQuery(
    queryKey,
    {
      url: `/api/v1/measure/topn`,
      json: data,
      method: 'POST',
    },
    { staleTime: 0 },
  );
}

export function getSecondaryDataModel(type, group, name) {
  return fetchWithQuery(['secondaryDataModel', type, group, name], {
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'GET',
  });
}

export function createSecondaryDataModel(type, data) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/${type}/schema`,
      method: 'POST',
      json: data,
    },
    [['secondaryDataModel'], ['groupResources']],
  );
}

export function updateSecondaryDataModel(type, group, name, data) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/${type}/schema/${group}/${name}`,
      method: 'PUT',
      json: data,
    },
    [
      ['secondaryDataModel', type, group, name],
      ['groupResources', type, group],
    ],
  );
}

export function deleteSecondaryDataModel(type, group, name) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/${type}/schema/${group}/${name}`,
      method: 'DELETE',
    },
    [
      ['secondaryDataModel', type, group, name],
      ['groupResources', type, group],
    ],
  );
}

export function fetchProperties(data) {
  const queryKey = ['properties', JSON.stringify(data)];
  return fetchWithQuery(
    queryKey,
    {
      url: `/api/v1/property/data/query`,
      method: 'POST',
      json: data,
    },
    { staleTime: 0 },
  );
}

export function deleteProperty(group, name) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/property/schema/${group}/${name}`,
      method: 'DELETE',
    },
    [['properties'], ['groupResources']],
  );
}

export function updateProperty(group, name, data) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/property/schema/${group}/${name}`,
      method: 'PUT',
      json: data,
    },
    [
      ['properties'],
      ['groupResources'],
    ],
  );
}

export function createProperty(data) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/property/schema`,
      method: 'POST',
      json: data,
    },
    [['properties'], ['groupResources']],
  );
}

export function applyProperty(group, name, id, data) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/property/data/${group}/${name}/${id}`,
      method: 'PUT',
      json: data,
    },
    [['properties'], ['groupResources']],
  );
}

export function getTrace(group, name) {
  return fetchWithQuery(['trace', group, name], {
    url: `/api/v1/trace/schema/${group}/${name}`,
    method: 'GET',
  });
}

export function createTrace(json) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/trace/schema`,
      method: 'POST',
      json,
    },
    [['trace'], ['groupResources']],
  );
}

export function updateTrace(group, name, json) {
  return mutateWithInvalidation(
    {
      url: `/api/v1/trace/schema/${group}/${name}`,
      json,
      method: 'PUT',
    },
    [
      ['trace', group, name],
      ['groupResources'],
    ],
  );
}

export function queryTraces(json) {
  const queryKey = ['traceQuery', JSON.stringify(json)];
  return fetchWithQuery(
    queryKey,
    {
      url: `/api/v1/trace/data`,
      json,
      method: 'POST',
    },
    { staleTime: 0 },
  );
}

export function executeBydbQLQuery(data) {
  const queryKey = ['bydbql', JSON.stringify(data)];
  return fetchWithQuery(
    queryKey,
    {
      url: `/api/v1/bydbql/query`,
      json: data,
      method: 'POST',
    },
    { staleTime: 0 },
  );
}
