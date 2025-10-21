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

import { httpQuery } from './base';

export function getGroupList() {
  return httpQuery({
    url: '/api/v1/group/schema/lists',
    method: 'GET',
  });
}

export function getAllTypesOfResourceList(type, name) {
  return httpQuery({
    url: `/api/v1/${type}/schema/lists/${name}`,
    method: 'GET',
  });
}

export function getResourceOfAllType(type, group, name) {
  return httpQuery({
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'GET',
  });
}

export function getTableList(data, type) {
  return httpQuery({
    url: `/api/v1/${type}/data`,
    json: data,
    method: 'POST',
  });
}

export function deleteAllTypesOfResource(type, group, name) {
  return httpQuery({
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'DELETE',
  });
}

export function deleteGroup(group) {
  return httpQuery({
    url: `/api/v1/group/schema/${group}`,
    method: 'DELETE',
  });
}

export function createGroup(data) {
  return httpQuery({
    url: `/api/v1/group/schema`,
    method: 'POST',
    json: data,
  });
}

export function editGroup(group, data) {
  return httpQuery({
    url: `/api/v1/group/schema/${group}`,
    method: 'PUT',
    json: data,
  });
}

export function createResources(type, data) {
  return httpQuery({
    url: `/api/v1/${type}/schema`,
    method: 'POST',
    json: data,
  });
}

export function editResources(type, group, name, data) {
  return httpQuery({
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'PUT',
    json: data,
  });
}

export function getindexRuleList(name) {
  return httpQuery({
    url: `/api/v1/index-rule/schema/lists/${name}`,
    method: 'GET',
  });
}

export function getindexRuleBindingList(name) {
  return httpQuery({
    url: `/api/v1/index-rule-binding/schema/lists/${name}`,
    method: 'GET',
  });
}

export function getTopNAggregationList(name) {
  return httpQuery({
    url: `/api/v1/topn-agg/schema/lists/${name}`,
    method: 'GET',
  });
}

export function getTopNAggregationData(data) {
  return httpQuery({
    url: `/api/v1/measure/topn`,
    json: data,
    method: 'POST',
  });
}

export function getSecondaryDataModel(type, group, name) {
  return httpQuery({
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'GET',
  });
}

export function createSecondaryDataModel(type, data) {
  return httpQuery({
    url: `/api/v1/${type}/schema`,
    method: 'POST',
    json: data,
  });
}

export function updateSecondaryDataModel(type, group, name, data) {
  return httpQuery({
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'PUT',
    json: data,
  });
}

export function deleteSecondaryDataModel(type, group, name) {
  return httpQuery({
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'DELETE',
  });
}

export function fetchProperties(data) {
  return httpQuery({
    url: `/api/v1/property/data/query`,
    method: 'POST',
    json: data,
  });
}

export function deleteProperty(group, name) {
  return httpQuery({
    url: `/api/v1/property/schema/${group}/${name}`,
    method: 'DELETE',
  });
}

export function updateProperty(group, name, data) {
  return httpQuery({
    url: `/api/v1/property/schema/${group}/${name}`,
    method: 'PUT',
    json: data,
  });
}

export function createProperty(data) {
  return httpQuery({
    url: `/api/v1/property/schema`,
    method: 'POST',
    json: data,
  });
}

export function applyProperty(group, name, id, data) {
  return httpQuery({
    url: `/api/v1/property/data/${group}/${name}/${id}`,
    method: 'PUT',
    json: data,
  });
}

export function getTrace(group, name) {
  return httpQuery({
    url: `/api/v1/trace/schema/${group}/${name}`,
    method: 'get',
  });
}

export function createTrace(json) {
  return httpQuery({
    url: `/api/v1/trace/schema`,
    method: 'POST',
    json,
  });
}

export function updateTrace(group, name, json) {
  return httpQuery({
    url: `/api/v1/trace/schema/${group}/${name}`,
    json,
    method: 'PUT',
  });
}

export function queryTraces(json) {
  return httpQuery({
    url: `/api/v1/trace/data`,
    json,
    method: 'POST',
  });
}
