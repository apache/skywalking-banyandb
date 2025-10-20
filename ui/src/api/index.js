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

// api file

import request from '@/utils/axios';
import { httpQuery } from './base';

// Some request methods
export function getGroupList() {
  return request({
    url: '/api/v1/group/schema/lists',
    method: 'get',
  });
}

export function getAllTypesOfResourceList(type, name) {
  return request({
    url: `/api/v1/${type}/schema/lists/${name}`,
    method: 'get',
  });
}

export function getStreamOrMeasure(type, group, name) {
  return request({
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'get',
  });
}

export function getTableList(data, type) {
  return request({
    url: `/api/v1/${type}/data`,
    data: data,
    method: 'post',
  });
}

export function deleteAllTypesOfResource(type, group, name) {
  return request({
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'delete',
  });
}

export function deleteGroup(group) {
  return request({
    url: `/api/v1/group/schema/${group}`,
    method: 'delete',
  });
}

export function createGroup(data) {
  return request({
    url: `/api/v1/group/schema`,
    method: 'post',
    data: data,
  });
}

export function editGroup(group, data) {
  return request({
    url: `/api/v1/group/schema/${group}`,
    method: 'put',
    data: data,
  });
}

export function createResources(type, data) {
  return request({
    url: `/api/v1/${type}/schema`,
    method: 'post',
    data: data,
  });
}

export function editResources(type, group, name, data) {
  return request({
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'put',
    data: data,
  });
}

export function getindexRuleList(name) {
  return request({
    url: `/api/v1/index-rule/schema/lists/${name}`,
    method: 'get',
  });
}

export function getindexRuleBindingList(name) {
  return request({
    url: `/api/v1/index-rule-binding/schema/lists/${name}`,
    method: 'get',
  });
}

export function getTopNAggregationList(name) {
  return request({
    url: `/api/v1/topn-agg/schema/lists/${name}`,
    method: 'get',
  });
}

export function getTopNAggregationData(data) {
  return request({
    url: `/api/v1/measure/topn`,
    data,
    method: 'post',
  });
}

export function getSecondaryDataModel(type, group, name) {
  return request({
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'get',
  });
}

export function createSecondaryDataModel(type, data) {
  return request({
    url: `/api/v1/${type}/schema`,
    method: 'post',
    data: data,
  });
}

export function updateSecondaryDataModel(type, group, name, data) {
  return request({
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'put',
    data: data,
  });
}

export function deleteSecondaryDataModel(type, group, name) {
  return request({
    url: `/api/v1/${type}/schema/${group}/${name}`,
    method: 'delete',
  });
}

export function fetchProperties(data) {
  return request({
    url: `/api/v1/property/data/query`,
    method: 'post',
    data,
  });
}

export function deleteProperty(group, name) {
  return request({
    url: `/api/v1/property/schema/${group}/${name}`,
    method: 'delete',
  });
}

export function updateProperty(group, name, data) {
  return request({
    url: `/api/v1/property/schema/${group}/${name}`,
    method: 'put',
    data: data,
  });
}

export function createProperty(data) {
  return request({
    url: `/api/v1/property/schema`,
    method: 'post',
    data: data,
  });
}

export function applyProperty(group, name, id, data) {
  return request({
    url: `/api/v1/property/data/${group}/${name}/${id}`,
    method: 'put',
    data: data,
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
