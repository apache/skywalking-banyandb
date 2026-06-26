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

import type { GroupListResponse, QueryRequest, QueryResponse } from 'canopy-shared';

import type { DataSource } from './DataSource.js';

interface BanyanGroupListResponse {
  groups?: GroupListResponse['groups'];
}

export class ApiDataSource implements DataSource {
  async listGroups(): Promise<GroupListResponse> {
    const res = await fetch('/api/v1/group/schema/lists');
    if (!res.ok) {
      throw new Error(`Failed to load groups: ${res.status} ${res.statusText}`);
    }
    const data = (await res.json()) as BanyanGroupListResponse;
    return { groups: data.groups ?? [] };
  }

  async getResource(_catalog: string, _group: string, _name: string): Promise<unknown> {
    throw new Error('getResource not implemented in M2');
  }

  async runQuery(_request: QueryRequest): Promise<QueryResponse> {
    throw new Error('runQuery not implemented in M2');
  }
}

export const apiDataSource = new ApiDataSource();
