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

import { BASE_PATH, SCHEMA_TYPES, ROUTE_PARAMS } from '../constants';
import { createStartRoute } from '../routeFactory';

/**
 * Property routes
 * Includes: CRUD only
 */
export default {
  path: `${BASE_PATH}/property`,
  name: 'Property',
  redirect: `${BASE_PATH}/property/start`,
  component: () => import('@/views/Property/index.vue'),
  children: [
    createStartRoute(SCHEMA_TYPES.PROPERTY),
    {
      path: ROUTE_PARAMS.OPERATOR_READ,
      name: SCHEMA_TYPES.PROPERTY,
      component: () => import('@/components/Property/PropertyRead.vue'),
    },
    {
      path: ROUTE_PARAMS.OPERATOR_CREATE,
      name: `create-${SCHEMA_TYPES.PROPERTY}`,
      component: () => import('@/views/Property/createEdit.vue'),
    },
    {
      path: ROUTE_PARAMS.OPERATOR_EDIT,
      name: `edit-${SCHEMA_TYPES.PROPERTY}`,
      component: () => import('@/views/Property/createEdit.vue'),
    },
  ],
};

