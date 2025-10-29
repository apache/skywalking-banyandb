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
export const MENU_ACTIVE_COLOR = 'var(--color-main)';
export const MENU_DEFAULT_PATH = '/banyandb/query';

// Menu configuration
export const MenuConfig = [
  {
    index: '/banyandb/query',
    label: 'Query',
  },
  {
    index: 'management',
    label: 'Management',
    children: [
      { index: '/banyandb/stream', label: 'Stream' },
      { index: '/banyandb/measure', label: 'Measure' },
      { index: '/banyandb/trace', label: 'Trace' },
      { index: '/banyandb/property', label: 'Property' },
    ],
  },
  {
    index: '/banyandb/dashboard',
    label: 'Monitoring',
  },
];
