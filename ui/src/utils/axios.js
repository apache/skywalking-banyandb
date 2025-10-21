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

// axios response interceptors file

// Deprecated axios wrapper
// This file used to provide an axios instance. The project has migrated API calls
// to use `httpQuery` (fetch-based) in `ui/src/api/base.js`.
//
// Keep a minimal stub to surface a clear runtime error if accidentally imported.

export default function deprecatedAxios() {
  throw new Error(
    "ui/src/utils/axios.js is deprecated. Use httpQuery from '@/api/base' instead.",
  );
}
