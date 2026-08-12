// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import "sync/atomic"

// Process-wide observability counter. Integration tests snapshot QueryCount()
// before the test table runs and assert the delta is > 0 to prove the
// vectorized stream path actually fired (vs silently falling back to the row
// path for a shape it does not vectorize).
var queryCount atomic.Int64

// QueryCount returns the cumulative number of vectorized stream queries executed
// by this process.
func QueryCount() int64 { return queryCount.Load() }

// IncrQueryCount increments the process-wide vectorized stream query counter.
// Called when the vectorized execution path is taken for a stream query.
func IncrQueryCount() { queryCount.Add(1) }
