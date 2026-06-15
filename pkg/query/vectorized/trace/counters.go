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

package trace

import "sync/atomic"

// Process-wide observability counter. Integration tests snapshot QueryCount()
// before the test table runs and assert the delta is > 0 to prove the
// vectorized path actually fired (vs silently never being reached).
var queryCount atomic.Int64

// QueryCount returns the cumulative number of vectorized trace queries
// successfully started by this process.
func QueryCount() int64 { return queryCount.Load() }

// IncrQueryCount increments the process-wide vectorized trace query counter.
// Called by banyand/trace when the vectorized path is taken.
func IncrQueryCount() { queryCount.Add(1) }
