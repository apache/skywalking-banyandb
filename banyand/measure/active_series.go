// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package measure

import (
	"time"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
)

// countsActiveSeries reports whether writes of m count toward the group's active series: a Top-N result measure has a
// writer per liaison, and a sharding key outside the entity spreads one series over several shards.
func countsActiveSeries(m *databasev1.Measure) bool {
	return m.GetMetadata().GetName() != TopNSchemaName && validate.CheckShardingKeySubset(m) == nil
}

// SetActiveSeriesWindowForTest forwards to storage.SetActiveSeriesWindowForTest for in-process cluster tests.
// The window is a process-wide setting: it applies to every tracker created afterwards, in all databases of the process.
func SetActiveSeriesWindowForTest(d time.Duration) (restore func()) {
	return storage.SetActiveSeriesWindowForTest(d)
}
