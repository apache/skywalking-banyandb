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

package pub

import (
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// lifecycleMigrationScope produces the banyandb_lifecycle_migration_* family: the
// tier-migration mirror of the banyandb_queue_pub_* family (see queuePubScope).
var lifecycleMigrationScope = observability.RootScope.SubScope("lifecycle_migration")

// pubMigrationMetrics mirrors pubMetrics for the lifecycle tier-migration
// publisher. That publisher is built via NewWithoutMetadata (no metadata), so the
// regular pubMetrics stay nil and emit nothing; this parallel family is registered
// from an explicitly supplied MetricsRegistry instead, on its own scope so the
// migration layer can be queried independently of the write-pipeline pub family.
type pubMigrationMetrics struct {
	totalStarted       meter.Counter
	totalFinished      meter.Counter
	totalLatency       meter.Histogram
	totalErr           meter.Counter
	sentBytes          meter.Counter
	totalBatchStarted  meter.Counter
	totalBatchFinished meter.Counter
	totalBatchLatency  meter.Histogram
}

func newPubMigrationMetrics(factory observability.Factory) *pubMigrationMetrics {
	labels := []string{"operation", "group", "remote_node", "remote_role", "remote_tier"}
	return &pubMigrationMetrics{
		totalStarted:       factory.NewCounter("total_started", labels...),
		totalFinished:      factory.NewCounter("total_finished", labels...),
		totalLatency:       factory.NewHistogram("total_latency", meter.DefBuckets, labels...),
		totalErr:           factory.NewCounter("total_err", append(labels, "error_type")...),
		sentBytes:          factory.NewCounter("sent_bytes", labels...),
		totalBatchStarted:  factory.NewCounter("total_batch_started", labels...),
		totalBatchFinished: factory.NewCounter("total_batch_finished", labels...),
		totalBatchLatency:  factory.NewHistogram("total_batch_latency", meter.BatchBuckets, labels...),
	}
}
