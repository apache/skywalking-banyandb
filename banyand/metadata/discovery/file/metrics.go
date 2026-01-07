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

package file

import (
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

type metrics struct {
	fileLoadCount       meter.Counter
	fileLoadFailedCount meter.Counter
	fileLoadDuration    meter.Histogram
	totalNodesCount     meter.Gauge
}

// newMetrics creates a new metrics instance.
func newMetrics(factory observability.Factory) *metrics {
	return &metrics{
		fileLoadCount:       factory.NewCounter("file_load_count"),
		fileLoadFailedCount: factory.NewCounter("file_load_failed_count"),
		fileLoadDuration:    factory.NewHistogram("file_load_duration", meter.DefBuckets),
		totalNodesCount:     factory.NewGauge("total_nodes_count"),
	}
}
