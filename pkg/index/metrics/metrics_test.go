// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
)

func seriesOf(t *testing.T, reg *prometheus.Registry, name string) int {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == name {
			return len(family.GetMetric())
		}
	}
	return 0
}

func TestDeleteAllRemovesDocCountSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	scope := meter.NewHierarchicalScope("banyandb", "_").SubScope("test")
	factory := services.NewFactory(prom.NewProvider(scope, reg), nil, nil)
	m := NewMetrics(factory, "seg")

	m.ObserveNative(7, 1024, "20240501")
	m.ObserveNative(3, 512, "20240502")
	const docCount = "banyandb_test_inverted_index_total_doc_count"
	require.Equal(t, 2, seriesOf(t, reg, docCount))

	m.DeleteAll("20240501")
	require.Equal(t, 1, seriesOf(t, reg, docCount))
	require.Equal(t, 1, seriesOf(t, reg, "banyandb_test_inverted_index_native_snapshot_bytes"))
}
