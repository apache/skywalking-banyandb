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

package schema_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
	"github.com/apache/skywalking-banyandb/pkg/schema"
)

// TestNewMetricsCreatesTotalPanicsAtZero asserts that total_panics exists at zero before any panic, so that
// increase() sees the first one.
func TestNewMetricsCreatesTotalPanicsAtZero(t *testing.T) {
	reg := prometheus.NewRegistry()
	schema.NewMetrics(services.NewFactory(prom.NewProvider(meter.NewHierarchicalScope("test", "_"), reg), nil, nil))

	families, err := reg.Gather()
	require.NoError(t, err)
	values := make(map[string]float64)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			values[family.GetName()] = metric.GetCounter().GetValue()
		}
	}
	require.Equal(t, map[string]float64{"test_total_panics": 0}, values)
}
