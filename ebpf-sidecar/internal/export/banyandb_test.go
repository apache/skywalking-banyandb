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

package export

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/metrics"
)

func TestBanyanDBExporter(t *testing.T) {
	t.Run("NewBanyanDBExporter", func(t *testing.T) {
		logger := zaptest.NewLogger(t)

		t.Run("valid config", func(t *testing.T) {
			config := BanyanDBConfig{
				Endpoint:    "localhost:17912",
				Group:       "test-group",
				MeasureName: "test-measure",
			}

			exporter, err := NewBanyanDBExporter(config, logger)
			require.NoError(t, err)
			assert.NotNil(t, exporter)
			assert.Equal(t, "localhost:17912", exporter.config.Endpoint)
			assert.Equal(t, "test-group", exporter.config.Group)
			assert.Equal(t, "test-measure", exporter.config.MeasureName)
		})

		t.Run("empty endpoint", func(t *testing.T) {
			config := BanyanDBConfig{
				Group: "test-group",
			}

			exporter, err := NewBanyanDBExporter(config, logger)
			assert.Error(t, err)
			assert.Nil(t, exporter)
			assert.Contains(t, err.Error(), "endpoint is required")
		})

		t.Run("defaults", func(t *testing.T) {
			config := BanyanDBConfig{
				Endpoint: "localhost:17912",
			}

			exporter, err := NewBanyanDBExporter(config, logger)
			require.NoError(t, err)
			assert.Equal(t, "ebpf-metrics", exporter.config.Group)
			assert.Equal(t, "system-metrics", exporter.config.MeasureName)
			assert.Equal(t, 1000, exporter.config.BatchSize)
			assert.Equal(t, 10*time.Second, exporter.config.FlushInterval)
		})
	})

	t.Run("createDataPoint", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		config := DefaultBanyanDBConfig()
		exporter, err := NewBanyanDBExporter(config, logger)
		require.NoError(t, err)

		metric := metrics.Metric{
			Name:  "test_metric",
			Value: 42.5,
			Type:  metrics.MetricTypeGauge,
			Labels: map[string]string{
				"pid":    "1234",
				"advice": "dontneed",
			},
		}

		now := time.Now()
		dp := exporter.createDataPoint("test_module", metric, now)

		assert.NotNil(t, dp)
		assert.NotNil(t, dp.Timestamp)
		assert.Len(t, dp.TagFamilies, 1)
		assert.Len(t, dp.Fields, 1)

		// Check tags include module name and metric name
		tags := dp.TagFamilies[0].Tags
		assert.GreaterOrEqual(t, len(tags), 2)
	})

	t.Run("buffer management", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		config := BanyanDBConfig{
			Endpoint:  "localhost:17912",
			BatchSize: 2, // Small batch for testing
		}

		exporter, err := NewBanyanDBExporter(config, logger)
		require.NoError(t, err)

		// Create test metrics
		store := metrics.NewStore()
		ms := metrics.NewMetricSet()
		ms.AddGauge("metric1", 10.0, nil)
		ms.AddGauge("metric2", 20.0, nil)
		ms.AddGauge("metric3", 30.0, nil)
		store.Update("test", ms)

		// Since we can't actually connect to BanyanDB in unit tests,
		// we just verify the buffer management logic
		assert.Equal(t, 0, len(exporter.buffer))

		// Simulate adding metrics to buffer
		now := time.Now()
		for _, m := range ms.GetMetrics() {
			dp := exporter.createDataPoint("test", m, now)
			exporter.buffer = append(exporter.buffer, dp)
		}

		assert.Equal(t, 3, len(exporter.buffer))
	})
}

func TestDefaultBanyanDBConfig(t *testing.T) {
	config := DefaultBanyanDBConfig()

	assert.Equal(t, "localhost:17912", config.Endpoint)
	assert.Equal(t, "ebpf-metrics", config.Group)
	assert.Equal(t, "system-metrics", config.MeasureName)
	assert.Equal(t, 10*time.Second, config.Timeout)
	assert.Equal(t, 1000, config.BatchSize)
	assert.Equal(t, 10*time.Second, config.FlushInterval)
}

// TestBanyanDBExportIntegration would test actual connection.
// This is skipped in unit tests as it requires a running BanyanDB instance
func TestBanyanDBExportIntegration(t *testing.T) {
	t.Skip("Integration test requires running BanyanDB instance")

	logger := zaptest.NewLogger(t)
	config := DefaultBanyanDBConfig()

	exporter, err := NewBanyanDBExporter(config, logger)
	require.NoError(t, err)
	defer exporter.Close()

	ctx := context.Background()
	err = exporter.Connect(ctx)
	if err != nil {
		t.Skip("Cannot connect to BanyanDB, skipping integration test")
	}

	// Create test metrics
	store := metrics.NewStore()
	ms := metrics.NewMetricSet()
	ms.AddCounter("ebpf_test_counter", 100.0, map[string]string{"test": "true"})
	ms.AddGauge("ebpf_test_gauge", 50.0, nil)
	store.Update("integration_test", ms)

	// Export metrics
	err = exporter.Export(ctx, store)
	assert.NoError(t, err)
}
