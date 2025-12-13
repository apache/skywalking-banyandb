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

package exporter

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/internal/metrics"
)

func TestNewDatasourceCollector(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000)
	collector := NewDatasourceCollector(fr)

	assert.NotNil(t, collector)
	assert.Equal(t, fr, collector.flightRecorder)
	assert.NotNil(t, collector.descs)
	assert.Empty(t, collector.descs)
}

func TestDatasourceCollector_Describe_EmptyFlightRecorder(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000)
	collector := NewDatasourceCollector(fr)

	ch := make(chan *prometheus.Desc, 100)
	collector.Describe(ch)
	close(ch)

	descs := make([]*prometheus.Desc, 0)
	for desc := range ch {
		descs = append(descs, desc)
	}

	assert.Empty(t, descs)
}

func TestDatasourceCollector_Describe_WithMetrics(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000000)
	collector := NewDatasourceCollector(fr)

	// Add some metrics to FlightRecorder
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "http_requests_total",
			Value:  100.0,
			Labels: []metrics.Label{{Name: "method", Value: "GET"}},
			Desc:   "Total number of HTTP requests",
		},
		{
			Name:   "http_requests_total",
			Value:  200.0,
			Labels: []metrics.Label{{Name: "method", Value: "POST"}},
			Desc:   "Total number of HTTP requests",
		},
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
			Desc:   "CPU usage percentage",
		},
	}

	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	ch := make(chan *prometheus.Desc, 100)
	collector.Describe(ch)
	close(ch)

	descs := make([]*prometheus.Desc, 0)
	for desc := range ch {
		descs = append(descs, desc)
	}

	assert.Greater(t, len(descs), 0)
}

func TestDatasourceCollector_Collect_WithMetrics(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000000)
	collector := NewDatasourceCollector(fr)

	// Add metrics to FlightRecorder
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "http_requests_total",
			Value:  100.0,
			Labels: []metrics.Label{{Name: "method", Value: "GET"}},
			Desc:   "Total number of HTTP requests",
		},
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
			Desc:   "CPU usage percentage",
		},
	}

	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	ch := make(chan prometheus.Metric, 100)
	collector.Collect(ch)
	close(ch)

	metricsCollected := make([]prometheus.Metric, 0)
	for metric := range ch {
		metricsCollected = append(metricsCollected, metric)
	}

	assert.Greater(t, len(metricsCollected), 0)

	// Verify metric values
	for _, metric := range metricsCollected {
		desc := metric.Desc()
		assert.NotNil(t, desc)
	}
}

func TestDatasourceCollector_Collect_MultipleDatasources(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000000)
	collector := NewDatasourceCollector(fr)

	// Add first batch of metrics
	rawMetrics1 := []metrics.RawMetric{
		{
			Name:   "metric1",
			Value:  10.0,
			Labels: []metrics.Label{{Name: "label1", Value: "value1"}},
			Desc:   "First metric",
		},
	}

	updateErr1 := fr.Update(rawMetrics1)
	require.NoError(t, updateErr1)

	// Add second batch of metrics
	rawMetrics2 := []metrics.RawMetric{
		{
			Name:   "metric2",
			Value:  20.0,
			Labels: []metrics.Label{{Name: "label2", Value: "value2"}},
			Desc:   "Second metric",
		},
	}

	updateErr2 := fr.Update(rawMetrics2)
	require.NoError(t, updateErr2)

	ch := make(chan prometheus.Metric, 100)
	collector.Collect(ch)
	close(ch)

	metricsCollected := make([]prometheus.Metric, 0)
	for metric := range ch {
		metricsCollected = append(metricsCollected, metric)
	}

	assert.Greater(t, len(metricsCollected), 0)
}

func TestDatasourceCollector_Collect_EmptyRingBuffer(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000)
	collector := NewDatasourceCollector(fr)

	// Create a datasource with empty ring buffers
	ds := flightrecorder.NewDatasource()
	ds.Capacity = 1000

	ch := make(chan prometheus.Metric, 100)
	collector.Collect(ch)
	close(ch)

	metricsCollected := make([]prometheus.Metric, 0)
	for metric := range ch {
		metricsCollected = append(metricsCollected, metric)
	}

	// Should not collect metrics from empty buffers
	assert.Empty(t, metricsCollected)
}

func TestDatasourceCollector_Collect_LabelPreservation(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000000)
	collector := NewDatasourceCollector(fr)

	// Add metric with multiple labels
	rawMetrics := []metrics.RawMetric{
		{
			Name: "http_requests_total",
			Value: 100.0,
			Labels: []metrics.Label{
				{Name: "method", Value: "GET"},
				{Name: "status", Value: "200"},
			},
			Desc: "Total number of HTTP requests",
		},
	}

	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	ch := make(chan prometheus.Metric, 100)
	collector.Collect(ch)
	close(ch)

	metricsCollected := make([]prometheus.Metric, 0)
	for metric := range ch {
		metricsCollected = append(metricsCollected, metric)
	}

	assert.Greater(t, len(metricsCollected), 0)

	// Verify labels are preserved
	for _, metric := range metricsCollected {
		desc := metric.Desc()
		assert.NotNil(t, desc)
		// Check that label names are present in the description
		// The descriptor string format includes variableLabels
		descStr := desc.String()
		assert.Contains(t, descStr, "method")
		assert.Contains(t, descStr, "status")
		// Also verify by checking the descriptor key format
		labelNames := getLabelNamesFromDesc(desc)
		if len(labelNames) > 0 {
			assert.Contains(t, labelNames, "method")
			assert.Contains(t, labelNames, "status")
		}
	}
}

func TestDatasourceCollector_Collect_HELPTextInclusion(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000000)
	collector := NewDatasourceCollector(fr)

	helpText := "This is a test metric description"
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "test_metric",
			Value:  42.0,
			Labels: []metrics.Label{},
			Desc:   helpText,
		},
	}

	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	ch := make(chan *prometheus.Desc, 100)
	collector.Describe(ch)
	close(ch)

	descs := make([]*prometheus.Desc, 0)
	for desc := range ch {
		descs = append(descs, desc)
	}

	assert.Greater(t, len(descs), 0)

	// Verify HELP text is included
	for _, desc := range descs {
		if desc.String() != "" {
			// The description should contain the help text
			assert.Contains(t, desc.String(), helpText)
		}
	}
}

func TestDatasourceCollector_Collect_ConcurrentAccess(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000000)
	collector := NewDatasourceCollector(fr)

	// Add metrics
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "concurrent_metric",
			Value:  50.0,
			Labels: []metrics.Label{},
			Desc:   "Concurrent access test",
		},
	}

	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	// Test concurrent Describe and Collect calls
	done := make(chan bool, 2)

	go func() {
		ch := make(chan *prometheus.Desc, 100)
		collector.Describe(ch)
		close(ch)
		done <- true
	}()

	go func() {
		ch := make(chan prometheus.Metric, 100)
		collector.Collect(ch)
		close(ch)
		done <- true
	}()

	// Wait for both goroutines to complete
	<-done
	<-done

	// If we get here without a race condition, the test passes
	assert.True(t, true)
}

func TestParseMetricKey_SimpleMetric(t *testing.T) {
	metricKeyStr := "cpu_usage"
	parsedKey, err := parseMetricKey(metricKeyStr)

	require.NoError(t, err)
	assert.Equal(t, "cpu_usage", parsedKey.name)
	assert.Empty(t, parsedKey.labels)
}

func TestParseMetricKey_WithLabels(t *testing.T) {
	metricKeyStr := `http_requests_total{method="GET",status="200"}`
	parsedKey, err := parseMetricKey(metricKeyStr)

	require.NoError(t, err)
	assert.Equal(t, "http_requests_total", parsedKey.name)
	assert.Len(t, parsedKey.labels, 2)
	assert.Equal(t, "method", parsedKey.labels[0].Name)
	assert.Equal(t, "GET", parsedKey.labels[0].Value)
	assert.Equal(t, "status", parsedKey.labels[1].Name)
	assert.Equal(t, "200", parsedKey.labels[1].Value)
}

func TestParseMetricKey_SingleLabel(t *testing.T) {
	metricKeyStr := `metric_name{label1="value1"}`
	parsedKey, err := parseMetricKey(metricKeyStr)

	require.NoError(t, err)
	assert.Equal(t, "metric_name", parsedKey.name)
	assert.Len(t, parsedKey.labels, 1)
	assert.Equal(t, "label1", parsedKey.labels[0].Name)
	assert.Equal(t, "value1", parsedKey.labels[0].Value)
}

func TestParseMetricKey_InvalidFormat(t *testing.T) {
	invalidKeys := []string{
		"",
		"invalid{",
		"invalid}",
		"{label=\"value\"}",
		"metric name",
		"metric-name",
	}

	for _, invalidKey := range invalidKeys {
		t.Run(invalidKey, func(t *testing.T) {
			_, err := parseMetricKey(invalidKey)
			assert.Error(t, err)
		})
	}
}

func TestGetDescriptorKey_SimpleMetric(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000)
	collector := NewDatasourceCollector(fr)

	key := collector.getDescriptorKey("cpu_usage", []metrics.Label{})
	assert.Equal(t, "cpu_usage", key)
}

func TestGetDescriptorKey_WithLabels(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000)
	collector := NewDatasourceCollector(fr)

	labels := []metrics.Label{
		{Name: "method", Value: "GET"},
		{Name: "status", Value: "200"},
	}

	key := collector.getDescriptorKey("http_requests_total", labels)
	// Label names should be sorted
	assert.Contains(t, key, "http_requests_total")
	assert.Contains(t, key, "method")
	assert.Contains(t, key, "status")
}

// getLabelNamesFromDesc extracts label names from a prometheus.Desc.
// This is a helper function for testing.
func getLabelNamesFromDesc(desc *prometheus.Desc) []string {
	// prometheus.Desc doesn't expose label names directly, so we parse from String()
	descStr := desc.String()
	labelNames := make([]string, 0)

	// The descriptor string format is: "Desc{fqName: "name", help: "help", constLabels: {}, variableLabels: [label1 label2]}"
	// We need to extract the variableLabels section
	startIdx := strings.Index(descStr, "variableLabels: [")
	if startIdx == -1 {
		return labelNames
	}

	startIdx += len("variableLabels: [")
	endIdx := strings.Index(descStr[startIdx:], "]")
	if endIdx == -1 {
		return labelNames
	}

	labelsStr := descStr[startIdx : startIdx+endIdx]
	if labelsStr == "" {
		return labelNames
	}

	// Split by space and filter out empty strings
	parts := strings.Fields(labelsStr)
	for _, part := range parts {
		if part != "" {
			labelNames = append(labelNames, part)
		}
	}

	return labelNames
}

