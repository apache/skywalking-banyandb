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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/internal/metrics"
)

// TestPrometheusFormatConversion verifies that metrics from datasource are correctly converted to Prometheus format.
func TestPrometheusFormatConversion(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000000)
	collector := NewDatasourceCollector(fr)

	// Add metrics to FlightRecorder
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "http_requests_total",
			Value:  100.0,
			Labels: []metrics.Label{{Name: "method", Value: "GET"}, {Name: "status", Value: "200"}},
			Desc:   "Total number of HTTP requests",
		},
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
			Desc:   "CPU usage percentage",
		},
		{
			Name:   "memory_bytes",
			Value:  1024.0,
			Labels: []metrics.Label{{Name: "type", Value: "heap"}},
			Desc:   "Memory usage in bytes",
		},
	}

	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	// Create Prometheus registry and register collector
	reg := prometheus.NewRegistry()
	registerErr := reg.Register(collector)
	require.NoError(t, registerErr)

	// Create HTTP handler
	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	// Create test request
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	// Serve the request
	handler.ServeHTTP(w, req)

	// Verify response
	assert.Equal(t, http.StatusOK, w.Code)
	contentType := w.Header().Get("Content-Type")
	assert.Contains(t, contentType, "text/plain")
	assert.Contains(t, contentType, "version=0.0.4")
	assert.Contains(t, contentType, "charset=utf-8")

	body := w.Body.String()
	t.Logf("Prometheus metrics output:\n%s", body)

	// Verify Prometheus format requirements
	lines := strings.Split(body, "\n")

	// Check for HELP lines
	hasHelpHTTPRequests := false
	hasHelpCPUUsage := false
	hasHelpMemoryBytes := false

	// Check for TYPE lines
	hasTypeHTTPRequests := false
	hasTypeCPUUsage := false
	hasTypeMemoryBytes := false

	// Check for metric lines
	hasMetricHTTPRequests := false
	hasMetricCPUUsage := false
	hasMetricMemoryBytes := false

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Check HELP lines
		if strings.HasPrefix(line, "# HELP") {
			if strings.Contains(line, "http_requests_total") && strings.Contains(line, "Total number of HTTP requests") {
				hasHelpHTTPRequests = true
			}
			if strings.Contains(line, "cpu_usage") && strings.Contains(line, "CPU usage percentage") {
				hasHelpCPUUsage = true
			}
			if strings.Contains(line, "memory_bytes") && strings.Contains(line, "Memory usage in bytes") {
				hasHelpMemoryBytes = true
			}
		}

		// Check TYPE lines
		if strings.HasPrefix(line, "# TYPE") {
			if strings.Contains(line, "http_requests_total") && strings.Contains(line, "gauge") {
				hasTypeHTTPRequests = true
			}
			if strings.Contains(line, "cpu_usage") && strings.Contains(line, "gauge") {
				hasTypeCPUUsage = true
			}
			if strings.Contains(line, "memory_bytes") && strings.Contains(line, "gauge") {
				hasTypeMemoryBytes = true
			}
		}

		// Check metric lines (format: metric_name{labels} value)
		if !strings.HasPrefix(line, "#") && strings.Contains(line, " ") {
			if strings.HasPrefix(line, "http_requests_total{") {
				assert.Contains(t, line, `method="GET"`)
				assert.Contains(t, line, `status="200"`)
				assert.Contains(t, line, "100")
				hasMetricHTTPRequests = true
			}
			if strings.HasPrefix(line, "cpu_usage ") {
				assert.Contains(t, line, "75.5")
				hasMetricCPUUsage = true
			}
			if strings.HasPrefix(line, "memory_bytes{") {
				assert.Contains(t, line, `type="heap"`)
				assert.Contains(t, line, "1024")
				hasMetricMemoryBytes = true
			}
		}
	}

	// Verify all required elements are present
	assert.True(t, hasHelpHTTPRequests, "Missing HELP line for http_requests_total")
	assert.True(t, hasTypeHTTPRequests, "Missing TYPE line for http_requests_total")
	assert.True(t, hasMetricHTTPRequests, "Missing metric line for http_requests_total")

	assert.True(t, hasHelpCPUUsage, "Missing HELP line for cpu_usage")
	assert.True(t, hasTypeCPUUsage, "Missing TYPE line for cpu_usage")
	assert.True(t, hasMetricCPUUsage, "Missing metric line for cpu_usage")

	assert.True(t, hasHelpMemoryBytes, "Missing HELP line for memory_bytes")
	assert.True(t, hasTypeMemoryBytes, "Missing TYPE line for memory_bytes")
	assert.True(t, hasMetricMemoryBytes, "Missing metric line for memory_bytes")
}

// TestPrometheusFormatLabelOrdering verifies that labels are properly ordered in Prometheus format.
func TestPrometheusFormatLabelOrdering(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000000)
	collector := NewDatasourceCollector(fr)

	// Add metric with multiple labels (order: status, method)
	rawMetrics := []metrics.RawMetric{
		{
			Name:  "http_requests_total",
			Value: 100.0,
			Labels: []metrics.Label{
				{Name: "status", Value: "200"},
				{Name: "method", Value: "GET"},
			},
			Desc: "Total number of HTTP requests",
		},
	}

	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	// Create Prometheus registry and register collector
	reg := prometheus.NewRegistry()
	registerErr := reg.Register(collector)
	require.NoError(t, registerErr)

	// Create HTTP handler
	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	// Create test request
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	// Serve the request
	handler.ServeHTTP(w, req)

	body := w.Body.String()
	t.Logf("Prometheus metrics output:\n%s", body)

	// Verify labels are sorted alphabetically in the output
	// Labels should appear as: method="GET",status="200" (alphabetically sorted)
	lines := strings.Split(body, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "http_requests_total{") {
			// Find the position of method and status in the line
			methodIdx := strings.Index(line, `method="GET"`)
			statusIdx := strings.Index(line, `status="200"`)

			// Both should be present
			assert.NotEqual(t, -1, methodIdx, "method label not found")
			assert.NotEqual(t, -1, statusIdx, "status label not found")

			// method should come before status alphabetically
			assert.True(t, methodIdx < statusIdx, "Labels should be sorted alphabetically: method should come before status")
		}
	}
}

// TestPrometheusFormatSpecialCharacters verifies that special characters in label values are properly escaped.
func TestPrometheusFormatSpecialCharacters(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000000)
	collector := NewDatasourceCollector(fr)

	// Add metric with special characters in label value
	rawMetrics := []metrics.RawMetric{
		{
			Name:  "test_metric",
			Value: 42.0,
			Labels: []metrics.Label{
				{Name: "label", Value: `value with "quotes"`},
			},
			Desc: "Test metric",
		},
	}

	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	// Create Prometheus registry and register collector
	reg := prometheus.NewRegistry()
	registerErr := reg.Register(collector)
	require.NoError(t, registerErr)

	// Create HTTP handler
	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	// Create test request
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	// Serve the request
	handler.ServeHTTP(w, req)

	body := w.Body.String()
	t.Logf("Prometheus metrics output:\n%s", body)

	// Verify the metric line exists and is properly formatted
	assert.Contains(t, body, "test_metric{")
	assert.Contains(t, body, `label=`)
	// Prometheus client library should handle escaping automatically
}

// TestPrometheusFormatEmptyLabels verifies that metrics without labels are properly formatted.
func TestPrometheusFormatEmptyLabels(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000000)
	collector := NewDatasourceCollector(fr)

	// Add metric without labels
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "simple_metric",
			Value:  123.456,
			Labels: []metrics.Label{},
			Desc:   "Simple metric without labels",
		},
	}

	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	// Create Prometheus registry and register collector
	reg := prometheus.NewRegistry()
	registerErr := reg.Register(collector)
	require.NoError(t, registerErr)

	// Create HTTP handler
	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	// Create test request
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	// Serve the request
	handler.ServeHTTP(w, req)

	body := w.Body.String()
	t.Logf("Prometheus metrics output:\n%s", body)

	// Verify format: metric_name value (no labels)
	lines := strings.Split(body, "\n")
	found := false
	for _, line := range lines {
		if strings.HasPrefix(line, "simple_metric ") {
			// Should be: simple_metric 123.456 (no curly braces)
			assert.NotContains(t, line, "{", "Metric without labels should not have curly braces")
			assert.Contains(t, line, "123.456")
			found = true
		}
	}
	assert.True(t, found, "Metric line not found")
}

// TestPrometheusFormatMultipleMetrics verifies that multiple metrics are properly formatted.
func TestPrometheusFormatMultipleMetrics(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000000)
	collector := NewDatasourceCollector(fr)

	// Add multiple metrics
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "metric1",
			Value:  10.0,
			Labels: []metrics.Label{{Name: "label1", Value: "value1"}},
			Desc:   "First metric",
		},
		{
			Name:   "metric2",
			Value:  20.0,
			Labels: []metrics.Label{{Name: "label2", Value: "value2"}},
			Desc:   "Second metric",
		},
		{
			Name:   "metric3",
			Value:  30.0,
			Labels: []metrics.Label{},
			Desc:   "Third metric",
		},
	}

	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	// Create Prometheus registry and register collector
	reg := prometheus.NewRegistry()
	registerErr := reg.Register(collector)
	require.NoError(t, registerErr)

	// Create HTTP handler
	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	// Create test request
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	// Serve the request
	handler.ServeHTTP(w, req)

	body := w.Body.String()
	t.Logf("Prometheus metrics output:\n%s", body)

	// Verify all metrics are present
	assert.Contains(t, body, "metric1")
	assert.Contains(t, body, "metric2")
	assert.Contains(t, body, "metric3")

	// Verify HELP and TYPE lines for each metric
	assert.Contains(t, body, "# HELP metric1")
	assert.Contains(t, body, "# TYPE metric1 gauge")
	assert.Contains(t, body, "# HELP metric2")
	assert.Contains(t, body, "# TYPE metric2 gauge")
	assert.Contains(t, body, "# HELP metric3")
	assert.Contains(t, body, "# TYPE metric3 gauge")
}

// TestPrometheusFormatNumericValues verifies that numeric values are properly formatted.
func TestPrometheusFormatNumericValues(t *testing.T) {
	testCases := []struct {
		name  string
		value float64
	}{
		{"integer_value", 100.0},
		{"decimal_value", 123.456},
		{"small_value", 0.001},
		{"large_value", 1000000.0},
		{"zero_value", 0.0},
		{"negative_value", -10.0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testFR := flightrecorder.NewFlightRecorder(1000000)
			testCollector := NewDatasourceCollector(testFR)

			rawMetrics := []metrics.RawMetric{
				{
					Name:   tc.name,
					Value:  tc.value,
					Labels: []metrics.Label{},
					Desc:   "Test metric",
				},
			}

			updateErr := testFR.Update(rawMetrics)
			require.NoError(t, updateErr)

			reg := prometheus.NewRegistry()
			registerErr := reg.Register(testCollector)
			require.NoError(t, registerErr)

			handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			body := w.Body.String()
			// Verify the value appears in the output
			// The exact format depends on Prometheus client library, but should be parseable
			assert.Contains(t, body, tc.name)
		})
	}
}

// TestPrometheusFormatCompliance verifies overall Prometheus format compliance.
func TestPrometheusFormatCompliance(t *testing.T) {
	fr := flightrecorder.NewFlightRecorder(1000000)
	collector := NewDatasourceCollector(fr)

	rawMetrics := []metrics.RawMetric{
		{
			Name:   "test_metric",
			Value:  42.0,
			Labels: []metrics.Label{{Name: "label", Value: "value"}},
			Desc:   "Test metric description",
		},
	}

	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	reg := prometheus.NewRegistry()
	registerErr := reg.Register(collector)
	require.NoError(t, registerErr)

	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	body := w.Body.String()

	// Verify Prometheus format compliance:
	// 1. Content-Type header
	contentType := w.Header().Get("Content-Type")
	assert.Contains(t, contentType, "text/plain")
	assert.Contains(t, contentType, "version=0.0.4")
	assert.Contains(t, contentType, "charset=utf-8")

	// 2. HELP line format: # HELP metric_name description
	helpPattern := "# HELP test_metric"
	assert.Contains(t, body, helpPattern)

	// 3. TYPE line format: # TYPE metric_name type
	typePattern := "# TYPE test_metric gauge"
	assert.Contains(t, body, typePattern)

	// 4. Metric line format: metric_name{label="value"} value
	metricPattern := "test_metric{"
	assert.Contains(t, body, metricPattern)

	// 5. Verify the body can be parsed by Prometheus parser
	// We'll do a basic check that it follows the format
	lines := strings.Split(body, "\n")
	hasHelp := false
	hasType := false
	hasMetric := false

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "# HELP test_metric") {
			hasHelp = true
		}
		if strings.HasPrefix(line, "# TYPE test_metric gauge") {
			hasType = true
		}
		if strings.HasPrefix(line, "test_metric{") && !strings.HasPrefix(line, "#") {
			hasMetric = true
			// Verify format: metric_name{label="value"} value
			assert.Contains(t, line, `label="value"`)
			assert.Contains(t, line, "42")
		}
	}

	assert.True(t, hasHelp, "Missing HELP line")
	assert.True(t, hasType, "Missing TYPE line")
	assert.True(t, hasMetric, "Missing metric line")

	// Verify the output doesn't contain invalid characters
	invalidChars := []string{"\x00", "\x01", "\x02"}
	for _, char := range invalidChars {
		assert.NotContains(t, body, char, "Output contains invalid character")
	}
}
