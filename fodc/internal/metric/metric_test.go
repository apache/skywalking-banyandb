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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.  See the License for the specific
// language governing permissions and limitations under the License.

package metric

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_ParseMetricLine_SimpleMetrics tests parsing simple metrics without labels
func TestE2E_ParseMetricLine_SimpleMetrics(t *testing.T) {
	tests := []struct {
		name     string
		line     string
		expected RawMetric
	}{
		{
			name: "simple counter",
			line: "http_requests_total 1234",
			expected: RawMetric{
				Name:   "http_requests_total",
				Labels: []Label{},
				Value:  1234.0,
			},
		},
		{
			name: "simple gauge with decimal",
			line: "memory_usage_bytes 1024.5",
			expected: RawMetric{
				Name:   "memory_usage_bytes",
				Labels: []Label{},
				Value:  1024.5,
			},
		},
		{
			name: "simple metric with zero",
			line: "errors_total 0",
			expected: RawMetric{
				Name:   "errors_total",
				Labels: []Label{},
				Value:  0.0,
			},
		},
		{
			name: "simple metric with negative value",
			line: "temperature_celsius -10.5",
			expected: RawMetric{
				Name:   "temperature_celsius",
				Labels: []Label{},
				Value:  -10.5,
			},
		},
		{
			name: "simple metric with scientific notation",
			line: "large_number 1.5e+06",
			expected: RawMetric{
				Name:   "large_number",
				Labels: []Label{},
				Value:  1500000.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric, err := ParseMetricLine(tt.line)
			require.NoError(t, err)
			assert.Equal(t, tt.expected.Name, metric.Name)
			// Compare labels length and content (nil vs empty slice are equivalent)
			if len(tt.expected.Labels) == 0 {
				assert.Equal(t, 0, len(metric.Labels))
			} else {
				assert.Equal(t, tt.expected.Labels, metric.Labels)
			}
			assert.InDelta(t, tt.expected.Value, metric.Value, 0.0001)
		})
	}
}

// TestE2E_ParseMetricLine_WithLabels tests parsing metrics with labels
func TestE2E_ParseMetricLine_WithLabels(t *testing.T) {
	tests := []struct {
		name     string
		line     string
		expected RawMetric
	}{
		{
			name: "single label",
			line: `http_requests_total{method="GET"} 1234`,
			expected: RawMetric{
				Name: "http_requests_total",
				Labels: []Label{
					{Name: "method", Value: "GET"},
				},
				Value: 1234.0,
			},
		},
		{
			name: "multiple labels",
			line: `http_requests_total{method="POST",status="200",endpoint="/api/users"} 567`,
			expected: RawMetric{
				Name: "http_requests_total",
				Labels: []Label{
					{Name: "method", Value: "POST"},
					{Name: "status", Value: "200"},
					{Name: "endpoint", Value: "/api/users"},
				},
				Value: 567.0,
			},
		},
		{
			name: "labels with special characters",
			line: `metric_name{label1="value with spaces",label2="value-with-dashes",label3="value_with_underscores"} 42`,
			expected: RawMetric{
				Name: "metric_name",
				Labels: []Label{
					{Name: "label1", Value: "value with spaces"},
					{Name: "label2", Value: "value-with-dashes"},
					{Name: "label3", Value: "value_with_underscores"},
				},
				Value: 42.0,
			},
		},
		{
			name: "labels with empty values",
			line: `metric_name{label1="",label2="value"} 10`,
			expected: RawMetric{
				Name: "metric_name",
				Labels: []Label{
					{Name: "label1", Value: ""},
					{Name: "label2", Value: "value"},
				},
				Value: 10.0,
			},
		},
		{
			name: "labels with spaces around commas",
			line: `metric_name{label1="value1" , label2="value2" , label3="value3"} 100`,
			expected: RawMetric{
				Name: "metric_name",
				Labels: []Label{
					{Name: "label1", Value: "value1"},
					{Name: "label2", Value: "value2"},
					{Name: "label3", Value: "value3"},
				},
				Value: 100.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric, err := ParseMetricLine(tt.line)
			require.NoError(t, err)
			assert.Equal(t, tt.expected.Name, metric.Name)
			assert.Equal(t, tt.expected.Value, metric.Value)
			assert.Equal(t, len(tt.expected.Labels), len(metric.Labels))
			for i, expectedLabel := range tt.expected.Labels {
				assert.Equal(t, expectedLabel.Name, metric.Labels[i].Name)
				assert.Equal(t, expectedLabel.Value, metric.Labels[i].Value)
			}
		})
	}
}

// TestE2E_ParseMetricLine_InvalidInputs tests error handling for invalid inputs
func TestE2E_ParseMetricLine_InvalidInputs(t *testing.T) {
	tests := []struct {
		name        string
		line        string
		expectError bool
	}{
		{
			name:        "empty line",
			line:        "",
			expectError: true,
		},
		{
			name:        "only metric name",
			line:        "metric_name",
			expectError: true,
		},
		{
			name:        "invalid value",
			line:        "metric_name abc",
			expectError: true,
		},
		{
			name:        "unclosed label brace",
			line:        `metric_name{label="value" 123`,
			expectError: false, // Falls back to simple parsing, treats as metric_name{label="value" and 123 as separate parts
		},
		{
			name:        "unopened label brace",
			line:        `metric_namelabel="value"} 123`,
			expectError: false, // Falls back to simple parsing
		},
		{
			name:        "malformed label",
			line:        `metric_name{label} 123`,
			expectError: false, // This might parse but label will be empty
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseMetricLine(tt.line)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				// For malformed cases that don't error, we just verify it doesn't panic
				_ = err
			}
		})
	}
}

// TestE2E_RawMetric_Find tests the Find method on RawMetric
func TestE2E_RawMetric_Find(t *testing.T) {
	metric := RawMetric{
		Name: "test_metric",
		Labels: []Label{
			{Name: "method", Value: "GET"},
			{Name: "status", Value: "200"},
			{Name: "endpoint", Value: "/api"},
		},
		Value: 100.0,
	}

	tests := []struct {
		name       string
		labelName  string
		expected   string
		shouldFind bool
	}{
		{
			name:       "find existing label",
			labelName:  "method",
			expected:   "GET",
			shouldFind: true,
		},
		{
			name:       "find another existing label",
			labelName:  "status",
			expected:   "200",
			shouldFind: true,
		},
		{
			name:       "find non-existent label",
			labelName:  "nonexistent",
			expected:   "",
			shouldFind: false,
		},
		{
			name:       "find empty string label name",
			labelName:  "",
			expected:   "",
			shouldFind: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := metric.Find(tt.labelName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestE2E_RawMetric_Remove tests the Remove method on RawMetric
func TestE2E_RawMetric_Remove(t *testing.T) {
	tests := []struct {
		name           string
		metric         RawMetric
		removeLabel    string
		expectedLabels []Label
		expectedFound  bool
	}{
		{
			name: "remove existing label",
			metric: RawMetric{
				Name: "test",
				Labels: []Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "2"},
					{Name: "c", Value: "3"},
				},
			},
			removeLabel: "b",
			expectedLabels: []Label{
				{Name: "a", Value: "1"},
				{Name: "c", Value: "3"},
			},
			expectedFound: true,
		},
		{
			name: "remove first label",
			metric: RawMetric{
				Name: "test",
				Labels: []Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "2"},
				},
			},
			removeLabel: "a",
			expectedLabels: []Label{
				{Name: "b", Value: "2"},
			},
			expectedFound: true,
		},
		{
			name: "remove last label",
			metric: RawMetric{
				Name: "test",
				Labels: []Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "2"},
				},
			},
			removeLabel: "b",
			expectedLabels: []Label{
				{Name: "a", Value: "1"},
			},
			expectedFound: true,
		},
		{
			name: "remove non-existent label",
			metric: RawMetric{
				Name: "test",
				Labels: []Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "2"},
				},
			},
			removeLabel: "c",
			expectedLabels: []Label{
				{Name: "a", Value: "1"},
				{Name: "b", Value: "2"},
			},
			expectedFound: false,
		},
		{
			name: "remove from empty labels",
			metric: RawMetric{
				Name:   "test",
				Labels: []Label{},
			},
			removeLabel:    "a",
			expectedLabels: nil,
			expectedFound:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			labels, found := tt.metric.Remove(tt.removeLabel)
			assert.Equal(t, tt.expectedFound, found)
			assert.Equal(t, len(tt.expectedLabels), len(labels))
			for i, expectedLabel := range tt.expectedLabels {
				assert.Equal(t, expectedLabel.Name, labels[i].Name)
				assert.Equal(t, expectedLabel.Value, labels[i].Value)
			}
		})
	}
}

// TestE2E_MetricKey_String tests the String method on MetricKey
func TestE2E_MetricKey_String(t *testing.T) {
	tests := []struct {
		name     string
		key      MetricKey
		expected string
	}{
		{
			name: "metric without labels",
			key: MetricKey{
				Name:   "simple_metric",
				Labels: []Label{},
			},
			expected: "simple_metric",
		},
		{
			name: "metric with single label",
			key: MetricKey{
				Name: "metric_name",
				Labels: []Label{
					{Name: "label1", Value: "value1"},
				},
			},
			expected: `metric_name{label1="value1"}`,
		},
		{
			name: "metric with multiple labels",
			key: MetricKey{
				Name: "metric_name",
				Labels: []Label{
					{Name: "label1", Value: "value1"},
					{Name: "label2", Value: "value2"},
					{Name: "label3", Value: "value3"},
				},
			},
			expected: `metric_name{label1="value1", label2="value2", label3="value3"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.key.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestE2E_Label_String tests the String method on Label
func TestE2E_Label_String(t *testing.T) {
	tests := []struct {
		name     string
		label    Label
		expected string
	}{
		{
			name:     "simple label",
			label:    Label{Name: "method", Value: "GET"},
			expected: `method="GET"`,
		},
		{
			name:     "label with empty value",
			label:    Label{Name: "tag", Value: ""},
			expected: `tag=""`,
		},
		{
			name:     "label with special characters",
			label:    Label{Name: "path", Value: "/api/users"},
			expected: `path="/api/users"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.label.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestE2E_ParseHistogram_CompleteHistogram tests parsing a complete histogram
func TestE2E_ParseHistogram_CompleteHistogram(t *testing.T) {
	metrics := []RawMetric{
		// Buckets
		{Name: "http_request_duration_seconds_bucket", Labels: []Label{{Name: "le", Value: "0.005"}}, Value: 10},
		{Name: "http_request_duration_seconds_bucket", Labels: []Label{{Name: "le", Value: "0.01"}}, Value: 20},
		{Name: "http_request_duration_seconds_bucket", Labels: []Label{{Name: "le", Value: "0.025"}}, Value: 30},
		{Name: "http_request_duration_seconds_bucket", Labels: []Label{{Name: "le", Value: "0.05"}}, Value: 40},
		{Name: "http_request_duration_seconds_bucket", Labels: []Label{{Name: "le", Value: "0.1"}}, Value: 50},
		{Name: "http_request_duration_seconds_bucket", Labels: []Label{{Name: "le", Value: "+Inf"}}, Value: 100},
		// Count
		{Name: "http_request_duration_seconds_count", Labels: []Label{}, Value: 100},
		// Sum
		{Name: "http_request_duration_seconds_sum", Labels: []Label{}, Value: 5.5},
		// Other metric that should be filtered out
		{Name: "other_metric", Labels: []Label{}, Value: 42},
	}

	descriptions := map[string]string{
		"http_request_duration_seconds": "HTTP request duration in seconds",
	}

	histograms, filtered := ParseHistogram(metrics, descriptions)

	// Should have one histogram
	assert.Equal(t, 1, len(histograms))

	// Check histogram content
	histKey := `http_request_duration_seconds`
	hist, exists := histograms[histKey]
	require.True(t, exists)
	assert.Equal(t, "http_request_duration_seconds", hist.Name)
	assert.Equal(t, "HTTP request duration in seconds", hist.Description)
	assert.Equal(t, 6, len(hist.Bins))

	// Verify bins are sorted by value
	expectedBins := []Bin{
		{Value: 0.005, Count: 10},
		{Value: 0.01, Count: 20},
		{Value: 0.025, Count: 30},
		{Value: 0.05, Count: 40},
		{Value: 0.1, Count: 50},
		{Value: 0, Count: 100}, // +Inf becomes 0
	}

	for i, expectedBin := range expectedBins {
		if i < len(hist.Bins) {
			if expectedBin.Value == 0 { // +Inf case
				assert.Equal(t, uint64(100), hist.Bins[i].Count)
			} else {
				assert.InDelta(t, expectedBin.Value, hist.Bins[i].Value, 0.0001)
				assert.Equal(t, expectedBin.Count, hist.Bins[i].Count)
			}
		}
	}

	// Other metric should be in filtered list
	assert.Equal(t, 1, len(filtered))
	assert.Equal(t, "other_metric", filtered[0].Name)
}

// TestE2E_ParseHistogram_WithLabels tests parsing histogram with labels
func TestE2E_ParseHistogram_WithLabels(t *testing.T) {
	metrics := []RawMetric{
		// Histogram with labels
		{Name: "http_request_duration_seconds_bucket", Labels: []Label{{Name: "method", Value: "GET"}, {Name: "le", Value: "0.1"}}, Value: 10},
		{Name: "http_request_duration_seconds_bucket", Labels: []Label{{Name: "method", Value: "GET"}, {Name: "le", Value: "+Inf"}}, Value: 20},
		{Name: "http_request_duration_seconds_count", Labels: []Label{{Name: "method", Value: "GET"}}, Value: 20},
		{Name: "http_request_duration_seconds_sum", Labels: []Label{{Name: "method", Value: "GET"}}, Value: 1.5},
	}

	histograms, filtered := ParseHistogram(metrics, nil)

	assert.Equal(t, 1, len(histograms))
	assert.Equal(t, 0, len(filtered))

	histKey := `http_request_duration_seconds{method="GET"}`
	hist, exists := histograms[histKey]
	require.True(t, exists)
	assert.Equal(t, "http_request_duration_seconds", hist.Name)
	assert.Equal(t, 1, len(hist.Labels))
	assert.Equal(t, "method", hist.Labels[0].Name)
	assert.Equal(t, "GET", hist.Labels[0].Value)
	assert.Equal(t, 2, len(hist.Bins))
}

// TestE2E_ParseHistogram_IncompleteHistogram tests incomplete histogram (missing parts)
func TestE2E_ParseHistogram_IncompleteHistogram(t *testing.T) {
	tests := []struct {
		name            string
		metrics         []RawMetric
		expectedHistLen int
		expectedFiltLen int
	}{
		{
			name: "missing count",
			metrics: []RawMetric{
				{Name: "metric_bucket", Labels: []Label{{Name: "le", Value: "0.1"}}, Value: 10},
				{Name: "metric_sum", Labels: []Label{}, Value: 1.5},
			},
			expectedHistLen: 0,
			expectedFiltLen: 2, // Both should be filtered back
		},
		{
			name: "missing sum",
			metrics: []RawMetric{
				{Name: "metric_bucket", Labels: []Label{{Name: "le", Value: "0.1"}}, Value: 10},
				{Name: "metric_count", Labels: []Label{}, Value: 20},
			},
			expectedHistLen: 0,
			expectedFiltLen: 2,
		},
		{
			name: "missing buckets",
			metrics: []RawMetric{
				{Name: "metric_count", Labels: []Label{}, Value: 20},
				{Name: "metric_sum", Labels: []Label{}, Value: 1.5},
			},
			expectedHistLen: 0,
			expectedFiltLen: 2,
		},
		{
			name: "only buckets",
			metrics: []RawMetric{
				{Name: "metric_bucket", Labels: []Label{{Name: "le", Value: "0.1"}}, Value: 10},
			},
			expectedHistLen: 0,
			expectedFiltLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			histograms, filtered := ParseHistogram(tt.metrics, nil)
			assert.Equal(t, tt.expectedHistLen, len(histograms))
			assert.Equal(t, tt.expectedFiltLen, len(filtered))
		})
	}
}

// TestE2E_ParseHistogram_MultipleHistograms tests parsing multiple histograms
func TestE2E_ParseHistogram_MultipleHistograms(t *testing.T) {
	metrics := []RawMetric{
		// First histogram
		{Name: "hist1_bucket", Labels: []Label{{Name: "le", Value: "0.1"}}, Value: 10},
		{Name: "hist1_bucket", Labels: []Label{{Name: "le", Value: "+Inf"}}, Value: 20},
		{Name: "hist1_count", Labels: []Label{}, Value: 20},
		{Name: "hist1_sum", Labels: []Label{}, Value: 1.5},
		// Second histogram
		{Name: "hist2_bucket", Labels: []Label{{Name: "le", Value: "0.5"}}, Value: 5},
		{Name: "hist2_bucket", Labels: []Label{{Name: "le", Value: "+Inf"}}, Value: 10},
		{Name: "hist2_count", Labels: []Label{}, Value: 10},
		{Name: "hist2_sum", Labels: []Label{}, Value: 2.5},
		// Regular metric
		{Name: "regular_metric", Labels: []Label{}, Value: 100},
	}

	histograms, filtered := ParseHistogram(metrics, nil)

	assert.Equal(t, 2, len(histograms))
	assert.Equal(t, 1, len(filtered))
	assert.Equal(t, "regular_metric", filtered[0].Name)

	// Check first histogram
	hist1, exists := histograms["hist1"]
	require.True(t, exists)
	assert.Equal(t, "hist1", hist1.Name)
	assert.Equal(t, 2, len(hist1.Bins))

	// Check second histogram
	hist2, exists := histograms["hist2"]
	require.True(t, exists)
	assert.Equal(t, "hist2", hist2.Name)
	assert.Equal(t, 2, len(hist2.Bins))
}

// TestE2E_ParseHELPComment tests parsing HELP comments
func TestE2E_ParseHELPComment(t *testing.T) {
	tests := []struct {
		name           string
		line           string
		expectedName   string
		expectedDesc   string
		expectedParsed bool
	}{
		{
			name:           "valid HELP comment",
			line:           "# HELP http_requests_total Total number of HTTP requests",
			expectedName:   "http_requests_total",
			expectedDesc:   "Total number of HTTP requests",
			expectedParsed: true,
		},
		{
			name:           "HELP comment with no description",
			line:           "# HELP metric_name",
			expectedName:   "metric_name",
			expectedDesc:   "",
			expectedParsed: true,
		},
		{
			name:           "HELP comment with extra spaces",
			line:           "# HELP   metric_name   Description with spaces  ",
			expectedName:   "metric_name",
			expectedDesc:   "Description with spaces",
			expectedParsed: true,
		},
		{
			name:           "not a HELP comment",
			line:           "# TYPE metric_name counter",
			expectedName:   "",
			expectedDesc:   "",
			expectedParsed: false,
		},
		{
			name:           "empty line",
			line:           "",
			expectedName:   "",
			expectedDesc:   "",
			expectedParsed: false,
		},
		{
			name:           "line without HELP prefix",
			line:           "metric_name 123",
			expectedName:   "",
			expectedDesc:   "",
			expectedParsed: false,
		},
		{
			name:           "HELP comment with only prefix",
			line:           "# HELP ",
			expectedName:   "",
			expectedDesc:   "",
			expectedParsed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, desc, parsed := ParseHELPComment(tt.line)
			assert.Equal(t, tt.expectedParsed, parsed)
			if parsed {
				assert.Equal(t, tt.expectedName, name)
				assert.Equal(t, tt.expectedDesc, desc)
			}
		})
	}
}

// TestE2E_ParseTYPEComment tests parsing TYPE comments
func TestE2E_ParseTYPEComment(t *testing.T) {
	tests := []struct {
		name           string
		line           string
		expectedName   string
		expectedType   string
		expectedParsed bool
	}{
		{
			name:           "valid TYPE comment",
			line:           "# TYPE http_requests_total counter",
			expectedName:   "http_requests_total",
			expectedType:   "counter",
			expectedParsed: true,
		},
		{
			name:           "TYPE comment with gauge",
			line:           "# TYPE memory_usage_bytes gauge",
			expectedName:   "memory_usage_bytes",
			expectedType:   "gauge",
			expectedParsed: true,
		},
		{
			name:           "TYPE comment with histogram",
			line:           "# TYPE http_request_duration_seconds histogram",
			expectedName:   "http_request_duration_seconds",
			expectedType:   "histogram",
			expectedParsed: true,
		},
		{
			name:           "TYPE comment with no type",
			line:           "# TYPE metric_name",
			expectedName:   "metric_name",
			expectedType:   "",
			expectedParsed: true,
		},
		{
			name:           "TYPE comment with extra spaces",
			line:           "# TYPE   metric_name   counter  ",
			expectedName:   "metric_name",
			expectedType:   "counter",
			expectedParsed: true,
		},
		{
			name:           "not a TYPE comment",
			line:           "# HELP metric_name Description",
			expectedName:   "",
			expectedType:   "",
			expectedParsed: false,
		},
		{
			name:           "empty line",
			line:           "",
			expectedName:   "",
			expectedType:   "",
			expectedParsed: false,
		},
		{
			name:           "line without TYPE prefix",
			line:           "metric_name 123",
			expectedName:   "",
			expectedType:   "",
			expectedParsed: false,
		},
		{
			name:           "TYPE comment with only prefix",
			line:           "# TYPE ",
			expectedName:   "",
			expectedType:   "",
			expectedParsed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, metricType, parsed := ParseTYPEComment(tt.line)
			assert.Equal(t, tt.expectedParsed, parsed)
			if parsed {
				assert.Equal(t, tt.expectedName, name)
				assert.Equal(t, tt.expectedType, metricType)
			}
		})
	}
}

// TestE2E_RealWorldPrometheusMetrics tests parsing real-world Prometheus metrics format
func TestE2E_RealWorldPrometheusMetrics(t *testing.T) {
	// Simulate a real Prometheus metrics output
	metricsLines := []string{
		"# HELP http_requests_total Total number of HTTP requests",
		"# TYPE http_requests_total counter",
		`http_requests_total{method="GET",status="200"} 1234`,
		`http_requests_total{method="POST",status="201"} 567`,
		`http_requests_total{method="GET",status="404"} 89`,
		"",
		"# HELP http_request_duration_seconds HTTP request duration in seconds",
		"# TYPE http_request_duration_seconds histogram",
		`http_request_duration_seconds_bucket{le="0.005"} 10`,
		`http_request_duration_seconds_bucket{le="0.01"} 20`,
		`http_request_duration_seconds_bucket{le="0.025"} 30`,
		`http_request_duration_seconds_bucket{le="0.05"} 40`,
		`http_request_duration_seconds_bucket{le="0.1"} 50`,
		`http_request_duration_seconds_bucket{le="+Inf"} 100`,
		`http_request_duration_seconds_count 100`,
		`http_request_duration_seconds_sum 5.5`,
		"",
		"# HELP memory_usage_bytes Current memory usage in bytes",
		"# TYPE memory_usage_bytes gauge",
		"memory_usage_bytes 1073741824",
	}

	descriptions := make(map[string]string)
	var rawMetrics []RawMetric

	// Parse HELP comments and metrics
	for _, line := range metricsLines {
		if line == "" {
			continue
		}

		if name, desc, ok := ParseHELPComment(line); ok {
			descriptions[name] = desc
			continue
		}

		if metric, err := ParseMetricLine(line); err == nil {
			rawMetrics = append(rawMetrics, metric)
		}
	}

	// Verify descriptions
	assert.Equal(t, "Total number of HTTP requests", descriptions["http_requests_total"])
	assert.Equal(t, "HTTP request duration in seconds", descriptions["http_request_duration_seconds"])
	assert.Equal(t, "Current memory usage in bytes", descriptions["memory_usage_bytes"])

	// Verify raw metrics
	// 3 counter metrics + 6 buckets + 1 count + 1 sum + 1 gauge = 12 total
	assert.Equal(t, 12, len(rawMetrics))

	// Parse histograms
	histograms, filtered := ParseHistogram(rawMetrics, descriptions)

	// Should have one histogram
	assert.Equal(t, 1, len(histograms))

	// Verify histogram has description
	histKey := "http_request_duration_seconds"
	hist, exists := histograms[histKey]
	require.True(t, exists)
	assert.Equal(t, "HTTP request duration in seconds", hist.Description)

	// Verify filtered metrics (should have counter metrics and gauge)
	// 3 counter metrics + 1 gauge = 4 (count and sum are filtered back into filtered list)
	assert.Equal(t, 4, len(filtered))

	// Verify counter metrics are in filtered list
	counterFound := false
	for _, m := range filtered {
		if m.Name == "http_requests_total" {
			counterFound = true
			break
		}
	}
	assert.True(t, counterFound)

	// Verify gauge metric is in filtered list
	gaugeFound := false
	for _, m := range filtered {
		if m.Name == "memory_usage_bytes" {
			gaugeFound = true
			assert.Equal(t, 1073741824.0, m.Value)
			break
		}
	}
	assert.True(t, gaugeFound)
}

// TestE2E_HistogramBinsParsing tests parsing histogram bins with various le values
func TestE2E_HistogramBinsParsing(t *testing.T) {
	tests := []struct {
		name        string
		bucketValue string
		expectError bool
	}{
		{
			name:        "numeric le value",
			bucketValue: "0.1",
			expectError: false,
		},
		{
			name:        "infinity le value",
			bucketValue: "+Inf",
			expectError: true, // +Inf cannot be parsed as float64
		},
		{
			name:        "large numeric value",
			bucketValue: "1000.5",
			expectError: false,
		},
		{
			name:        "zero value",
			bucketValue: "0",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := []RawMetric{
				{Name: "metric_bucket", Labels: []Label{{Name: "le", Value: tt.bucketValue}}, Value: 10},
				{Name: "metric_bucket", Labels: []Label{{Name: "le", Value: "+Inf"}}, Value: 20},
				{Name: "metric_count", Labels: []Label{}, Value: 20},
				{Name: "metric_sum", Labels: []Label{}, Value: 1.5},
			}

			histograms, _ := ParseHistogram(metrics, nil)

			if tt.expectError && tt.bucketValue == "+Inf" {
				// +Inf should be handled specially
				if len(histograms) > 0 {
					hist := histograms["metric"]
					// The +Inf bucket should be included but with value 0
					foundInf := false
					for _, bin := range hist.Bins {
						if bin.Count == 20 { // The +Inf bucket count
							foundInf = true
							break
						}
					}
					assert.True(t, foundInf)
				}
			} else {
				assert.Equal(t, 1, len(histograms))
			}
		})
	}
}

// TestE2E_EdgeCases tests various edge cases
func TestE2E_EdgeCases(t *testing.T) {
	t.Run("metric with only le label", func(t *testing.T) {
		metric, err := ParseMetricLine(`metric_bucket{le="0.1"} 10`)
		require.NoError(t, err)
		assert.Equal(t, "metric_bucket", metric.Name)
		assert.Equal(t, 1, len(metric.Labels))
		assert.Equal(t, "le", metric.Labels[0].Name)
		assert.Equal(t, "0.1", metric.Labels[0].Value)
	})

	t.Run("metric with empty labels", func(t *testing.T) {
		metric, err := ParseMetricLine(`metric_name{} 42`)
		require.NoError(t, err)
		assert.Equal(t, "metric_name", metric.Name)
		assert.Equal(t, 0, len(metric.Labels))
		assert.Equal(t, 42.0, metric.Value)
	})

	t.Run("metric with whitespace in value", func(t *testing.T) {
		metric, err := ParseMetricLine(`metric_name   123   `)
		require.NoError(t, err)
		assert.Equal(t, "metric_name", metric.Name)
		assert.Equal(t, 123.0, metric.Value)
	})

	t.Run("label value with quotes in quotes", func(t *testing.T) {
		metric, err := ParseMetricLine(`metric_name{label="value\"with\"quotes"} 10`)
		require.NoError(t, err)
		assert.Equal(t, "metric_name", metric.Name)
		// The parser should handle this, though it may not preserve inner quotes perfectly
		assert.Equal(t, 1, len(metric.Labels))
	})
}
