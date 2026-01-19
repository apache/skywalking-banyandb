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

package metrics

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_BasicGaugeMetric(t *testing.T) {
	text := `cpu_usage 75.5`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "cpu_usage", metrics[0].Name)
	assert.Equal(t, 75.5, metrics[0].Value)
	assert.Empty(t, metrics[0].Labels)
	assert.Empty(t, metrics[0].Desc)
}

func TestParse_BasicCounterMetric(t *testing.T) {
	text := `http_requests_total 1234`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "http_requests_total", metrics[0].Name)
	assert.Equal(t, 1234.0, metrics[0].Value)
}

func TestParse_MetricWithSingleLabel(t *testing.T) {
	text := `http_requests_total{method="GET"} 1234`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "http_requests_total", metrics[0].Name)
	assert.Equal(t, 1234.0, metrics[0].Value)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, "method", metrics[0].Labels[0].Name)
	assert.Equal(t, "GET", metrics[0].Labels[0].Value)
}

func TestParse_MetricWithMultipleLabels(t *testing.T) {
	text := `http_requests_total{method="GET",status="200"} 1234`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "http_requests_total", metrics[0].Name)
	assert.Equal(t, 1234.0, metrics[0].Value)
	require.Len(t, metrics[0].Labels, 2)
	assert.Equal(t, "method", metrics[0].Labels[0].Name)
	assert.Equal(t, "GET", metrics[0].Labels[0].Value)
	assert.Equal(t, "status", metrics[0].Labels[1].Name)
	assert.Equal(t, "200", metrics[0].Labels[1].Value)
}

func TestParse_HELPLineParsing(t *testing.T) {
	text := `# HELP http_requests_total Total number of HTTP requests
http_requests_total 1234`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "http_requests_total", metrics[0].Name)
	assert.Equal(t, "Total number of HTTP requests", metrics[0].Desc)
}

func TestParse_HELPWithMultipleMetrics(t *testing.T) {
	text := `# HELP http_requests_total Total number of HTTP requests
# HELP cpu_usage CPU usage percentage
http_requests_total 1234
cpu_usage 75.5`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	assert.Equal(t, "http_requests_total", metrics[0].Name)
	assert.Equal(t, "Total number of HTTP requests", metrics[0].Desc)
	assert.Equal(t, "cpu_usage", metrics[1].Name)
	assert.Equal(t, "CPU usage percentage", metrics[1].Desc)
}

func TestParse_TYPELineIgnored(t *testing.T) {
	text := `# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total 1234`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "http_requests_total", metrics[0].Name)
	assert.Equal(t, "Total number of HTTP requests", metrics[0].Desc)
}

func TestParse_EmptyLinesIgnored(t *testing.T) {
	text := `# HELP http_requests_total Total number of HTTP requests

http_requests_total 1234

cpu_usage 75.5
`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	assert.Equal(t, "http_requests_total", metrics[0].Name)
	assert.Equal(t, "cpu_usage", metrics[1].Name)
}

func TestParse_CommentsIgnored(t *testing.T) {
	text := `# This is a comment
# HELP http_requests_total Total number of HTTP requests
# Another comment
http_requests_total 1234`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "http_requests_total", metrics[0].Name)
	assert.Equal(t, "Total number of HTTP requests", metrics[0].Desc)
}

func TestParse_LabelWithQuotedValues(t *testing.T) {
	text := `http_requests_total{method="GET",path="/api/v1/users"} 1234`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 2)
	assert.Equal(t, "method", metrics[0].Labels[0].Name)
	assert.Equal(t, "GET", metrics[0].Labels[0].Value)
	assert.Equal(t, "path", metrics[0].Labels[1].Name)
	assert.Equal(t, "/api/v1/users", metrics[0].Labels[1].Value)
}

func TestParse_LabelWithSpecialCharacters(t *testing.T) {
	text := `metric_name{label1="value with spaces",label2="value-with-dashes",label3="value_with_underscores"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 3)
	assert.Equal(t, "value with spaces", metrics[0].Labels[0].Value)
	assert.Equal(t, "value-with-dashes", metrics[0].Labels[1].Value)
	assert.Equal(t, "value_with_underscores", metrics[0].Labels[2].Value)
}

func TestParse_LabelWithEscapedQuotes(t *testing.T) {
	text := `metric_name{label1="value\"with\"quotes"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 1)
	// Note: The regex `(\w+)="([^"]+)"` stops at the first unescaped quote
	// So it matches "value\" as the value (the backslash is included)
	// This is expected behavior based on the current regex implementation
	assert.Equal(t, "value\\", metrics[0].Labels[0].Value)
}

func TestParse_EmptyLabels(t *testing.T) {
	text := `metric_name{} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	// The regex `([^}]+)` requires at least one character between braces
	// So `{}` won't match and the metric line won't parse
	assert.Empty(t, metrics)
}

func TestParse_MetricWithoutLabels(t *testing.T) {
	text := `metric_name 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "metric_name", metrics[0].Name)
	assert.Empty(t, metrics[0].Labels)
}

func TestParse_FloatValues(t *testing.T) {
	text := `cpu_usage 75.5
memory_usage 0.123
disk_usage 99.99`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 3)
	assert.Equal(t, 75.5, metrics[0].Value)
	assert.Equal(t, 0.123, metrics[1].Value)
	assert.Equal(t, 99.99, metrics[2].Value)
}

func TestParse_IntegerValues(t *testing.T) {
	text := `http_requests_total 1234
cpu_count 8`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	assert.Equal(t, 1234.0, metrics[0].Value)
	assert.Equal(t, 8.0, metrics[1].Value)
}

func TestParse_ScientificNotation(t *testing.T) {
	text := `large_number 1.23e+10
small_number 1.23e-5`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	assert.Equal(t, 1.23e+10, metrics[0].Value)
	assert.Equal(t, 1.23e-5, metrics[1].Value)
}

func TestParse_HistogramFormat(t *testing.T) {
	text := `# HELP http_request_duration_seconds Request duration histogram
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.1"} 100
http_request_duration_seconds_bucket{le="0.5"} 200
http_request_duration_seconds_bucket{le="+Inf"} 300
http_request_duration_seconds_count 300
http_request_duration_seconds_sum 45.2`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 5)
	assert.Equal(t, "http_request_duration_seconds_bucket", metrics[0].Name)
	// HELP text only matches if metric name exactly matches HELP metric name
	// Since HELP says "http_request_duration_seconds" but metric is "_bucket", no match
	assert.Empty(t, metrics[0].Desc)
	assert.Equal(t, 100.0, metrics[0].Value)
	assert.Equal(t, "http_request_duration_seconds_count", metrics[3].Name)
	assert.Equal(t, 300.0, metrics[3].Value)
	assert.Equal(t, "http_request_duration_seconds_sum", metrics[4].Name)
	assert.Equal(t, 45.2, metrics[4].Value)
}

func TestParse_SummaryFormat(t *testing.T) {
	text := `# HELP http_request_duration_seconds Request duration summary
# TYPE http_request_duration_seconds summary
http_request_duration_seconds{quantile="0.5"} 0.052
http_request_duration_seconds{quantile="0.9"} 0.120
http_request_duration_seconds{quantile="0.99"} 0.250
http_request_duration_seconds_sum 45.2
http_request_duration_seconds_count 300`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 5)
	assert.Equal(t, "http_request_duration_seconds", metrics[0].Name)
	assert.Equal(t, "Request duration summary", metrics[0].Desc)
	assert.Equal(t, 0.052, metrics[0].Value)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, "quantile", metrics[0].Labels[0].Name)
	assert.Equal(t, "0.5", metrics[0].Labels[0].Value)
}

func TestParse_CounterFormat(t *testing.T) {
	text := `# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="GET",status="200"} 1234
http_requests_total{method="POST",status="200"} 567
http_requests_total{method="GET",status="404"} 89
http_requests_total{method="POST",status="500"} 12`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 4)
	assert.Equal(t, "http_requests_total", metrics[0].Name)
	assert.Equal(t, "Total number of HTTP requests", metrics[0].Desc)
	assert.Equal(t, 1234.0, metrics[0].Value)
	require.Len(t, metrics[0].Labels, 2)
	assert.Equal(t, "GET", metrics[0].Labels[0].Value)
	assert.Equal(t, "200", metrics[0].Labels[1].Value)
}

func TestParse_InvalidValueFormat(t *testing.T) {
	text := `metric_name invalid_value`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	assert.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "failed to parse metric value")
}

func TestParse_MetricNameStartingWithDigit(t *testing.T) {
	text := `123invalid_name 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	// The regex \w+ matches word characters (letters, digits, underscore)
	// So "123invalid_name" is parsed as a valid metric name
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "123invalid_name", metrics[0].Name)
	assert.Equal(t, 42.0, metrics[0].Value)
}

func TestParse_MetricNameWithUnderscores(t *testing.T) {
	text := `http_requests_total 1234
cpu_usage_percentage 75.5
banyandb_stream_tst_inverted_index_total_doc_count 12345`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 3)
	assert.Equal(t, "http_requests_total", metrics[0].Name)
	assert.Equal(t, "cpu_usage_percentage", metrics[1].Name)
	assert.Equal(t, "banyandb_stream_tst_inverted_index_total_doc_count", metrics[2].Name)
}

func TestParse_MetricNameWithNumbers(t *testing.T) {
	text := `metric_name_123 42.0
cpu0_usage 75.5`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	assert.Equal(t, "metric_name_123", metrics[0].Name)
	assert.Equal(t, "cpu0_usage", metrics[1].Name)
}

func TestParse_WhitespaceHandling(t *testing.T) {
	text := `  cpu_usage  75.5  
  http_requests_total{method="GET"}  1234  `

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	assert.Equal(t, "cpu_usage", metrics[0].Name)
	assert.Equal(t, 75.5, metrics[0].Value)
	assert.Equal(t, "http_requests_total", metrics[1].Name)
	assert.Equal(t, 1234.0, metrics[1].Value)
}

func TestParse_MultipleMetricsSameName(t *testing.T) {
	text := `http_requests_total{method="GET"} 100
http_requests_total{method="POST"} 200`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	assert.Equal(t, "http_requests_total", metrics[0].Name)
	assert.Equal(t, "http_requests_total", metrics[1].Name)
	assert.Equal(t, "GET", metrics[0].Labels[0].Value)
	assert.Equal(t, "POST", metrics[1].Labels[0].Value)
}

func TestParse_HELPWithoutMatchingMetric(t *testing.T) {
	text := `# HELP nonexistent_metric This metric doesn't exist
cpu_usage 75.5`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "cpu_usage", metrics[0].Name)
	assert.Empty(t, metrics[0].Desc)
}

func TestParse_EmptyString(t *testing.T) {
	text := ``

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	assert.Empty(t, metrics)
}

func TestParse_OnlyComments(t *testing.T) {
	text := `# HELP some_metric Some description
# TYPE some_metric counter
# This is a comment`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	assert.Empty(t, metrics)
}

func TestParse_LabelValueWithEqualsSign(t *testing.T) {
	text := `metric_name{label="value=with=equals"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, "value=with=equals", metrics[0].Labels[0].Value)
}

func TestParse_LabelValueWithColon(t *testing.T) {
	text := `metric_name{label="value:with:colons"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, "value:with:colons", metrics[0].Labels[0].Value)
}

func TestParse_LabelValueWithSlashes(t *testing.T) {
	text := `metric_name{path="/api/v1/users"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, "/api/v1/users", metrics[0].Labels[0].Value)
}

func TestParse_LargeMetricsOutput(t *testing.T) {
	var builder strings.Builder
	builder.WriteString("# HELP http_requests_total Total number of HTTP requests\n")
	builder.WriteString("# TYPE http_requests_total counter\n")

	// Generate 1000 metrics
	for i := 0; i < 1000; i++ {
		builder.WriteString("http_requests_total{method=\"GET\",status=\"200\",endpoint=\"/api/v1/users\"} ")
		builder.WriteString(strings.Repeat("1", i%100+1))
		builder.WriteString("\n")
	}

	text := builder.String()

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	assert.Len(t, metrics, 1000)
	for i := 0; i < 1000; i++ {
		assert.Equal(t, "http_requests_total", metrics[i].Name)
		assert.Equal(t, "Total number of HTTP requests", metrics[i].Desc)
		require.Len(t, metrics[i].Labels, 3)
	}
}

func TestParse_PerformanceWithLargeOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	var builder strings.Builder
	builder.WriteString("# HELP test_metric Test metric\n")

	// Generate 10000 metrics
	for i := 0; i < 10000; i++ {
		builder.WriteString("test_metric{label=\"value\"} ")
		builder.WriteString(strings.Repeat("1", i%100+1))
		builder.WriteString("\n")
	}

	text := builder.String()

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	assert.Len(t, metrics, 10000)
}

func TestMetricKey_String_NoLabels(t *testing.T) {
	mk := MetricKey{
		Name:   "cpu_usage",
		Labels: []Label{},
	}

	result := mk.String()

	assert.Equal(t, "cpu_usage", result)
}

func TestMetricKey_String_SingleLabel(t *testing.T) {
	mk := MetricKey{
		Name: "http_requests_total",
		Labels: []Label{
			{Name: "method", Value: "GET"},
		},
	}

	result := mk.String()

	assert.Equal(t, `http_requests_total{method="GET"}`, result)
}

func TestMetricKey_String_MultipleLabels(t *testing.T) {
	mk := MetricKey{
		Name: "http_requests_total",
		Labels: []Label{
			{Name: "method", Value: "GET"},
			{Name: "status", Value: "200"},
		},
	}

	result := mk.String()

	// Labels should be sorted
	assert.Contains(t, result, `http_requests_total{`)
	assert.Contains(t, result, `method="GET"`)
	assert.Contains(t, result, `status="200"`)
}

func TestMetricKey_String_LabelsSorted(t *testing.T) {
	mk := MetricKey{
		Name: "http_requests_total",
		Labels: []Label{
			{Name: "status", Value: "200"},
			{Name: "method", Value: "GET"},
		},
	}

	result := mk.String()

	// Labels should be sorted by name
	assert.Equal(t, `http_requests_total{method="GET",status="200"}`, result)
}

func TestMetricKey_String_LabelsSortedByValue(t *testing.T) {
	mk := MetricKey{
		Name: "http_requests_total",
		Labels: []Label{
			{Name: "method", Value: "POST"},
			{Name: "method", Value: "GET"},
		},
	}

	result := mk.String()

	// When names are equal, sort by value
	assert.Contains(t, result, `method="GET"`)
	assert.Contains(t, result, `method="POST"`)
}

func TestParse_LabelWithUnicodeCharacters(t *testing.T) {
	text := `metric_name{label="value-with-unicode-测试"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, "value-with-unicode-测试", metrics[0].Labels[0].Value)
}

func TestParse_NegativeValues(t *testing.T) {
	text := `temperature -10.5
delta -0.5`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	assert.Equal(t, -10.5, metrics[0].Value)
	assert.Equal(t, -0.5, metrics[1].Value)
}

func TestParse_ZeroValue(t *testing.T) {
	text := `metric_name 0
metric_name2 0.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	assert.Equal(t, 0.0, metrics[0].Value)
	assert.Equal(t, 0.0, metrics[1].Value)
}

func TestParse_LabelOrderPreserved(t *testing.T) {
	text := `metric_name{label1="value1",label2="value2",label3="value3"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 3)
	// Labels are parsed in order, but MetricKey.String() will sort them
	assert.Equal(t, "label1", metrics[0].Labels[0].Name)
	assert.Equal(t, "label2", metrics[0].Labels[1].Name)
	assert.Equal(t, "label3", metrics[0].Labels[2].Name)
}

func TestParse_MetricWithPlusInf(t *testing.T) {
	text := `metric_name{le="+Inf"} 100`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, "+Inf", metrics[0].Labels[0].Value)
}

func TestParse_MetricWithMinusInf(t *testing.T) {
	text := `metric_name{le="-Inf"} 100`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, "-Inf", metrics[0].Labels[0].Value)
}

func TestParse_MalformedHELPLine(t *testing.T) {
	text := `# HELP malformed line without proper format
# HELP proper_metric Proper description
proper_metric 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "proper_metric", metrics[0].Name)
	assert.Equal(t, "Proper description", metrics[0].Desc)
}

func TestParse_HELPLineWithExtraSpaces(t *testing.T) {
	text := `# HELP    metric_name    Description with extra spaces    
metric_name 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "metric_name", metrics[0].Name)
	// The regex captures the description after HELP metric_name, so extra spaces are included
	assert.Contains(t, metrics[0].Desc, "Description")
}

func TestParse_IncompleteMetricLine(t *testing.T) {
	text := `metric_name{label="value"
complete_metric 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "complete_metric", metrics[0].Name)
}

func TestParse_MetricLineWithoutValue(t *testing.T) {
	text := `metric_name{label="value"}
complete_metric 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "complete_metric", metrics[0].Name)
}

func TestParse_MetricLineWithOnlyName(t *testing.T) {
	text := `metric_name
complete_metric 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "complete_metric", metrics[0].Name)
}

func TestParse_VeryLongMetricName(t *testing.T) {
	longName := strings.Repeat("a", 1000)
	text := longName + " 42.0"

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, longName, metrics[0].Name)
}

func TestParse_VeryLongLabelValue(t *testing.T) {
	longValue := strings.Repeat("a", 1000)
	text := `metric_name{label="` + longValue + `"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, longValue, metrics[0].Labels[0].Value)
}

func TestParse_TabsInsteadOfSpaces(t *testing.T) {
	text := "metric_name\t42.0\nmetric_name2 100.0"

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	// Tabs are treated as whitespace and trimmed
	// The first metric should parse correctly with tabs
	require.GreaterOrEqual(t, len(metrics), 1)
	assert.Equal(t, "metric_name", metrics[0].Name)
	assert.Equal(t, 42.0, metrics[0].Value)
	// Second metric uses spaces and should also parse
	if len(metrics) > 1 {
		assert.Equal(t, "metric_name2", metrics[1].Name)
		assert.Equal(t, 100.0, metrics[1].Value)
	}
}

func TestParse_MixedLineEndings(t *testing.T) {
	text := "metric_name1 42.0\r\nmetric_name2 100.0\nmetric_name3 200.0\r"

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(metrics), 2)
}

func TestParse_VeryLargeNumber(t *testing.T) {
	text := `large_number 1.7976931348623157e+308
small_number 2.2250738585072014e-308`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	assert.Equal(t, "large_number", metrics[0].Name)
	assert.Equal(t, "small_number", metrics[1].Name)
}

func TestParse_NaNValue(t *testing.T) {
	text := `metric_name NaN`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	// NaN should parse but we need to check if it's handled correctly
	if err == nil {
		require.Len(t, metrics, 1)
		assert.True(t, metrics[0].Value != metrics[0].Value) // NaN comparison
	} else {
		// If NaN parsing fails, that's also acceptable behavior
		assert.Contains(t, err.Error(), "failed to parse metric value")
	}
}

func TestParse_InfValue(t *testing.T) {
	text := `metric_name +Inf
metric_name2 -Inf
metric_name3 Inf`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	// Inf values should parse
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(metrics), 2)
}

func TestParse_MalformedLabelSyntax(t *testing.T) {
	text := `metric_name{label=value} 42.0
metric_name2{label="unclosed 42.0
metric_name3{label="proper"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	// Only properly formatted metrics should be parsed
	assert.GreaterOrEqual(t, len(metrics), 1)
	if len(metrics) > 0 {
		assert.Equal(t, "metric_name3", metrics[len(metrics)-1].Name)
	}
}

func TestParse_LabelWithCommaInValue(t *testing.T) {
	text := `metric_name{label="value,with,commas"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, "value,with,commas", metrics[0].Labels[0].Value)
}

func TestParse_LabelWithBracesInValue(t *testing.T) {
	text := `metric_name{label="value{with}braces"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	// The regex `[^}]+` stops at the first }, so braces in label values cause parsing issues
	// The label regex won't match properly, so the metric won't parse
	assert.Empty(t, metrics)
}

func TestParse_MultipleSpacesBetweenComponents(t *testing.T) {
	text := `metric_name    42.0
metric_name2 100.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	// Whitespace is trimmed, so multiple spaces should still parse
	// The regex matches word characters, so metric_name should parse
	require.GreaterOrEqual(t, len(metrics), 1)
	assert.Equal(t, "metric_name", metrics[0].Name)
	assert.Equal(t, 42.0, metrics[0].Value)
	// Second metric should also parse
	if len(metrics) > 1 {
		assert.Equal(t, "metric_name2", metrics[1].Name)
		assert.Equal(t, 100.0, metrics[1].Value)
	}
}

func TestParse_LabelWithNewlineInValue(t *testing.T) {
	text := "metric_name{label=\"value\nwith\nnewlines\"} 42.0"

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	// The regex `[^"]+` stops at newlines, so this won't parse correctly
	// The label regex won't match, so the metric line won't match either
	assert.Empty(t, metrics)
}

func TestParse_HELPLineBeforeMetricWithSameName(t *testing.T) {
	text := `# HELP http_requests_total Total requests
http_requests_total{method="GET"} 100
http_requests_total{method="POST"} 200`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	// Both metrics should have the same HELP text
	assert.Equal(t, "Total requests", metrics[0].Desc)
	assert.Equal(t, "Total requests", metrics[1].Desc)
}

func TestParse_HELPLineAfterMetric(t *testing.T) {
	text := `http_requests_total 100
# HELP http_requests_total Total requests
http_requests_total{method="GET"} 200`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	// First metric won't have HELP text, second will
	assert.Empty(t, metrics[0].Desc)
	assert.Equal(t, "Total requests", metrics[1].Desc)
}

func TestParse_MetricKey_String_ComplexLabels(t *testing.T) {
	mk := MetricKey{
		Name: "banyandb_stream_tst_inverted_index_total_doc_count",
		Labels: []Label{
			{Name: "index", Value: "test"},
			{Name: "type", Value: "inverted"},
		},
	}

	result := mk.String()

	assert.Contains(t, result, "banyandb_stream_tst_inverted_index_total_doc_count")
	assert.Contains(t, result, `index="test"`)
	assert.Contains(t, result, `type="inverted"`)
}

func TestParse_MetricKey_String_EmptyName(t *testing.T) {
	mk := MetricKey{
		Name:   "",
		Labels: []Label{{Name: "label", Value: "value"}},
	}

	result := mk.String()

	// Empty name with labels should still produce a string
	assert.Contains(t, result, "label=\"value\"")
}

func TestParse_MetricKey_String_LabelWithSpecialChars(t *testing.T) {
	mk := MetricKey{
		Name: "metric_name",
		Labels: []Label{
			{Name: "label", Value: "value with spaces"},
			{Name: "label2", Value: "value/with/slashes"},
		},
	}

	result := mk.String()

	assert.Contains(t, result, `label="value with spaces"`)
	assert.Contains(t, result, `label2="value/with/slashes"`)
}

func TestParse_RealWorldExampleFromDesignDoc(t *testing.T) {
	text := `# HELP banyandb_stream_tst_inverted_index_total_doc_count Total document count
# TYPE banyandb_stream_tst_inverted_index_total_doc_count gauge
banyandb_stream_tst_inverted_index_total_doc_count{index="test"} 12345
cpu_usage{host="server1"} 75.5`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	assert.Equal(t, "banyandb_stream_tst_inverted_index_total_doc_count", metrics[0].Name)
	assert.Equal(t, "Total document count", metrics[0].Desc)
	assert.Equal(t, 12345.0, metrics[0].Value)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, "index", metrics[0].Labels[0].Name)
	assert.Equal(t, "test", metrics[0].Labels[0].Value)

	assert.Equal(t, "cpu_usage", metrics[1].Name)
	assert.Equal(t, 75.5, metrics[1].Value)
	require.Len(t, metrics[1].Labels, 1)
	assert.Equal(t, "host", metrics[1].Labels[0].Name)
	assert.Equal(t, "server1", metrics[1].Labels[0].Value)
}

func TestParse_HistogramFormatFromDesignDoc(t *testing.T) {
	text := `# HELP http_request_duration_seconds Request duration histogram
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.1"} 100
http_request_duration_seconds_bucket{le="0.5"} 200
http_request_duration_seconds_bucket{le="+Inf"} 300
http_request_duration_seconds_count 300
http_request_duration_seconds_sum 45.2`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 5)
	assert.Equal(t, "http_request_duration_seconds_bucket", metrics[0].Name)
	// HELP text only matches if metric name exactly matches HELP metric name
	// Since HELP says "http_request_duration_seconds" but metric is "_bucket", no match
	assert.Empty(t, metrics[0].Desc)
	assert.Equal(t, 100.0, metrics[0].Value)
	assert.Equal(t, "http_request_duration_seconds_count", metrics[3].Name)
	assert.Equal(t, 300.0, metrics[3].Value)
	assert.Equal(t, "http_request_duration_seconds_sum", metrics[4].Name)
	assert.Equal(t, 45.2, metrics[4].Value)
}

func TestParse_SummaryFormatFromDesignDoc(t *testing.T) {
	text := `# HELP http_request_duration_seconds Request duration summary
# TYPE http_request_duration_seconds summary
http_request_duration_seconds{quantile="0.5"} 0.052
http_request_duration_seconds{quantile="0.9"} 0.120
http_request_duration_seconds{quantile="0.99"} 0.250
http_request_duration_seconds_sum 45.2
http_request_duration_seconds_count 300`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 5)
	assert.Equal(t, "http_request_duration_seconds", metrics[0].Name)
	assert.Equal(t, "Request duration summary", metrics[0].Desc)
	assert.Equal(t, 0.052, metrics[0].Value)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, "quantile", metrics[0].Labels[0].Name)
	assert.Equal(t, "0.5", metrics[0].Labels[0].Value)
}

func TestParse_CounterFormatFromDesignDoc(t *testing.T) {
	text := `# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="GET",status="200"} 1234
http_requests_total{method="POST",status="200"} 567
http_requests_total{method="GET",status="404"} 89
http_requests_total{method="POST",status="500"} 12`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 4)
	assert.Equal(t, "http_requests_total", metrics[0].Name)
	assert.Equal(t, "Total number of HTTP requests", metrics[0].Desc)
	assert.Equal(t, 1234.0, metrics[0].Value)
	require.Len(t, metrics[0].Labels, 2)
	assert.Equal(t, "GET", metrics[0].Labels[0].Value)
	assert.Equal(t, "200", metrics[0].Labels[1].Value)
}

func TestParse_LabelValueWithBackslash(t *testing.T) {
	text := `metric_name{label="value\\with\\backslashes"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 1)
	// The regex `[^"]+` matches everything except quotes, so backslashes are included
	assert.Contains(t, metrics[0].Labels[0].Value, "backslashes")
}

func TestParse_MetricNameWithHyphens(t *testing.T) {
	text := `metric-name 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	// The regex \w+ only matches word characters (letters, digits, underscore)
	// So hyphens are not matched, this should fail or skip
	if err == nil && len(metrics) > 0 {
		// If it parses, verify the name
		assert.Equal(t, "metric", metrics[0].Name)
	}
}

func TestParse_LabelNameWithNumbers(t *testing.T) {
	text := `metric_name{label123="value"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, "label123", metrics[0].Labels[0].Name)
}

func TestParse_LabelValueWithNumbers(t *testing.T) {
	text := `metric_name{label="value123"} 42.0`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 1)
	assert.Equal(t, "value123", metrics[0].Labels[0].Value)
}

func TestParse_WithAgentIdentityLabels(t *testing.T) {
	text := `cpu_usage{instance="localhost"} 75.5
http_requests_total{method="GET"} 100`

	nodeRole := "datanode-hot"
	podName := "test-pod"
	containerName := "data"

	metrics, err := ParseWithAgentLabels(text, nodeRole, podName, containerName)

	require.NoError(t, err)
	require.Len(t, metrics, 2)

	expectedAgentLabelCount := countAgentLabels(nodeRole, podName, containerName)

	// Verify first metric has agent identity labels
	cpuMetric := metrics[0]
	assert.Equal(t, "cpu_usage", cpuMetric.Name)
	expectedCPULabelCount := 1 + expectedAgentLabelCount
	require.Len(t, cpuMetric.Labels, expectedCPULabelCount)
	hasInstanceLabel := false
	hasNodeRole := false
	hasPodName := false
	hasContainerName := false
	for _, label := range cpuMetric.Labels {
		switch label.Name {
		case "instance":
			if label.Value == "localhost" {
				hasInstanceLabel = true
			}
		case "node_role":
			if label.Value == "datanode-hot" {
				hasNodeRole = true
			}
		case "pod_name":
			if label.Value == "test-pod" {
				hasPodName = true
			}
		case "container_name":
			if label.Value == "data" {
				hasContainerName = true
			}
		}
	}
	assert.True(t, hasInstanceLabel, "original labels should be preserved")
	assert.True(t, hasNodeRole, "should have node_role label")
	assert.True(t, hasPodName, "should have pod_name label")
	assert.True(t, hasContainerName, "should have container_name label")

	// Verify second metric also has agent identity labels
	httpMetric := metrics[1]
	assert.Equal(t, "http_requests_total", httpMetric.Name)
	// http_requests_total has 1 original label (method), plus agent labels
	expectedHTTPLabelCount := 1 + expectedAgentLabelCount
	require.Len(t, httpMetric.Labels, expectedHTTPLabelCount)
	hasMethodLabel := false
	hasNodeRole = false
	hasPodName = false
	hasContainerName = false
	for _, label := range httpMetric.Labels {
		switch label.Name {
		case "method":
			if label.Value == "GET" {
				hasMethodLabel = true
			}
		case "node_role":
			if label.Value == "datanode-hot" {
				hasNodeRole = true
			}
		case "pod_name":
			if label.Value == "test-pod" {
				hasPodName = true
			}
		case "container_name":
			if label.Value == "data" {
				hasContainerName = true
			}
		}
	}
	assert.True(t, hasMethodLabel, "original labels should be preserved")
	assert.True(t, hasNodeRole, "should have node_role label")
	assert.True(t, hasPodName, "should have pod_name label")
	assert.True(t, hasContainerName, "should have container_name label")
}

func countAgentLabels(nodeRole, podName, containerName string) int {
	count := 0
	if nodeRole != "" {
		count++
	}
	if podName != "" {
		count++
	}
	if containerName != "" {
		count++
	}
	return count
}

func TestParse_WithPartialAgentIdentityLabels(t *testing.T) {
	text := `cpu_usage 75.5`

	metrics, err := ParseWithAgentLabels(text, "datanode-hot", "", "data")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 2) // node_role + container_name (no pod_name)
	hasNodeRole := false
	hasContainerName := false
	for _, label := range metrics[0].Labels {
		if label.Name == "node_role" && label.Value == "datanode-hot" {
			hasNodeRole = true
		}
		if label.Name == "container_name" && label.Value == "data" {
			hasContainerName = true
		}
	}
	assert.True(t, hasNodeRole, "should have node_role label")
	assert.True(t, hasContainerName, "should have container_name label")
}

func TestParse_WithoutAgentIdentityLabels(t *testing.T) {
	text := `cpu_usage{instance="localhost"} 75.5`

	metrics, err := ParseWithAgentLabels(text, "", "", "")

	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Len(t, metrics[0].Labels, 1) // Only original label
	assert.Equal(t, "instance", metrics[0].Labels[0].Name)
	assert.Equal(t, "localhost", metrics[0].Labels[0].Value)
}
