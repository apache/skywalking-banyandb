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

package api

import (
	"bufio"
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
)

func renderLine(t *testing.T, m *metrics.AggregatedMetric) string {
	t.Helper()
	var buf bytes.Buffer
	bw := bufio.NewWriter(&buf)
	var lw metricLineWriter
	lw.write(bw, m)
	require.NoError(t, bw.Flush())
	return buf.String()
}

func TestMetricLineWriter_SortsByLabelKeyAndEscapes(t *testing.T) {
	m := &metrics.AggregatedMetric{
		Name:  "banyandb_test",
		Value: 1.5,
		Labels: map[string]string{
			"ab":       "second",
			"a":        `q"uote\back` + "\nline",
			"pod_name": "hot-0",
		},
	}
	// Same order the legacy formatter produced by sorting the rendered `k="v"` pairs.
	require.Equal(t,
		"banyandb_test{a=\"q\\\"uote\\\\back\\nline\",ab=\"second\",pod_name=\"hot-0\"} 1.5\n",
		renderLine(t, m))
}

func TestMetricLineWriter_KeysWithDigitsKeepLegacyOrder(t *testing.T) {
	// Digits sort before '=', letters and '_' after it, so the rendered-pair order is
	// A, a0, a, a_ even though plain key order would be A, a, a0, a_.
	m := &metrics.AggregatedMetric{
		Name:   "v",
		Value:  1,
		Labels: map[string]string{"a": "3", "a0": "2", "a_": "4", "A": "1"},
	}
	require.Equal(t, "v{A=\"1\",a0=\"2\",a=\"3\",a_=\"4\"} 1\n", renderLine(t, m))
}

func TestCompareRenderedKeys_MatchesSortingRenderedPairs(t *testing.T) {
	keys := []string{"a", "a0", "a_", "A", "ab", "a9z", "b", "_a", "Z0", "a1"}
	for _, x := range keys {
		for _, y := range keys {
			want := strings.Compare(x+`="`, y+`="`)
			got := compareRenderedKeys(x, y)
			require.Equal(t, want < 0, got < 0, "%q vs %q", x, y)
			require.Equal(t, want == 0, got == 0, "%q vs %q", x, y)
		}
	}
}

func TestMetricLineWriter_NoLabels(t *testing.T) {
	m := &metrics.AggregatedMetric{Name: "up", Value: 1}
	require.Equal(t, "up 1\n", renderLine(t, m))
	m.Labels = map[string]string{}
	require.Equal(t, "up 1\n", renderLine(t, m))
}

func TestMetricLineWriter_ValueFormatting(t *testing.T) {
	for _, tc := range []struct {
		want  string
		value float64
	}{
		{"0", 0}, {"1", 1}, {"0.0001", 0.0001}, {"123456789", 123456789}, {"0.00000025", 2.5e-7},
	} {
		m := &metrics.AggregatedMetric{Name: "v", Value: tc.value}
		require.Equal(t, "v "+tc.want+"\n", renderLine(t, m), "value %v", tc.value)
	}
}

func TestMetricLineWriter_AllocatesAtMostOncePerLine(t *testing.T) {
	m := &metrics.AggregatedMetric{Name: "banyandb_queue_sub_total_latency_bucket", Value: 42}
	m.Labels = map[string]string{
		"container_name": "data", "group": "sw_trace", "le": "0.005", "node_role": "data",
		"node_type": "hot", "operation": "write", "pod_name": "demo-banyandb-data-hot-0",
		"remote_node": "demo-banyandb-liaison-0.demo-banyandb-liaison-headless.skywalking-showcase:17912",
		"remote_role": "liaison", "remote_tier": "hot",
	}
	var lw metricLineWriter
	bw := bufio.NewWriterSize(&bytes.Buffer{}, 1<<20)
	lw.write(bw, m) // warm the reusable buffers
	allocs := testing.AllocsPerRun(1000, func() { lw.write(bw, m) })
	require.LessOrEqual(t, allocs, 1.0, "one line must not allocate per label")
}

func TestWritePrometheusText_MatchesLegacyOnTypedFamilies(t *testing.T) {
	list := []*metrics.AggregatedMetric{
		{Name: "banyandb_up", Type: "gauge", Description: "Up.", Value: 1, Labels: map[string]string{"pod_name": "a"}},
		{Name: "banyandb_lat_bucket", Type: "histogram", Description: "Lat.", Value: 3, Labels: map[string]string{"le": "0.1", "pod_name": "a"}},
		{Name: "banyandb_lat_bucket", Type: "histogram", Description: "Lat.", Value: 5, Labels: map[string]string{"le": "+Inf", "pod_name": "a"}},
		{Name: "banyandb_lat_sum", Type: "histogram", Description: "Lat.", Value: 0.4, Labels: map[string]string{"pod_name": "a"}},
		{Name: "banyandb_lat_count", Type: "histogram", Description: "Lat.", Value: 5, Labels: map[string]string{"pod_name": "a"}},
		{Name: "banyandb_errors_total", Type: "counter", Value: 7, Labels: map[string]string{"pod_name": "b"}},
		{Name: "banyandb_lat_count", Value: 9, Labels: map[string]string{"pod_name": "legacy"}}, // untyped, absorbed by the typed family
		{Name: "banyandb_digits", Type: "gauge", Value: 2, Labels: map[string]string{"a": "x", "a0": "y", "a_": "z", "shard": "0", "shard0": "1"}},
	}
	s := &Server{}
	require.Equal(t, legacyFormatPrometheusText(list), s.formatPrometheusText(list))
}

func TestWritePrometheusText_UnbufferedWriterIsFlushed(t *testing.T) {
	list := []*metrics.AggregatedMetric{{Name: "banyandb_up", Type: "gauge", Value: 1}}
	var buf bytes.Buffer
	s := &Server{}
	require.NoError(t, s.writePrometheusText(&buf, list))
	require.Equal(t, "# TYPE banyandb_up gauge\nbanyandb_up 1\n", buf.String())
}

// legacyFormatPrometheusText is the pre-streaming formatter, kept verbatim as the golden
// reference for typed families. Its untyped path iterated maps and was not deterministic,
// so untyped coverage stays with the behavioral tests in server_test.go.
func legacyFormatPrometheusText(aggregatedMetrics []*metrics.AggregatedMetric) string {
	if len(aggregatedMetrics) == 0 {
		return ""
	}
	var typed, untyped []*metrics.AggregatedMetric
	for _, m := range aggregatedMetrics {
		if m.Type != "" {
			typed = append(typed, m)
		} else {
			untyped = append(untyped, m)
		}
	}
	var builder strings.Builder
	typedFamilyOrder := make([]string, 0)
	typedFamilies := make(map[string]*metricGroup)
	for _, m := range typed {
		base := typedFamilyBase(m.Name, m.Type)
		grp, exists := typedFamilies[base]
		if !exists {
			grp = &metricGroup{name: base, description: m.Description, metricType: m.Type}
			typedFamilies[base] = grp
			typedFamilyOrder = append(typedFamilyOrder, base)
		}
		grp.metrics = append(grp.metrics, m)
	}
	for _, m := range untyped {
		if grp := matchTypedFamily(typedFamilies, m.Name); grp != nil {
			grp.metrics = append(grp.metrics, m)
		}
	}
	sort.Strings(typedFamilyOrder)
	for _, base := range typedFamilyOrder {
		grp := typedFamilies[base]
		if grp.description != "" {
			builder.WriteString(fmt.Sprintf("# HELP %s %s\n", base, grp.description))
		}
		builder.WriteString(fmt.Sprintf("# TYPE %s %s\n", base, grp.metricType))
		for _, m := range grp.metrics {
			labelParts := make([]string, 0, len(m.Labels))
			for k, v := range m.Labels {
				labelParts = append(labelParts, fmt.Sprintf(`%s="%s"`, k, labelValueEscaper.Replace(v)))
			}
			sort.Strings(labelParts)
			labelStr := ""
			if len(labelParts) > 0 {
				labelStr = "{" + strings.Join(labelParts, ",") + "}"
			}
			builder.WriteString(fmt.Sprintf("%s%s %s\n", m.Name, labelStr, formatFloat(m.Value)))
		}
	}
	return builder.String()
}
