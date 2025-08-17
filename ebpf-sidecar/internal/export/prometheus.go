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
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/metrics"
)

// PrometheusExporter formats metrics in Prometheus text format
type PrometheusExporter struct {
	prefix string
}

// NewPrometheusExporter creates a new Prometheus exporter
func NewPrometheusExporter(prefix string) *PrometheusExporter {
	if prefix == "" {
		prefix = "ebpf"
	}
	return &PrometheusExporter{
		prefix: prefix,
	}
}

// Export writes metrics in Prometheus format to the writer
func (e *PrometheusExporter) Export(w io.Writer, store *metrics.Store) error {
	if store == nil {
		return fmt.Errorf("metrics store is nil")
	}

	// Get all metrics from all modules
	allMetrics := store.GetAll()
	
	// Group metrics by name for proper Prometheus format
	metricGroups := make(map[string][]*metrics.Metric)
	
	for _, metricSet := range allMetrics {
		for _, metric := range metricSet.GetMetrics() {
			name := e.formatMetricName(metric.Name)
			metricGroups[name] = append(metricGroups[name], &metric)
		}
	}
	
	// Sort metric names for consistent output
	names := make([]string, 0, len(metricGroups))
	for name := range metricGroups {
		names = append(names, name)
	}
	sort.Strings(names)
	
	// Write each metric group
	for _, name := range names {
		metricsInGroup := metricGroups[name]
		if len(metricsInGroup) == 0 {
			continue
		}
		
		// Write HELP and TYPE once per metric name
		firstMetric := metricsInGroup[0]
		
		// Write HELP line
		help := firstMetric.Help
		if help == "" {
			help = fmt.Sprintf("%s metric", name)
		}
		fmt.Fprintf(w, "# HELP %s %s\n", name, help)
		
		// Write TYPE line
		promType := e.mapMetricType(firstMetric.Type)
		fmt.Fprintf(w, "# TYPE %s %s\n", name, promType)
		
		// Write metric values
		for _, metric := range metricsInGroup {
			labelStr := e.formatLabels(metric.Labels)
			if labelStr != "" {
				fmt.Fprintf(w, "%s{%s} %v\n", name, labelStr, metric.Value)
			} else {
				fmt.Fprintf(w, "%s %v\n", name, metric.Value)
			}
		}
		
		// Add blank line between metrics
		fmt.Fprintln(w)
	}
	
	return nil
}

// formatMetricName ensures metric name follows Prometheus conventions
func (e *PrometheusExporter) formatMetricName(name string) string {
	// Ensure prefix
	if !strings.HasPrefix(name, e.prefix+"_") {
		name = e.prefix + "_" + name
	}
	
	// Replace invalid characters
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, ".", "_")
	
	return name
}

// mapMetricType converts internal metric type to Prometheus type
func (e *PrometheusExporter) mapMetricType(t metrics.MetricType) string {
	switch t {
	case metrics.MetricTypeCounter:
		return "counter"
	case metrics.MetricTypeGauge:
		return "gauge"
	case metrics.MetricTypeHistogram:
		return "histogram"
	default:
		return "untyped"
	}
}

// formatLabels formats labels for Prometheus
func (e *PrometheusExporter) formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	
	// Sort label keys for consistent output
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	
	// Build label string
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		v := labels[k]
		// Escape special characters in label values
		v = strings.ReplaceAll(v, `\`, `\\`)
		v = strings.ReplaceAll(v, `"`, `\"`)
		v = strings.ReplaceAll(v, "\n", `\n`)
		parts = append(parts, fmt.Sprintf(`%s="%s"`, k, v))
	}
	
	return strings.Join(parts, ",")
}

// ExportSummary generates a summary of available metrics
func (e *PrometheusExporter) ExportSummary(w io.Writer, store *metrics.Store) error {
	allMetrics := store.GetAll()
	
	fmt.Fprintf(w, "# eBPF Sidecar Metrics Summary\n")
	fmt.Fprintf(w, "# Modules: %d\n", len(allMetrics))
	
	totalMetrics := 0
	for module, metricSet := range allMetrics {
		count := metricSet.Count()
		totalMetrics += count
		fmt.Fprintf(w, "# Module '%s': %d metrics\n", module, count)
	}
	
	fmt.Fprintf(w, "# Total metrics: %d\n", totalMetrics)
	fmt.Fprintln(w)
	
	return nil
}