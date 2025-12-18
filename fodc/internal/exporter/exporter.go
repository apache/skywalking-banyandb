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

// Package exporter implements Prometheus metrics exporter for FlightRecorder Datasources.
package exporter

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/apache/skywalking-banyandb/fodc/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/internal/metrics"
)

var (
	// metricKeyRegex matches metric key format: metric_name{label1="value1",label2="value2"} or metric_name.
	metricKeyRegex = regexp.MustCompile(`^(\w+)(?:\{([^}]+)\})?$`)
	// labelRegex matches label format: label_name="label_value".
	labelRegex = regexp.MustCompile(`(\w+)="([^"]+)"`)
)

// parsedMetricKey represents a parsed metric key with name and labels.
type parsedMetricKey struct {
	name   string
	labels []metrics.Label
}

// DatasourceCollector implements prometheus.Collector interface to expose Datasource metrics.
type DatasourceCollector struct {
	flightRecorder *flightrecorder.FlightRecorder
	descs          map[string]*prometheus.Desc
	mu             sync.RWMutex
}

// NewDatasourceCollector creates a new DatasourceCollector instance.
func NewDatasourceCollector(fr *flightrecorder.FlightRecorder) *DatasourceCollector {
	return &DatasourceCollector{
		flightRecorder: fr,
		descs:          make(map[string]*prometheus.Desc),
	}
}

// Describe implements prometheus.Collector interface.
// It sends metric descriptors to the channel for Prometheus registry.
func (dc *DatasourceCollector) Describe(ch chan<- *prometheus.Desc) {
	datasources := dc.flightRecorder.GetDatasources()
	for _, ds := range datasources {
		metricsMap := ds.GetMetrics()
		descriptions := ds.GetDescriptions()

		for metricKeyStr, metricBuffer := range metricsMap {
			if metricBuffer.Len() == 0 {
				continue
			}

			parsedKey, parseErr := parseMetricKey(metricKeyStr)
			if parseErr != nil {
				continue
			}

			descKey := dc.getDescriptorKey(parsedKey.name, parsedKey.labels)
			desc := dc.registerMetricDesc(parsedKey.name, parsedKey.labels, descriptions[parsedKey.name], descKey)
			if desc != nil {
				ch <- desc
			}
		}
	}
}

// Collect implements prometheus.Collector interface.
// It collects metrics from all Datasources and sends them to the channel.
func (dc *DatasourceCollector) Collect(ch chan<- prometheus.Metric) {
	datasources := dc.flightRecorder.GetDatasources()
	for _, ds := range datasources {
		metricsMap := ds.GetMetrics()
		descriptions := ds.GetDescriptions()
		timestamps := ds.GetTimestamps()

		for metricKeyStr, metricBuffer := range metricsMap {
			if metricBuffer.Len() == 0 {
				continue
			}

			parsedKey, parseErr := parseMetricKey(metricKeyStr)
			if parseErr != nil {
				continue
			}

			descKey := dc.getDescriptorKey(parsedKey.name, parsedKey.labels)
			desc := dc.registerMetricDesc(parsedKey.name, parsedKey.labels, descriptions[parsedKey.name], descKey)
			if desc == nil {
				continue
			}

			currentValue := metricBuffer.GetCurrentValue()
			// Extract label names and values in sorted order to match descriptor
			labelNames := make([]string, len(parsedKey.labels))
			labelValueMap := make(map[string]string)
			for idx, label := range parsedKey.labels {
				labelNames[idx] = label.Name
				labelValueMap[label.Name] = label.Value
			}
			sort.Strings(labelNames)

			// Extract label values in the same order as sorted label names
			labelValues := make([]string, len(labelNames))
			for idx, labelName := range labelNames {
				labelValues[idx] = labelValueMap[labelName]
			}

			metric, createErr := prometheus.NewConstMetric(
				desc,
				prometheus.GaugeValue,
				currentValue,
				labelValues...,
			)
			if createErr != nil {
				continue
			}

			// Include timestamp from TimestampRingBuffer when available
			if timestamps.Len() > 0 {
				timestampValue := timestamps.GetCurrentValue()
				if timestampValue > 0 {
					timestamp := time.Unix(timestampValue, 0)
					metric = prometheus.NewMetricWithTimestamp(timestamp, metric)
				}
			}

			ch <- metric
		}
	}
}

// parseMetricKey parses a metric key string into name and labels.
// Metric key format: "metric_name{label1=\"value1\",label2=\"value2\"}" or "metric_name".
// Labels are sorted by name to ensure consistency with MetricKey.String() format.
func parseMetricKey(metricKeyStr string) (*parsedMetricKey, error) {
	matches := metricKeyRegex.FindStringSubmatch(metricKeyStr)
	if matches == nil {
		return nil, fmt.Errorf("invalid metric key format: %s", metricKeyStr)
	}

	metricName := matches[1]
	labelStr := matches[2]

	var labels []metrics.Label
	if labelStr != "" {
		labelMatches := labelRegex.FindAllStringSubmatch(labelStr, -1)
		for _, labelMatch := range labelMatches {
			labels = append(labels, metrics.Label{
				Name:  labelMatch[1],
				Value: labelMatch[2],
			})
		}
		// Sort labels by name to ensure consistency with MetricKey.String() format
		sort.Slice(labels, func(i, j int) bool {
			if labels[i].Name != labels[j].Name {
				return labels[i].Name < labels[j].Name
			}
			return labels[i].Value < labels[j].Value
		})
	}

	return &parsedMetricKey{
		name:   metricName,
		labels: labels,
	}, nil
}

// registerMetricDesc creates or retrieves cached Prometheus descriptor for a metric.
// Uses metric name and sorted labels as cache key.
// Returns Prometheus descriptor for metric registration.
// Reuses descriptors to minimize memory allocation.
func (dc *DatasourceCollector) registerMetricDesc(name string, labels []metrics.Label, help string, descKey string) *prometheus.Desc {
	// Double-checked locking pattern for thread-safe descriptor creation
	dc.mu.RLock()
	desc, exists := dc.descs[descKey]
	dc.mu.RUnlock()

	if exists {
		return desc
	}

	// Need to create descriptor - acquire write lock
	dc.mu.Lock()
	// Check again after acquiring write lock (double-check)
	desc, exists = dc.descs[descKey]
	if exists {
		dc.mu.Unlock()
		return desc
	}

	// Create new descriptor
	if help == "" {
		help = fmt.Sprintf("Metric %s from FlightRecorder", name)
	}

	// Extract and sort label names to ensure consistency
	labelNames := make([]string, len(labels))
	for idx, label := range labels {
		labelNames[idx] = label.Name
	}
	sort.Strings(labelNames)

	desc = prometheus.NewDesc(
		name,
		help,
		labelNames,
		nil,
	)
	dc.descs[descKey] = desc
	dc.mu.Unlock()

	return desc
}

// getDescriptorKey generates a unique key for a metric descriptor.
// The key is based on metric name and sorted label names.
func (dc *DatasourceCollector) getDescriptorKey(metricName string, labels []metrics.Label) string {
	if len(labels) == 0 {
		return metricName
	}

	labelNames := make([]string, len(labels))
	for idx, label := range labels {
		labelNames[idx] = label.Name
	}
	sort.Strings(labelNames)

	return fmt.Sprintf("%s{%s}", metricName, strings.Join(labelNames, ","))
}
