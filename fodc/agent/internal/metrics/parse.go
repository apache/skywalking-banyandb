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

// Package metrics implements parsing of Prometheus text format metrics.
package metrics

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Label represents a metric label as a key-value pair.
type Label struct {
	Name  string
	Value string
}

// RawMetric represents a parsed Prometheus metric.
type RawMetric struct {
	Name   string
	Desc   string
	Labels []Label
	Value  float64
}

// MetricKey is used for uniquely identifying metrics.
type MetricKey struct {
	Name   string
	Labels []Label
}

// String generates a canonical representation of the MetricKey.
func (mk MetricKey) String() string {
	if len(mk.Labels) == 0 {
		return mk.Name
	}

	sortedLabels := make([]Label, len(mk.Labels))
	copy(sortedLabels, mk.Labels)
	sort.Slice(sortedLabels, func(i, j int) bool {
		if sortedLabels[i].Name != sortedLabels[j].Name {
			return sortedLabels[i].Name < sortedLabels[j].Name
		}
		return sortedLabels[i].Value < sortedLabels[j].Value
	})

	var labelParts []string
	for _, label := range sortedLabels {
		labelParts = append(labelParts, fmt.Sprintf(`%s="%s"`, label.Name, label.Value))
	}

	return fmt.Sprintf("%s{%s}", mk.Name, strings.Join(labelParts, ","))
}

var (
	helpLineRegex = regexp.MustCompile(`^#\s+HELP\s+(\S+)\s+(.+)$`)
	// metricLineRegex matches metric lines: metric_name{label1="value1",label2="value2"} value.
	metricLineRegex = regexp.MustCompile(`^(\w+)(?:\{([^}]+)\})?\s+(.+)$`)
	labelRegex      = regexp.MustCompile(`(\w+)="([^"]+)"`)
)

// ParseWithAgentLabels parses Prometheus text format metrics and returns structured RawMetric objects.
func ParseWithAgentLabels(text string, nodeRole, podName, containerName string) ([]RawMetric, error) {
	lines := strings.Split(text, "\n")
	var metrics []RawMetric
	helpMap := make(map[string]string)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			if matches := helpLineRegex.FindStringSubmatch(line); matches != nil {
				metricName := matches[1]
				helpText := matches[2]
				helpMap[metricName] = helpText
			}
			continue
		}

		matches := metricLineRegex.FindStringSubmatch(line)
		if matches == nil {
			continue
		}

		metricName := matches[1]
		labelStr := matches[2]
		valueStr := matches[3]

		var labels []Label
		if labelStr != "" {
			labelMatches := labelRegex.FindAllStringSubmatch(labelStr, -1)
			for _, labelMatch := range labelMatches {
				labels = append(labels, Label{
					Name:  labelMatch[1],
					Value: labelMatch[2],
				})
			}
		}

		// Add agent identity labels if provided
		if nodeRole != "" {
			labels = append(labels, Label{
				Name:  "node_role",
				Value: nodeRole,
			})
		}
		if podName != "" {
			labels = append(labels, Label{
				Name:  "pod_name",
				Value: podName,
			})
		}
		if containerName != "" {
			labels = append(labels, Label{
				Name:  "container_name",
				Value: containerName,
			})
		}

		value, parseErr := strconv.ParseFloat(valueStr, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse metric value for %s: %w", metricName, parseErr)
		}

		desc := helpMap[metricName]

		metrics = append(metrics, RawMetric{
			Name:   metricName,
			Labels: labels,
			Value:  value,
			Desc:   desc,
		})
	}

	return metrics, nil
}
