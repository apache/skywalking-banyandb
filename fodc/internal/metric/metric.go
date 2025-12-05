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

// Package metric provides functionality for parsing and processing Prometheus metrics.
package metric

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ErrInvalidMetricLine is returned when a metric line is invalid.
var ErrInvalidMetricLine = errors.New("invalid metric line")

// Label represents a Prometheus metric label.
type Label struct {
	Name  string
	Value string
}

func (l *Label) String() string {
	return fmt.Sprintf(`%s="%s"`, l.Name, l.Value)
}

// Key uniquely identifies a metric by name and labels.
type Key struct {
	Name   string
	Labels []Label
}

func (mk *Key) String() string {
	if len(mk.Labels) == 0 {
		return mk.Name
	}

	var sb strings.Builder
	sb.WriteString(mk.Name)
	sb.WriteString("{")

	for i, l := range mk.Labels {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(l.String())
	}

	sb.WriteString("}")
	return sb.String()
}

// RawMetric represents a parsed Prometheus metric line.
type RawMetric struct { //nolint:govet // fieldalignment: field order optimized for readability
	Labels      []Label
	Value       float64
	Name        string
	Description string // Description from HELP comment
}

// Find searches for a label by name.
func (m *RawMetric) Find(name string) string {
	for _, l := range m.Labels {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// Remove removes a label by name and returns the remaining labels.
func (m *RawMetric) Remove(name string) ([]Label, bool) {
	if len(m.Labels) == 0 {
		return nil, false
	}

	labels := make([]Label, 0, len(m.Labels))
	found := false
	for _, l := range m.Labels {
		if l.Name != name {
			labels = append(labels, l)
		} else {
			found = true
		}
	}
	return labels, found
}

// ParseMetricLine parses a Prometheus metric line into a RawMetric.
func ParseMetricLine(line string) (RawMetric, error) {
	name, labels, valueStr, err := splitMetricLine(line)
	if err != nil {
		return RawMetric{}, err
	}

	var value float64
	_, err = fmt.Sscanf(valueStr, "%f", &value)
	if err != nil {
		return RawMetric{}, fmt.Errorf("%w: value is not a number", ErrInvalidMetricLine)
	}

	return RawMetric{
		Name:   name,
		Labels: labels,
		Value:  value,
	}, nil
}

func splitMetricLine(line string) (string, []Label, string, error) {
	labelStartIdx := strings.Index(line, "{")
	labelEndIdx := strings.Index(line, "}")

	var labels []Label
	var name, valueStr string

	if labelStartIdx != -1 && labelEndIdx != -1 && labelEndIdx > labelStartIdx {
		name = line[:labelStartIdx]

		labels = parseLabels(line[labelStartIdx+1 : labelEndIdx])

		valueStr = strings.TrimSpace(line[labelEndIdx+1:])
	} else {
		parts := strings.Fields(line)
		if len(parts) != 2 {
			return "", nil, "", ErrInvalidMetricLine
		}

		name = parts[0]
		valueStr = parts[1]
	}
	return name, labels, valueStr, nil
}

func parseLabels(labelString string) []Label {
	var labels []Label

	if labelString == "" {
		return labels
	}

	labelPairs := strings.Split(labelString, ",")
	for _, pair := range labelPairs {
		pair = strings.TrimSpace(pair)
		parts := strings.Split(pair, "=")
		if len(parts) == 2 {
			// Remove quotes from value
			value := parts[1]
			if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
				value = value[1 : len(value)-1]
			}

			labels = append(labels, Label{
				Name:  parts[0],
				Value: value,
			})
		}
	}
	return labels
}

// Bin represents a histogram bin.
type Bin struct {
	Value float64
	Count uint64
}

// Histogram represents a Prometheus histogram metric.
type Histogram struct {
	Name        string
	Description string // Description from HELP comment
	Labels      []Label
	Bins        []Bin
}

const (
	bucketSuffix = "_bucket"
	countSuffix  = "_count"
	sumSuffix    = "_sum"
)

// ParseHistogram extracts histogram metrics from raw metrics.
// descriptions map can be provided to associate descriptions with metrics.
func ParseHistogram(metrics []RawMetric, descriptions map[string]string) (map[string]Histogram, []RawMetric) {
	type histogramMetrics struct {
		count   *RawMetric
		sum     *RawMetric
		buckets []RawMetric
	}

	filteredMetrics := make([]RawMetric, 0, len(metrics))

	histograms := make(map[string]histogramMetrics)
	for _, m := range metrics {
		name := trimHistogramSuffix(m.Name)
		if len(name) < len(m.Name) {
			labels, _ := m.Remove("le")
			sort.Slice(labels, func(i, j int) bool {
				return labels[i].Name < labels[j].Name
			})

			mk := Key{
				Name:   name,
				Labels: labels,
			}

			s := mk.String()
			h := histograms[s]

			switch {
			case strings.HasSuffix(m.Name, bucketSuffix):
				h.buckets = append(h.buckets, m)
			case strings.HasSuffix(m.Name, countSuffix):
				h.count = &m
			case strings.HasSuffix(m.Name, sumSuffix):
				h.sum = &m
			}
			histograms[s] = h
		} else {
			filteredMetrics = append(filteredMetrics, m)
		}
	}

	out := make(map[string]Histogram, len(histograms))
	for k, hist := range histograms {
		name, labels, _, err := splitMetricLine(k + " 0")
		if err != nil {
			continue
		}

		release := func(h *histogramMetrics) {
			if h.count != nil {
				filteredMetrics = append(filteredMetrics, *h.count)
			}

			if h.sum != nil {
				filteredMetrics = append(filteredMetrics, *h.sum)
			}

			filteredMetrics = append(filteredMetrics, h.buckets...)
		}

		if len(hist.buckets) > 0 && hist.count != nil && hist.sum != nil {
			bins, err := parseHistogramBins(hist.buckets)
			if err == nil {
				// Get description from base metric name (without histogram suffixes)
				description := ""
				if descriptions != nil {
					description = descriptions[name]
				}
				out[k] = Histogram{
					Name:        name,
					Labels:      labels,
					Bins:        bins,
					Description: description,
				}
			} else {
				release(&hist)
			}
		} else {
			release(&hist)
		}
	}

	return out, filteredMetrics
}

func parseHistogramBins(bucketMetrics []RawMetric) ([]Bin, error) {
	buckets := make([]Bin, len(bucketMetrics))
	for i, b := range bucketMetrics {
		v := b.Find("le")
		if v == "" {
			return nil, fmt.Errorf("missing \"le\" tag")
		}

		binValue, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, err
		}

		buckets[i] = Bin{
			Value: binValue,
			Count: uint64(b.Value),
		}
	}
	return buckets, nil
}

func trimHistogramSuffix(s string) string {
	s = strings.TrimSuffix(s, bucketSuffix)
	s = strings.TrimSuffix(s, countSuffix)
	s = strings.TrimSuffix(s, sumSuffix)
	return s
}

// ParseHELPComment parses a Prometheus HELP comment line.
// Format: # HELP metric_name Description text.
func ParseHELPComment(line string) (string, string, bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "# HELP ") {
		return "", "", false
	}

	// Remove "# HELP " prefix
	rest := strings.TrimSpace(line[7:])
	if rest == "" {
		return "", "", false
	}

	// Find the first space to separate metric name from description
	firstSpace := strings.Index(rest, " ")
	if firstSpace == -1 {
		// No description, just metric name
		return rest, "", true
	}

	metricName := rest[:firstSpace]
	description := strings.TrimSpace(rest[firstSpace+1:])
	return metricName, description, true
}

// ParseTYPEComment parses a Prometheus TYPE comment line.
// Format: # TYPE metric_name type.
func ParseTYPEComment(line string) (string, string, bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "# TYPE ") {
		return "", "", false
	}

	// Remove "# TYPE " prefix
	rest := strings.TrimSpace(line[7:])
	if rest == "" {
		return "", "", false
	}

	// Find the first space to separate metric name from type
	firstSpace := strings.Index(rest, " ")
	if firstSpace == -1 {
		// No type, just metric name
		return rest, "", true
	}

	metricName := rest[:firstSpace]
	metricType := strings.TrimSpace(rest[firstSpace+1:])
	return metricName, metricType, true
}
