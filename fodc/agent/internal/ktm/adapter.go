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

// Package ktm provides adapters for KTM metrics.
package ktm

import (
	"sort"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/metrics"
	fodcmetrics "github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
)

// ToRawMetrics converts KTM metrics to FODC RawMetrics.
func ToRawMetrics(store *metrics.Store) []fodcmetrics.RawMetric {
	if store == nil {
		return nil
	}

	allMetrics := store.GetAll()
	var rawMetrics []fodcmetrics.RawMetric

	for _, metricSet := range allMetrics {
		for _, m := range metricSet.GetMetrics() {
			rm := fodcmetrics.RawMetric{
				Name:   m.Name,
				Value:  m.Value,
				Desc:   m.Help,
				Labels: make([]fodcmetrics.Label, 0, len(m.Labels)),
			}

			// Convert labels
			for k, v := range m.Labels {
				rm.Labels = append(rm.Labels, fodcmetrics.Label{
					Name:  k,
					Value: v,
				})
			}

			// Sort labels for consistency
			sort.Slice(rm.Labels, func(i, j int) bool {
				return rm.Labels[i].Name < rm.Labels[j].Name
			})

			rawMetrics = append(rawMetrics, rm)
		}
	}

	return rawMetrics
}
