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

// Package native provides a simple meter system for metrics. The metrics are aggregated by the meter provider.
package native

import (
	"sync"
	"time"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const (
	writeTimeout = 5 * time.Second
)

type metricWithLabelValues struct {
	labelValues []*modelv1.TagValue
	seriesHash  []byte
	metricValue float64
}

type metricVec struct {
	nodeInfo    NodeInfo
	scope       meter.Scope
	metrics     map[string]metricWithLabelValues
	measureName string
	mutex       sync.Mutex
}

func newMetricVec(measureName string, scope meter.Scope, nodeInfo NodeInfo) *metricVec {
	n := &metricVec{
		nodeInfo:    nodeInfo,
		scope:       scope,
		measureName: measureName,
		metrics:     map[string]metricWithLabelValues{},
	}
	return n
}

func (n *metricVec) Inc(delta float64, labelValues ...string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	tagValues := buildTagValues(n.nodeInfo, n.scope, labelValues...)
	hash := seriesHash(tagValues)
	key := string(hash)
	v, exist := n.metrics[key]
	if !exist {
		v = metricWithLabelValues{
			labelValues: tagValues,
			seriesHash:  hash,
		}
	}
	v.metricValue += delta
	n.metrics[key] = v
}

func (n *metricVec) Delete(labelValues ...string) bool {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	key := string(seriesHash(buildTagValues(n.nodeInfo, n.scope, labelValues...)))
	delete(n.metrics, key)
	return true
}

func (n *metricVec) Collect() (string, []metricWithLabelValues) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	var metrics []metricWithLabelValues
	for _, metric := range n.metrics {
		metrics = append(metrics, metric)
	}
	return n.measureName, metrics
}

func seriesHash(tagValues []*modelv1.TagValue) []byte {
	entities, err := pbv1.EntityValues(tagValues).ToEntity()
	if err != nil {
		log.Error().Err(err).Msg("Failed to convert tagValues to Entity")
	}
	return pbv1.HashEntity(entities)
}
