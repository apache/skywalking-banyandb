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
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

// NodeSelector has Locate method to select a nodeId.
type NodeSelector interface {
	Locate(group, name string, shardID uint32) (string, error)
	fmt.Stringer
}

type collector interface {
	Collect() (string, []metricWithLabelValues)
}

// MetricCollection contains all the native implementations of metrics.
type MetricCollection struct {
	pipeline     queue.Client
	nodeSelector NodeSelector
	collectors   []collector
}

// NewMetricsCollection creates a new MetricCollection.
func NewMetricsCollection(pipeline queue.Client, nodeSelector NodeSelector) *MetricCollection {
	return &MetricCollection{
		pipeline:     pipeline,
		nodeSelector: nodeSelector,
	}
}

// AddCollector Add native metric to MetricCollection.
func (m *MetricCollection) AddCollector(c collector) {
	m.collectors = append(m.collectors, c)
}

// FlushMetrics write all the metrics by flushing.
func (m *MetricCollection) FlushMetrics() {
	if len(m.collectors) == 0 {
		return
	}
	publisher := m.pipeline.NewBatchPublisher(writeTimeout)
	defer publisher.Close()
	var messages []bus.Message
	for _, collector := range m.collectors {
		name, metrics := collector.Collect()
		for _, metric := range metrics {
			iwr := m.buildIWR(name, metric)
			nodeID := ""
			var err error
			// only liaison node has a non-nil nodeSelector
			if m.nodeSelector != nil {
				nodeID, err = m.nodeSelector.Locate(iwr.GetRequest().GetMetadata().GetGroup(), iwr.GetRequest().GetMetadata().GetName(), uint32(0))
				if err != nil {
					log.Error().Err(err).Msg("Failed to locate nodeID")
				}
			}
			messages = append(messages, bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), nodeID, iwr))
		}
	}
	_, err := publisher.Publish(context.TODO(), data.TopicMeasureWrite, messages...)
	if err != nil {
		log.Error().Err(err).Msg("Failed to publish messasges")
	}
}

func (m *MetricCollection) buildIWR(metricName string, metric metricWithLabelValues) *measurev1.InternalWriteRequest {
	writeRequest := &measurev1.WriteRequest{
		MessageId: uint64(time.Now().UnixNano()),
		Metadata: &commonv1.Metadata{
			Group: ObservabilityGroupName,
			Name:  metricName,
		},
		DataPoint: &measurev1.DataPointValue{
			Timestamp: timestamppb.New(time.Now().Truncate(time.Second)),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: metric.labelValues,
				},
			},
			Fields: []*modelv1.FieldValue{
				{
					Value: &modelv1.FieldValue_Float{
						Float: &modelv1.Float{
							Value: metric.metricValue,
						},
					},
				},
			},
		},
	}
	return &measurev1.InternalWriteRequest{
		Request:      writeRequest,
		ShardId:      uint32(0),
		SeriesHash:   metric.seriesHash,
		EntityValues: metric.labelValues,
	}
}
