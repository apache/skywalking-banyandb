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
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	writeTimeout = 5 * time.Second
)

type metricWithLabelValues struct {
	labelValues []string
	metric      float64
}

type metricVec struct {
	scheduler   *timestamp.Scheduler
	pipeline    queue.Client
	scope       meter.Scope
	metrics     map[uint64]metricWithLabelValues
	measureName string
	mutex       sync.Mutex
}

func newMetricVec(measureName string, pipeline queue.Client, scope meter.Scope) *metricVec {
	clock, _ := timestamp.GetClock(context.TODO())
	n := &metricVec{
		measureName: measureName,
		pipeline:    pipeline,
		scope:       scope,
		scheduler:   timestamp.NewScheduler(log, clock),
		metrics:     map[uint64]metricWithLabelValues{},
	}
	err := n.scheduler.Register("flush metrics", cron.Descriptor, "@every 5s", func(_ time.Time, _ *logger.Logger) bool {
		n.flushMetrics()
		return true
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to register flushMetrics")
	}
	return n
}

func (n *metricVec) Delete(labelValues ...string) bool {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	delete(n.metrics, generateKey(labelValues))
	return true
}

func (n *metricVec) buildIWR(metric float64, labelValues ...string) *measurev1.InternalWriteRequest {
	tagValues := buildTagValues(n.scope, labelValues...)
	entities, err := pbv1.EntityValues(tagValues).ToEntity()
	if err != nil {
		log.Error().Err(err).Msg("Failed to convert tagValues to Entity")
	}
	writeRequest := &measurev1.WriteRequest{
		MessageId: uint64(time.Now().UnixNano()),
		Metadata: &commonv1.Metadata{
			Group: NativeObservabilityGroupName,
			Name:  n.measureName,
		},
		DataPoint: &measurev1.DataPointValue{
			Timestamp: timestamppb.New(time.Now().Truncate(time.Second)),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: tagValues,
				},
			},
			Fields: []*modelv1.FieldValue{
				{
					Value: &modelv1.FieldValue_Float{
						Float: &modelv1.Float{
							Value: metric,
						},
					},
				},
			},
		},
	}
	return &measurev1.InternalWriteRequest{
		Request:      writeRequest,
		ShardId:      uint32(0),
		SeriesHash:   pbv1.HashEntity(entities),
		EntityValues: tagValues,
	}
}

func (n *metricVec) flushMetrics() {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if len(n.metrics) == 0 {
		return
	}
	publisher := n.pipeline.NewBatchPublisher(writeTimeout)
	defer publisher.Close()
	var messages []bus.Message
	for _, v := range n.metrics {
		iwr := n.buildIWR(v.metric, v.labelValues...)
		messages = append(messages, bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), "", iwr))
	}
	_, err := publisher.Publish(data.TopicMeasureWrite, messages...)
	if err != nil {
		log.Error().Err(err).Msg("Failed to publish messasges")
	}
}

func generateKey(labelValues []string) uint64 {
	return convert.HashStr(strings.Join(labelValues, ""))
}
