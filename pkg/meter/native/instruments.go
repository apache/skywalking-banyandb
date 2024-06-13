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
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	writeTimeout = 5 * time.Second
)

type nativeInstrument struct {
	scheduler     *timestamp.Scheduler
	pipeline      queue.Client
	scope         meter.Scope
	measureName   string
	requestBuffer []*measurev1.InternalWriteRequest
	mutex         sync.Mutex
}

func NewNativeInstrument(measureName string, pipeline queue.Client, scope meter.Scope) *nativeInstrument {
	clock, _ := timestamp.GetClock(context.TODO())
	n := &nativeInstrument{
		measureName: measureName,
		pipeline:    pipeline,
		scope:       scope,
		scheduler:   timestamp.NewScheduler(log, clock),
	}
	err := n.scheduler.Register("flush messages", cron.Descriptor, "@every 5s", func(_ time.Time, _ *logger.Logger) bool {
		n.flushMessages()
		return true
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to register flushMessages")
	}
	return n
}

// Counter Only Methods.
func (n *nativeInstrument) Inc(_ float64, _ ...string) {}

// Gauge Only Methods.
func (n *nativeInstrument) Set(value float64, labelValues ...string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.requestBuffer = append(n.requestBuffer, n.buildIWR(value, labelValues...))
}

func (n *nativeInstrument) Add(_ float64, _ ...string) {}

// Histogram Only Methods.
func (n *nativeInstrument) Observe(_ float64, _ ...string) {}

// Shared Methods.
func (n *nativeInstrument) Delete(_ ...string) bool { return false }

func (n *nativeInstrument) buildIWR(value float64, labelValues ...string) *measurev1.InternalWriteRequest {
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
							Value: value,
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

func (n *nativeInstrument) flushMessages() {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if len(n.requestBuffer) == 0 {
		return
	}
	publisher := n.pipeline.NewBatchPublisher(writeTimeout)
	defer publisher.Close()
	var messages []bus.Message
	for _, iwr := range n.requestBuffer {
		messages = append (messages, bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), "", iwr))
	}
	_, err := publisher.Publish(data.TopicMeasureWrite, messages...)
	if err != nil {
		log.Error().Err(err).Msg("Failed to publish messasges")
	}
	// Clear the buffer and release the underlying array
	n.requestBuffer = nil
}
