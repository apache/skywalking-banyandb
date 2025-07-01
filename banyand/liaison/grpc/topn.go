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

package grpc

import (
	"context"
	"time"

	"github.com/apache/skywalking-banyandb/api/data"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var _ bus.MessageListener = (*topNHandler)(nil)

type topNHandler struct {
	*bus.UnImplementedHealthyListener
	nodeRegistry NodeRegistry
	pipeline     queue.Client
	l            *logger.Logger
}

func (t *topNHandler) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	events, ok := message.Data().([]any)
	if !ok {
		t.l.Warn().Msg("invalid event data type")
		return
	}
	if len(events) < 1 {
		t.l.Warn().Msg("empty event")
		return
	}
	publisher := t.pipeline.NewBatchPublisher(10 * time.Second)
	defer publisher.Close()
	for i := range events {
		iwr, ok := events[i].(*measurev1.InternalWriteRequest)
		if !ok {
			t.l.Error().Msg("received invalid message type in topNHandler")
			return
		}
		group := iwr.Request.Metadata.Group
		nodeID, err := t.nodeRegistry.Locate(group, iwr.Request.Metadata.Name, iwr.ShardId, 0)
		if err != nil {
			t.l.Error().Err(err).Msg("failed to locate node")
			continue
		}

		msg := bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), nodeID, iwr)
		_, err = publisher.Publish(ctx, data.TopicMeasureWrite, msg)
		if err != nil {
			t.l.Error().Err(err).Str("node", nodeID).Msg("failed to publish message")
		}
	}
	return
}
