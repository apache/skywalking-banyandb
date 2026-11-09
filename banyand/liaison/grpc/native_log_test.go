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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

type discardSender struct{}

func (discardSender) Send(*streamv1.WriteResponse) error { return nil }

// TestWriteHandlerKeepsReceivedLogEventsOffNativeStorage covers a producer,
// such as the FODC proxy, that writes its log events through a liaison's
// StreamService/Write. The handler logs a refused event with the event in
// the line: for an event of the log group that line must not be stored
// natively, where it would be a stored line about a stored line. The same
// refusal for an event of another group is stored as usual.
func TestWriteHandlerKeepsReceivedLogEventsOffNativeStorage(t *testing.T) {
	sink := test.NativeLoggingAtWarn(t)
	svc := newTestStreamService(nil, time.Millisecond)
	noTimestamp := &streamv1.WriteRequest{Element: &streamv1.ElementValue{}}

	svc.validateWriteRequest(noTimestamp, &commonv1.Metadata{Group: logger.NativeLogGroup, Name: "log"}, discardSender{})
	require.Zero(t, sink.Count(), "the handler stored its line about a refused log event")

	svc.validateWriteRequest(noTimestamp, &commonv1.Metadata{Group: "g", Name: "s"}, discardSender{})
	require.Equal(t, 1, sink.Count(), "the handler did not store its line about a refused event of another group")
}

// refusingPublisher logs about its node under the logger the caller's context
// carries, as the batch publisher of the queue does, and fails every publish.
type refusingPublisher struct{}

func (refusingPublisher) Publish(ctx context.Context, _ bus.Topic, _ ...bus.Message) (bus.Future, error) {
	logger.FetchOrDefault(ctx, "queue-pub", logger.GetLogger("queue-client")).Error().Msg("node refused the batch")
	return nil, errors.New("refused")
}

func (refusingPublisher) Close() (map[string]*common.Error, error) { return nil, nil }

// TestWriteSessionKeepsLogBatchesOffNativeStorage covers the batches the
// handler forwards to the data nodes. When one fails, both the publisher and
// the session log about it: for a batch of the log group neither line may be
// stored natively. The same failure for another group stores both.
func TestWriteSessionKeepsLogBatchesOffNativeStorage(t *testing.T) {
	sink := test.NativeLoggingAtWarn(t)
	failBatchOf := func(group string) {
		s := newWriteSession(context.Background(), writeSessionConfig{}, writeSessionDeps{
			newPublisher: func() queue.BatchPublisher { return refusingPublisher{} },
			send:         (&replyCollector{}).send,
			log:          logger.GetLogger("test"),
			metrics:      newBypassMetrics(),
			service:      "stream",
			topic:        data.TopicStreamWrite,
		})
		ack := writeAck{metadata: &commonv1.Metadata{Group: group, Name: "s"}, id: 1}
		require.NoError(t, s.Append("n1", group, 1, ack, func(bool) ([]byte, error) { return []byte("row"), nil }))
		_ = s.Finish()
	}

	failBatchOf(logger.NativeLogGroup)
	require.Zero(t, sink.Count(), "a failed batch of log events was logged where native logs are stored")

	failBatchOf("g")
	require.Equal(t, 2, sink.Count(), "a failed batch of another group did not store the publisher's and the session's lines")
}
