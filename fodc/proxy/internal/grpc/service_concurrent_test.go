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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
)

// overlapDetectingStream reports whether two goroutines were ever inside Send at the same
// time. It deliberately does not lock, so it observes the caller's serialization rather
// than imposing its own.
type overlapDetectingStream struct {
	*mockStreamMetricsServer
	inFlight atomic.Int32
	overlaps atomic.Int32
}

func (s *overlapDetectingStream) Send(_ *fodcv1.StreamMetricsResponse) error {
	if s.inFlight.Add(1) > 1 {
		s.overlaps.Add(1)
	}
	// Widen the window so an unserialized caller is caught reliably.
	time.Sleep(time.Millisecond)
	s.inFlight.Add(-1)
	return nil
}

// TestSendMetricsRequest_SerializesStreamSends pins the gRPC contract: a stream may have
// one sender at a time. Overlapping /metrics scrapes each ask every agent for metrics
// independently, so sendMetricsRequest is reached concurrently for the same connection.
// ac.mu is held for reading there and therefore cannot serialize the senders.
func TestSendMetricsRequest_SerializesStreamSends(t *testing.T) {
	stream := &overlapDetectingStream{
		mockStreamMetricsServer: newMockStreamMetricsServer(context.Background()),
	}
	conn := &agentConnection{agentID: "agent-1", metricsStream: stream}

	const senders = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < senders; i++ {
		wg.Add(1)
		//panicdiag:allow-rawgo test-only concurrency driver; a panic here must fail the test loudly rather than be recovered and hidden
		go func() {
			defer wg.Done()
			<-start
			require.NoError(t, conn.sendMetricsRequest(&fodcv1.StreamMetricsResponse{}))
		}()
	}
	close(start)
	wg.Wait()

	require.Zero(t, stream.overlaps.Load(), "concurrent Send on one gRPC stream is not supported")
}
