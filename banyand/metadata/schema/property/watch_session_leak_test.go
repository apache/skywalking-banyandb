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

package property

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// burstWatchStream answers every Recv at once, like a server replaying many schemas.
type burstWatchStream struct {
	grpc.BidiStreamingClient[schemav1.WatchSchemasRequest, schemav1.WatchSchemasResponse]
}

func (s *burstWatchStream) Send(*schemav1.WatchSchemasRequest) error { return nil }

func (s *burstWatchStream) Recv() (*schemav1.WatchSchemasResponse, error) {
	return &schemav1.WatchSchemasResponse{EventType: schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_INSERT}, nil
}

type burstUpdateClient struct {
	schemav1.SchemaUpdateServiceClient
}

func (c *burstUpdateClient) WatchSchemas(context.Context, ...grpc.CallOption) (
	grpc.BidiStreamingClient[schemav1.WatchSchemasRequest, schemav1.WatchSchemasResponse], error,
) {
	return &burstWatchStream{}, nil
}

// TestProcessWatchSessionReceiverExitsWithTheSession pins that the goroutine reading the watch stream exits when the
// session ends. It used to block forever sending to the receive channel nobody reads any more.
func TestProcessWatchSessionReceiverExitsWithTheSession(t *testing.T) {
	r := &SchemaRegistry{l: logger.GetLogger("watch-session-test"), cache: newSchemaCache()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := r.processWatchSession(ctx, &burstUpdateClient{}, &watchSession{syncReqCh: make(chan *syncMessage)})
	require.ErrorIs(t, err, context.Canceled)

	require.Eventually(t, func() bool {
		buf := make([]byte, 1<<20)
		return !strings.Contains(string(buf[:runtime.Stack(buf, true)]), "processWatchSession")
	}, 5*time.Second, 20*time.Millisecond, "the watch stream reader leaked")
}
