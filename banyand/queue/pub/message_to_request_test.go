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

package pub

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

// TestMessageToRequestGroup verifies that the group label is populated for both
// proto-message payloads (resolved from the request metadata) and pre-marshaled
// []byte payloads (carried explicitly on the bus message), so the queue metrics
// stay labeled by group on the secondary-index sync path.
func TestMessageToRequestGroup(t *testing.T) {
	t.Run("proto message resolves group from metadata", func(t *testing.T) {
		msg := bus.NewMessageWithNode(1, "node-1", &streamv1.InternalWriteRequest{
			Request: &streamv1.WriteRequest{
				Metadata: &commonv1.Metadata{Group: "stream-group"},
			},
		})
		r, err := messageToRequest(data.TopicStreamWrite, msg)
		require.NoError(t, err)
		require.Equal(t, "stream-group", r.GetGroup())
		require.NotNil(t, r.GetBody())
	})

	t.Run("byte payload carries explicit group", func(t *testing.T) {
		body := []byte{0x1, 0x2, 0x3}
		msg := bus.NewMessageWithNodeAndGroup(2, "node-1", "idx-group", body)
		r, err := messageToRequest(data.TopicStreamSeriesIndexWrite, msg)
		require.NoError(t, err)
		require.Equal(t, "idx-group", r.GetGroup())
		require.Equal(t, body, r.GetBody())
	})

	t.Run("byte payload without group leaves it empty", func(t *testing.T) {
		msg := bus.NewMessageWithNode(3, "node-1", []byte{0x9})
		r, err := messageToRequest(data.TopicStreamSeriesIndexWrite, msg)
		require.NoError(t, err)
		require.Empty(t, r.GetGroup())
	})
}
