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

// Package sub implements the queue server.
package sub

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type patternSendStream struct {
	grpc.ServerStream
	responses []*clusterv1.SendResponse
}

func (s *patternSendStream) Send(response *clusterv1.SendResponse) error {
	s.responses = append(s.responses, response)
	return nil
}
func (s *patternSendStream) Recv() (*clusterv1.SendRequest, error) { return nil, io.EOF }

func TestPatternUnknownTopicIsAnErrorResponse(t *testing.T) {
	server := &server{log: logger.GetLogger("pattern-topic-test")}
	stream := &patternSendStream{}
	req := &clusterv1.SendRequest{MessageId: 9, Topic: "internal-measure-pattern-query.unsupported"}
	topic, err := server.resolveTopic(stream, req, nil, &streamIdentity{})
	require.Error(t, err)
	require.Nil(t, topic)
	require.Len(t, stream.responses, 1)
	require.Equal(t, uint64(9), stream.responses[0].MessageId)
	require.Equal(t, "invalid topic", stream.responses[0].Error)
	require.Nil(t, stream.responses[0].Body)
}

func TestPatternTopicPreservesRawResponseAllowance(t *testing.T) {
	original := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	defer data.SetMeasureWireModeRaw(original)
	require.True(t, rawBodyAllowed(data.TopicInternalMeasurePatternQuery))
	server := &server{log: logger.GetLogger("pattern-topic-test")}
	stream := &patternSendStream{}
	body := []byte("bounded envelope")
	req := &clusterv1.SendRequest{MessageId: 10, Topic: data.TopicInternalMeasurePatternQuery.String(), Body: body}
	message, err := server.parseMessage(stream, req, data.TopicInternalMeasurePatternQuery, &streamIdentity{})
	require.NoError(t, err)
	require.Equal(t, body, message.Data())
}
