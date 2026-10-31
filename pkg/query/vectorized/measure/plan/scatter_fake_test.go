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

package plan

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/bus"
)

// chunkStream is a fake node stream that serves chunks, then ends with err or io.EOF.
type chunkStream struct {
	err    error
	node   string
	chunks []any
	mu     sync.Mutex
	served int
	closed bool
}

func (s *chunkStream) Node() string { return s.node }

func (s *chunkStream) Next(ctx context.Context) (bus.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return bus.Message{}, err
	}
	if s.closed {
		return bus.Message{}, errors.New("stream closed")
	}
	if len(s.chunks) > 0 {
		chunk := s.chunks[0]
		s.chunks = s.chunks[1:]
		s.served++
		return bus.NewMessage(1, chunk), nil
	}
	if s.err != nil {
		return bus.Message{}, s.err
	}
	return bus.Message{}, io.EOF
}

func (s *chunkStream) servedChunks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.served
}

func (s *chunkStream) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

// streamsOfBroadcast serves the answers of a fake broadcast as node streams of one chunk each, the way a data
// node with a small result answers a scan.
func streamsOfBroadcast(futures []bus.Future, broadcastErr error) ([]bus.NodeStream, error) {
	if broadcastErr != nil {
		return nil, broadcastErr
	}
	streams := make([]bus.NodeStream, 0, len(futures))
	for idx, future := range futures {
		stream := &chunkStream{node: "node-" + string(rune('a'+idx))}
		message, err := future.Get()
		if err != nil {
			stream.err = err
		} else {
			stream.chunks = []any{message.Data()}
		}
		streams = append(streams, stream)
	}
	return streams, nil
}

// scatterOfBroadcast answers a scan with the answers the fake's Broadcast gives.
func scatterOfBroadcast(broadcast func(time.Duration, bus.Topic, bus.Message) ([]bus.Future, error), topic bus.Topic, message bus.Message,
) ([]bus.NodeStream, error) {
	futures, err := broadcast(0, topic, message)
	return streamsOfBroadcast(futures, err)
}
