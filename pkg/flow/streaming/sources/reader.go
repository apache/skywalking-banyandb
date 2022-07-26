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

package sources

import (
	"context"
	"io"

	"github.com/apache/skywalking-banyandb/pkg/flow"
	streamingApi "github.com/apache/skywalking-banyandb/pkg/flow/streaming/api"
)

var _ streamingApi.Source = (*sourceReader)(nil)

type sourceReader struct {
	reader io.Reader
	out    chan interface{}

	bufSize int
}

func (r *sourceReader) Out() <-chan interface{} {
	return r.out
}

func (r *sourceReader) Setup(ctx context.Context) error {
	go r.run(ctx)
	return nil
}

func (r *sourceReader) run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		close(r.out)
	}()

	for {
		buf := make([]byte, r.bufSize)
		byteCnt, err := r.reader.Read(buf)

		if byteCnt > 0 {
			select {
			case r.out <- flow.NewStreamRecordWithoutTS(buf[0:byteCnt]):
			case <-ctx.Done():
				return
			}
		}
		if err != nil {
			// TODO: log error
			return
		}
	}
}

// Teardown does nothing since the source automatically close out channel if reader returns io.EOF
func (r *sourceReader) Teardown(context.Context) error {
	return nil
}

func (r *sourceReader) Exec(downstream streamingApi.Inlet) {
	go streamingApi.Transmit(downstream, r)
}

type ReaderSourceOpt func(*sourceReader)

func WithBufferSize(size int) ReaderSourceOpt {
	return func(source *sourceReader) {
		// use 4KB as the buf size
		source.bufSize = size
	}
}

// FromReader returns a *sourceReader which can be used to emit bytes
func FromReader(reader io.Reader, opts ...ReaderSourceOpt) streamingApi.Source {
	s := &sourceReader{
		reader:  reader,
		out:     make(chan interface{}, 1024),
		bufSize: 4 * 1024, // default buffer size is 4KB
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}
