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
	"reflect"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/flow"
)

var _ flow.Source = (*sourceChan)(nil)

type sourceChan struct {
	ch  interface{}
	out chan interface{}
}

func (s *sourceChan) Out() <-chan interface{} {
	return s.out
}

func (s *sourceChan) Setup(ctx context.Context) error {
	// ensure channel param is a chan type
	chanType := reflect.TypeOf(s.ch)
	if chanType.Kind() != reflect.Chan {
		return errors.New("sourceChan must have a channel")
	}
	chanVal := reflect.ValueOf(s.ch)

	if !chanVal.IsValid() {
		return errors.New("invalid channel")
	}

	go s.run(ctx, chanVal)
	return nil
}

func (s *sourceChan) run(ctx context.Context, chanVal reflect.Value) {
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		close(s.out)
	}()

	for {
		val, open := chanVal.Recv()
		if !open {
			return
		}
		select {
		case s.out <- tryExactTimestamp(val.Interface()):
		case <-ctx.Done():
			return
		}
	}
}

func (s *sourceChan) Teardown(ctx context.Context) error {
	return nil
}

func (s *sourceChan) Exec(downstream flow.Inlet) {
	go flow.Transmit(downstream, s)
}

func NewChannel(ch interface{}) flow.Source {
	return &sourceChan{
		ch:  ch,
		out: make(chan interface{}, 1024),
	}
}

func tryExactTimestamp(item any) flow.StreamRecord {
	if r, ok := item.(flow.StreamRecord); ok {
		return r
	}
	type timestampExtractor interface {
		TimestampMillis() int64
	}
	// otherwise, check if we can extract timestamp
	if extractor, ok := item.(timestampExtractor); ok {
		return flow.NewStreamRecord(item, extractor.TimestampMillis())
	}
	return flow.NewStreamRecordWithoutTS(item)
}
