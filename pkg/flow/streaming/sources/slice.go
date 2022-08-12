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

	"github.com/apache/skywalking-banyandb/pkg/flow/api"
	streamingApi "github.com/apache/skywalking-banyandb/pkg/flow/streaming/api"
)

var _ streamingApi.Source = (*sourceSlice)(nil)

type sourceSlice struct {
	slice interface{}
	out   chan interface{}
}

func (s *sourceSlice) Out() <-chan interface{} {
	return s.out
}

func (s *sourceSlice) Setup(ctx context.Context) error {
	// ensure slice param is a Slice type
	dataType := reflect.TypeOf(s.slice)
	if dataType.Kind() != reflect.Slice {
		return errors.New("sourceSlice must have a slice")
	}
	sliceVal := reflect.ValueOf(s.slice)

	go s.run(ctx, sliceVal)
	return nil
}

func (s *sourceSlice) run(ctx context.Context, sliceVal reflect.Value) {
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		close(s.out)
	}()

	for i := 0; i < sliceVal.Len(); i++ {
		val := sliceVal.Index(i)
		select {
		case s.out <- tryExactTimestamp(val.Interface()):
		case <-ctx.Done():
			return
		}
	}
}

func (s *sourceSlice) Teardown(ctx context.Context) error {
	return nil
}

func (s *sourceSlice) Exec(downstream streamingApi.Inlet) {
	go streamingApi.Transmit(downstream, s)
}

func NewSlice(slice interface{}) streamingApi.Source {
	return &sourceSlice{
		slice: slice,
		out:   make(chan interface{}),
	}
}

func tryExactTimestamp(item any) api.StreamRecord {
	if r, ok := item.(api.StreamRecord); ok {
		return r
	}
	type timestampExtractor interface {
		TimestampMillis() int64
	}
	// otherwise, check if we can extract timestamp
	if extractor, ok := item.(timestampExtractor); ok {
		return api.NewStreamRecord(item, extractor.TimestampMillis())
	}
	return api.NewStreamRecordWithoutTS(item)
}
