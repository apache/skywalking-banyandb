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

package sink

import (
	"context"

	"github.com/apache/skywalking-banyandb/pkg/streaming/api"
)

var _ api.Sink = (*Slice)(nil)

type Slice struct {
	slice []interface{}
	in    chan interface{}
}

func NewSlice() *Slice {
	return &Slice{
		slice: make([]interface{}, 0),
		in:    make(chan interface{}),
	}
}

func (s *Slice) Value() []interface{} {
	return s.slice
}

func (s *Slice) In() chan<- interface{} {
	return s.in
}

func (s *Slice) Setup(ctx context.Context) error {
	go s.run(ctx)

	return nil
}

func (s *Slice) run(ctx context.Context) {
	for {
		select {
		case item, ok := <-s.in:
			if !ok {
				return
			}
			s.slice = append(s.slice, item)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Slice) Teardown(ctx context.Context) error {
	return nil
}
