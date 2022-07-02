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

package streaming

import (
	"context"

	"github.com/apache/skywalking-banyandb/pkg/flow/api"
	streamingApi "github.com/apache/skywalking-banyandb/pkg/flow/streaming/api"
)

func (flow *streamingFlow) Filter(f interface{}) api.Flow {
	op, err := streamingApi.FilterFunc(f)
	if err != nil {
		flow.drainErr(err)
	}
	return flow.Transform(op)
}

func (flow *streamingFlow) Map(f interface{}) api.Flow {
	op, err := streamingApi.MapperFunc(f)
	if err != nil {
		flow.drainErr(err)
	}
	return flow.Transform(op)
}

// Transform represents a general unary transformation
// For example: filter, map, etc.
func (flow *streamingFlow) Transform(op streamingApi.UnaryOperation) api.Flow {
	flow.ops = append(flow.ops, newUnaryOp(op, 1))
	return flow
}

var _ streamingApi.Operator = (*unaryOperator)(nil)

type unaryOperator struct {
	op          streamingApi.UnaryOperation
	in          chan interface{}
	out         chan interface{}
	parallelism uint
}

func (u *unaryOperator) Setup(ctx context.Context) error {
	// run a background job as a consumer
	go u.run()
	return nil
}

func (u *unaryOperator) Exec(downstream streamingApi.Inlet) {
	// start a background job for transmission
	go streamingApi.Transmit(downstream, u)
}

func (u *unaryOperator) Teardown(ctx context.Context) error {
	// do nothing now
	return nil
}

func newUnaryOp(op streamingApi.UnaryOperation, parallelism uint) *unaryOperator {
	return &unaryOperator{
		op:          op,
		in:          make(chan interface{}),
		out:         make(chan interface{}),
		parallelism: parallelism,
	}
}

func (u *unaryOperator) In() chan<- interface{} {
	return u.in
}

func (u *unaryOperator) Out() <-chan interface{} {
	return u.out
}

func (u *unaryOperator) run() {
	semaphore := make(chan struct{}, u.parallelism)
	for elem := range u.in {
		if streamRecord, ok := elem.(api.StreamRecord); ok {
			semaphore <- struct{}{}
			go func(r api.StreamRecord) {
				defer func() { <-semaphore }()
				result := u.op.Apply(context.TODO(), r.Data())
				switch val := result.(type) {
				case nil:
					return
				default:
					u.out <- r.WithNewData(val)
				}
			}(streamRecord)
		}
		// TODO: else warning
	}
	for i := 0; i < int(u.parallelism); i++ {
		semaphore <- struct{}{}
	}
	close(u.out)
}
