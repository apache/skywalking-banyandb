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

	"github.com/apache/skywalking-banyandb/pkg/flow"
)

func (f *streamingFlow) Filter(predicate flow.UnaryOperation[bool]) flow.Flow {
	op, err := flow.FilterFunc(predicate)
	if err != nil {
		f.drainErr(err)
	}
	return f.Transform(op)
}

func (f *streamingFlow) Map(mapper flow.UnaryOperation[any]) flow.Flow {
	return f.Transform(mapper)
}

// Transform represents a general unary transformation
// For example: filter, map, etc.
func (f *streamingFlow) Transform(op flow.UnaryOperation[any]) flow.Flow {
	// TODO: support parallelism
	f.ops = append(f.ops, newUnaryOp(op, 1))
	return f
}

var _ flow.Operator = (*unaryOperator)(nil)

type unaryOperator struct {
	op  flow.UnaryOperation[any]
	in  chan flow.StreamRecord
	out chan flow.StreamRecord
	flow.ComponentState
	parallelism uint
}

func (u *unaryOperator) Setup(ctx context.Context) error {
	// run a background job as a consumer
	go u.run(ctx)
	return nil
}

func (u *unaryOperator) Exec(downstream flow.Inlet) {
	u.Add(1)
	// start a background job for transmission
	go flow.Transmit(&u.ComponentState, downstream, u)
}

func (u *unaryOperator) Teardown(_ context.Context) error {
	u.Wait()
	return nil
}

func newUnaryOp(op flow.UnaryOperation[any], parallelism uint) *unaryOperator {
	return &unaryOperator{
		op:          op,
		in:          make(chan flow.StreamRecord),
		out:         make(chan flow.StreamRecord),
		parallelism: parallelism,
	}
}

func (u *unaryOperator) In() chan<- flow.StreamRecord {
	return u.in
}

func (u *unaryOperator) Out() <-chan flow.StreamRecord {
	return u.out
}

func (u *unaryOperator) run(ctx context.Context) {
	semaphore := make(chan struct{}, u.parallelism)
	for elem := range u.in {
		semaphore <- struct{}{}
		go func(r flow.StreamRecord) {
			defer func() { <-semaphore }()
			result := u.op.Apply(ctx, r.Data())
			switch val := result.(type) {
			case nil:
				return
			default:
				u.out <- r.WithNewData(val)
			}
		}(elem)
	}
	for i := 0; i < int(u.parallelism); i++ {
		semaphore <- struct{}{}
	}
	close(u.out)
}
