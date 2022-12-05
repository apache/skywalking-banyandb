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

// Package streaming implement the flow framework to provide the sliding window, top-n aggregation, and etc.
package streaming

import (
	"context"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/pkg/flow"
)

var _ flow.Flow = (*streamingFlow)(nil)

type streamingFlow struct {
	ctx    context.Context
	source flow.Source
	sink   flow.Sink
	drain  chan error
	ops    []flow.Operator
}

// New returns a new streaming flow.
func New(source flow.Source) flow.Flow {
	return &streamingFlow{
		source: source,
		ops:    make([]flow.Operator, 0),
		drain:  make(chan error),
	}
}

func (f *streamingFlow) init() error {
	f.prepareContext()

	return nil
}

func (f *streamingFlow) prepareContext() {
	if f.ctx == nil {
		f.ctx = context.TODO()
	}

	// TODO: add more runtime utilities
}

func (f *streamingFlow) To(sink flow.Sink) flow.Flow {
	f.sink = sink
	return f
}

func (f *streamingFlow) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := f.source.Teardown(ctx)
	for _, op := range f.ops {
		err = multierr.Append(err, op.Teardown(ctx))
	}
	defer close(f.drain)
	return multierr.Append(err, f.sink.Teardown(ctx))
}

func (f *streamingFlow) Open() <-chan error {
	if err := f.init(); err != nil {
		go f.drainErr(err)
		return f.drain
	}

	// setup sources
	if err := f.source.Setup(f.ctx); err != nil {
		go f.drainErr(err)
		return f.drain
	}

	// setup all operators one by one
	for _, op := range f.ops {
		if err := op.Setup(f.ctx); err != nil {
			go f.drainErr(err)
			return f.drain
		}
	}

	// setup sink
	if err := f.sink.Setup(f.ctx); err != nil {
		go f.drainErr(err)
		return f.drain
	}

	// connect all operator and sink
	for i := len(f.ops) - 1; i >= 0; i-- {
		last := i == len(f.ops)-1
		if last {
			f.ops[i].Exec(f.sink)
		} else {
			f.ops[i].Exec(f.ops[i+1])
		}
	}
	// finally connect sources and the first operator
	f.source.Exec(f.ops[0])

	return f.drain
}

func (f *streamingFlow) drainErr(err error) {
	f.drain <- err
}
