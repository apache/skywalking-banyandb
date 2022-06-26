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
	"io"
	"reflect"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/flow/api"
	streamingApi "github.com/apache/skywalking-banyandb/pkg/flow/streaming/api"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming/sources"
)

var _ api.Flow = (*streamingFlow)(nil)

type streamingFlow struct {
	// sourceParam and sinkParam are generic source and sink
	sourceParam interface{}
	sinkParam   interface{}

	ctx    context.Context
	source streamingApi.Source
	sink   streamingApi.Sink
	ops    []streamingApi.Operator
	drain  chan error
}

func (flow *streamingFlow) Offset(i int) api.Flow {
	panic("offset is not supported in streaming context")
}

func (flow *streamingFlow) Limit(i int) api.Flow {
	panic("limit is not supported in streaming context")
}

func New(sourceParam interface{}) *streamingFlow {
	return &streamingFlow{
		sourceParam: sourceParam,
		ops:         make([]streamingApi.Operator, 0),
		drain:       make(chan error),
	}
}

func (flow *streamingFlow) init() error {
	flow.prepareContext()

	// check and build sources type
	if err := flow.buildSource(); err != nil {
		return err
	}

	// check and build sink type
	if err := flow.buildSink(); err != nil {
		return err
	}

	return nil
}

func (flow *streamingFlow) buildSource() error {
	if flow.sourceParam == nil {
		return errors.New("sources parameter is nil")
	}

	// check interface
	switch src := flow.sourceParam.(type) {
	case streamingApi.Source:
		flow.source = src
	case io.Reader:
		flow.source = sources.FromReader(src)
	}

	// check primitive
	srcType := reflect.TypeOf(flow.sourceParam)
	switch srcType.Kind() {
	case reflect.Slice:
		flow.source = sources.NewSlice(flow.sourceParam)
	case reflect.Chan:
		flow.source = sources.NewChannel(flow.sourceParam)
	}

	if flow.source == nil {
		return errors.New("invalid source")
	}

	return nil
}

func (flow *streamingFlow) buildSink() error {
	if flow.sinkParam == nil {
		return errors.New("sources parameter is nil")
	}

	// check interface
	switch snk := flow.sinkParam.(type) {
	case streamingApi.Sink:
		flow.sink = snk
	}

	if flow.sink == nil {
		return errors.New("invalid sink")
	}

	return nil
}

func (flow *streamingFlow) prepareContext() {
	if flow.ctx == nil {
		flow.ctx = context.TODO()
	}

	// TODO: add more runtime utilities
}

func (flow *streamingFlow) To(sink interface{}) api.Flow {
	flow.sinkParam = sink
	return flow
}

func (flow *streamingFlow) Open() <-chan error {
	if err := flow.init(); err != nil {
		flow.drainErr(err)
		return flow.drain
	}

	// open stream
	go func() {
		// setup sources
		if err := flow.source.Setup(flow.ctx); err != nil {
			flow.drainErr(err)
			return
		}

		// setup all operators one by one
		for _, op := range flow.ops {
			if err := op.Setup(flow.ctx); err != nil {
				flow.drainErr(err)
				return
			}
		}

		// setup sink
		if err := flow.sink.Setup(flow.ctx); err != nil {
			flow.drainErr(err)
		}

		// connect all operator and sink
		for i := len(flow.ops) - 1; i >= 0; i-- {
			last := i == len(flow.ops)-1
			if last {
				flow.ops[i].Exec(flow.sink)
			} else {
				flow.ops[i].Exec(flow.ops[i+1])
			}
		}
		// finally connect sources and the first operator
		flow.source.Exec(flow.ops[0])
	}()

	return flow.drain
}

func (flow *streamingFlow) drainErr(err error) {
	go func() { flow.drain <- err }()
}
