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

	"github.com/apache/skywalking-banyandb/pkg/flow"
	streamingApi "github.com/apache/skywalking-banyandb/pkg/flow/streaming/api"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming/sources"
)

var _ flow.Flow = (*streamingFlow)(nil)

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

func New(sourceParam interface{}) flow.Flow {
	return &streamingFlow{
		sourceParam: sourceParam,
		ops:         make([]streamingApi.Operator, 0),
		drain:       make(chan error),
	}
}

func (f *streamingFlow) init() error {
	f.prepareContext()

	// check and build sources type
	if err := f.buildSource(); err != nil {
		return err
	}

	// check and build sink type
	if err := f.buildSink(); err != nil {
		return err
	}

	return nil
}

func (f *streamingFlow) buildSource() error {
	if f.sourceParam == nil {
		return errors.New("sources parameter is nil")
	}

	// check interface
	switch src := f.sourceParam.(type) {
	case streamingApi.Source:
		f.source = src
	case io.Reader:
		f.source = sources.FromReader(src)
	}

	// check primitive
	srcType := reflect.TypeOf(f.sourceParam)
	switch srcType.Kind() {
	case reflect.Slice:
		f.source = sources.NewSlice(f.sourceParam)
	case reflect.Chan:
		f.source = sources.NewChannel(f.sourceParam)
	}

	if f.source == nil {
		return errors.New("invalid source")
	}

	return nil
}

func (f *streamingFlow) buildSink() error {
	if f.sinkParam == nil {
		return errors.New("sources parameter is nil")
	}

	// check interface
	switch snk := f.sinkParam.(type) {
	case streamingApi.Sink:
		f.sink = snk
	}

	if f.sink == nil {
		return errors.New("invalid sink")
	}

	return nil
}

func (f *streamingFlow) prepareContext() {
	if f.ctx == nil {
		f.ctx = context.TODO()
	}

	// TODO: add more runtime utilities
}

func (f *streamingFlow) To(sink interface{}) flow.Flow {
	f.sinkParam = sink
	return f
}

func (f *streamingFlow) OpenAsync() <-chan error {
	if err := f.init(); err != nil {
		f.drainErr(err)
		return f.drain
	}

	// open stream
	go func() {
		// setup sources
		if err := f.source.Setup(f.ctx); err != nil {
			f.drainErr(err)
			return
		}

		// setup all operators one by one
		for _, op := range f.ops {
			if err := op.Setup(f.ctx); err != nil {
				f.drainErr(err)
				return
			}
		}

		// setup sink
		if err := f.sink.Setup(f.ctx); err != nil {
			f.drainErr(err)
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
	}()

	return f.drain
}

func (f *streamingFlow) drainErr(err error) {
	go func() { f.drain <- err }()
}
