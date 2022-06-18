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

package flow

import (
	"context"
	"io"
	"reflect"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/streaming/api"
	"github.com/apache/skywalking-banyandb/pkg/streaming/sources"
)

type Flow struct {
	// sourceParam and sinkParam are generic source and sink
	sourceParam interface{}
	sinkParam   interface{}

	ctx    context.Context
	source api.Source
	sink   api.Sink
	ops    []api.Operator
	drain  chan error
}

func New(sourceParam interface{}) *Flow {
	return &Flow{
		sourceParam: sourceParam,
		ops:         make([]api.Operator, 0),
		drain:       make(chan error),
	}
}

func (flow *Flow) init() error {
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

func (flow *Flow) buildSource() error {
	if flow.sourceParam == nil {
		return errors.New("sources parameter is nil")
	}

	// check interface
	switch src := flow.sourceParam.(type) {
	case api.Source:
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

func (flow *Flow) buildSink() error {
	if flow.sinkParam == nil {
		return errors.New("sources parameter is nil")
	}

	// check interface
	switch snk := flow.sinkParam.(type) {
	case api.Sink:
		flow.sink = snk
	}

	if flow.sink == nil {
		return errors.New("invalid sink")
	}

	return nil
}

func (flow *Flow) prepareContext() {
	if flow.ctx == nil {
		flow.ctx = context.TODO()
	}

	//TODO: add more runtime utilities
}

func (flow *Flow) To(sink interface{}) *Flow {
	flow.sinkParam = sink
	return flow
}

func (flow *Flow) Open() <-chan error {
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

func (flow *Flow) drainErr(err error) {
	go func() { flow.drain <- err }()
}
