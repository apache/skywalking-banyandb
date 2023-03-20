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

// Package flow implements a streaming calculation framework.
package flow

import (
	"context"
	"io"
	"sync"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Data indicates a aggregated data.
type Data []any

// Flow is an abstraction of data flow for
// both Streaming and Batch.
type Flow interface {
	io.Closer
	// Filter is used to filter data.
	// The parameter f can be either predicate function for streaming,
	// or conditions for batch query.
	Filter(UnaryOperation[bool]) Flow
	// Map is used to transform data
	Map(UnaryOperation[any]) Flow
	// Window is used to split infinite data into "buckets" of finite size.
	// Currently, it is only applicable to streaming context.
	Window(WindowAssigner) WindowedFlow
	// To pipes data to the given sink
	To(sink Sink) Flow
	// Open opens the flow in the async mode for streaming scenario.
	// The first error is the error combination while opening all components,
	// while the second is a channel for receiving async errors.
	Open() <-chan error
}

// WindowedFlow is a flow which processes incoming elements based on window.
// The WindowedFlow can be created with a WindowAssigner.
type WindowedFlow interface {
	AllowedMaxWindows(windowCnt int) WindowedFlow
	// TopN applies a TopNAggregation to each Window.
	TopN(topNum int, opts ...any) Flow
}

// Window is a bucket of elements with a finite size.
// timedWindow is the only implementation now.
type Window interface {
	// MaxTimestamp returns the upper bound of the Window.
	// Unit: Millisecond
	MaxTimestamp() int64
}

// WindowAssigner is used to assign Window(s) for a given timestamp, and thus it can create a WindowedFlow.
type WindowAssigner interface {
	// AssignWindows assigns a slice of Window according to the given timestamp, e.g. eventTime.
	// The unit of the timestamp here is MilliSecond.
	AssignWindows(timestamp int64) ([]Window, error)
}

// AggregationOp defines the stateful operation for aggregation.
type AggregationOp interface {
	// Add puts a slice of elements as the input
	Add([]StreamRecord)
	// Snapshot takes a snapshot of the current state of the AggregationOp
	// Taking a snapshot will restore the dirty flag
	Snapshot() interface{}
	// Dirty flag means if any new item is added after the last snapshot
	Dirty() bool
}

// AggregationOpFactory is a factory to create AggregationOp.
type AggregationOpFactory func() AggregationOp

// StreamRecord is a container wraps user data and timestamp.
// It is the underlying transmission medium for the streaming processing.
type StreamRecord struct {
	data interface{}
	ts   int64
}

// NewStreamRecord returns a StreamRecord with data and timestamp.
func NewStreamRecord(data interface{}, ts int64) StreamRecord {
	return StreamRecord{
		data: data,
		ts:   ts,
	}
}

// NewStreamRecordWithTimestampPb returns a StreamRecord whose timestamp is parsed from protobuf's timestamp.
func NewStreamRecordWithTimestampPb(data interface{}, timestamp *timestamppb.Timestamp) StreamRecord {
	return StreamRecord{
		data: data,
		ts:   timestamp.GetSeconds()*1000 + int64(timestamp.GetNanos())/1000_000,
	}
}

// NewStreamRecordWithoutTS returns a StreamRecord with data only.
func NewStreamRecordWithoutTS(data interface{}) StreamRecord {
	return StreamRecord{
		data: data,
		ts:   -1,
	}
}

// WithNewData sets data to StreamRecord.
func (sr StreamRecord) WithNewData(data interface{}) StreamRecord {
	return StreamRecord{
		ts:   sr.ts,
		data: data,
	}
}

// TimestampMillis returns the timestamp in millisecond.
func (sr StreamRecord) TimestampMillis() int64 {
	return sr.ts
}

// Data returns the embedded data.
func (sr StreamRecord) Data() interface{} {
	return sr.data
}

// Equal checks if two StreamRecord are the same.
func (sr StreamRecord) Equal(other StreamRecord) bool {
	return sr.ts == other.ts && cmp.Equal(sr.data, other.data)
}

// Inlet represents a type that exposes one open input.
//
//go:generate mockgen -destination=./inlet_mock.go -package=flow github.com/apache/skywalking-banyandb/pkg/flow Inlet
type Inlet interface {
	In() chan<- StreamRecord
}

// Outlet represents a type that exposes one open output.
type Outlet interface {
	Out() <-chan StreamRecord
}

// Component is a lifecycle controller.
type Component interface {
	// Setup is the lifecycle hook for resource preparation, e.g. start background job for listening input channel.
	// It must be called before the flow starts to process elements.
	Setup(context.Context) error
	// Teardown is the lifecycle hook for shutting down the Component
	// Implementation should ENSURE that all resource has been correctly recycled before this method returns.
	Teardown(context.Context) error
}

// ComponentState watches whether the component is shutdown.
type ComponentState struct {
	sync.WaitGroup
}

// Source represents a set of stream processing steps that has one open output.
type Source interface {
	Outlet
	Component
	Exec(downstream Inlet)
}

// Operator represents a set of stream processing steps that has one open input and one open output.
type Operator interface {
	Inlet
	Outlet
	Component
	Exec(downstream Inlet)
}

// Sink represents a set of stream processing steps that has one open input.
type Sink interface {
	Inlet
	Component
}
