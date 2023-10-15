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

// Package executor defines the specifications accessing underlying data repositories.
package executor

import (
	"context"

	"github.com/apache/skywalking-banyandb/api/common"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

// ExecutionContext allows retrieving data from tsdb.
type ExecutionContext interface {
	Shards(entity tsdb.Entity) ([]tsdb.Shard, error)
	Shard(id common.ShardID) (tsdb.Shard, error)
	ParseTagFamily(family string, item tsdb.Item) (*modelv1.TagFamily, error)
}

// StreamExecutionContext allows retrieving data through the stream module.
type StreamExecutionContext interface {
	ExecutionContext
	ParseElementID(item tsdb.Item) (string, error)
}

// StreamExecutionContextKey is the key of stream execution context in context.Context.
type StreamExecutionContextKey struct{}

var streamExecutionContextKeyInstance = StreamExecutionContextKey{}

// WithStreamExecutionContext returns a new context with stream execution context.
func WithStreamExecutionContext(ctx context.Context, ec StreamExecutionContext) context.Context {
	return context.WithValue(ctx, streamExecutionContextKeyInstance, ec)
}

// FromStreamExecutionContext returns the stream execution context from context.Context.
func FromStreamExecutionContext(ctx context.Context) StreamExecutionContext {
	return ctx.Value(streamExecutionContextKeyInstance).(StreamExecutionContext)
}

// StreamExecutable allows querying in the stream schema.
type StreamExecutable interface {
	Execute(context.Context) ([]*streamv1.Element, error)
}

// MeasureExecutionContext allows retrieving data through the measure module.
type MeasureExecutionContext interface {
	ExecutionContext
	ParseField(name string, item tsdb.Item) (*measurev1.DataPoint_Field, error)
}

// MeasureExecutionContextKey is the key of measure execution context in context.Context.
type MeasureExecutionContextKey struct{}

var measureExecutionContextKeyInstance = MeasureExecutionContextKey{}

// WithMeasureExecutionContext returns a new context with measure execution context.
func WithMeasureExecutionContext(ctx context.Context, ec MeasureExecutionContext) context.Context {
	return context.WithValue(ctx, measureExecutionContextKeyInstance, ec)
}

// FromMeasureExecutionContext returns the measure execution context from context.Context.
func FromMeasureExecutionContext(ctx context.Context) MeasureExecutionContext {
	return ctx.Value(measureExecutionContextKeyInstance).(MeasureExecutionContext)
}

// MIterator allows iterating in a measure data set.
type MIterator interface {
	Next() bool

	Current() []*measurev1.DataPoint

	Close() error
}

// MeasureExecutable allows querying in the measure schema.
type MeasureExecutable interface {
	Execute(context.Context) (MIterator, error)
}

// DistributedExecutionContext allows retrieving data through the distributed module.
type DistributedExecutionContext interface {
	bus.Broadcaster
	TimeRange() *modelv1.TimeRange
}

// DistributedExecutionContextKey is the key of distributed execution context in context.Context.
type DistributedExecutionContextKey struct{}

var distributedExecutionContextKeyInstance = DistributedExecutionContextKey{}

// WithDistributedExecutionContext returns a new context with distributed execution context.
func WithDistributedExecutionContext(ctx context.Context, ec DistributedExecutionContext) context.Context {
	return context.WithValue(ctx, distributedExecutionContextKeyInstance, ec)
}

// FromDistributedExecutionContext returns the distributed execution context from context.Context.
func FromDistributedExecutionContext(ctx context.Context) DistributedExecutionContext {
	return ctx.Value(distributedExecutionContextKeyInstance).(DistributedExecutionContext)
}
