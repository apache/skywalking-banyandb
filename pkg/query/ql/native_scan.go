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

// Package ql bridges native measure execution into QL batch consumers.
package ql

import (
	"context"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// NativeScan consumes native batches without constructing intermediate protobuf rows.
// The source owns the schema and lends each batch until the next pull or Close; NativeScan forwards
// that ownership contract and transfers cleanup to the final egress pipeline.
type NativeScan struct {
	source vectorized.PullOperator
	closed bool
}

// NewNativeScan takes ownership of source.
func NewNativeScan(source vectorized.PullOperator) *NativeScan {
	return &NativeScan{source: source}
}

// Init initializes the native source.
func (s *NativeScan) Init(ctx context.Context) error { return s.source.Init(ctx) }

// OutputSchema preserves native output descriptors, including hidden columns.
func (s *NativeScan) OutputSchema() *vectorized.BatchSchema { return s.source.OutputSchema() }

// NextBatch consumes the next native batch and observes caller cancellation.
func (s *NativeScan) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if err := ctx.Err(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s.source.NextBatch(ctx)
}

// Close releases the native source and its reservations.
func (s *NativeScan) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.source.Close()
}

var _ vectorized.PullOperator = (*NativeScan)(nil)
