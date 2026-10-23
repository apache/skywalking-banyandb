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

package ql

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type scanSource struct {
	schema        *vectorized.BatchSchema
	batch         *vectorized.RecordBatch
	err           error
	pulls, closes int
}

func (*scanSource) Init(context.Context) error              { return nil }
func (s *scanSource) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *scanSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	s.pulls++
	return s.batch, s.err
}
func (s *scanSource) Close() error { s.closes++; return nil }

func TestNativeScanConsumesActualSource(t *testing.T) {
	src := &scanSource{schema: &vectorized.BatchSchema{}, batch: &vectorized.RecordBatch{}}
	scan := NewNativeScan(src)
	require.NoError(t, scan.Init(context.Background()))
	require.Same(t, src.schema, scan.OutputSchema())
	batch, err := scan.NextBatch(context.Background())
	require.NoError(t, err)
	require.Same(t, src.batch, batch)
	require.Equal(t, 1, src.pulls)
	src.err = errors.New("execution failed")
	_, err = scan.NextBatch(context.Background())
	require.ErrorIs(t, err, src.err)
	require.NoError(t, scan.Close())
	require.NoError(t, scan.Close())
	require.Equal(t, 1, src.closes)
}

func TestNativeScanCancellationClosesBeforePull(t *testing.T) {
	src := &scanSource{}
	scan := NewNativeScan(src)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := scan.NextBatch(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, src.pulls)
	require.Equal(t, 1, src.closes)
}
