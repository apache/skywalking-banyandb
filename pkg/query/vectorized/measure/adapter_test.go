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

package measure

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// buildAdapterPipeline wires a BatchScan into a minimal Pipeline against the
// supplied fake MeasureQueryResult.
func buildAdapterPipeline(t *testing.T, qr model.MeasureQueryResult) *vectorized.Pipeline {
	t.Helper()
	schema := minimalSchema()
	pool := vectorized.NewBatchPool(schema, 4)
	scan := NewBatchScan(qr, schema, pool, 4)
	p, err := vectorized.NewPipelineBuilder().From(scan).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := scan.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestVectorizedMIterator_Next_PullsAndSerializes(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 100, 200)}}
	p := buildAdapterPipeline(t, qr)
	it := newVectorizedMIterator(context.Background(), p)
	defer it.Close()

	if !it.Next() {
		t.Fatalf("Next must return true while pipeline has data; err=%v", it.Err())
	}
	dps := it.Current()
	if len(dps) != 2 {
		t.Fatalf("Current length: want 2, got %d", len(dps))
	}
}

func TestVectorizedMIterator_Next_ReturnsFalseOnEOF(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: nil}
	p := buildAdapterPipeline(t, qr)
	it := newVectorizedMIterator(context.Background(), p)
	defer it.Close()

	if it.Next() {
		t.Fatal("Next on empty source must return false")
	}
	if err := it.Err(); err != nil {
		t.Fatalf("EOF must not surface as error; got %v", err)
	}
}

func TestVectorizedMIterator_Next_ReturnsFalseOnError_ErrExposedViaErr(t *testing.T) {
	boom := errors.New("storage boom")
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResultErr(boom)}}
	p := buildAdapterPipeline(t, qr)
	it := newVectorizedMIterator(context.Background(), p)
	defer it.Close()

	if it.Next() {
		t.Fatal("Next must return false on storage error")
	}
	if !errors.Is(it.Err(), boom) {
		t.Fatalf("Err must surface the storage error; got %v", it.Err())
	}
}

func TestVectorizedMIterator_Current_ReturnsLastSerializedBatch(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 100)}}
	p := buildAdapterPipeline(t, qr)
	it := newVectorizedMIterator(context.Background(), p)
	defer it.Close()

	_ = it.Next()
	first := it.Current()
	if len(first) != 1 {
		t.Fatalf("first Current len: want 1, got %d", len(first))
	}
	// Current called repeatedly should keep returning the last batch.
	if got := it.Current(); len(got) != 1 {
		t.Fatalf("repeat Current must return same batch; got len %d", len(got))
	}
}

func TestVectorizedMIterator_Close_DelegatesToPipelineClose_Idempotent(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 100)}}
	p := buildAdapterPipeline(t, qr)
	it := newVectorizedMIterator(context.Background(), p)
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	// Pipeline.Close is idempotent at the pipeline level; calling adapter.Close
	// twice must not error.
	if err := it.Close(); err != nil {
		t.Fatalf("second Close must be no-op, got %v", err)
	}
}
