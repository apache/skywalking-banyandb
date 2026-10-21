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

package frame

import (
	"bytes"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestConcatFramesPreservesCellsAndOwnsOutput(t *testing.T) {
	batches := []*vectorized.RecordBatch{kitchenSink(17), kitchenSink(9), kitchenSink(0), kitchenSink(13)}
	batches[0].Selection = []uint16{15, 2, 2, 0}
	batches[1].Selection = []uint16{}
	batches[3].Selection = []uint16{12, 1, 7}
	var bodies [][]byte
	var expected [][]any
	for _, batch := range batches {
		body, err := Encode(batch)
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
		// Decode separately so nil proto and byte cells have wire semantics.
		decoded, err := Decode(body)
		if err != nil {
			t.Fatal(err)
		}
		for row := range decoded.Len {
			cells := make([]any, len(decoded.Columns))
			for col := range cells {
				cells[col] = cellOf(decoded.Columns[col], row)
				if text, ok := cells[col].(string); ok {
					cells[col] = strings.Clone(text)
				}
			}
			expected = append(expected, cells)
		}
		decoded.ReleaseOwners()
	}
	reserved := 0
	joined, err := Concat(bodies, func(size int) error { reserved = size; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if reserved != len(joined) || reserved != cap(joined) {
		t.Fatalf("reserved %d, len %d, cap %d", reserved, len(joined), cap(joined))
	}
	for _, body := range bodies {
		clear(body)
	}
	decoded, err := Decode(joined)
	if err != nil {
		t.Fatal(err)
	}
	defer decoded.ReleaseOwners()
	if decoded.Len != len(expected) {
		t.Fatalf("rows %d, want %d", decoded.Len, len(expected))
	}
	for row, cells := range expected {
		for col, want := range cells {
			got := cellOf(decoded.Columns[col], row)
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("row %d col %d: got %v, want %v", row, col, got, want)
			}
		}
	}
}

func TestFrameOutputReservationPrecedesAllocation(t *testing.T) {
	batch := kitchenSink(19)
	sentinel := errors.New("reject output allocation")
	body, err := EncodeWithReservation(batch, func(int) error { return sentinel })
	if !errors.Is(err, sentinel) || body != nil {
		t.Fatalf("body %v, err %v", body, err)
	}
	encoded, err := Encode(batch)
	if err != nil {
		t.Fatal(err)
	}
	body, err = Concat([][]byte{encoded, encoded}, func(int) error { return sentinel })
	if !errors.Is(err, sentinel) || body != nil {
		t.Fatalf("body %v, err %v", body, err)
	}
}

func TestConcatRejectsTruncatedAndMismatchedFrames(t *testing.T) {
	body, err := Encode(kitchenSink(5))
	if err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut < len(body); cut++ {
		if _, err = Concat([][]byte{body, body[:cut]}, nil); err == nil {
			t.Fatalf("accepted frame truncated to %d bytes", cut)
		}
	}
	other := kitchenSink(2)
	definitions := append([]vectorized.ColumnDef(nil), other.Schema.Columns...)
	definitions[0].Name = "different"
	other.Schema = vectorized.NewBatchSchema(definitions)
	mismatch, err := Encode(other)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Concat([][]byte{body, mismatch}, nil); err == nil {
		t.Fatal("accepted mismatched schema")
	}
	if !bytes.Equal(body[:MagicLen], Magic[:]) {
		t.Fatal("input frame changed")
	}
}

func TestInspectScalarFrameAndConstantExpansion(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Role: vectorized.RoleTag, Name: "s", Type: vectorized.ColumnTypeString}})
	batch := vectorized.NewRecordBatch(schema, 0)
	batch.Columns[0] = vectorized.NewConstString("wide-value", 100)
	batch.Len = 100
	body, err := Encode(batch)
	if err != nil {
		t.Fatal(err)
	}
	layout, err := Inspect(body)
	if err != nil {
		t.Fatal(err)
	}
	if !layout.Scalar || layout.Header.NumRows != 100 || layout.ConstantPayloadBytes != 1000 {
		t.Fatalf("layout %+v", layout)
	}
	protoBody, err := Encode(kitchenSink(1))
	if err != nil {
		t.Fatal(err)
	}
	layout, err = Inspect(protoBody)
	if err != nil || layout.Scalar {
		t.Fatalf("protobuf layout %+v, error %v", layout, err)
	}
	bomb := constColumnsFrame(t, 1<<19, 1000, vectorized.ColumnTypeString)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = Inspect(bomb)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("amplification frame error %v", err)
	}
	if allocation := after.TotalAlloc - before.TotalAlloc; allocation > 1<<20 {
		t.Fatalf("inspection allocated %d bytes", allocation)
	}
	for cut := 0; cut < len(body); cut++ {
		if _, err = Inspect(body[:cut]); err == nil {
			t.Fatalf("accepted truncation at %d", cut)
		}
	}
}
