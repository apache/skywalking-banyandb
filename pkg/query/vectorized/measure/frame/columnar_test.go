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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"testing"
	"unsafe"

	"google.golang.org/protobuf/proto"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	baseframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/frame"
)

// cellOf returns row i of col as a comparable value: nil for a null cell.
func cellOf(col vectorized.Column, i int) any {
	if col.IsNull(i) {
		return nil
	}
	switch c := col.(type) {
	case *vectorized.ConstColumn:
		return cellOf(c.Expand(), i)
	case *vectorized.TypedColumn[int64]:
		return c.Data()[i]
	case *vectorized.TypedColumn[float64]:
		return math.Float64bits(c.Data()[i])
	case *vectorized.TypedColumn[string]:
		return c.Data()[i]
	case *vectorized.TypedColumn[[]byte]:
		return fmt.Sprintf("%x/%t", c.Data()[i], c.Data()[i] == nil)
	case *vectorized.TypedColumn[*modelv1.TagValue]:
		if c.Data()[i] == nil {
			return "" // the wire sends a nil cell as an empty message
		}
		return c.Data()[i].String()
	case *vectorized.TypedColumn[*modelv1.FieldValue]:
		if c.Data()[i] == nil {
			return ""
		}
		return c.Data()[i].String()
	}
	panic(fmt.Sprintf("unsupported column %T", col))
}

// requireSameBatch fails unless got holds exactly the active rows of want, cell for cell.
func requireSameBatch(t *testing.T, want, got *vectorized.RecordBatch) {
	t.Helper()
	if len(want.Schema.Columns) != len(got.Schema.Columns) {
		t.Fatalf("columns: want %d, got %d", len(want.Schema.Columns), len(got.Schema.Columns))
	}
	if want.ActiveLen() != got.Len {
		t.Fatalf("rows: want %d, got %d", want.ActiveLen(), got.Len)
	}
	for ci, def := range want.Schema.Columns {
		gotDef := got.Schema.Columns[ci]
		if def.Name != gotDef.Name || def.Role != gotDef.Role || def.Type != gotDef.Type || def.TagFamily != gotDef.TagFamily {
			t.Fatalf("column %d def: want %+v, got %+v", ci, def, gotDef)
		}
		if got.Columns[ci].Len() != got.Len {
			t.Fatalf("column %d (%s): len %d, want %d", ci, def.Name, got.Columns[ci].Len(), got.Len)
		}
		for i := range got.Len {
			row := i
			if want.Selection != nil {
				row = int(want.Selection[i])
			}
			if w, g := cellOf(want.Columns[ci], row), cellOf(got.Columns[ci], i); w != g {
				t.Fatalf("column %d (%s) row %d: want %v, got %v", ci, def.Name, i, w, g)
			}
		}
	}
}

// kitchenSink builds a batch with every column kind the wire carries: nulls, empty
// values, constants, proto cells (including mixed variants) and arrays as TagValue cells.
func kitchenSink(rows int) *vectorized.RecordBatch {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Name: "_timestamp", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Name: "_sid", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "raw", Type: vectorized.ColumnTypeBytes},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "entity", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "ids", Name: "id", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "ids", Name: "arr", Type: vectorized.ColumnTypeTagValue},
		{Role: vectorized.RoleField, Name: "latency", Type: vectorized.ColumnTypeFloat64},
		{Role: vectorized.RoleField, Name: "count", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: "mixed", Type: vectorized.ColumnTypeFieldValue},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "constnum", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "constnull", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "constbytes", Type: vectorized.ColumnTypeBytes},
		{Role: vectorized.RoleField, Name: "constf", Type: vectorized.ColumnTypeFloat64},
	})
	b := vectorized.NewRecordBatch(schema, rows)
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic fixture
	for i := range rows {
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(int64(1_700_000_000_000 + i))
		b.Columns[1].(*vectorized.TypedColumn[int64]).Append(int64(i % 5))
		svc := b.Columns[2].(*vectorized.TypedColumn[string])
		switch i % 7 {
		case 3:
			svc.AppendNull()
		case 5:
			svc.Append("")
		default:
			svc.Append(fmt.Sprintf("service-%d-%s", rng.Intn(50), string(rune('a'+i%26))))
		}
		raw := b.Columns[3].(*vectorized.TypedColumn[[]byte])
		switch i % 6 {
		case 2:
			raw.AppendNull()
		case 4:
			raw.Append([]byte{})
		default:
			raw.Append([]byte{byte(i), 0xff, byte(i >> 8)})
		}
		b.Columns[4].(*vectorized.TypedColumn[string]).Append("the-entity")
		id := b.Columns[5].(*vectorized.TypedColumn[int64])
		if i%4 == 1 {
			id.AppendNull()
		} else {
			id.Append(int64(rng.Intn(1000) - 500))
		}
		arr := b.Columns[6].(*vectorized.TypedColumn[*modelv1.TagValue])
		switch i % 5 {
		case 0:
			arr.Append(&modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"a", "b", fmt.Sprint(i)}}}})
		case 1:
			arr.Append(&modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{int64(i), -1}}}})
		case 2:
			arr.AppendNull()
		case 3:
			arr.Append(nil)
		default:
			arr.Append(&modelv1.TagValue{})
		}
		lat := b.Columns[7].(*vectorized.TypedColumn[float64])
		switch i % 9 {
		case 4:
			lat.AppendNull()
		case 8:
			lat.Append(math.NaN())
		default:
			lat.Append(rng.Float64() * 100)
		}
		b.Columns[8].(*vectorized.TypedColumn[int64]).Append(int64(i) * -3)
		mixed := b.Columns[9].(*vectorized.TypedColumn[*modelv1.FieldValue])
		switch i % 4 {
		case 0:
			mixed.Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(i)}}})
		case 1:
			mixed.Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: "s"}}})
		case 2:
			mixed.AppendNull()
		default:
			mixed.Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: []byte{1, 2, 3}}})
		}
	}
	b.Columns[10] = vectorized.NewConstInt64(-42, rows)
	b.Columns[11] = vectorized.NewConstNull(vectorized.ColumnTypeString, rows)
	b.Columns[12] = vectorized.NewConstBytes([]byte("const-bytes"), rows)
	b.Columns[13] = vectorized.NewConstFloat64(2.5, rows)
	b.Len = rows
	return b
}

func TestColumnar_GoldenBytes(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Role: vectorized.RoleField, Name: "n", Type: vectorized.ColumnTypeInt64}})
	b := vectorized.NewRecordBatch(schema, 3)
	col := b.Columns[0].(*vectorized.TypedColumn[int64])
	col.Append(10)
	col.Append(20)
	col.Append(30)
	b.Len = 3
	got, err := EncodeVersion(b, WireVersionColumnar)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0x00, 'V', 'F', 'R', 0x04, // magic, version 4
		0x03, 0x01, // nrows, ncols
		0x06, 0x01, 0x00, // role field, type int64, flat
		0x01, 'n', // name
		0x00,       // tag family length
		0x00,       // null count
		0x00, 0x00, // pad 14 -> 16
		10, 0, 0, 0, 0, 0, 0, 0,
		20, 0, 0, 0, 0, 0, 0, 0,
		30, 0, 0, 0, 0, 0, 0, 0,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("frame mismatch:\n got  %x\n want %x", got, want)
	}
}

func TestColumnar_GoldenBytes_StringWithNull(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Role: vectorized.RoleTag, TagFamily: "f", Name: "s", Type: vectorized.ColumnTypeString}})
	b := vectorized.NewRecordBatch(schema, 3)
	col := b.Columns[0].(*vectorized.TypedColumn[string])
	col.Append("ab")
	col.AppendNull()
	col.Append("cde")
	b.Len = 3
	got, err := EncodeVersion(b, WireVersionColumnar)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0x00, 'V', 'F', 'R', 0x04,
		0x03, 0x01,
		0x05, 0x03, 0x00, // role tag, type string, flat
		0x01, 's',
		0x01, 'f',
		0x01,                       // one null
		0x00,                       // pad 15 -> 16
		0b101, 0, 0, 0, 0, 0, 0, 0, // validity: rows 0 and 2 valid, padded to 8
		0, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 5, 0, 0, 0, // offsets 0, 2, 2, 5
		'a', 'b', 'c', 'd', 'e', 0, 0, 0, // values padded from 5 to 8
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("frame mismatch:\n got  %x\n want %x", got, want)
	}
}

func TestColumnar_ConstGoldenBytes(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Role: vectorized.RoleTag, TagFamily: "f", Name: "e", Type: vectorized.ColumnTypeString}})
	b := vectorized.NewRecordBatch(schema, 0)
	b.Columns[0] = vectorized.NewConstString("xyz", 100)
	b.Len = 100
	got, err := EncodeVersion(b, WireVersionColumnar)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0x00, 'V', 'F', 'R', 0x04,
		100, 0x01,
		0x05, 0x03, 0x01, // constant
		0x01, 'e',
		0x01, 'f',
		0x00, // not null
		0x00, // pad 14 -> 16 (7+3+2+2+1 = 15)
		3, 0, 0, 0, 'x', 'y', 'z', 0,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("frame mismatch:\n got  %x\n want %x", got, want)
	}
	decoded, decodeErr := Decode(got)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	defer decoded.ReleaseOwners()
	requireSameBatch(t, b, decoded)
}

func TestColumnar_RoundTripEquivalentToRowLayout(t *testing.T) {
	for _, rows := range []int{0, 1, 2, 7, 8, 9, 63, 64, 65, 1000} {
		b := kitchenSink(rows)
		for _, withSelection := range []bool{false, true} {
			src := b
			if withSelection && rows > 3 {
				cp := *b
				for i := 0; i < rows; i += 3 {
					cp.Selection = append(cp.Selection, uint16(i))
				}
				if cp.Selection == nil {
					cp.Selection = []uint16{}
				}
				src = &cp
			}
			v1, v1Err := EncodeVersion(src, WireVersion)
			if v1Err != nil {
				t.Fatalf("rows=%d: v1 encode: %v", rows, v1Err)
			}
			v2, v2Err := EncodeVersion(src, WireVersionColumnar)
			if v2Err != nil {
				t.Fatalf("rows=%d: v2 encode: %v", rows, v2Err)
			}
			if len(v2)%8 != 0 {
				t.Fatalf("rows=%d: frame length %d is not a multiple of 8", rows, len(v2))
			}
			dec1, dec1Err := Decode(v1)
			if dec1Err != nil {
				t.Fatalf("rows=%d: v1 decode: %v", rows, dec1Err)
			}
			dec2, dec2Err := Decode(v2)
			if dec2Err != nil {
				t.Fatalf("rows=%d: v2 decode: %v", rows, dec2Err)
			}
			requireSameBatch(t, src, dec2)
			requireSameBatch(t, dec1, dec2)
			dec2.ReleaseOwners()
		}
	}
}

// misaligned returns a copy of frame whose first byte is not 8-byte aligned.
func misaligned(frame []byte) []byte {
	words := make([]uint64, (len(frame)+16)/8+1)
	base := unsafe.Slice((*byte)(unsafe.Pointer(&words[0])), len(words)*8)
	out := base[1 : 1+len(frame)]
	copy(out, frame)
	return out
}

func TestColumnar_AlignedMessageIsBorrowedInPlace(t *testing.T) {
	prev := baseframe.SetPoisonReleasedMessages(false)
	defer baseframe.SetPoisonReleasedMessages(prev)
	b := kitchenSink(300)
	frame, err := EncodeVersion(b, WireVersionColumnar)
	if err != nil {
		t.Fatal(err)
	}
	words := make([]uint64, (len(frame)+7)/8)
	aligned := unsafe.Slice((*byte)(unsafe.Pointer(&words[0])), len(frame))
	copy(aligned, frame)
	before := vectorized.OutstandingOwnerPins()
	decoded, decodeErr := Decode(aligned)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	ts := decoded.Columns[0].(*vectorized.TypedColumn[int64])
	if !ts.Borrowed() {
		t.Fatalf("timestamp column was copied, want a view of the message")
	}
	dataPtr := uintptr(unsafe.Pointer(unsafe.SliceData(ts.Data())))
	msgPtr := uintptr(unsafe.Pointer(unsafe.SliceData(aligned)))
	if dataPtr < msgPtr || dataPtr >= msgPtr+uintptr(len(aligned)) {
		t.Fatalf("timestamp data is not inside the message")
	}
	if got := vectorized.OutstandingOwnerPins() - before; got != 1 {
		t.Fatalf("pins held by the decoded batch = %d, want 1", got)
	}
	requireSameBatch(t, b, decoded)
	decoded.ReleaseOwners()
	if got := vectorized.OutstandingOwnerPins() - before; got != 0 {
		t.Fatalf("pins after release = %d, want 0", got)
	}
}

func TestColumnar_MisalignedMessageIsCopiedOnce(t *testing.T) {
	prev := baseframe.SetPoisonReleasedMessages(false)
	defer baseframe.SetPoisonReleasedMessages(prev)
	b := kitchenSink(300)
	frame, err := EncodeVersion(b, WireVersionColumnar)
	if err != nil {
		t.Fatal(err)
	}
	odd := misaligned(frame)
	if uintptr(unsafe.Pointer(&odd[0]))&7 == 0 {
		t.Fatal("test buffer is aligned")
	}
	decoded, decodeErr := Decode(odd)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	requireSameBatch(t, b, decoded)
	ts := decoded.Columns[0].(*vectorized.TypedColumn[int64])
	if uintptr(unsafe.Pointer(unsafe.SliceData(ts.Data())))&7 != 0 {
		t.Fatalf("decoded numeric data is misaligned")
	}
	// The decoded batch must not point into the caller's misaligned buffer.
	for i := range odd {
		odd[i] = 0xAB
	}
	requireSameBatch(t, b, decoded)
	decoded.ReleaseOwners()
}

func TestColumnar_PoisonedAfterRelease(t *testing.T) {
	prev := baseframe.SetPoisonReleasedMessages(true)
	defer baseframe.SetPoisonReleasedMessages(prev)
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "f", Name: "s", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "n", Type: vectorized.ColumnTypeInt64},
	})
	b := vectorized.NewRecordBatch(schema, 2)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("hello")
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("world")
	b.Columns[1].(*vectorized.TypedColumn[int64]).Append(7)
	b.Columns[1].(*vectorized.TypedColumn[int64]).Append(8)
	b.Len = 2
	frame, err := EncodeVersion(b, WireVersionColumnar)
	if err != nil {
		t.Fatal(err)
	}
	decoded, decodeErr := Decode(frame)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	str := decoded.Columns[0].(*vectorized.TypedColumn[string]).Data()[0]
	num := decoded.Columns[1].(*vectorized.TypedColumn[int64]).Data()
	if str != "hello" || num[0] != 7 {
		t.Fatalf("before release: %q %d", str, num[0])
	}
	decoded.ReleaseOwners()
	if str == "hello" || num[0] == 7 {
		t.Fatalf("after release the borrowed cells still read %q %d, want poison", str, num[0])
	}
	// The caller's own buffer is never poisoned.
	again, againErr := Decode(frame)
	if againErr != nil {
		t.Fatalf("decoding the same frame again: %v", againErr)
	}
	again.ReleaseOwners()
}

func TestColumnar_TruncationNeverPanics(t *testing.T) {
	for _, rows := range []int{0, 1, 5, 40} {
		frame, err := EncodeVersion(kitchenSink(rows), WireVersionColumnar)
		if err != nil {
			t.Fatal(err)
		}
		before := vectorized.OutstandingOwnerPins()
		for cut := 0; cut < len(frame); cut++ {
			decoded, decodeErr := Decode(frame[:cut])
			if decodeErr == nil {
				decoded.ReleaseOwners()
				t.Fatalf("rows=%d: truncated to %d of %d bytes decoded without error", rows, cut, len(frame))
			}
			if !errors.Is(decodeErr, ErrTruncated) && !errors.Is(decodeErr, ErrBadVersion) && !errors.Is(decodeErr, ErrBadMagic) {
				t.Fatalf("rows=%d cut=%d: unexpected error class: %v", rows, cut, decodeErr)
			}
		}
		if got := vectorized.OutstandingOwnerPins() - before; got != 0 {
			t.Fatalf("failed decodes leaked %d pins", got)
		}
	}
}

func TestColumnar_TrailingBytesRejected(t *testing.T) {
	frame, err := EncodeVersion(kitchenSink(10), WireVersionColumnar)
	if err != nil {
		t.Fatal(err)
	}
	if _, decodeErr := Decode(append(frame, 0, 0, 0, 0, 0, 0, 0, 0)); !errors.Is(decodeErr, ErrTruncated) {
		t.Fatalf("trailing bytes: got %v, want ErrTruncated", decodeErr)
	}
}

func TestColumnar_CorruptionNeverPanics(t *testing.T) {
	frame, err := EncodeVersion(kitchenSink(33), WireVersionColumnar)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic fixture
	for iter := 0; iter < 30000; iter++ {
		mutated := append([]byte(nil), frame...)
		for k := rng.Intn(4) + 1; k > 0; k-- {
			switch rng.Intn(3) {
			case 0:
				mutated[rng.Intn(len(mutated))] ^= byte(1 << uint(rng.Intn(8)))
			case 1:
				mutated[rng.Intn(len(mutated))] = byte(rng.Intn(256))
			default:
				// Corrupt a 4-byte window, the width of an offset.
				at := rng.Intn(len(mutated) - 4)
				copy(mutated[at:], []byte{byte(rng.Intn(256)), byte(rng.Intn(256)), byte(rng.Intn(256)), 0x7f})
			}
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("iteration %d: Decode panicked: %v\nframe %x", iter, r, mutated)
				}
			}()
			decoded, decodeErr := Decode(mutated)
			if decodeErr == nil {
				// A frame that still parses must be readable end to end.
				for ci := range decoded.Columns {
					for i := range decoded.Len {
						_ = cellOf(decoded.Columns[ci], i)
					}
				}
				decoded.ReleaseOwners()
			}
		}()
	}
}

func TestColumnar_HugeHeaderRowCountRejected(t *testing.T) {
	frame := []byte{0x00, 'V', 'F', 'R', 0x04, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x01, 0x06, 0x01, 0x01, 0x01, 'n', 0x00, 0x01}
	if _, err := Decode(frame); err == nil {
		t.Fatal("a frame claiming 2^32 rows decoded")
	}
	// A constant column may not expand past its limit.
	frame = []byte{0x00, 'V', 'F', 'R', 0x04, 0x80, 0x80, 0x80, 0x08, 0x01, 0x06, 0x01, 0x01, 0x01, 'n', 0x00, 0x00, 0x00, 0x00, 0x00, 1, 0, 0, 0, 0, 0, 0, 0}
	if _, err := Decode(frame); err == nil {
		t.Fatal("a constant column of 2^27 rows decoded")
	}
}

func TestColumnar_ConstOverLimitIsExpanded(t *testing.T) {
	rows := 1<<20 + 5
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Role: vectorized.RoleField, Name: "n", Type: vectorized.ColumnTypeInt64}})
	b := vectorized.NewRecordBatch(schema, 0)
	b.Columns[0] = vectorized.NewConstInt64(9, rows)
	b.Len = rows
	frame, err := EncodeVersion(b, WireVersionColumnar)
	if err != nil {
		t.Fatal(err)
	}
	decoded, decodeErr := Decode(frame)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	defer decoded.ReleaseOwners()
	if got := decoded.Columns[0].(*vectorized.TypedColumn[int64]).Data(); len(got) != rows || got[rows-1] != 9 {
		t.Fatalf("expanded column wrong: len=%d", len(got))
	}
}

func TestColumnar_DecodeDispatchesOnVersion(t *testing.T) {
	b := kitchenSink(20)
	for _, version := range []uint8{WireVersion, WireVersionColumnar} {
		frame, err := EncodeVersion(b, version)
		if err != nil {
			t.Fatal(err)
		}
		if frame[MagicLen] != version {
			t.Fatalf("frame carries version %d, want %d", frame[MagicLen], version)
		}
		decoded, decodeErr := Decode(frame)
		if decodeErr != nil {
			t.Fatalf("version %d: %v", version, decodeErr)
		}
		requireSameBatch(t, b, decoded)
		decoded.ReleaseOwners()
	}
	for _, bad := range []uint8{0, 1, 2, 5, 255} {
		frame := []byte{0x00, 'V', 'F', 'R', bad, 0, 0}
		if _, err := Decode(frame); !errors.Is(err, ErrBadVersion) {
			t.Fatalf("version %d: got %v, want ErrBadVersion", bad, err)
		}
	}
	if _, err := EncodeVersion(b, 9); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("encode version 9: got %v", err)
	}
}

func TestColumnar_NegotiateVersion(t *testing.T) {
	defer SetMaxEmitVersion(SetMaxEmitVersion(WireVersionColumnar))
	cases := []struct {
		name      string
		requested uint32
		nodeMax   uint8
		want      uint8
	}{
		{"old requester, new node", 0, WireVersionColumnar, WireVersion},
		{"old requester, old node", 0, WireVersion, WireVersion},
		{"new requester, old node", uint32(WireVersionColumnar), WireVersion, WireVersion},
		{"new requester, new node", uint32(WireVersionColumnar), WireVersionColumnar, WireVersionColumnar},
		{"requester v3, new node", uint32(WireVersion), WireVersionColumnar, WireVersion},
		{"future requester, new node", 9, WireVersionColumnar, WireVersionColumnar},
		{"garbage requester below v3", 2, WireVersionColumnar, WireVersion},
	}
	for _, tc := range cases {
		SetMaxEmitVersion(tc.nodeMax)
		if got := NegotiateVersion(tc.requested); got != tc.want {
			t.Errorf("%s: NegotiateVersion(%d) with node max %d = %d, want %d", tc.name, tc.requested, tc.nodeMax, got, tc.want)
		}
	}
	if prev := SetMaxEmitVersion(0); prev != WireVersionColumnar {
		t.Fatalf("previous cap = %d", prev)
	}
	if got := NegotiateVersion(9); got != WireVersion {
		t.Fatalf("a cap below the original version clamps to it, got %d", got)
	}
}

func TestColumnar_AdvertisedVersion(t *testing.T) {
	defer SetAdvertisedVersion(SetAdvertisedVersion(WireVersionColumnar))
	SetAdvertisedVersion(WireVersionColumnar)
	if got := AdvertisedVersion(); got != uint32(WireVersionColumnar) {
		t.Fatalf("default advertisement = %d", got)
	}
	SetAdvertisedVersion(0)
	if got := AdvertisedVersion(); got != 0 {
		t.Fatalf("a liaison that predates negotiation advertises nothing, got %d", got)
	}
	SetAdvertisedVersion(200)
	if got := AdvertisedVersion(); got != uint32(WireVersionColumnar) {
		t.Fatalf("advertisement is clamped to the newest version, got %d", got)
	}
}

func TestColumnar_UnsupportedColumnTypesFailLoud(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Role: vectorized.RoleTag, Name: "a", Type: vectorized.ColumnTypeStrArray}})
	b := vectorized.NewRecordBatch(schema, 1)
	b.Len = 0
	if _, err := EncodeVersion(b, WireVersionColumnar); !errors.Is(err, ErrUnsupportedColumnType) {
		t.Fatalf("got %v, want ErrUnsupportedColumnType", err)
	}
	mismatch := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Role: vectorized.RoleTag, Name: "a", Type: vectorized.ColumnTypeString}})
	mb := vectorized.NewRecordBatch(mismatch, 1)
	mb.Columns[0] = vectorized.NewInt64Column(1)
	if _, err := EncodeVersion(mb, WireVersionColumnar); !errors.Is(err, ErrUnsupportedColumnType) {
		t.Fatalf("got %v, want ErrUnsupportedColumnType", err)
	}
	if _, err := EncodeVersion(nil, WireVersionColumnar); err == nil {
		t.Fatal("nil batch encoded")
	}
}

func TestColumnar_ProtoCellsRoundTripThroughProto(t *testing.T) {
	b := kitchenSink(50)
	frame, err := EncodeVersion(b, WireVersionColumnar)
	if err != nil {
		t.Fatal(err)
	}
	decoded, decodeErr := Decode(frame)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	defer decoded.ReleaseOwners()
	want := b.Columns[6].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()
	got := decoded.Columns[6].(*vectorized.TypedColumn[*modelv1.TagValue])
	for i := range want {
		if got.IsNull(i) {
			continue
		}
		if !proto.Equal(want[i], got.Data()[i]) && want[i] != nil {
			t.Fatalf("row %d: %v != %v", i, want[i], got.Data()[i])
		}
	}
}

// constColumnsFrame hand-builds a columnar frame of ncols constant columns of rows rows
// each, so a test can declare more expansion than the encoder would ever produce.
func constColumnsFrame(t *testing.T, rows, ncols int, ctype vectorized.ColumnType) []byte {
	t.Helper()
	role, roleErr := codec.RoleToWire(vectorized.RoleTag)
	if roleErr != nil {
		t.Fatal(roleErr)
	}
	wireType, typeErr := codec.TypeToWire(ctype)
	if typeErr != nil {
		t.Fatal(typeErr)
	}
	out := append([]byte(nil), codec.Magic[:]...)
	out = append(out, codec.ColumnarWireVersion)
	out = binary.AppendUvarint(out, uint64(rows))
	out = binary.AppendUvarint(out, uint64(ncols))
	for i := range ncols {
		name := fmt.Sprintf("c%d", i)
		out = append(out, role, wireType, 1)
		out = binary.AppendUvarint(out, uint64(len(name)))
		out = append(out, name...)
		out = append(out, 0, 1) // empty family, NullCount 1: a null constant carries no value.
		for len(out)%8 != 0 {
			out = append(out, 0)
		}
	}
	return out
}

func TestColumnar_ConstExpansionBudgetRejectsAmplificationFrame(t *testing.T) {
	// About 16 KB declaring 1M rows of 1000 constant columns: 24 GiB of slice headers.
	frame := constColumnsFrame(t, 1<<20, 1000, vectorized.ColumnTypeBytes)
	if len(frame) > 32<<10 {
		t.Fatalf("fixture is %d bytes, want a tiny frame", len(frame))
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	decoded, err := Decode(frame)
	runtime.ReadMemStats(&after)
	if err == nil {
		decoded.ReleaseOwners()
		t.Fatal("amplification frame decoded")
	}
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("got %v, want ErrTruncated", err)
	}
	// Only the columns inside the budget may have been expanded before the rejection.
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 2*(256<<20)+(64<<20) {
		t.Fatalf("rejecting the frame allocated %d bytes", grown)
	}
}

func TestColumnar_ConstExpansionBudgetBoundary(t *testing.T) {
	// Ten 1M-row bytes columns (24 MiB each plus bitmaps) fit; a frame of far more is rejected
	// by the first column past the budget without expanding it.
	if _, err := Decode(constColumnsFrame(t, 1<<20, 10, vectorized.ColumnTypeBytes)); err != nil {
		t.Fatalf("10 constant columns within budget: %v", err)
	}
	if _, err := Decode(constColumnsFrame(t, 1<<20, 11, vectorized.ColumnTypeBytes)); err == nil {
		t.Fatal("11 constant columns over budget decoded")
	}
}

func TestColumnar_EncoderFlattensConstColumnsOverBudget(t *testing.T) {
	// The encoder honors the decoder's budget, so a legitimate wide batch still round-trips.
	const rows, ncols = 1 << 20, 12
	defs := make([]vectorized.ColumnDef, 0, ncols+1)
	for i := range ncols {
		defs = append(defs, vectorized.ColumnDef{Role: vectorized.RoleTag, TagFamily: "default", Name: fmt.Sprintf("s%d", i), Type: vectorized.ColumnTypeString})
	}
	defs = append(defs, vectorized.ColumnDef{Role: vectorized.RoleField, Name: "f", Type: vectorized.ColumnTypeInt64})
	b := vectorized.NewRecordBatch(vectorized.NewBatchSchema(defs), 0)
	for i := range ncols {
		b.Columns[i] = vectorized.NewConstString("x", rows)
	}
	flat := vectorized.NewInt64Column(rows)
	for i := range rows {
		flat.Append(int64(i))
	}
	b.Columns[ncols] = flat
	b.Len = rows
	frame, err := EncodeVersion(b, WireVersionColumnar)
	if err != nil {
		t.Fatal(err)
	}
	decoded, decodeErr := Decode(frame)
	if decodeErr != nil {
		t.Fatalf("legitimate large frame rejected: %v", decodeErr)
	}
	defer decoded.ReleaseOwners()
	if decoded.Len != rows {
		t.Fatalf("rows %d, want %d", decoded.Len, rows)
	}
	for i := range ncols {
		if got := decoded.Columns[i].(*vectorized.TypedColumn[string]).Data(); got[0] != "x" || got[rows-1] != "x" {
			t.Fatalf("column %d wrong", i)
		}
	}
}
