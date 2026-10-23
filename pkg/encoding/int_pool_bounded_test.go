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

package encoding

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/pool"
)

func TestIntegerEncodePoolsPreserveReusableBacking(t *testing.T) {
	intBacking := &Int64List{L: make([]int64, 8192)}
	intPool := installInt64DecodePool(t, intBacking)
	ints := GenerateInt64List(64)
	if cap(ints.L) != 8192 {
		t.Errorf("generic int64 borrower discarded reusable backing: capacity %d", cap(ints.L))
	}
	ReleaseInt64List(ints)
	if intPool.RefsCount() != 0 {
		t.Fatal("int64 borrower leaked its pool loan")
	}
	uintBacking := &Uint64List{L: make([]uint64, 8192)}
	uintPool := installUint64DecodePool(t, uintBacking)
	uints := GenerateUint64List(64)
	if cap(uints.L) != 8192 {
		t.Errorf("generic uint64 borrower discarded reusable backing: capacity %d", cap(uints.L))
	}
	ReleaseUint64List(uints)
	if uintPool.RefsCount() != 0 {
		t.Fatal("uint64 borrower leaked its pool loan")
	}
}

func TestInt64DecodeBoundsOversizedPoolLoan(t *testing.T) {
	values := []int64{10, 14, 20, 28}
	for _, encodeType := range []EncodeType{EncodeTypeDelta, EncodeTypeDeltaOfDelta} {
		t.Run(fmt.Sprint(encodeType), func(t *testing.T) {
			var encoded []byte
			var first int64
			if encodeType == EncodeTypeDelta {
				encoded, first = int64ListDeltaToBytes(nil, values)
			} else {
				encoded, first = int64sDeltaOfDeltaToBytes(nil, values)
			}
			backing := &Int64List{L: make([]int64, 8192)}
			loanPool := installInt64DecodePool(t, backing)
			got, err := BytesToInt64List(nil, encoded, encodeType, first, len(values))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, values) {
				t.Fatalf("decoded %v, want %v", got, values)
			}
			if cap(backing.L) > 2048 {
				t.Fatalf("decode retained oversized scratch capacity %d", cap(backing.L))
			}
			if loanPool.RefsCount() != 0 {
				t.Fatal("int64 decoder leaked its pool loan")
			}
		})
	}
}

func TestBytesDecodeBoundsOversizedPoolLoan(t *testing.T) {
	values := [][]byte{[]byte("alpha"), nil, {}, []byte("beta")}
	block := EncodeBytesBlock(nil, values)
	dict := NewDictionary()
	for _, value := range values {
		if !dict.Add(value) {
			t.Fatal("dictionary unexpectedly full")
		}
	}
	dictionary := dict.Encode(nil)
	for _, mode := range []string{"bytes", "bytes with tail", "dictionary", "dictionary values"} {
		t.Run(mode, func(t *testing.T) {
			backing := &Uint64List{L: make([]uint64, 8192)}
			loanPool := installUint64DecodePool(t, backing)
			var got [][]byte
			var err error
			switch mode {
			case "bytes":
				got, err = (&BytesBlockDecoder{}).Decode(nil, block, uint64(len(values)))
			case "bytes with tail":
				var tail []byte
				got, tail, err = (&BytesBlockDecoder{}).DecodeWithTail(nil, append(append([]byte(nil), block...), 42), uint64(len(values)))
				if !bytes.Equal(tail, []byte{42}) {
					t.Fatalf("unexpected decode tail %v", tail)
				}
			case "dictionary":
				got, err = NewDictionary().Decode(nil, dictionary, uint64(len(values)))
			case "dictionary values":
				got, err = DecodeDictionaryValues(dictionary)
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, values) {
				t.Fatalf("decoded %v, want %v", got, values)
			}
			if cap(backing.L) > 2048 {
				t.Fatalf("decode retained oversized scratch capacity %d", cap(backing.L))
			}
			if loanPool.RefsCount() != 0 {
				t.Fatal("byte decoder leaked its pool loan")
			}
		})
	}
}

func installInt64DecodePool(t *testing.T, backing *Int64List) *pool.Synced[*Int64List] {
	t.Helper()
	previous := int64ListPool
	loanPool := &pool.Synced[*Int64List]{}
	loanPool.New = func() any { return backing }
	int64ListPool = loanPool
	t.Cleanup(func() { int64ListPool = previous })
	return loanPool
}

func installUint64DecodePool(t *testing.T, backing *Uint64List) *pool.Synced[*Uint64List] {
	t.Helper()
	previous := uint64ListPool
	loanPool := &pool.Synced[*Uint64List]{}
	loanPool.New = func() any { return backing }
	uint64ListPool = loanPool
	t.Cleanup(func() { uint64ListPool = previous })
	return loanPool
}

func BenchmarkIntegerEncodeAlternatingSizes(b *testing.B) {
	large := make([]int64, 8192)
	for idx := range large {
		large[idx] = int64(idx*idx + 7*idx)
	}
	dst := make([]byte, 0, len(large)*10)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		dst, _, _ = Int64ListToBytes(dst[:0], large)
		dst, _, _ = Int64ListToBytes(dst[:0], large[:64])
	}
}

func BenchmarkBytesEncodeAlternatingSizes(b *testing.B) {
	large := make([][]byte, 8192)
	for idx := range large {
		large[idx] = []byte{byte(idx), byte(idx >> 8), byte(idx % 17)}
	}
	dst := make([]byte, 0, len(large)*10)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		dst = EncodeBytesBlock(dst[:0], large)
		dst = EncodeBytesBlock(dst[:0], large[:64])
	}
}
