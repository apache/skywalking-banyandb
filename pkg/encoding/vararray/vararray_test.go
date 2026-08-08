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

package vararray

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unmarshalReference is a verbatim copy of the pre-fast-path UnmarshalVarArray
// implementation. It is the oracle for the differential tests: any change to
// UnmarshalVarArray must agree with this function on the returned triple AND on
// the post-call contents of src, for every input.
//
// Do not "simplify" this function. It is deliberately frozen.
func unmarshalReference(src []byte, idx int) (int, int, error) {
	if idx >= len(src) {
		return 0, 0, errors.New("empty entity value")
	}
	if src[idx] == EntityDelimiter {
		return idx, idx + 1, nil
	}
	writeIdx := idx
	for readIdx := idx; readIdx < len(src); readIdx++ {
		b := src[readIdx]
		switch {
		case b == Escape:
			if readIdx+1 >= len(src) {
				return 0, 0, errors.New("invalid escape character")
			}
			readIdx++
			src[writeIdx] = src[readIdx]
			writeIdx++
		case b == EntityDelimiter:
			return writeIdx, readIdx + 1, nil
		default:
			src[writeIdx] = b
			writeIdx++
		}
	}
	return 0, 0, errors.New("invalid variable array")
}

func TestUnmarshalVarArray_Table(t *testing.T) {
	tests := []struct {
		name    string
		src     string
		wantVal string
		errMsg  string
		idx     int
		wantEnd int
		wantNxt int
		wantErr bool
	}{
		{name: "idx beyond length", src: "abc", idx: 3, wantErr: true, errMsg: "empty entity value"},
		{name: "empty src", src: "", idx: 0, wantErr: true, errMsg: "empty entity value"},
		{name: "empty value", src: "|", idx: 0, wantEnd: 0, wantNxt: 1, wantVal: ""},
		{name: "no delimiter", src: "abc", idx: 0, wantErr: true, errMsg: "invalid variable array"},
		{name: "invalid escape at end", src: "abc\\", idx: 0, wantErr: true, errMsg: "invalid escape character"},
		{name: "lone escape", src: "\\", idx: 0, wantErr: true, errMsg: "invalid escape character"},
		{name: "simple value", src: "abc|", idx: 0, wantEnd: 3, wantNxt: 4, wantVal: "abc"},
		{name: "escaped delimiter", src: "a\\|b|", idx: 0, wantEnd: 3, wantNxt: 5, wantVal: "a|b"},
		{name: "escaped escape", src: "c\\\\d|", idx: 0, wantEnd: 3, wantNxt: 5, wantVal: "c\\d"},
		{name: "value is a single escaped delimiter", src: "\\||", idx: 0, wantEnd: 1, wantNxt: 3, wantVal: "|"},
		{name: "consecutive escaped escapes", src: "\\\\\\\\|", idx: 0, wantEnd: 2, wantNxt: 5, wantVal: "\\\\"},
		// The escape here sits AFTER the terminating delimiter of the entry being
		// decoded. A decoder that searches the whole remaining buffer for an escape
		// before deciding whether the entry is escape-free would mis-handle this.
		{name: "escape after this entry's delimiter", src: "ab|c\\|d|", idx: 0, wantEnd: 2, wantNxt: 3, wantVal: "ab"},
		{name: "second entry carries the escape", src: "ab|c\\|d|", idx: 3, wantEnd: 6, wantNxt: 8, wantVal: "c|d"},
		{name: "empty entry mid sequence", src: "a||b|", idx: 1, wantEnd: 1, wantNxt: 2, wantVal: ""},
		{name: "second empty entry mid sequence", src: "a||b|", idx: 2, wantEnd: 2, wantNxt: 3, wantVal: ""},
		{name: "entry after empty entry", src: "a||b|", idx: 3, wantEnd: 4, wantNxt: 5, wantVal: "b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := []byte(tt.src)
			end, next, err := UnmarshalVarArray(src, tt.idx)
			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.errMsg, err.Error())
				assert.Equal(t, 0, end)
				assert.Equal(t, 0, next)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantEnd, end)
			assert.Equal(t, tt.wantNxt, next)
			assert.Equal(t, tt.wantVal, string(src[tt.idx:end]))
		})
	}
}

// TestUnmarshalVarArray_MutationContract pins the documented in-place behavior.
// Nothing else in the repository asserts what src looks like after the call, yet
// three call sites deep-copy their input specifically because of it.
func TestUnmarshalVarArray_MutationContract(t *testing.T) {
	t.Run("escape-free entry leaves src byte-identical", func(t *testing.T) {
		const encoded = "abc|def|"
		src := []byte(encoded)
		_, _, err := UnmarshalVarArray(src, 0)
		require.NoError(t, err)
		assert.Equal(t, encoded, string(src), "an entry with no escape must not modify the buffer")
	})

	t.Run("empty entry leaves src byte-identical", func(t *testing.T) {
		const encoded = "|abc|"
		src := []byte(encoded)
		_, _, err := UnmarshalVarArray(src, 0)
		require.NoError(t, err)
		assert.Equal(t, encoded, string(src))
	})

	t.Run("escaped entry shifts bytes left in place", func(t *testing.T) {
		// "a\|b|" decodes to "a|b": the escape is removed by shifting the tail
		// left one byte, so the buffer is rewritten and the trailing byte is stale.
		src := []byte("a\\|b|")
		end, next, err := UnmarshalVarArray(src, 0)
		require.NoError(t, err)
		assert.Equal(t, "a|b", string(src[0:end]))
		assert.Equal(t, 5, next)
		assert.Equal(t, "a|bb|", string(src), "decoded bytes are written over the source in place")
	})
}

func TestUnmarshalVarArray_Iteration(t *testing.T) {
	var encoded []byte
	values := [][]byte{[]byte("a|b"), []byte("c\\d"), []byte(""), []byte("plain"), []byte("|"), []byte("\\")}
	for _, v := range values {
		encoded = MarshalVarArray(encoded, v)
	}
	var decoded [][]byte
	for idx := 0; idx < len(encoded); {
		end, next, err := UnmarshalVarArray(encoded, idx)
		require.NoError(t, err)
		decoded = append(decoded, append([]byte(nil), encoded[idx:end]...))
		idx = next
	}
	require.Len(t, decoded, len(values))
	for i := range values {
		// Compared as strings: an empty entry decodes to a nil slice rather than
		// an empty one, which is the same value for every caller of this codec.
		assert.Equal(t, string(values[i]), string(decoded[i]), "entry %d", i)
	}
}

// alphabet is restricted to one ordinary byte plus the two bytes with special
// meaning, so short exhaustive enumeration covers every structural arrangement
// of delimiters and escapes rather than sampling them randomly.
var alphabet = []byte{'a', EntityDelimiter, Escape}

// enumerate calls fn with every string over alphabet of length 1..maxLen.
func enumerate(maxLen int, fn func(src []byte)) {
	for length := 1; length <= maxLen; length++ {
		word := make([]byte, length)
		var rec func(pos int)
		rec = func(pos int) {
			if pos == length {
				fn(word)
				return
			}
			for _, c := range alphabet {
				word[pos] = c
				rec(pos + 1)
			}
		}
		rec(0)
	}
}

// TestUnmarshalVarArray_DifferentialExhaustive compares UnmarshalVarArray against
// the frozen reference over every string of length 1..6 built from {'a', '|', '\'},
// at every start index. Both the returned triple and the post-call buffer must match.
func TestUnmarshalVarArray_DifferentialExhaustive(t *testing.T) {
	cases := 0
	enumerate(6, func(src []byte) {
		for idx := 0; idx <= len(src); idx++ {
			got := append([]byte(nil), src...)
			want := append([]byte(nil), src...)
			gotEnd, gotNext, gotErr := UnmarshalVarArray(got, idx)
			wantEnd, wantNext, wantErr := unmarshalReference(want, idx)

			input := fmt.Sprintf("src=%q idx=%d", src, idx)
			if wantErr != nil {
				require.Error(t, gotErr, input)
				require.Equal(t, wantErr.Error(), gotErr.Error(), input)
			} else {
				require.NoError(t, gotErr, input)
			}
			require.Equal(t, wantEnd, gotEnd, "end mismatch: %s", input)
			require.Equal(t, wantNext, gotNext, "next mismatch: %s", input)
			require.True(t, bytes.Equal(want, got),
				"post-call buffer mismatch: %s reference=%q actual=%q", input, want, got)
			cases++
		}
	})
	t.Logf("compared %d (input, idx) pairs", cases)
}

func FuzzUnmarshalVarArray_Differential(f *testing.F) {
	f.Add([]byte("abc|"), 0)
	f.Add([]byte("a\\|b|"), 0)
	f.Add([]byte("ab|c\\|d|"), 3)
	f.Add([]byte("\\\\|"), 0)
	f.Add([]byte(""), 0)
	f.Add([]byte("no-delimiter"), 0)
	f.Fuzz(func(t *testing.T, src []byte, idx int) {
		if idx < 0 || idx > len(src) {
			t.Skip()
		}
		got := append([]byte(nil), src...)
		want := append([]byte(nil), src...)
		gotEnd, gotNext, gotErr := UnmarshalVarArray(got, idx)
		wantEnd, wantNext, wantErr := unmarshalReference(want, idx)

		if (gotErr == nil) != (wantErr == nil) {
			t.Fatalf("error mismatch for src=%q idx=%d: got %v, reference %v", src, idx, gotErr, wantErr)
		}
		if gotErr != nil && gotErr.Error() != wantErr.Error() {
			t.Fatalf("error text mismatch for src=%q idx=%d: got %q, reference %q", src, idx, gotErr, wantErr)
		}
		if gotEnd != wantEnd || gotNext != wantNext {
			t.Fatalf("index mismatch for src=%q idx=%d: got (%d,%d), reference (%d,%d)", src, idx, gotEnd, gotNext, wantEnd, wantNext)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("post-call buffer mismatch for src=%q idx=%d: got %q, reference %q", src, idx, got, want)
		}
	})
}

func FuzzVarArrayRoundTrip(f *testing.F) {
	f.Add([]byte("plain"), []byte("a|b"), []byte("c\\d"))
	f.Add([]byte(""), []byte("|"), []byte("\\"))
	f.Add([]byte("\\\\"), []byte("||"), []byte(""))
	f.Fuzz(func(t *testing.T, a, b, c []byte) {
		values := [][]byte{a, b, c}
		var encoded []byte
		for _, v := range values {
			encoded = MarshalVarArray(encoded, v)
		}
		var decoded [][]byte
		for idx := 0; idx < len(encoded); {
			end, next, err := UnmarshalVarArray(encoded, idx)
			if err != nil {
				t.Fatalf("round-trip decode failed for %q: %v", values, err)
			}
			decoded = append(decoded, append([]byte(nil), encoded[idx:end]...))
			idx = next
		}
		if len(decoded) != len(values) {
			t.Fatalf("entry count mismatch for %q: got %d, want %d", values, len(decoded), len(values))
		}
		for i := range values {
			if !bytes.Equal(decoded[i], values[i]) {
				t.Fatalf("entry %d mismatch: got %q, want %q", i, decoded[i], values[i])
			}
		}
	})
}

// benchEntry builds a var-array holding one entry of the requested payload length.
// When escaped is set every fourth byte is a delimiter, so the payload cannot be
// decoded without shifting bytes left.
func benchEntry(payloadLen int, escaped bool) []byte {
	payload := make([]byte, payloadLen)
	for i := range payload {
		if escaped && i%4 == 3 {
			payload[i] = EntityDelimiter
			continue
		}
		payload[i] = 'a'
	}
	return MarshalVarArray(nil, payload)
}

func BenchmarkUnmarshalVarArray(b *testing.B) {
	for _, payloadLen := range []int{8, 16, 64, 256} {
		// The escape-free arm needs no per-iteration restore: decoding an entry
		// with no escape only ever writes each byte back over itself.
		b.Run(fmt.Sprintf("clean/len=%d", payloadLen), func(b *testing.B) {
			src := benchEntry(payloadLen, false)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := UnmarshalVarArray(src, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
		// The escaped arm mutates src, so it must be restored every iteration.
		// The restore is inside the timed region and is identical before and
		// after any decoder change, so the arm still compares like for like.
		b.Run(fmt.Sprintf("escaped/len=%d", payloadLen), func(b *testing.B) {
			src := benchEntry(payloadLen, true)
			scratch := make([]byte, len(src))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(scratch, src)
				if _, _, err := UnmarshalVarArray(scratch, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
