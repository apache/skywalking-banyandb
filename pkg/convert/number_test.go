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

package convert

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInt64ToBytes(t *testing.T) {
	testCases := []struct {
		expected []byte
		input    int64
	}{
		{[]byte{127, 255, 255, 255, 255, 255, 255, 156}, -100},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 254}, -2},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255}, -1},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0}, 0},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 1}, 1},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 2}, 2},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 100}, 100},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Int64ToBytes(%d)", tc.input), func(t *testing.T) {
			result := Int64ToBytes(tc.input)
			if !bytes.Equal(result, tc.expected) {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestFloat64ToOrderedBytes_Roundtrip(t *testing.T) {
	testCases := []struct {
		input float64
	}{
		{0.0},
		{1.0},
		{-1.0},
		{123.456},
		{-123.456},
		{math.MaxFloat64},
		{math.SmallestNonzeroFloat64},
		{-math.MaxFloat64},
		{-math.SmallestNonzeroFloat64},
		{math.Inf(1)},
		{math.Inf(-1)},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("roundtrip(%v)", tc.input), func(t *testing.T) {
			encoded := Float64ToOrderedBytes(tc.input)
			require.Len(t, encoded, 8, "encoded bytes should be 8 bytes long")
			decoded := OrderedBytesToFloat64(encoded)
			require.Equal(t, tc.input, decoded, "roundtrip should preserve value")
		})
	}
}

func TestFloat64ToOrderedBytes_Ordering(t *testing.T) {
	pairs := []struct {
		smaller float64
		larger  float64
	}{
		{-100.0, -1.0},
		{-1.0, 0.0},
		{0.0, 1.0},
		{1.0, 100.0},
		{100.0, 1000.0},
		{-1000.0, -100.0},
		{-0.001, 0.001},
		{math.SmallestNonzeroFloat64, 1.0},
		{-1.0, math.SmallestNonzeroFloat64},
		{math.Inf(-1), math.Inf(1)},
		{math.Inf(-1), -100.0},
	}

	for _, pair := range pairs {
		t.Run(fmt.Sprintf("order(%v<%v)", pair.smaller, pair.larger), func(t *testing.T) {
			smallBytes := Float64ToOrderedBytes(pair.smaller)
			largeBytes := Float64ToOrderedBytes(pair.larger)
			require.True(t, bytes.Compare(smallBytes, largeBytes) < 0,
				"expected bytes(%v) < bytes(%v), got %v vs %v",
				pair.smaller, pair.larger, smallBytes, largeBytes)
		})
	}
}

func TestAppendFloat64Bytes(t *testing.T) {
	dst := []byte{0x01, 0x02}
	result := AppendFloat64Bytes(dst, 1.5)
	require.Equal(t, []byte{0x01, 0x02}, result[:2], "should preserve original bytes")
	require.Len(t, result, 10, "should append 8 bytes to 2-byte dst")

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(1.5))
	require.Equal(t, buf[:], result[2:], "appended bytes should match Float64ToBytes")

	// Verify roundtrip
	decoded := BytesToFloat64(result[2:])
	require.Equal(t, 1.5, decoded)
}

func TestBoolToBytes(t *testing.T) {
	testCases := []struct {
		expected []byte
		input    bool
	}{
		{[]byte{1}, true},
		{[]byte{0}, false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("BoolToBytes(%t)", tc.input), func(t *testing.T) {
			result := BoolToBytes(tc.input)
			if !bytes.Equal(result, tc.expected) {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}
