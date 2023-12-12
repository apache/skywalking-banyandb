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

package encoding_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/encoding"
)

func TestUint16ToBytes(t *testing.T) {
	testCases := []struct {
		name string
		v    uint16
	}{
		{
			name: "Zero",
			v:    0,
		},
		{
			name: "PositiveSmall",
			v:    12345,
		},
		{
			name: "PositiveLarge",
			v:    54321,
		},
		{
			name: "MaxUint16",
			v:    65535,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dst := make([]byte, 0)
			encoded := encoding.Uint16ToBytes(dst, tc.v)

			// Check that the encoded bytes can be decoded back to the original uint16
			decoded := encoding.BytesToUint16(encoded)
			require.Equal(t, tc.v, decoded, "Decoded uint16 should be equal to the original uint16")
		})
	}
}

func TestUint32ToBytes(t *testing.T) {
	testCases := []struct {
		name string
		v    uint32
	}{
		{
			name: "Zero",
			v:    0,
		},
		{
			name: "PositiveSmall",
			v:    123456789,
		},
		{
			name: "PositiveLarge",
			v:    987654321,
		},
		{
			name: "MaxUint32",
			v:    4294967295,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dst := make([]byte, 0)
			encoded := encoding.Uint32ToBytes(dst, tc.v)

			// Check that the encoded bytes can be decoded back to the original uint32
			decoded := encoding.BytesToUint32(encoded)
			require.Equal(t, tc.v, decoded, "Decoded uint32 should be equal to the original uint32")
		})
	}
}

func TestUint64ToBytes(t *testing.T) {
	testCases := []struct {
		name string
		v    uint64
	}{
		{
			name: "Zero",
			v:    0,
		},
		{
			name: "PositiveSmall",
			v:    1234567890123456789,
		},
		{
			name: "PositiveLarge",
			v:    9876543210987654321,
		},
		{
			name: "MaxUint64",
			v:    18446744073709551615,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dst := make([]byte, 0)
			encoded := encoding.Uint64ToBytes(dst, tc.v)

			// Check that the encoded bytes can be decoded back to the original uint64
			decoded := encoding.BytesToUint64(encoded)
			require.Equal(t, tc.v, decoded, "Decoded uint64 should be equal to the original uint64")
		})
	}
}

func TestInt64ToBytes(t *testing.T) {
	testCases := []struct {
		name string
		v    int64
	}{
		{
			name: "Zero",
			v:    0,
		},
		{
			name: "Positive",
			v:    1234567890,
		},
		{
			name: "Negative",
			v:    -1234567890,
		},
		{
			name: "MaxInt64",
			v:    9223372036854775807,
		},
		{
			name: "MinInt64",
			v:    -9223372036854775808,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dst := make([]byte, 0)
			encoded := encoding.Int64ToBytes(dst, tc.v)

			// Check that the encoded bytes can be decoded back to the original int64
			decoded := encoding.BytesToInt64(encoded)
			require.Equal(t, tc.v, decoded, "Decoded int64 should be equal to the original int64")
		})
	}
}

func TestBytesToVarInt64List(t *testing.T) {
	testCases := []struct {
		name string
		v    []int64
	}{
		{
			name: "Zero",
			v:    []int64{0},
		},
		{
			name: "Positive",
			v:    []int64{1234567890},
		},
		{
			name: "Negative",
			v:    []int64{-1234567890},
		},
		{
			name: "MultipleValues",
			v:    []int64{0, 1234567890, -1234567890, 9223372036854775807, -9223372036854775808},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dst := make([]byte, 0)
			dst = encoding.VarInt64ListToBytes(dst, tc.v)

			decoded := make([]int64, len(tc.v))
			dst, err := encoding.BytesToVarInt64List(decoded, dst)
			require.NoError(t, err, "BytesToVarInt64s should not return an error")
			require.Len(t, dst, 0, "BytesToVarInt64s should consume all bytes")
			require.Equal(t, tc.v, decoded, "Decoded int64s should be equal to the original int64s")
		})
	}
}

func TestBytesToVarUint64List(t *testing.T) {
	testCases := []struct {
		name string
		v    []uint64
	}{
		{
			name: "Zero",
			v:    []uint64{0},
		},
		{
			name: "PositiveSmall",
			v:    []uint64{1234567890123456789},
		},
		{
			name: "PositiveLarge",
			v:    []uint64{9876543210987654321},
		},
		{
			name: "MaxUint64",
			v:    []uint64{18446744073709551615},
		},
		{
			name: "MultipleValues",
			v:    []uint64{0, 1234567890123456789, 9876543210987654321, 18446744073709551615},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dst := make([]byte, 0)
			dst = encoding.VarUint64sToBytes(dst, tc.v)

			decoded := make([]uint64, len(tc.v))
			dst, err := encoding.BytesToVarUint64s(decoded, dst)
			require.NoError(t, err, "BytesToVarUint64s should not return an error")
			require.Len(t, dst, 0, "BytesToVarUint64s should consume all bytes")
			require.Equal(t, tc.v, decoded, "Decoded uint64s should be equal to the original uint64s")
		})
	}
}
