// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package nativeice

import "fmt"

const prefixCodedInt64ShiftStart byte = 0x20

// EncodePrefixCodedInt64 encodes a full-precision signed integer in the
// shift-zero format used by the established date/time stored-field format.
// The returned bytes are owned by the caller.
func EncodePrefixCodedInt64(value int64) []byte {
	return EncodePrefixCodedInt64Shift(value, 0)
}

// EncodePrefixCodedInt64Shift encodes an int64 numeric term at the requested
// precision shift. It matches the legacy numeric/date term format, where each
// byte carries seven sortable bits and the header records the shift.
func EncodePrefixCodedInt64Shift(value int64, shift uint) []byte {
	if shift > 63 {
		return nil
	}
	nChars := ((63 - shift) / 7) + 1
	encoded := make([]byte, nChars+1)
	encoded[0] = prefixCodedInt64ShiftStart + byte(shift)
	sortable := uint64(value) ^ 0x8000000000000000
	sortable >>= shift
	for index := len(encoded) - 1; index > 0; index-- {
		encoded[index] = byte(sortable & 0x7f)
		sortable >>= 7
	}
	return encoded
}

// DecodePrefixCodedInt64 decodes the shift-zero prefix-coded signed integer
// used by the established date/time stored-field format. It intentionally
// accepts only shift-zero values: date/time fields are full-precision int64
// nanoseconds, while shifted terms are query-analysis tokens rather than a
// stored timestamp value.
func DecodePrefixCodedInt64(value []byte) (int64, error) {
	if len(value) == 0 || value[0] != prefixCodedInt64ShiftStart {
		return 0, fmt.Errorf("invalid prefix-coded int64 header: %w", ErrCorrupt)
	}
	const shiftZeroLength = 11
	if len(value) != shiftZeroLength {
		return 0, fmt.Errorf("prefix-coded int64 length %d, want %d: %w", len(value), shiftZeroLength, ErrCorrupt)
	}
	if value[1] > 1 {
		return 0, fmt.Errorf("prefix-coded int64 high digit 0x%x overflows int64: %w", value[1], ErrCorrupt)
	}
	var sortable uint64
	for _, encodedByte := range value[1:] {
		if encodedByte > 0x7f {
			return 0, fmt.Errorf("invalid prefix-coded int64 byte 0x%x: %w", encodedByte, ErrCorrupt)
		}
		sortable = (sortable << 7) | uint64(encodedByte)
	}
	return int64(sortable ^ 0x8000000000000000), nil
}
