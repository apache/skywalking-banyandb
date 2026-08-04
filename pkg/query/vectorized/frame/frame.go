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
	"errors"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// MagicLen is the length of a frame magic prefix in bytes.
const MagicLen = 4

// MinHeaderLen is the smallest possible frame header — 4 magic bytes, 1
// version byte, and the minimal 1-byte uvarint encodings of NumRows=0 and
// NumCols=0.
const MinHeaderLen = MagicLen + 1 + 1 + 1

// Header is the parsed frame header (everything up to but not including the
// first column block).
type Header struct {
	NumRows     uint64
	NumCols     uint64
	Magic       [4]byte
	WireVersion uint8
}

// Sentinel errors. Decode and ValidateHeader wrap these with context so
// callers can errors.Is against specific failure classes — most importantly,
// ErrBadMagic at the very first byte is the engineered fail-loud guard the
// raw-wire hard-cutover model relies on.
var (
	// ErrTruncated signals a frame whose length is below the minimum header
	// length, or whose declared lengths run past the buffer.
	ErrTruncated = errors.New("vectorized.frame: truncated frame")

	// ErrBadMagic signals a frame whose leading 4 bytes do not match the
	// codec's Magic. A flag-off (proto) body received on the raw path fails
	// here loudly, never silently mis-decoded.
	ErrBadMagic = errors.New("vectorized.frame: bad magic")

	// ErrBadVersion signals a frame whose WireVersion byte does not match the
	// codec's WireVersion. Hard-cutover means there is no recovery — the
	// receiver must surface this loudly.
	ErrBadVersion = errors.New("vectorized.frame: bad wire version")

	// ErrUnsupportedColumnType signals that a column whose
	// vectorized.ColumnType has no wire mapping crossed the codec. Surfacing
	// this at encode/decode time prevents silently-wrong wire bytes.
	ErrUnsupportedColumnType = errors.New("vectorized.frame: unsupported column type")

	// ErrUnsupportedColumnRole signals that a column whose
	// vectorized.ColumnRole has no wire mapping crossed the codec.
	ErrUnsupportedColumnRole = errors.New("vectorized.frame: unsupported column role")

	// errNilBatch signals that Encode was handed a nil batch or nil schema.
	errNilBatch = errors.New("vectorized.frame: nil batch or schema")
)

// Codec is a parameterized encoder/decoder for the shared vec columnar frame.
// The byte layout is fixed and engine-agnostic; only the frame signature
// (Magic), the format version (WireVersion), and the numeric role/type wire
// mappings are supplied per engine. Two codecs configured with the same Magic,
// WireVersion, and mapping tables produce byte-identical frames — the property
// the golden-bytes cross-implementation test verifies against measure's
// shipped frame package.
type Codec struct {
	// RoleToWire maps a vectorized.ColumnRole to its wire byte, or returns
	// ErrUnsupportedColumnRole for a role the engine does not emit.
	RoleToWire func(vectorized.ColumnRole) (uint8, error)
	// WireToRole is the inverse of RoleToWire.
	WireToRole func(uint8) (vectorized.ColumnRole, error)
	// TypeToWire maps a vectorized.ColumnType to its wire byte, or returns
	// ErrUnsupportedColumnType for a type the engine does not emit.
	TypeToWire func(vectorized.ColumnType) (uint8, error)
	// WireToType is the inverse of TypeToWire.
	WireToType  func(uint8) (vectorized.ColumnType, error)
	Magic       [4]byte
	WireVersion uint8
}
