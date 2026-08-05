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

// Package frame binds the shared vec columnar frame codec
// (pkg/query/vectorized/frame) to the measure engine. It supplies measure's
// frame signature, wire version and role/type numbering; the byte layout is
// the shared base's. It is the non-proto, vec-native columnar binary frame
// carried as the SendResponse.body for TopicInternalMeasureQuery under the
// G9f throughout-vec design.
//
// Wire layout (version 3):
//
//	off  len       field            description
//	---  --------  ---------------  ---------------------------------------------
//	  0  4         Magic            fixed [0x00,'V','F','R'] — first byte 0x00
//	                                forces a flag-off proto.Unmarshal into
//	                                *measurev1.InternalQueryResponse{} to return
//	                                a non-nil error deterministically (G9f spec
//	                                Principle 3, verified against
//	                                google.golang.org/protobuf@v1.36.11).
//	  4  1         WireVersion      currently 3.
//	  5  uvarint   NumRows          number of (active) rows.
//	  ?  uvarint   NumCols          number of column blocks.
//	  ?  ...       Columns          NumCols column blocks, in schema order.
//
// Each column block carries role+type discriminators, the column name, a tag
// family name (empty for non-RoleTag columns), a validity bitmap and a
// type-specific data section; the exact byte shapes live in the shared base's
// encode.go/decode.go.
//
// v1 → v2: each column block carries a uvarint(TagFamilyLen) + TagFamily byte
// run after the Name so the row path's TagFamilyGroups grouping does not
// collapse every projected tag family into the empty-name family.
//
// v2 → v3: adds two column types — TagValue (5) and FieldValue (6) — that
// carry proto-marshaled TagValue / FieldValue bytes per cell, so cross-group
// queries with type-divergent tag/field declarations cross the wire intact.
package frame

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	baseframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/frame"
)

// WireVersion is the on-wire frame format version emitted by Encode. The
// flag-on decoder MUST reject frames carrying any other version with a loud
// typed error: the G9f hard-cutover model forbids dual-wire, so a version skew
// on the wire is by definition a botched operator rollout, not a coexistence
// to negotiate.
const WireVersion uint8 = 3

// Magic is the 4-byte prefix every raw measure frame body MUST begin with. The
// first byte is data.RawFrameMagicLeadingByte (0x00), which decodes as a varint
// tag for protobuf field number 0 — forcing a flag-off node's proto.Unmarshal
// of a raw frame body into *measurev1.InternalQueryResponse{} to return a
// non-nil error (G9f spec Principle 3). The remaining bytes 'V','F','R' are a
// distinctive signature so a flag-on decoder can recognize a valid frame.
var Magic = [4]byte{data.RawFrameMagicLeadingByte, 'V', 'F', 'R'}

// MagicLen is the length of Magic in bytes.
const MagicLen = baseframe.MagicLen

// MinHeaderLen is the smallest possible frame header — 4 magic bytes, 1
// version byte, and the minimal 1-byte uvarint encodings of NumRows=0 and
// NumCols=0.
const MinHeaderLen = baseframe.MinHeaderLen

// Header is the parsed frame header (everything up to but not including the
// first column block).
type Header = baseframe.Header

// Sentinel errors re-exported from the shared base so callers keep using
// errors.Is against frame.ErrX; the identities are shared across the boundary.
var (
	// ErrTruncated signals a frame whose length is below the minimum header
	// length, or whose declared lengths run past the buffer.
	ErrTruncated = baseframe.ErrTruncated

	// ErrBadMagic signals a frame whose leading 4 bytes do not match Magic. A
	// flag-off (proto) body received on the raw path fails here loudly.
	ErrBadMagic = baseframe.ErrBadMagic

	// ErrBadVersion signals a frame whose WireVersion byte does not match the
	// current WireVersion.
	ErrBadVersion = baseframe.ErrBadVersion

	// ErrUnsupportedColumnType signals that a column whose ColumnType has no
	// wire mapping crossed the codec.
	ErrUnsupportedColumnType = baseframe.ErrUnsupportedColumnType

	// ErrUnsupportedColumnRole signals that a column whose ColumnRole has no
	// wire mapping crossed the codec.
	ErrUnsupportedColumnRole = baseframe.ErrUnsupportedColumnRole
)

// Measure wire-role values. Explicit and stable: add new roles at the end,
// never reorder.
const (
	wireRoleTimestamp uint8 = 1
	wireRoleVersion   uint8 = 2
	wireRoleSeriesID  uint8 = 3
	wireRoleShardID   uint8 = 4
	wireRoleTag       uint8 = 5
	wireRoleField     uint8 = 6
)

// Measure wire-type values. Explicit and stable. TagValue/FieldValue carry
// proto-marshaled bytes per cell (v3).
const (
	wireTypeInt64      uint8 = 1
	wireTypeFloat64    uint8 = 2
	wireTypeString     uint8 = 3
	wireTypeBytes      uint8 = 4
	wireTypeTagValue   uint8 = 5
	wireTypeFieldValue uint8 = 6
)

// codec is the measure-parameterized shared frame codec. All measure frame I/O
// flows through this single instance so the wire mapping cannot diverge between
// Encode and Decode.
var codec = baseframe.Codec{
	Magic:       Magic,
	WireVersion: WireVersion,
	RoleToWire:  roleToWire,
	WireToRole:  wireToRole,
	TypeToWire:  typeToWire,
	WireToType:  wireToType,
}

func roleToWire(r vectorized.ColumnRole) (uint8, error) {
	switch r {
	case vectorized.RoleTimestamp:
		return wireRoleTimestamp, nil
	case vectorized.RoleVersion:
		return wireRoleVersion, nil
	case vectorized.RoleSeriesID:
		return wireRoleSeriesID, nil
	case vectorized.RoleShardID:
		return wireRoleShardID, nil
	case vectorized.RoleTag:
		return wireRoleTag, nil
	case vectorized.RoleField:
		return wireRoleField, nil
	default:
		return 0, fmt.Errorf("%w: %d", baseframe.ErrUnsupportedColumnRole, r)
	}
}

func wireToRole(b uint8) (vectorized.ColumnRole, error) {
	switch b {
	case wireRoleTimestamp:
		return vectorized.RoleTimestamp, nil
	case wireRoleVersion:
		return vectorized.RoleVersion, nil
	case wireRoleSeriesID:
		return vectorized.RoleSeriesID, nil
	case wireRoleShardID:
		return vectorized.RoleShardID, nil
	case wireRoleTag:
		return vectorized.RoleTag, nil
	case wireRoleField:
		return vectorized.RoleField, nil
	default:
		return 0, fmt.Errorf("%w: %d", baseframe.ErrUnsupportedColumnRole, b)
	}
}

func typeToWire(t vectorized.ColumnType) (uint8, error) {
	switch t { //nolint:exhaustive // measure emits only the scalar/passthrough subset; other types fall through to the error return
	case vectorized.ColumnTypeInt64:
		return wireTypeInt64, nil
	case vectorized.ColumnTypeFloat64:
		return wireTypeFloat64, nil
	case vectorized.ColumnTypeString:
		return wireTypeString, nil
	case vectorized.ColumnTypeBytes:
		return wireTypeBytes, nil
	case vectorized.ColumnTypeTagValue:
		return wireTypeTagValue, nil
	case vectorized.ColumnTypeFieldValue:
		return wireTypeFieldValue, nil
	default:
		return 0, fmt.Errorf("%w: %s", baseframe.ErrUnsupportedColumnType, t.String())
	}
}

func wireToType(b uint8) (vectorized.ColumnType, error) {
	switch b {
	case wireTypeInt64:
		return vectorized.ColumnTypeInt64, nil
	case wireTypeFloat64:
		return vectorized.ColumnTypeFloat64, nil
	case wireTypeString:
		return vectorized.ColumnTypeString, nil
	case wireTypeBytes:
		return vectorized.ColumnTypeBytes, nil
	case wireTypeTagValue:
		return vectorized.ColumnTypeTagValue, nil
	case wireTypeFieldValue:
		return vectorized.ColumnTypeFieldValue, nil
	default:
		return 0, fmt.Errorf("%w: %d", baseframe.ErrUnsupportedColumnType, b)
	}
}

// Encode serializes a measure vec columnar RecordBatch into a raw frame body.
func Encode(b *vectorized.RecordBatch) ([]byte, error) {
	return codec.Encode(b)
}

// Decode parses a measure raw frame body back into a RecordBatch.
func Decode(b []byte) (*vectorized.RecordBatch, error) {
	return codec.Decode(b)
}

// ValidateHeader is the fail-loud preflight a decoder calls before parsing any
// column data. It rejects a frame whose magic or wire-version does not match
// this codec, and returns the parsed Header plus the bytes consumed.
func ValidateHeader(b []byte) (Header, int, error) {
	return codec.ValidateHeader(b)
}
