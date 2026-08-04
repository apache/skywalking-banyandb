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
// (pkg/query/vectorized/frame) to the stream engine. It supplies stream's frame
// signature, wire version and role/type numbering; the byte layout is the
// shared base's, identical to measure's.
package frame

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	baseframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/frame"
)

// WireVersion is the on-wire frame format version emitted by Encode. Stream
// starts at 1: it is a fresh engine binding, not a continuation of measure's
// version history.
const WireVersion uint8 = 1

// Magic is the 4-byte prefix every raw stream frame body begins with. The first
// byte is data.RawFrameMagicLeadingByte (0x00) so a flag-off proto.Unmarshal of
// a raw body fails loud; 'V','F','R' is the frame signature.
var Magic = [4]byte{data.RawFrameMagicLeadingByte, 'V', 'F', 'R'}

// Stream wire-role values. Explicit and stable: add new roles at the end, never
// reorder. Stream only emits timestamp / element-id / series-id / tag /
// order-key columns.
const (
	wireRoleTimestamp uint8 = 1
	wireRoleElementID uint8 = 2
	wireRoleSeriesID  uint8 = 3
	wireRoleTag       uint8 = 4
	wireRoleOrderKey  uint8 = 5
)

// Stream wire-type values. Explicit and stable. Stream emits only these four
// types: Int64 (ts/elementID/seriesID), Bytes (order key) and TagValue
// passthrough (tags); String is retained for completeness of the primitive set.
const (
	wireTypeInt64    uint8 = 1
	wireTypeString   uint8 = 2
	wireTypeBytes    uint8 = 3
	wireTypeTagValue uint8 = 4
)

// codec is the stream-parameterized shared frame codec. All stream frame I/O
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
	case vectorized.RoleElementID:
		return wireRoleElementID, nil
	case vectorized.RoleSeriesID:
		return wireRoleSeriesID, nil
	case vectorized.RoleTag:
		return wireRoleTag, nil
	case vectorized.RoleOrderKey:
		return wireRoleOrderKey, nil
	default:
		return 0, fmt.Errorf("%w: %d", baseframe.ErrUnsupportedColumnRole, r)
	}
}

func wireToRole(b uint8) (vectorized.ColumnRole, error) {
	switch b {
	case wireRoleTimestamp:
		return vectorized.RoleTimestamp, nil
	case wireRoleElementID:
		return vectorized.RoleElementID, nil
	case wireRoleSeriesID:
		return vectorized.RoleSeriesID, nil
	case wireRoleTag:
		return vectorized.RoleTag, nil
	case wireRoleOrderKey:
		return vectorized.RoleOrderKey, nil
	default:
		return 0, fmt.Errorf("%w: %d", baseframe.ErrUnsupportedColumnRole, b)
	}
}

func typeToWire(t vectorized.ColumnType) (uint8, error) {
	switch t { //nolint:exhaustive // stream emits only int64/string/bytes/tagvalue; other types fall through to the error return
	case vectorized.ColumnTypeInt64:
		return wireTypeInt64, nil
	case vectorized.ColumnTypeString:
		return wireTypeString, nil
	case vectorized.ColumnTypeBytes:
		return wireTypeBytes, nil
	case vectorized.ColumnTypeTagValue:
		return wireTypeTagValue, nil
	default:
		return 0, fmt.Errorf("%w: %s", baseframe.ErrUnsupportedColumnType, t.String())
	}
}

func wireToType(b uint8) (vectorized.ColumnType, error) {
	switch b {
	case wireTypeInt64:
		return vectorized.ColumnTypeInt64, nil
	case wireTypeString:
		return vectorized.ColumnTypeString, nil
	case wireTypeBytes:
		return vectorized.ColumnTypeBytes, nil
	case wireTypeTagValue:
		return vectorized.ColumnTypeTagValue, nil
	default:
		return 0, fmt.Errorf("%w: %d", baseframe.ErrUnsupportedColumnType, b)
	}
}

// Encode serializes a stream vec columnar RecordBatch into a raw frame body.
func Encode(b *vectorized.RecordBatch) ([]byte, error) {
	return codec.Encode(b)
}

// Decode parses a stream raw frame body back into a RecordBatch.
func Decode(b []byte) (*vectorized.RecordBatch, error) {
	return codec.Decode(b)
}
