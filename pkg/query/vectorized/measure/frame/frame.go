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

// Package frame defines the non-proto, vec-native columnar binary frame
// carried as the SendResponse.body for TopicInternalMeasureQuery under the
// G9f throughout-vec design. Encoders live here (G9f.2); the matching
// decoder lands in G9f.3. The frame format is version-stamped and uses
// frame-internal type/role enums with explicit numeric values so the wire
// is stable against pkg/query/vectorized internal enum refactors.
//
// Wire layout (version 1):
//
//	off  len       field            description
//	---  --------  ---------------  ---------------------------------------------
//	  0  4         Magic            fixed [0x00,'V','F','R'] — first byte 0x00
//	                                forces a flag-off proto.Unmarshal into
//	                                *measurev1.InternalQueryResponse{} to return
//	                                a non-nil error deterministically (G9f spec
//	                                Principle 3, verified against
//	                                google.golang.org/protobuf@v1.36.11).
//	  4  1         WireVersion      currently 1.
//	  5  uvarint   NumRows          number of (active) rows.
//	  ?  uvarint   NumCols          number of column blocks.
//	  ?  ...       Columns          NumCols column blocks, in schema order.
//
// Each column block (header + body):
//
//	off  len       field            description
//	---  --------  ---------------  ---------------------------------------------
//	  0  1         Role             frameColRole — explicit-value enum mirroring
//	                                vectorized.ColumnRole.
//	  1  1         Type             frameColType — explicit-value enum mirroring
//	                                vectorized.ColumnType.
//	  2  uvarint   NameLen          length of the UTF-8 column name.
//	  ?  NameLen   Name             column name bytes.
//	  ?  uvarint   TagFamilyLen     length of the UTF-8 tag family name (v2).
//	                                Empty for non-RoleTag columns.
//	  ?  TFL       TagFamily        tag family name bytes.
//	  ?  ⌈N/8⌉     Validity bitmap  N = NumRows; bit i set ⇒ row i is NULL
//	                                (matches the project's validityBitmap: bit=1
//	                                means null). Empty for N=0.
//	  ?  ...       Data             type-specific; see encodeColumn for shapes.
//
// Per-type body encoding (N = NumRows; null-row slots are present but the
// validity bitmap is the source of truth for nullness — decoders MUST treat
// null-row data bytes as undefined):
//
//   - Int64:     N × 8 bytes little-endian.
//   - Float64:   N × 8 bytes IEEE-754 little-endian.
//   - String:    For each row in order: uvarint(len) + len UTF-8 bytes.
//                Null rows have len=0 and 0 bytes; the validity bitmap
//                disambiguates "null" from "empty string".
//   - Bytes:     Same shape as String, opaque bytes.
package frame

import (
	"errors"

	"github.com/apache/skywalking-banyandb/api/data"
)

// Magic is the 4-byte prefix every raw vec columnar frame body MUST begin
// with. The first byte is data.RawFrameMagicLeadingByte (0x00), which decodes
// as a varint tag for protobuf field number 0 — protowire.ConsumeTag rejects
// it with errCodeFieldNumber and proto/decode.go converts the negative tag
// length into a hard errDecode *before* any unknown-field skip (verified
// against google.golang.org/protobuf@v1.36.11). That deterministically forces
// a flag-off node's proto.Unmarshal of a raw frame body into
// *measurev1.InternalQueryResponse{} to return a non-nil error, collapsing
// the "garbage-but-parsed silently-empty" failure mode into an unmistakable
// hard decode error (G9f spec Principle 3, codec contract). The remaining
// three bytes 'V','F','R' are a distinctive signature so a flag-on decoder
// can recognise a valid frame from random noise on the same wire.
var Magic = [4]byte{data.RawFrameMagicLeadingByte, 'V', 'F', 'R'}

// WireVersion is the on-wire frame format version emitted by Encode. The
// flag-on decoder MUST reject frames carrying any other version with a
// loud typed error: the G9f hard-cutover model forbids dual-wire, so a
// version skew on the wire is by definition a botched operator rollout,
// not a coexistence to negotiate.
//
// v1 → v2: each column block now also carries a uvarint(TagFamilyLen) +
// TagFamily byte run right after the Name. Without it, the row-side
// serializer's TagFamilyGroups grouping collapsed every projected tag
// family into the empty-name family on the receive side and produced
// `tagFamilies[].name == ""` on the wire, diverging from the row path's
// expected output.
//
// v2 → v3: adds two column types — frameColTagValueProto (5) and
// frameColFieldValueProto (6) — that carry proto-marshaled TagValue /
// FieldValue bytes per cell. These let cross-group queries with
// type-divergent tag/field declarations (e.g. entity_id is STRING in
// sw_metric vs INT in sw_updated) cross the wire intact: a typed wire
// column can't represent mixed oneof variants, but proto-bytes-per-cell
// can. The decoder reconstructs them as TypedColumn[*modelv1.TagValue]
// / [*FieldValue] passthrough columns so serializeBatchToProto handles
// them via the fast pointer-return path.
const WireVersion uint8 = 3

// MagicLen is the length of Magic in bytes.
const MagicLen = 4

// MinHeaderLen is the smallest possible frame header — 4 magic bytes,
// 1 version byte, and the minimal 1-byte uvarint encodings of NumRows=0
// and NumCols=0.
const MinHeaderLen = MagicLen + 1 + 1 + 1

// Header is the parsed frame header (everything up to but not including the
// first column block).
type Header struct {
	Magic       [4]byte
	NumRows     uint64
	NumCols     uint64
	WireVersion uint8
}

// Sentinel errors. ValidateHeader (and the future decoder) wrap these with
// context so callers can errors.Is against specific failure classes — most
// importantly, ErrBadMagic at the very first byte is the engineered fail-loud
// guard the G9f spec relies on.
var (
	// ErrTruncated signals a frame whose length is below the minimum header
	// length, or whose declared lengths run past the buffer.
	ErrTruncated = errors.New("vectorized.measure.frame: truncated frame")

	// ErrBadMagic signals a frame whose leading 4 bytes do not match Magic.
	// In particular a flag-off (proto) body received on the raw path will
	// fail here loudly, never silently mis-decoded.
	ErrBadMagic = errors.New("vectorized.measure.frame: bad magic")

	// ErrBadVersion signals a frame whose WireVersion byte does not match
	// the current WireVersion. Hard-cutover means there is no recovery —
	// the receiver must surface this loudly.
	ErrBadVersion = errors.New("vectorized.measure.frame: bad wire version")

	// ErrUnsupportedColumnType signals that the encoder was handed a column
	// whose pkg/query/vectorized.ColumnType does not have a frame mapping.
	// Surfacing this at encode time prevents silently-wrong wire bytes.
	ErrUnsupportedColumnType = errors.New("vectorized.measure.frame: unsupported column type")

	// ErrUnsupportedColumnRole signals that the encoder was handed a column
	// whose pkg/query/vectorized.ColumnRole does not have a frame mapping.
	ErrUnsupportedColumnRole = errors.New("vectorized.measure.frame: unsupported column role")
)

// frameColType is the explicit-value, wire-stable column-type discriminator.
// Numeric values MUST NOT change between WireVersion bumps (or, if they do,
// the WireVersion MUST bump and decoders MUST refuse the old version).
type frameColType uint8

// Frame column-type values. Add new types at the end; never reorder.
const (
	frameColInt64           frameColType = 1
	frameColFloat64         frameColType = 2
	frameColString          frameColType = 3
	frameColBytes           frameColType = 4
	frameColTagValueProto   frameColType = 5
	frameColFieldValueProto frameColType = 6
)

// frameColRole is the explicit-value, wire-stable column-role discriminator.
// Same stability contract as frameColType.
type frameColRole uint8

// Frame column-role values. Add new roles at the end; never reorder.
const (
	frameRoleTimestamp frameColRole = 1
	frameRoleVersion   frameColRole = 2
	frameRoleSeriesID  frameColRole = 3
	frameRoleShardID   frameColRole = 4
	frameRoleTag       frameColRole = 5
	frameRoleField     frameColRole = 6
)
