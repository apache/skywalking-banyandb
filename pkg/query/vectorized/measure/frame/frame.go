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
//
// Version 4 (WireVersionColumnar) is the columnar layout: the same magic and
// header prefix, but each column sends its validity, offsets and value buffers
// as they sit in memory, padded to 8 bytes, so the receiver builds columns by
// slicing the message (see the shared base's columnar.go for the byte layout).
// Version 3 stays the version every node understands; a data node emits version
// 4 only when the requester says it can decode it (NegotiateVersion), and a
// requester decodes either version, which the frame carries. Rolling upgrades
// therefore never put a frame on the wire that the receiver cannot read.
package frame

import (
	"fmt"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	baseframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/frame"
)

// WireVersion is the row-oriented frame format version emitted by Encode: the one version
// every node of every release understands, and the one a data node uses unless the requester
// advertised that it can decode WireVersionColumnar (see NegotiateVersion). A decoder rejects
// a frame of any version it does not know with a loud typed error, so a skew that negotiation
// should have prevented surfaces as ErrBadVersion rather than as misread data.
const WireVersion uint8 = 3

// WireVersionColumnar is the version of the Arrow-compatible columnar layout.
const WireVersionColumnar uint8 = 4

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
	// ColumnarWireVersion selects the columnar layout for EncodeVersion and Decode.
	ColumnarWireVersion: WireVersionColumnar,
	RoleToWire:          roleToWire,
	WireToRole:          wireToRole,
	TypeToWire:          typeToWire,
	WireToType:          wireToType,
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

// framesEncoded and framesDecoded count the frames this process has encoded and decoded, by
// wire version, so a test can tell which layout actually crossed the wire.
var framesEncoded, framesDecoded [WireVersionColumnar + 1]atomic.Int64

// EncodedCount returns the number of frames of the given wire version this process has encoded.
func EncodedCount(version uint8) int64 {
	if version > WireVersionColumnar {
		return 0
	}
	return framesEncoded[version].Load()
}

// DecodedCount returns the number of frames of the given wire version this process has decoded.
func DecodedCount(version uint8) int64 {
	if version > WireVersionColumnar {
		return 0
	}
	return framesDecoded[version].Load()
}

// EncodeVersion serializes b with the given wire version: WireVersion (3) is the
// row-oriented layout every node understands, WireVersionColumnar (4) the columnar one.
func EncodeVersion(b *vectorized.RecordBatch, version uint8) ([]byte, error) {
	var body []byte
	var err error
	switch version {
	case WireVersion:
		body, err = codec.Encode(b)
	case WireVersionColumnar:
		body, err = codec.EncodeColumnar(b)
	default:
		return nil, fmt.Errorf("%w: cannot encode version %d", ErrBadVersion, version)
	}
	if err == nil {
		framesEncoded[version].Add(1)
	}
	return body, err
}

// Decode parses a measure raw frame body of either wire version back into a RecordBatch.
// A columnar frame's batch borrows the body and pins it through Owners: the caller releases
// the batch (RecordBatch.ReleaseOwners) when it is done with it, and copies any cell it
// keeps beyond that.
func Decode(b []byte) (*vectorized.RecordBatch, error) {
	version := WireVersion
	var batch *vectorized.RecordBatch
	var err error
	if len(b) > MagicLen && [4]byte(b[:MagicLen]) == Magic && b[MagicLen] == WireVersionColumnar {
		version = WireVersionColumnar
		batch, err = codec.DecodeColumnar(b)
	} else {
		batch, err = codec.Decode(b)
	}
	if err == nil {
		framesDecoded[version].Add(1)
	}
	return batch, err
}

// maxWireVersionEnv caps the newest frame version this process emits and advertises,
// so an operator can hold a cluster on the original layout during a risky rollout.
const maxWireVersionEnv = "BANYAND_MEASURE_FRAME_MAX_VERSION"

var (
	// emitCap is the newest version this process emits when a requester accepts it.
	emitCap atomic.Uint32
	// acceptCap is the newest version this process advertises in the requests it sends;
	// zero advertises nothing, which is what a process that predates negotiation does.
	acceptCap atomic.Uint32
)

func init() {
	emitCap.Store(uint32(WireVersionColumnar))
	acceptCap.Store(uint32(WireVersionColumnar))
	if v, parseErr := strconv.ParseUint(os.Getenv(maxWireVersionEnv), 10, 8); parseErr == nil && v < uint64(WireVersionColumnar) {
		if v < uint64(WireVersion) {
			v = uint64(WireVersion)
		}
		emitCap.Store(uint32(v))
		acceptCap.Store(uint32(v))
	}
}

// AdvertisedVersion is the value a liaison puts in InternalQueryRequest.max_frame_version:
// the newest frame version it asks data nodes to emit. Zero means it asks for nothing newer
// than the original WireVersion.
func AdvertisedVersion() uint32 { return acceptCap.Load() }

// SetAdvertisedVersion caps the version this process advertises and returns the previous cap.
// A version below WireVersionColumnar makes a requester that never asks for a columnar frame;
// zero advertises nothing, like a process that predates negotiation. It simulates a liaison of
// an older release in tests. The process still decodes every version.
func SetAdvertisedVersion(version uint8) uint8 {
	return uint8(acceptCap.Swap(uint32(min(version, WireVersionColumnar))))
}

// SetMaxEmitVersion caps the newest frame version this process emits and returns the previous
// cap. It simulates a data node of an older release in tests.
func SetMaxEmitVersion(version uint8) uint8 {
	return uint8(emitCap.Swap(uint32(min(max(version, WireVersion), WireVersionColumnar))))
}

// NegotiateVersion returns the frame version a data node emits for a requester that
// advertised requested (InternalQueryRequest.max_frame_version): the lower of the
// requester's version and this node's newest. A requester that advertised nothing (zero)
// predates negotiation and gets the original WireVersion.
func NegotiateVersion(requested uint32) uint8 {
	if requested < uint32(WireVersionColumnar) {
		return WireVersion
	}
	return min(WireVersionColumnar, uint8(emitCap.Load()))
}

// ValidateHeader is the fail-loud preflight a decoder calls before parsing any
// column data. It rejects a frame whose magic or wire-version does not match
// this codec, and returns the parsed Header plus the bytes consumed.
func ValidateHeader(b []byte) (Header, int, error) {
	return codec.ValidateHeader(b)
}
