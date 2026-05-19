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
	"encoding/binary"
	"fmt"
)

// ValidateHeader is the fail-loud preflight a decoder calls before parsing
// any column data. It is the SOLE technical guard for the G9f hard-cutover
// model (Principle 3): a frame whose magic does not match Magic, or whose
// wire-version is not WireVersion, is rejected loudly here — never silently
// mis-decoded.
//
// It does NOT parse the columnar body; that is the decoder's job (G9f.3).
// Returns the parsed Header, the number of bytes consumed from b (so the
// caller can slice b[bytesRead:] to reach the first column block), and a
// non-nil error wrapping one of ErrTruncated, ErrBadMagic, ErrBadVersion on
// any failure.
func ValidateHeader(b []byte) (Header, int, error) {
	if len(b) < MinHeaderLen {
		return Header{}, 0, fmt.Errorf("%w: have %d bytes, need at least %d", ErrTruncated, len(b), MinHeaderLen)
	}
	var h Header
	copy(h.Magic[:], b[:MagicLen])
	if h.Magic != Magic {
		return Header{}, 0, fmt.Errorf("%w: got %#x, want %#x", ErrBadMagic, h.Magic[:], Magic[:])
	}
	h.WireVersion = b[MagicLen]
	if h.WireVersion != WireVersion {
		return Header{}, 0, fmt.Errorf("%w: got %d, want %d", ErrBadVersion, h.WireVersion, WireVersion)
	}
	offset := MagicLen + 1
	nrows, nrowsLen := binary.Uvarint(b[offset:])
	if nrowsLen <= 0 {
		return Header{}, 0, fmt.Errorf("%w: malformed NumRows varint at offset %d", ErrTruncated, offset)
	}
	h.NumRows = nrows
	offset += nrowsLen
	ncols, ncolsLen := binary.Uvarint(b[offset:])
	if ncolsLen <= 0 {
		return Header{}, 0, fmt.Errorf("%w: malformed NumCols varint at offset %d", ErrTruncated, offset)
	}
	h.NumCols = ncols
	offset += ncolsLen
	return h, offset, nil
}
