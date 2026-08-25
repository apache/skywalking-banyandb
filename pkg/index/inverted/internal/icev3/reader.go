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

package icev3

import "errors"

// ErrCorruptSegment classifies an index directory whose newest usable
// generation cannot be decoded within the format's structural rules: a segment
// file shorter than its 60-byte footer, an unsupported segment version, a root
// offset or section length that leaves its containing range, or a manifest
// record that does not parse. Callers use errors.Is to distinguish a corrupt
// directory from a directory that merely holds no committed generation yet.
var ErrCorruptSegment = errors.New("icev3: corrupt index")

// MaxSectionBytes bounds every length, count and offset span the reader will
// materialize from a file before it has validated that span against the
// containing file. A declared section larger than this is rejected as corrupt
// rather than allocated, so a malformed or hostile directory cannot drive the
// reader into an unbounded allocation.
const MaxSectionBytes int64 = 64 << 20

// LiveDocCount returns the number of live documents in the ICE v3 index
// directory at dir, reading the newest committed generation whose manifest and
// referenced segment files decode successfully.
//
// The live count is the sum, over the segment records of the selected
// manifest, of the segment's footer document count minus the cardinality of
// that segment's logical deletion bitmap.
//
// LiveDocCount only reads. It creates no file, acquires no exclusive directory
// lock, and leaves every path, byte, size, mtime and directory entry under dir
// unchanged, so it is safe to call while another process holds the directory
// as a writer. It never panics: a directory that holds no decodable generation
// yields a zero count together with an error, and a directory whose newest
// generation violates the format's structural rules yields a zero count
// together with an error matching ErrCorruptSegment.
func LiveDocCount(_ string) (int64, error) {
	return 0, errors.New("icev3: LiveDocCount is not wired to a native reader yet")
}
