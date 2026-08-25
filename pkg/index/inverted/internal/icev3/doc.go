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

// Package icev3 reads BanyanDB-owned inverted index directories in the ICE v3
// segment / snapshot v3 manifest format without opening a Bluge reader and
// without acquiring the exclusive directory lock.
//
// # Boundary
//
// The exported surface of this package is closed. It is exactly:
//
//	func LiveDocCount(dir string) (int64, error)
//	var  ErrCorruptSegment error
//	const MaxSectionBytes  int64
//
// Everything else — footer parsing, root-offset validation, snapshot manifest
// decoding, generation selection, deletion-mask handling, file access strategy
// (pread, mmap, or otherwise) and every type involved in them — is
// unexported and free to change. Contract tests assert observable behavior of
// the three symbols above and of the caller-facing seam
// inverted.ReadOnlyDocCount; they never reach behind them.
//
// The package deliberately imports neither github.com/blugelabs/bluge nor
// github.com/blugelabs/ice (nor their pinned SkyAPM replacements). That
// exclusion is a checked invariant, not a convention.
//
// # Format
//
// A directory holds immutable `<hex>.seg` segment files and `<hex>.snp`
// manifest files. A manifest selects the live segment generation and carries
// the logical deletion bitmaps; a segment file ends with a fixed 60-byte
// big-endian footer. The reserved CRC32 field at the end of both file kinds is
// preserved for layout compatibility and is never calculated, validated or
// used. The authoritative description is
// docs/design/archive/0.12.0/native-inverted-index/write-format.html
// (BDB-NIDX-SPEC-001 revision 0.2, sections 08 and 09).
package icev3
