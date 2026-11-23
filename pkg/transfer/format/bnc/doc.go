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

// Package bnc implements the .bnc native container of data export: the engine files of one
// shard or one segment index directory packed back to back, byte for byte, followed by a
// footer that names them.
//
// Layout, all integers little endian:
//
//	member 0 | … | member n-1 | footer | footer CRC-32 (u32) | footer length (u32) | "BNCA"
//
// The footer, whose CRC-32 (IEEE) and length follow it:
//
//	uvarint volume_seq   number of this volume in its container, from 1
//	byte    last         1 when this is the last volume of the container, else 0
//	uvarint count        number of members, at least 1
//	repeat count times:
//	  uvarint path_len
//	  bytes   path       UTF-8, '/'-separated, relative to the container root
//	  u64     size       bytes of this member
//	  u32     crc32      IEEE CRC-32 of those bytes
//	  uvarint file_offset where these bytes start in their file, 0 for a whole file
//	  uvarint file_size  size of the whole file, equal to size for a whole file
//
// Every uvarint is minimally encoded. Offsets in the container are not stored: a member starts
// where the one before it ends, and the last one ends where the footer starts. The last magic
// byte is the format version.
//
// A container larger than a volume limit is written as several volumes, each a complete
// container within the limit. A file that crosses the end of a volume is split: its first
// piece ends that volume and the rest continues as the first member of the next one, so the
// volumes in order, joined by a VolumeSet, hold every file whole. A part, too, may span
// volumes: an importer joins every volume of a container before it groups the parts.
//
// What the container can and cannot prove. Open, Verify and a VolumeSet check that the bytes
// are a well-formed container whose members match their CRC-32s and whose volumes join. They
// cannot prove that it is the container the caller expects, or that it is whole:
//
//   - a container carries no identity: a valid container of another artifact, unit or node
//     passes every check;
//   - a prefix that ends exactly at an inner boundary, such as a first member that is itself a
//     container, or a set of volumes whose last one says it is last, opens and verifies;
//   - a file the producer never packed, such as a missing secondary index part, leaves nothing
//     to detect; ShardLayout.Orphans holds leftovers of incomplete parts, not omissions.
//
// The manifest is the final authority: compare the Summary of every volume with the manifest
// entry of that path before trusting a container.
//
// Reading a file through VolumeSet.Open checks the CRC-32 of each piece on the Read that
// returns its last byte, and Close returns the first error a Read returned. Helpers that stop
// at an exact length, such as io.CopyN and io.ReadFull, drop that final error: check Close
// before committing the bytes anywhere. Checking while streaming this way costs one pass over
// the data; running Verify first and then reading again costs two.
//
// The package must not import anything under banyand/ so that data nodes and clients share
// exactly the same code.
package bnc
