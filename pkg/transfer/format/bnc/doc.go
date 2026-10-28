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
// The footer is a uvarint entry count followed, per member, by a uvarint path length, the
// path, the member size (u64), the member CRC-32 (u32, IEEE), and two uvarints: where the
// member starts in its file and the size of the whole file. Offsets in the container are not
// stored: a member starts where the one before it ends, and the last one ends where the
// footer starts. The last magic byte is the format version.
//
// A container larger than a volume limit is written as several volumes, each a complete
// container within the limit. A file that crosses the end of a volume is split: its first
// piece ends that volume and the rest continues as the first member of the next one, so the
// volumes in order, joined by a VolumeSet, hold every file whole.
//
// The package must not import anything under banyand/ so that data nodes and clients share
// exactly the same code.
package bnc
