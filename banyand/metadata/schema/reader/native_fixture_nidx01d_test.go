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

package reader

// The NIDX-01D corpus is the schema catalog issue #14011 declares: one shard of
// the property-backed `_schema` catalog holding five logical properties across
// seven physical revisions, with p5's stored `_source` deliberately made
// malformed. Every byte was produced by BanyanDB's compatibility writer
// through the previous release's series-store boundary and is checked in;
// production code never generates it. Full provenance (the producing oracle,
// a per-file content hash, and every declared revision) is recorded in
// testdata/nidx01d/provenance.json beside the corpus; there is no generator
// test here, because regenerating the corpus would require the retired
// third-party index library this repository no longer depends on, so the
// checked-in bytes and their manifest are the provenance.
//
// It is an independent fixture in the sense the milestone needs: the expected
// results the tests below assert are issue #14011's declaration, and the
// bytes are an oracle's output that no reader under test produced.
const (
	nidx01dRoot     = "testdata/nidx01d"
	nidx01dShardDir = nidx01dRoot + "/shard-0"

	// nidx01dGroup is the resource group the group-scoped properties of the
	// corpus belong to; nidx01dGroupName is the group p4 itself declares.
	nidx01dGroup     = "g1"
	nidx01dGroupName = "g4"

	// The property identifiers below are the literals the catalog's own
	// identifier format yields for the corpus's properties, pinned here so
	// an assertion compares against a declared string rather than against a
	// value recomputed the way the reader derives it.
	nidx01dPropID1 = "stream_g1/s1"
	nidx01dPropID3 = "stream_g1/s3"
	nidx01dPropID4 = "group_g4"
)
