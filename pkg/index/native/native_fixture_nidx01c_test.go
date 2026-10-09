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

package native

// The NIDX-01C corpus extends the checked-in legacy corpus with the live series
// documents issue #14010 declares. It is two independent index directories:
// sourceA, holding the walk the milestone is specified against, and sourceB,
// which repeats one of sourceA's documents so the series-union case has a
// second source. Every byte was produced by the retired compatibility writer
// through the retired compatibility store boundary and checked in; the generator
// that produced it ran against the now-removed legacy engine and has been
// deleted along with it.
const (
	nidx01cRoot       = "../testdata/nidx01c"
	nidx01cSourceADir = nidx01cRoot + "/sourceA"
	nidx01cSourceBDir = nidx01cRoot + "/sourceB"

	// nidx01cTagName is the one repeated tag name issue #14010 declares. It is
	// stored and not indexed, which is how the production write path records a
	// non-indexed tag.
	nidx01cTagName = "color"

	// The literals below are the documents issue #14010 declares, not values
	// any reader computes. Document labels 101, 202 and 303 are the issue's
	// names for the three documents; they are not series identifiers, because
	// pkg/pb/v1.Series derives its identifier by hashing the marshaled entity
	// buffer and a fixture cannot choose that hash. Each label's identity is
	// the raw byte string below, which the store records as the document's
	// identity field.
	nidx01cLabel101 = "101"
	nidx01cLabel202 = "202"
	nidx01cLabel303 = "303"

	// The stored timestamp bytes below are the compatibility writer's encoding
	// of Unix nanoseconds 100, 200 and 300, pinned here as literals. Each is a
	// shift marker of 0x20 followed by the ten seven-bit groups, most
	// significant first, of the sortable form of the signed value -- 100 is
	// 0x8000000000000064, whose groups are 01 00 00 00 00 00 00 00 00 64 -- so
	// a reviewer can re-derive them by hand rather than by running a decoder.
	//
	// They are the compatibility claim this milestone makes: the native walk
	// must hand a caller the same bytes the retired reader hands it today, so a
	// caller that decodes them keeps working unchanged.
	nidx01cStoredTimestamp100Hex = "2001000000000000000064"
	nidx01cStoredTimestamp200Hex = "2001000000000000000148"
	nidx01cStoredTimestamp300Hex = "200100000000000000022c"
)

// nidx01cDocument is one declared document of the corpus: its issue label, the
// raw identity bytes the store records, its repeated tag values in the declared
// order, and its timestamp and version.
type nidx01cDocument struct {
	label string
	// storedTimestampHex is the compatibility writer's encoding of timestamp as
	// the segment records it. Unlike the identity, the tag values and the
	// version, a stored timestamp is not a BanyanDB encoding, so the corpus
	// pins the oracle's bytes rather than restating a BanyanDB one.
	storedTimestampHex string
	identity           []byte
	tagValues          []string
	timestamp          int64
	version            int64
	deleted            bool
}

// nidx01cSourceADocuments is sourceA's declared content, in the order issue
// #14010 lists it. 101 carries the binary identity 0x010203 the issue names,
// which is deliberately not valid UTF-8 and not a marshaled series buffer, so
// the walk's byte fidelity is observable rather than inferred from text.
var nidx01cSourceADocuments = []nidx01cDocument{
	{
		label:              nidx01cLabel101,
		identity:           []byte{0x01, 0x02, 0x03},
		tagValues:          []string{"blue", "green"},
		timestamp:          100,
		storedTimestampHex: nidx01cStoredTimestamp100Hex,
		version:            2,
	},
	{
		label:              nidx01cLabel202,
		identity:           []byte{0x04, 0x05, 0x06},
		tagValues:          []string{"red"},
		timestamp:          200,
		storedTimestampHex: nidx01cStoredTimestamp200Hex,
		version:            1,
	},
	{
		label:              nidx01cLabel303,
		identity:           []byte{0x07, 0x08, 0x09},
		tagValues:          []string{"gray"},
		timestamp:          300,
		storedTimestampHex: nidx01cStoredTimestamp300Hex,
		version:            3,
		deleted:            true,
	},
}

// nidx01cSourceBDocuments repeats 101 so the series-union case reads the same
// document from a second source.
var nidx01cSourceBDocuments = []nidx01cDocument{nidx01cSourceADocuments[0]}
