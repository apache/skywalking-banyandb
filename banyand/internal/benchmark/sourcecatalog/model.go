// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Package sourcecatalog validates and catalogs the immutable downloaded trace shard.
package sourcecatalog

import dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"

const (
	// DownloadedShardManifestSHA256 identifies the frozen full-shard source.
	DownloadedShardManifestSHA256 = "7291ff8abedb1c6d31bb98356a89ed0bee4020d1ff0f602376b9028d0cf8c510"
	catalogVersion                = 1
)

// Options configures an immutable source-catalog build.
type Options struct {
	SourcePath   string
	OutputPath   string
	Expectations Expectations
	Format       dumptrace.PartFormat
}

// ExpectedIndex defines the frozen physical population of one secondary index.
type ExpectedIndex struct {
	PartCount uint64
	RowCount  uint64
	Bytes     uint64
}

// ExpectedCarrier defines the allowlisted closure population in one outside part.
type ExpectedCarrier struct {
	TraceCount uint64
	RowCount   uint64
}

// ExpectedPopulation defines one selected core population and its closure carriers.
type ExpectedPopulation struct {
	Carriers   map[uint64]ExpectedCarrier
	PartIDs    []uint64
	TraceCount uint64
	RowCount   uint64
	BlockCount uint64
	CoreBytes  uint64
}

// Expectations defines the complete frozen source contract.
type Expectations struct {
	Indexes        map[string]ExpectedIndex
	ManifestSHA256 string
	Small          ExpectedPopulation
	Mature         ExpectedPopulation
	PartCount      uint64
	TraceCount     uint64
	RowCount       uint64
	CoreBytes      uint64
}

// DownloadedShardExpectations returns the immutable production source contract.
func DownloadedShardExpectations() Expectations {
	return Expectations{
		ManifestSHA256: DownloadedShardManifestSHA256,
		PartCount:      26,
		TraceCount:     37_288,
		RowCount:       162_238,
		CoreBytes:      40_879_799,
		Indexes: map[string]ExpectedIndex{
			"latency":    {PartCount: 26, RowCount: 162_238, Bytes: 2_798_752},
			"start_time": {PartCount: 26, RowCount: 162_238, Bytes: 2_404_977},
		},
		Small: ExpectedPopulation{
			PartIDs:    []uint64{0x21f1, 0x2223, 0x222c, 0x2233, 0x223a, 0x2248, 0x2256, 0x2259, 0x2264, 0x2265, 0x2266, 0x226b},
			TraceCount: 254,
			RowCount:   1_214,
			BlockCount: 272,
			CoreBytes:  301_026,
			Carriers: map[uint64]ExpectedCarrier{
				0x2218: {TraceCount: 8, RowCount: 63},
				0x222b: {TraceCount: 6, RowCount: 28},
				0x2267: {TraceCount: 1, RowCount: 6},
				0x226a: {TraceCount: 1, RowCount: 15},
				0x226c: {TraceCount: 2, RowCount: 18},
			},
		},
		Mature: ExpectedPopulation{
			PartIDs:    []uint64{0x05ee, 0x0b5b, 0x1002, 0x1191, 0x165a, 0x1c36},
			TraceCount: 31_832,
			RowCount:   138_686,
			BlockCount: 31_844,
			CoreBytes:  34_856_465,
			Carriers: map[uint64]ExpectedCarrier{
				0x21d4: {TraceCount: 31, RowCount: 245},
			},
		},
	}
}

// Catalog is the deterministic source-catalog summary written to catalog.json.
type Catalog struct {
	Indexes              map[string]IndexCatalog  `json:"indexes"`
	Ledgers              map[string]LedgerCatalog `json:"ledgers"`
	SourceManifestSHA256 string                   `json:"sourceManifestSHA256"`
	Core                 CoreCatalog              `json:"core"`
	Small                PopulationCatalog        `json:"small"`
	Mature               PopulationCatalog        `json:"mature"`
	SourceFiles          uint64                   `json:"sourceFiles"`
	SourceBytes          uint64                   `json:"sourceBytes"`
	Version              int                      `json:"version"`
}

// CoreCatalog summarizes the full core source population.
type CoreCatalog struct {
	LogicalChecksum string `json:"logicalChecksum"`
	PartCount       uint64 `json:"partCount"`
	TraceCount      uint64 `json:"traceCount"`
	RowCount        uint64 `json:"rowCount"`
	BlockCount      uint64 `json:"blockCount"`
	CompressedBytes uint64 `json:"compressedBytes"`
}

// IndexCatalog summarizes one complete secondary-index source population.
type IndexCatalog struct {
	LogicalChecksum string `json:"logicalChecksum"`
	PartCount       uint64 `json:"partCount"`
	RowCount        uint64 `json:"rowCount"`
	Bytes           uint64 `json:"bytes"`
}

// LedgerCatalog identifies one immutable per-trace ledger.
type LedgerCatalog struct {
	File            string `json:"file"`
	SHA256          string `json:"sha256"`
	LogicalChecksum string `json:"logicalChecksum"`
	TraceCount      uint64 `json:"traceCount"`
	RowCount        uint64 `json:"rowCount"`
}

// PopulationCatalog records selected traces and their exact outside closure.
type PopulationCatalog struct {
	TraceIDs        []string         `json:"traceIDs"`
	ClosureTraceIDs []string         `json:"closureTraceIDs"`
	PartIDs         []string         `json:"partIDs"`
	PartTemplates   []PartTemplate   `json:"partTemplates"`
	Carriers        []CarrierCatalog `json:"carriers"`
	TraceCount      uint64           `json:"traceCount"`
	RowCount        uint64           `json:"rowCount"`
	BlockCount      uint64           `json:"blockCount"`
	CompressedBytes uint64           `json:"compressedBytes"`
}

// PartTemplate records the physical shape of one observed source write.
type PartTemplate struct {
	PartID                string `json:"partID"`
	Blocks                uint64 `json:"blocks"`
	Rows                  uint64 `json:"rows"`
	CompressedCoreBytes   uint64 `json:"compressedCoreBytes"`
	UncompressedSpanBytes uint64 `json:"uncompressedSpanBytes"`
}

// CarrierCatalog records only allowlisted trace rows from one outside part.
type CarrierCatalog struct {
	PartID     string   `json:"partID"`
	TraceIDs   []string `json:"traceIDs"`
	TraceCount uint64   `json:"traceCount"`
	RowCount   uint64   `json:"rowCount"`
}
