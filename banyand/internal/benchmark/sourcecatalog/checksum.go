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

package sourcecatalog

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"sort"

	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
)

type traceDigest struct {
	parts map[uint64]*partDigest
	rows  [][sha256.Size]byte
}

type partDigest struct {
	rows [][sha256.Size]byte
}

type digestSet map[string]*traceDigest

type ledgerRow struct {
	TraceID string          `json:"traceID"`
	SHA256  string          `json:"sha256"`
	Parts   []ledgerPartRow `json:"parts"`
	Rows    uint64          `json:"rows"`
}

type ledgerPartRow struct {
	PartID string `json:"partID"`
	SHA256 string `json:"sha256"`
	Rows   uint64 `json:"rows"`
}

func (ds digestSet) add(traceID string, partID uint64, rowHash [sha256.Size]byte) {
	traceData := ds[traceID]
	if traceData == nil {
		traceData = &traceDigest{parts: make(map[uint64]*partDigest)}
		ds[traceID] = traceData
	}
	traceData.rows = append(traceData.rows, rowHash)
	partData := traceData.parts[partID]
	if partData == nil {
		partData = &partDigest{}
		traceData.parts[partID] = partData
	}
	partData.rows = append(partData.rows, rowHash)
}

func hashCoreRow(row dumptrace.Row) [sha256.Size]byte {
	digest := sha256.New()
	writeBytes(digest, []byte(row.TraceID))
	writeBytes(digest, []byte(row.SpanID))
	writeInt64(digest, row.Timestamp)
	writeBytes(digest, row.Span)
	tagNames := make([]string, 0, len(row.Tags))
	for tagName := range row.Tags {
		tagNames = append(tagNames, tagName)
	}
	sort.Strings(tagNames)
	writeUint64(digest, uint64(len(tagNames)))
	for _, tagName := range tagNames {
		writeBytes(digest, []byte(tagName))
		writeUint64(digest, uint64(row.TagTypes[tagName]))
		writeBytes(digest, row.Tags[tagName])
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func hashIndexRow(traceID string, row sidx.RawRow) [sha256.Size]byte {
	digest := sha256.New()
	writeBytes(digest, []byte(traceID))
	writeInt64(digest, row.Key)
	writeUint64(digest, uint64(row.SeriesID))
	writeBytes(digest, row.Data)
	tags := append([]sidx.Tag(nil), row.Tags...)
	sort.Slice(tags, func(leftIdx, rightIdx int) bool {
		return tags[leftIdx].Name < tags[rightIdx].Name
	})
	writeUint64(digest, uint64(len(tags)))
	for _, tag := range tags {
		writeBytes(digest, []byte(tag.Name))
		writeUint64(digest, uint64(tag.ValueType))
		writeBytes(digest, tag.Value)
		writeUint64(digest, uint64(len(tag.ValueArr)))
		for _, value := range tag.ValueArr {
			writeBytes(digest, value)
		}
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func writeLedger(path string, digests digestSet) (LedgerCatalog, error) {
	file, createErr := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if createErr != nil {
		return LedgerCatalog{}, fmt.Errorf("cannot create ledger %q: %w", path, createErr)
	}
	buffered := bufio.NewWriter(file)
	fileDigest := sha256.New()
	logicalDigest := sha256.New()
	writer := &digestWriter{writers: []hashOrWriter{buffered, fileDigest}}
	traceIDs := sortedTraceIDs(digests)
	var totalRows uint64
	for _, traceID := range traceIDs {
		traceData := digests[traceID]
		traceHash := combineRowHashes(traceData.rows)
		partIDs := make([]uint64, 0, len(traceData.parts))
		for partID := range traceData.parts {
			partIDs = append(partIDs, partID)
		}
		sort.Slice(partIDs, func(leftIdx, rightIdx int) bool { return partIDs[leftIdx] < partIDs[rightIdx] })
		parts := make([]ledgerPartRow, 0, len(partIDs))
		for _, partID := range partIDs {
			partData := traceData.parts[partID]
			partHash := combineRowHashes(partData.rows)
			parts = append(parts, ledgerPartRow{
				PartID: formatPartID(partID),
				SHA256: hex.EncodeToString(partHash[:]),
				Rows:   uint64(len(partData.rows)),
			})
		}
		entry := ledgerRow{
			TraceID: traceID,
			SHA256:  hex.EncodeToString(traceHash[:]),
			Parts:   parts,
			Rows:    uint64(len(traceData.rows)),
		}
		line, marshalErr := json.Marshal(entry)
		if marshalErr != nil {
			rowErr := fmt.Errorf("cannot marshal ledger row for trace %q: %w", traceID, marshalErr)
			return LedgerCatalog{}, closeLedgerWithError(file, path, rowErr)
		}
		line = append(line, '\n')
		if _, writeErr := writer.Write(line); writeErr != nil {
			rowErr := fmt.Errorf("cannot write ledger row for trace %q: %w", traceID, writeErr)
			return LedgerCatalog{}, closeLedgerWithError(file, path, rowErr)
		}
		writeBytes(logicalDigest, []byte(traceID))
		writeUint64(logicalDigest, uint64(len(traceData.rows)))
		mustWriteDigest(logicalDigest, traceHash[:])
		totalRows += uint64(len(traceData.rows))
	}
	if flushErr := buffered.Flush(); flushErr != nil {
		ledgerErr := fmt.Errorf("cannot flush ledger %q: %w", path, flushErr)
		return LedgerCatalog{}, closeLedgerWithError(file, path, ledgerErr)
	}
	if syncErr := file.Sync(); syncErr != nil {
		ledgerErr := fmt.Errorf("cannot sync ledger %q: %w", path, syncErr)
		return LedgerCatalog{}, closeLedgerWithError(file, path, ledgerErr)
	}
	if closeErr := file.Close(); closeErr != nil {
		return LedgerCatalog{}, fmt.Errorf("cannot close ledger %q: %w", path, closeErr)
	}
	return LedgerCatalog{
		File:            filepath.Base(path),
		SHA256:          hex.EncodeToString(fileDigest.Sum(nil)),
		LogicalChecksum: hex.EncodeToString(logicalDigest.Sum(nil)),
		TraceCount:      uint64(len(traceIDs)),
		RowCount:        totalRows,
	}, nil
}

func closeLedgerWithError(file *os.File, path string, cause error) error {
	if closeErr := file.Close(); closeErr != nil {
		return errors.Join(cause, fmt.Errorf("cannot close ledger %q: %w", path, closeErr))
	}
	return cause
}

type hashOrWriter interface {
	Write([]byte) (int, error)
}

type digestWriter struct {
	writers []hashOrWriter
}

func (dw *digestWriter) Write(data []byte) (int, error) {
	for _, writer := range dw.writers {
		written, writeErr := writer.Write(data)
		if writeErr != nil {
			return written, writeErr
		}
		if written != len(data) {
			return written, fmt.Errorf("short ledger write: got %d bytes, want %d", written, len(data))
		}
	}
	return len(data), nil
}

func combineRowHashes(rows [][sha256.Size]byte) [sha256.Size]byte {
	ordered := append([][sha256.Size]byte(nil), rows...)
	sort.Slice(ordered, func(leftIdx, rightIdx int) bool {
		return string(ordered[leftIdx][:]) < string(ordered[rightIdx][:])
	})
	digest := sha256.New()
	for _, rowHash := range ordered {
		mustWriteDigest(digest, rowHash[:])
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func sortedTraceIDs(digests digestSet) []string {
	traceIDs := make([]string, 0, len(digests))
	for traceID := range digests {
		traceIDs = append(traceIDs, traceID)
	}
	sort.Strings(traceIDs)
	return traceIDs
}

func writeBytes(writer hash.Hash, data []byte) {
	writeUint64(writer, uint64(len(data)))
	mustWriteDigest(writer, data)
}

func writeUint64(writer hash.Hash, value uint64) {
	var buffer [8]byte
	binary.BigEndian.PutUint64(buffer[:], value)
	mustWriteDigest(writer, buffer[:])
}

func writeInt64(writer hash.Hash, value int64) {
	writeUint64(writer, uint64(value))
}

func mustWriteDigest(writer hash.Hash, data []byte) {
	if _, writeErr := writer.Write(data); writeErr != nil {
		panic(fmt.Sprintf("hash implementation returned an error: %v", writeErr))
	}
}
