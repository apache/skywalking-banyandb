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

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The NIDX-01A corpus is an ICE v3 / snapshot v3 directory produced by the
// retired compatibility writer through BanyanDB's store boundary and checked
// in as bytes. Tests read it; the generator that produced it ran against the
// now-removed legacy engine and has been deleted along with it.
const (
	nidx01aRoot     = "../testdata/nidx01a"
	nidx01aIndexDir = nidx01aRoot + "/index"
	nidx01aManifest = nidx01aRoot + "/provenance.json"

	// nidx01aVisibleCount is the declared visible document count of the corpus.
	// It is the literal from issue #14008, not a value any reader computes:
	// two documents are created, none is deleted, so two are visible.
	nidx01aVisibleCount = int64(2)

	segExt = ".seg"
	snpExt = ".snp"

	dirInventoryAttemptLimit = 100
	dirInventoryRetryDelay   = time.Millisecond
)

// nidx01aDocIDs are the logical documents the corpus contains, in insertion
// order, as named by issue #14008.
var nidx01aDocIDs = []uint64{11, 12}

// nidx01aProvenance is the manifest checked in beside the corpus bytes. It
// records everything a reviewer needs to re-derive the corpus without reading
// the generator: which oracle produced it, how to run that oracle, which
// logical documents went in, what the bytes hash to, and what the declared
// visible count is.
type nidx01aProvenance struct {
	Oracle           map[string]string `json:"oracle"`
	FileSHA256       map[string]string `json:"file_sha256"`
	ReservedCRC32    map[string]string `json:"reserved_crc32"`
	GeneratorCommand string            `json:"generator_command"`
	Notes            string            `json:"notes"`
	LogicalDocuments []uint64          `json:"logical_documents"`
	VisibleCount     int64             `json:"visible_count"`
}

// loadNIDX01AProvenance reads the checked-in manifest. The generator that
// produced the corpus and this manifest ran against the now-removed legacy
// engine and has been deleted along with it; the corpus and manifest remain
// checked in as fixed bytes this contract reads.
func loadNIDX01AProvenance(t *testing.T) nidx01aProvenance {
	t.Helper()
	raw, err := os.ReadFile(nidx01aManifest)
	require.NoError(t, err, "the NIDX-01A provenance manifest must be checked in")
	var manifest nidx01aProvenance
	require.NoError(t, json.Unmarshal(raw, &manifest))
	return manifest
}

// copyIndexDir copies the persisted index files in src into a fresh directory
// under the test's temporary space and returns it. Runtime files are outside
// the ICE grammar and are not part of a copied index generation.
func copyIndexDir(t *testing.T, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "index")
	require.NoError(t, os.MkdirAll(dst, 0o755))
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, entry := range entries {
		extension := filepath.Ext(entry.Name())
		if entry.IsDir() || extension != segExt && extension != snpExt {
			continue
		}
		payload, readErr := os.ReadFile(filepath.Join(src, entry.Name()))
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(filepath.Join(dst, entry.Name()), payload, 0o600))
	}
	return dst
}

// newestSegmentFile returns the path of the highest-numbered segment file in
// dir. Segment and snapshot identifiers are numbered independently, so tests
// that damage "the" segment locate it by scanning rather than by pairing names.
func newestSegmentFile(t *testing.T, dir string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*"+segExt))
	require.NoError(t, err)
	require.NotEmpty(t, matches, "index directory %s holds no segment file", dir)
	sort.Strings(matches)
	return matches[len(matches)-1]
}

// dirInventory records every observable property of every entry in dir that a
// read-only call must leave alone: the set of names, and each entry's size,
// mode, modification time, and content hash. Access time is deliberately
// excluded -- reading a file is allowed to update it.
func dirInventory(t *testing.T, dir string) []string {
	t.Helper()
	var lastInventoryErr error
	for attempt := 0; attempt < dirInventoryAttemptLimit; attempt++ {
		inventory, inventoryErr := readDirectoryInventory(dir)
		if inventoryErr == nil {
			return inventory
		}
		if !errors.Is(inventoryErr, os.ErrNotExist) {
			require.NoError(t, inventoryErr)
			return nil
		}
		lastInventoryErr = inventoryErr
		if attempt+1 < dirInventoryAttemptLimit {
			time.Sleep(dirInventoryRetryDelay)
		}
	}
	t.Fatalf("directory inventory for %s did not observe a complete entry set: %v", dir, lastInventoryErr)
	return nil
}

func readDirectoryInventory(dir string) ([]string, error) {
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		return nil, readErr
	}
	inventory := make([]string, 0, len(entries))
	for _, entry := range entries {
		info, infoErr := entry.Info()
		if infoErr != nil {
			return nil, infoErr
		}
		line := entry.Name() + " dir=" + strconv.FormatBool(entry.IsDir()) +
			" size=" + strconv.FormatInt(info.Size(), 10) +
			" mode=" + info.Mode().String() +
			" mtime=" + strconv.FormatInt(info.ModTime().UnixNano(), 10)
		if !entry.IsDir() {
			payload, payloadErr := os.ReadFile(filepath.Join(dir, entry.Name()))
			if payloadErr != nil {
				return nil, payloadErr
			}
			sum := sha256.Sum256(payload)
			line += " sha256=" + hex.EncodeToString(sum[:])
		}
		inventory = append(inventory, line)
	}
	sort.Strings(inventory)
	return inventory, nil
}
