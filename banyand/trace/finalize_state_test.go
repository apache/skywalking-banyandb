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

package trace

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// TestFinalizeState_RoundTrip verifies write then read returns the same state.
func TestFinalizeState_RoundTrip(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	dir := t.TempDir()

	want := finalizeState{
		LastFinalizedAt:    time.Unix(0, 1_700_000_000_000_000_000).UTC().Format(time.RFC3339Nano),
		FinalizeGeneration: 5,
		UnsampledBytes:     12345,
		FinalizeRounds:     3,
		Terminal:           true,
	}
	require.NoError(t, writeFinalizeState(fileSystem, dir, want))

	got := readFinalizeState(fileSystem, dir)
	assert.Equal(t, want, got)
}

// TestFinalizeState_MissingFileIsZero verifies a shard with no finalize file loads
// as never-finalized (the pre-finalize / fresh-shard common case).
func TestFinalizeState_MissingFileIsZero(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	dir := t.TempDir()

	got := readFinalizeState(fileSystem, dir)
	assert.Equal(t, finalizeState{}, got, "missing finalize file must decode to zero state")
	assert.Zero(t, got.FinalizeGeneration)
}

// TestFinalizeState_CorruptIsZero verifies a corrupt finalize file fails open to the
// zero value rather than panicking (finalize must never crash the node).
func TestFinalizeState_CorruptIsZero(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	dir := t.TempDir()

	_, err := fileSystem.Write([]byte("{not valid json"), filepath.Join(dir, finalizeStateFilename), storage.FilePerm)
	require.NoError(t, err)

	assert.Equal(t, finalizeState{}, readFinalizeState(fileSystem, dir),
		"corrupt finalize file must fail open to zero state")
}

// TestPartMetadata_FinalizeGenRoundTrip verifies the FinalizeGen field survives a
// JSON round-trip and that old-format metadata (no finalizeGen key) loads as 0.
func TestPartMetadata_FinalizeGenRoundTrip(t *testing.T) {
	pm := partMetadata{
		CompressedSizeBytes:       100,
		UncompressedSpanSizeBytes: 200,
		TotalCount:                3,
		BlocksCount:               1,
		MinTimestamp:              10,
		MaxTimestamp:              20,
		FinalizeGen:               7,
	}
	data, err := json.Marshal(&pm)
	require.NoError(t, err)

	var got partMetadata
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, uint64(7), got.FinalizeGen, "FinalizeGen must survive round-trip")

	// Old-format metadata (no finalizeGen key) loads as unstamped (0).
	var old partMetadata
	require.NoError(t, json.Unmarshal([]byte(`{"compressedSizeBytes":1,"totalCount":1,"minTimestamp":1,"maxTimestamp":2}`), &old))
	assert.Zero(t, old.FinalizeGen, "old-format part metadata must load as unstamped")

	// omitempty: a zero FinalizeGen is not serialized.
	zeroPM := partMetadata{TotalCount: 1, MinTimestamp: 1, MaxTimestamp: 2}
	zeroData, err := json.Marshal(&zeroPM)
	require.NoError(t, err)
	assert.NotContains(t, string(zeroData), "finalizeGen", "zero FinalizeGen must be omitted")
}
