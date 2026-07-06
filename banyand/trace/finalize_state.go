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

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// finalizeStateFilename is the per-shard finalization-sampling state document,
// stored in the shard directory alongside snapshots.
const finalizeStateFilename = "finalize.json"

// finalizeState is the per-shard finalization-sampling state, persisted atomically
// in the shard directory (Rev 8: per-shard, NOT in the per-segment metadata). It is
// the sole authority for a shard's finalize generation, cooldown clock, round count,
// and arrival-based unsampled-bytes counter. The finalize worker is the only writer
// (serialized node-wide); readers seed the cached tsTable fields at open. A missing
// or corrupt file decodes to the zero value ("never finalized"), so pre-finalize
// shards and partial writes both fail open.
type finalizeState struct {
	// LastFinalizedAt is the RFC3339Nano wall-clock of the last successful round;
	// empty means never finalized. It is the cooldown clock.
	LastFinalizedAt string `json:"lastFinalizedAt,omitempty"`
	// FinalizeGeneration is the shard's monotonic current generation; 0 = never
	// finalized. A part is selectable when its FinalizeGen < this value.
	FinalizeGeneration uint64 `json:"finalizeGeneration,omitempty"`
	// UnsampledBytes accumulates uncompressed span bytes of parts that arrived into
	// this shard after it cooled/was-finalized; it gates the re-round threshold and
	// resets to 0 after a successful round.
	UnsampledBytes int64 `json:"unsampledBytes,omitempty"`
	// FinalizeRounds counts rounds run over this shard's lifetime; the hard cap.
	FinalizeRounds int `json:"finalizeRounds,omitempty"`
	// Terminal is set once FinalizeRounds hits max_finalize_rounds; the shard is
	// then never re-scanned (all future late data is an accepted miss).
	Terminal bool `json:"terminal,omitempty"`
}

// readFinalizeState reads the shard's finalize state. A missing, empty, or corrupt
// file yields the zero value (never finalized): finalize is best-effort and must
// never panic or block on its own state, so any read/parse error fails open.
func readFinalizeState(fileSystem fs.FileSystem, shardRoot string) finalizeState {
	data, err := fileSystem.Read(filepath.Join(shardRoot, finalizeStateFilename))
	if err != nil || len(data) == 0 {
		return finalizeState{}
	}
	var st finalizeState
	if unmarshalErr := json.Unmarshal(data, &st); unmarshalErr != nil {
		return finalizeState{}
	}
	return st
}

// writeFinalizeState atomically persists st to the shard directory via temp+rename
// (fileSystem.WriteAtomic fsyncs the file then the parent dir), so a crash leaves
// either the previous valid state or none — never a torn document.
func writeFinalizeState(fileSystem fs.FileSystem, shardRoot string, st finalizeState) error {
	data, err := json.Marshal(st)
	if err != nil {
		return errors.WithMessage(err, "cannot marshal finalize state")
	}
	statePath := filepath.Join(shardRoot, finalizeStateFilename)
	n, err := fileSystem.WriteAtomic(data, statePath, storage.FilePerm)
	if err != nil {
		return errors.WithMessage(err, "cannot write finalize state")
	}
	if n != len(data) {
		return errors.Errorf("short write to %s: got %d want %d", statePath, n, len(data))
	}
	return nil
}
