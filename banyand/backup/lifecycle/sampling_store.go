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

package lifecycle

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const samplingArtifactVersion = 1

type samplingCursor struct {
	Part  string `json:"part"`
	Block int    `json:"block"`
	Row   int    `json:"row"`
}

type samplingCheckpoint struct {
	ManifestDigest string         `json:"manifest_sha256"`
	Cursor         samplingCursor `json:"cursor"`
	Sequence       uint64         `json:"sequence"`
	Delivered      bool           `json:"delivered"`
}

type samplingInventoryFile struct {
	Path   string `json:"path"`
	Digest string `json:"sha256"`
	Size   int64  `json:"size"`
}

type samplingManifest struct {
	FailOpen         map[string]uint64       `json:"fail_open"`
	PolicyDigest     string                  `json:"policy_digest"`
	IndexDigest      string                  `json:"index_digest"`
	ManifestDigest   string                  `json:"manifest_sha256"`
	Snapshot         string                  `json:"snapshot"`
	Group            string                  `json:"group"`
	SourceStage      string                  `json:"source_stage"`
	TargetStage      string                  `json:"target_stage"`
	TableDigest      string                  `json:"table_digest"`
	SourceInterval   []byte                  `json:"source_interval"`
	Parts            []string                `json:"parts"`
	Schema           []byte                  `json:"schema"`
	PluginDigests    []string                `json:"plugin_digests"`
	TargetInterval   []byte                  `json:"target_interval"`
	Policy           []byte                  `json:"policy"`
	GroupConfig      []byte                  `json:"group_config"`
	Inventory        []samplingInventoryFile `json:"inventory"`
	KeptSpans        uint64                  `json:"kept_spans"`
	SegmentEnd       int64                   `json:"segment_end"`
	SegmentStart     int64                   `json:"segment_start"`
	Traces           uint64                  `json:"traces"`
	KeptTraces       uint64                  `json:"kept_traces"`
	Spans            uint64                  `json:"spans"`
	Version          int                     `json:"version"`
	PreparationNanos int64                   `json:"preparation_nanos"`
	Replicas         uint32                  `json:"replicas"`
	TargetShardNum   uint32                  `json:"target_shards"`
	Shard            uint32                  `json:"shard"`
}

// atomicSamplingJSON makes durability errors visible to the migration caller.
func atomicSamplingJSON(path string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".commit-*")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if _, err = file.Write(payload); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(temporary, path); err != nil {
		return err
	}
	return syncSamplingDirectory(filepath.Dir(path))
}

func syncSamplingDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	err = directory.Sync()
	closeErr := directory.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func samplingFileDigest(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err = io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

type samplingDecisionWriter struct {
	table    *os.File
	index    *os.File
	previous string
	count    uint64
}

func newSamplingDecisionWriter(dir string) (*samplingDecisionWriter, error) {
	table, err := os.Create(filepath.Join(dir, "decisions"))
	if err != nil {
		return nil, err
	}
	index, err := os.Create(filepath.Join(dir, "offsets"))
	if err != nil {
		table.Close()
		return nil, err
	}
	return &samplingDecisionWriter{table: table, index: index}, nil
}

func (w *samplingDecisionWriter) append(id string, keep bool) error {
	if w.count > 0 && id <= w.previous {
		return fmt.Errorf("sampling decisions are not strictly ordered: %q", id)
	}
	if len(id) > 1<<20 {
		return fmt.Errorf("trace ID exceeds decision record limit")
	}
	offset, err := w.table.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	var index [8]byte
	binary.LittleEndian.PutUint64(index[:], uint64(offset))
	if _, err = w.index.Write(index[:]); err != nil {
		return err
	}
	var header [5]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(id)))
	if keep {
		header[4] = 1
	}
	if _, err = w.table.Write(header[:]); err != nil {
		return err
	}
	if _, err = io.WriteString(w.table, id); err != nil {
		return err
	}
	w.previous = id
	w.count++
	return nil
}

func (w *samplingDecisionWriter) close() error {
	var first error
	for _, file := range []*os.File{w.table, w.index} {
		if err := file.Sync(); first == nil {
			first = err
		}
		if err := file.Close(); first == nil {
			first = err
		}
	}
	return first
}

type samplingDecisionTable struct {
	table *os.File
	index *os.File
	count uint64
}

func openSamplingDecisionTable(dir string, manifest *samplingManifest) (*samplingDecisionTable, error) {
	for name, expected := range map[string]string{"decisions": manifest.TableDigest, "offsets": manifest.IndexDigest} {
		digest, err := samplingFileDigest(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		if digest != expected {
			return nil, fmt.Errorf("sampling %s checksum mismatch", name)
		}
	}
	table, err := os.Open(filepath.Join(dir, "decisions"))
	if err != nil {
		return nil, err
	}
	index, err := os.Open(filepath.Join(dir, "offsets"))
	if err != nil {
		table.Close()
		return nil, err
	}
	info, err := index.Stat()
	if err != nil {
		table.Close()
		index.Close()
		return nil, fmt.Errorf("stat sampling decision index: %w", err)
	}
	if info.Size() != int64(manifest.Traces)*8 {
		table.Close()
		index.Close()
		return nil, fmt.Errorf("sampling index count mismatch")
	}
	return &samplingDecisionTable{table: table, index: index, count: manifest.Traces}, nil
}

func (t *samplingDecisionTable) lookup(id string) (bool, error) {
	low, high := uint64(0), t.count
	for low < high {
		mid := low + (high-low)/2
		var offset [8]byte
		if _, err := t.index.ReadAt(offset[:], int64(mid)*8); err != nil {
			return false, err
		}
		position := int64(binary.LittleEndian.Uint64(offset[:]))
		var header [5]byte
		if _, err := t.table.ReadAt(header[:], position); err != nil {
			return false, err
		}
		length := binary.LittleEndian.Uint32(header[:4])
		if length > 1<<20 || header[4] > 1 {
			return false, fmt.Errorf("invalid sampling decision record")
		}
		value := make([]byte, length)
		if _, err := t.table.ReadAt(value, position+5); err != nil {
			return false, err
		}
		key := string(value)
		if key == id {
			return header[4] == 1, nil
		}
		if key < id {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return false, fmt.Errorf("trace %q missing from authoritative sampling decisions", id)
}

func (t *samplingDecisionTable) close() error {
	first := t.table.Close()
	second := t.index.Close()
	if first != nil {
		return first
	}
	return second
}

func makeSamplingDirectory(path string) error {
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("sampling directory is not a directory: %s", path)
		}
		if err = syncSamplingDirectory(path); err != nil {
			return err
		}
		return syncSamplingDirectory(filepath.Dir(path))
	} else if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(path)
	if parent == path {
		return fmt.Errorf("cannot create sampling root %s", path)
	}
	if err := makeSamplingDirectory(parent); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0o700); err != nil && !os.IsExist(err) {
		return err
	}
	if err := syncSamplingDirectory(path); err != nil {
		return err
	}
	return syncSamplingDirectory(parent)
}

type samplingCheckpointRecord struct {
	Digest     string             `json:"sha256"`
	Checkpoint samplingCheckpoint `json:"checkpoint"`
}

func saveSamplingCheckpoint(path string, checkpoint samplingCheckpoint) error {
	payload, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(payload)
	return atomicSamplingJSON(path, samplingCheckpointRecord{Checkpoint: checkpoint, Digest: hex.EncodeToString(digest[:])})
}

func readSamplingCheckpoint(payload []byte) (samplingCheckpoint, error) {
	var record samplingCheckpointRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return samplingCheckpoint{}, err
	}
	encoded, err := json.Marshal(record.Checkpoint)
	if err != nil {
		return samplingCheckpoint{}, err
	}
	digest := sha256.Sum256(encoded)
	if record.Digest != hex.EncodeToString(digest[:]) {
		return samplingCheckpoint{}, fmt.Errorf("sampling checkpoint checksum mismatch")
	}
	if record.Checkpoint.Sequence == 0 && record.Checkpoint.Cursor.Part != "" && !record.Checkpoint.Delivered {
		return samplingCheckpoint{}, fmt.Errorf("sampling checkpoint cursor without confirmed sequence")
	}
	return record.Checkpoint, nil
}

func sealSamplingManifest(manifest *samplingManifest) error {
	manifest.ManifestDigest = ""
	payload, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(payload)
	manifest.ManifestDigest = hex.EncodeToString(digest[:])
	return nil
}

func verifySamplingManifest(manifest *samplingManifest) error {
	copyManifest := *manifest
	if err := sealSamplingManifest(&copyManifest); err != nil {
		return err
	}
	if manifest.ManifestDigest != copyManifest.ManifestDigest {
		return fmt.Errorf("sampling manifest checksum mismatch")
	}
	return nil
}
