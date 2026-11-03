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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

// recoverSamplingAttempts runs before legacy progress can hide a prepared shard.
func recoverSamplingAttempts(root string, progress *Progress) (map[string]*commonv1.Group, error) {
	groups := make(map[string]*commonv1.Group)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) && path == root {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && strings.HasPrefix(entry.Name(), ".preparing-") {
			return filepath.SkipDir
		}
		if !entry.IsDir() && entry.Name() == "mode.json" {
			return recoverSamplingMode(path, progress, groups)
		}
		if entry.IsDir() || entry.Name() != "manifest.json" {
			return nil
		}
		return recoverSamplingManifest(path, progress, groups)
	})
	return groups, err
}

type samplingModeRecord struct {
	Prepared    map[string]string `json:"prepared,omitempty"`
	Digest      string            `json:"sha256"`
	Snapshot    string            `json:"snapshot"`
	GroupConfig []byte            `json:"group_config,omitempty"`
	Sampled     bool              `json:"sampled"`
}

func selectSamplingMode(root, group, snapshot string, legacy bool, desired bool) (bool, error) {
	if root == "" {
		return desired && !legacy, nil
	}
	path := filepath.Join(root, group, "mode.json")
	payload, err := os.ReadFile(path)
	if err == nil {
		var mode samplingModeRecord
		if err = json.Unmarshal(payload, &mode); err != nil {
			return false, err
		}
		if mode.Snapshot != snapshot {
			return false, fmt.Errorf("sampling mode snapshot mismatch")
		}
		if err = validateSamplingMode(filepath.Dir(path), &mode); err != nil {
			return false, err
		}
		return mode.Sampled, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err = makeSamplingDirectory(filepath.Dir(path)); err != nil {
		return false, err
	}
	mode := samplingModeRecord{Snapshot: snapshot, Sampled: desired && !legacy}
	if err = saveSamplingMode(path, mode); err != nil {
		return false, err
	}
	return mode.Sampled, nil
}

func hasPreparedSampling(root, group string) (bool, error) {
	found := false
	err := filepath.WalkDir(filepath.Join(root, group), func(_ string, entry os.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && strings.HasPrefix(entry.Name(), ".preparing-") {
			return filepath.SkipDir
		}
		if !entry.IsDir() && entry.Name() == "manifest.json" {
			found = true
		}
		return nil
	})
	return found, err
}

func validateSamplingMode(directory string, mode *samplingModeRecord) error {
	copyMode := *mode
	copyMode.Digest = ""
	encoded, err := json.Marshal(copyMode)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(encoded)
	if mode.Digest != hex.EncodeToString(digest[:]) {
		return fmt.Errorf("sampling mode checksum mismatch")
	}
	for relative, digest := range mode.Prepared {
		path := filepath.Join(directory, relative)
		if !strings.HasPrefix(path, directory+string(filepath.Separator)) {
			return fmt.Errorf("prepared sampling path escapes mode directory")
		}
		payload, err := os.ReadFile(filepath.Join(path, "manifest.json"))
		if err != nil {
			return fmt.Errorf("required prepared sampling artifact missing: %w", err)
		}
		var manifest samplingManifest
		if err = json.Unmarshal(payload, &manifest); err != nil {
			return err
		}
		if err = verifySamplingManifest(&manifest); err != nil {
			return err
		}
		if manifest.ManifestDigest != digest {
			return fmt.Errorf("prepared sampling artifact identity mismatch")
		}
	}
	return nil
}

func registerPreparedSampling(root string, prepared *preparedTraceMigration) error {
	directory := filepath.Join(root, prepared.manifest.Group)
	path := filepath.Join(directory, "mode.json")
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var mode samplingModeRecord
	if err = json.Unmarshal(payload, &mode); err != nil {
		return err
	}
	if err = validateSamplingMode(directory, &mode); err != nil {
		return err
	}
	if !mode.Sampled || mode.Snapshot != prepared.manifest.Snapshot {
		return fmt.Errorf("prepared sampling mode ownership mismatch")
	}
	if mode.Prepared == nil {
		mode.Prepared = make(map[string]string)
	}
	relative, err := filepath.Rel(directory, prepared.directory)
	if err != nil {
		return err
	}
	mode.Prepared[relative] = prepared.manifest.ManifestDigest
	mode.GroupConfig = prepared.manifest.GroupConfig
	return saveSamplingMode(path, mode)
}

func saveSamplingMode(path string, mode samplingModeRecord) error {
	mode.Digest = ""
	encoded, err := json.Marshal(mode)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(encoded)
	mode.Digest = hex.EncodeToString(digest[:])
	return atomicSamplingJSON(path, mode)
}

func recoverSamplingMode(path string, progress *Progress, groups map[string]*commonv1.Group) error {
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var mode samplingModeRecord
	if err = json.Unmarshal(payload, &mode); err != nil {
		return err
	}
	if mode.Sampled {
		if err = validateSamplingMode(filepath.Dir(path), &mode); err != nil {
			return err
		}
		if progress.SnapshotTraceDir != "" && progress.SnapshotTraceDir != mode.Snapshot {
			return fmt.Errorf("conflicting sampled mode snapshots")
		}
		progress.SnapshotTraceDir = mode.Snapshot
		if len(mode.GroupConfig) > 0 {
			group := &commonv1.Group{}
			if err = proto.Unmarshal(mode.GroupConfig, group); err != nil {
				return err
			}
			groups[group.Metadata.Name] = group
		}

		if progress.samplingOwnedGroups == nil {
			progress.samplingOwnedGroups = make(map[string]bool)
		}
		progress.samplingOwnedGroups[filepath.Base(filepath.Dir(path))] = true
	}
	return nil
}

func recoverSamplingManifest(path string, progress *Progress, groups map[string]*commonv1.Group) error {
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	manifest := &samplingManifest{}
	if err = json.Unmarshal(payload, manifest); err != nil {
		return fmt.Errorf("recover sampling manifest %s: %w", path, err)
	}
	if err = verifySamplingManifest(manifest); err != nil {
		return err
	}
	if manifest.Version != samplingArtifactVersion {
		return fmt.Errorf("unsupported pending sampling manifest %s", path)
	}
	group := &commonv1.Group{}
	if err = proto.Unmarshal(manifest.GroupConfig, group); err != nil {
		return err
	}
	if group.Metadata == nil || group.Metadata.Name != manifest.Group {
		return fmt.Errorf("sampling manifest group mismatch %s", path)
	}
	if existing := groups[manifest.Group]; existing != nil && !proto.Equal(existing, group) {
		return fmt.Errorf("conflicting pinned sampling groups %s", manifest.Group)
	}
	if _, snapshotErr := os.Stat(manifest.Snapshot); snapshotErr != nil {
		return fmt.Errorf("required sampling snapshot unavailable: %w", snapshotErr)
	}
	groups[manifest.Group] = group
	if progress.SnapshotTraceDir != "" && progress.SnapshotTraceDir != manifest.Snapshot {
		return fmt.Errorf("conflicting sampling snapshot ownership")
	}
	progress.SnapshotTraceDir = manifest.Snapshot
	delete(progress.CompletedGroups, manifest.Group)
	delete(progress.DeletedTraceGroups, manifest.Group)
	return nil
}
