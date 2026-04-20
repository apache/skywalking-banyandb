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

package panicdiag

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var defaultArtifactRootHolder struct {
	sync.RWMutex
	root string
}

var defaultMaxArtifactsHolder struct {
	sync.RWMutex
	n int
}

// SetDefaultMaxArtifacts sets the process-wide default maximum number of crash
// artifact directories to retain. When a new artifact is written, directories
// beyond this count are removed oldest-first. Zero disables pruning.
func SetDefaultMaxArtifacts(n int) {
	defaultMaxArtifactsHolder.Lock()
	defer defaultMaxArtifactsHolder.Unlock()
	defaultMaxArtifactsHolder.n = n
}

// DefaultMaxArtifacts returns the process-wide default max artifact count.
func DefaultMaxArtifacts() int {
	defaultMaxArtifactsHolder.RLock()
	defer defaultMaxArtifactsHolder.RUnlock()
	return defaultMaxArtifactsHolder.n
}

// PruneArtifacts removes the oldest crash artifact directories under rootDir
// until at most maxArtifacts remain. Only directories containing a panic.json
// are considered; other directories are left untouched. When maxArtifacts is
// zero the call is a no-op.
func PruneArtifacts(rootDir string, maxArtifacts int) error {
	if maxArtifacts <= 0 || rootDir == "" {
		return nil
	}
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read artifact root for pruning: %w", err)
	}

	// os.ReadDir returns entries in lexicographic ascending order.
	// Artifact directory names are prefixed with a nanosecond-precision UTC
	// timestamp (e.g. 20060102T150405.123456789Z-…), so ascending lexicographic
	// order equals oldest-first — exactly what we need for LRU eviction.
	var artifactDirs []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, statErr := os.Stat(filepath.Join(rootDir, entry.Name(), panicRecordFileName)); os.IsNotExist(statErr) {
			continue
		}
		artifactDirs = append(artifactDirs, entry.Name())
	}

	if len(artifactDirs) <= maxArtifacts {
		return nil
	}

	// Remove the oldest (front of the slice) until we are within the limit.
	excess := artifactDirs[:len(artifactDirs)-maxArtifacts]
	for _, name := range excess {
		if removeErr := os.RemoveAll(filepath.Join(rootDir, name)); removeErr != nil {
			return fmt.Errorf("remove old artifact dir %s: %w", name, removeErr)
		}
	}
	return nil
}

// Collection contains the persisted diagnosis data for a single panic artifact.
type Collection struct {
	ArtifactDir string       `json:"artifactDir"`
	Files       []string     `json:"files"`
	Record      *PanicRecord `json:"record,omitempty"`
}

// SetDefaultArtifactRoot stores the process-wide default artifact root.
func SetDefaultArtifactRoot(root string) {
	defaultArtifactRootHolder.Lock()
	defer defaultArtifactRootHolder.Unlock()
	defaultArtifactRootHolder.root = root
}

// DefaultArtifactRoot returns the process-wide default artifact root.
func DefaultArtifactRoot() string {
	defaultArtifactRootHolder.RLock()
	defer defaultArtifactRootHolder.RUnlock()
	return defaultArtifactRootHolder.root
}

// ListCollections returns persisted diagnosis collections from the given artifact root.
func ListCollections(root string) ([]Collection, error) {
	if root == "" {
		root = DefaultArtifactRoot()
	}
	if root == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read diagnostics root: %w", err)
	}

	collections := make([]Collection, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		artifactDir := filepath.Join(root, entry.Name())
		collection, collectionErr := readCollection(artifactDir)
		if collectionErr != nil {
			return nil, collectionErr
		}
		if collection == nil {
			continue
		}
		collections = append(collections, *collection)
	}
	sort.Slice(collections, func(i, j int) bool {
		return collections[i].ArtifactDir > collections[j].ArtifactDir
	})
	return collections, nil
}

func readCollection(artifactDir string) (*Collection, error) {
	recordPath := filepath.Join(artifactDir, panicRecordFileName)
	recordData, err := os.ReadFile(recordPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read panic record: %w", err)
	}

	var record PanicRecord
	if err := json.Unmarshal(recordData, &record); err != nil {
		return nil, fmt.Errorf("unmarshal panic record: %w", err)
	}

	files, err := listArtifactFiles(artifactDir)
	if err != nil {
		return nil, err
	}
	return &Collection{
		ArtifactDir: filepath.Base(artifactDir),
		Files:       files,
		Record:      &record,
	}, nil
}

func listArtifactFiles(artifactDir string) ([]string, error) {
	entries, err := os.ReadDir(artifactDir)
	if err != nil {
		return nil, fmt.Errorf("read artifact dir: %w", err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		files = append(files, entry.Name())
	}
	sort.Strings(files)
	return files, nil
}
