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

// Package benchmark contains deterministic filesystem helpers shared by the
// trace merge performance harness.
package benchmark

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Manifest summarizes all regular files below a directory.
type Manifest struct {
	SHA256 string `json:"sha256"`
	Files  uint64 `json:"files"`
	Bytes  uint64 `json:"bytes"`
}

type manifestFile struct {
	path string
	rel  string
	size uint64
}

// TreeManifest implements the same digest as sorted sha256sum output over
// paths prefixed with "./".
func TreeManifest(root string) (Manifest, error) {
	absoluteRoot, absoluteErr := filepath.Abs(root)
	if absoluteErr != nil {
		return Manifest{}, fmt.Errorf("cannot resolve manifest root %q: %w", root, absoluteErr)
	}
	var files []manifestFile
	walkErr := filepath.WalkDir(absoluteRoot, func(path string, entry os.DirEntry, entryErr error) error {
		if entryErr != nil {
			return fmt.Errorf("cannot visit manifest path %q: %w", path, entryErr)
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		relativePath, relativeErr := filepath.Rel(absoluteRoot, path)
		if relativeErr != nil {
			return fmt.Errorf("cannot make %q relative to %q: %w", path, absoluteRoot, relativeErr)
		}
		relativePath = filepath.ToSlash(relativePath)
		if strings.ContainsAny(relativePath, "\r\n") {
			return fmt.Errorf("manifest path contains a newline: %q", relativePath)
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return fmt.Errorf("cannot stat manifest file %q: %w", path, infoErr)
		}
		files = append(files, manifestFile{path: path, rel: relativePath, size: uint64(info.Size())})
		return nil
	})
	if walkErr != nil {
		return Manifest{}, fmt.Errorf("cannot walk manifest root %q: %w", absoluteRoot, walkErr)
	}
	sort.Slice(files, func(leftIdx, rightIdx int) bool {
		return files[leftIdx].rel < files[rightIdx].rel
	})
	aggregate := sha256.New()
	var result Manifest
	for _, fileEntry := range files {
		fileHash, hashErr := hashFile(fileEntry.path)
		if hashErr != nil {
			return Manifest{}, fmt.Errorf("cannot hash manifest entry %q: %w", fileEntry.rel, hashErr)
		}
		if _, writeErr := fmt.Fprintf(aggregate, "%s  ./%s\n", fileHash, fileEntry.rel); writeErr != nil {
			return Manifest{}, fmt.Errorf("cannot update manifest digest: %w", writeErr)
		}
		result.Files++
		result.Bytes += fileEntry.size
	}
	result.SHA256 = hex.EncodeToString(aggregate.Sum(nil))
	return result, nil
}

func hashFile(path string) (string, error) {
	file, openErr := os.Open(path)
	if openErr != nil {
		return "", fmt.Errorf("cannot open manifest file %q: %w", path, openErr)
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, file)
	closeErr := file.Close()
	var hashErr error
	if copyErr != nil {
		hashErr = fmt.Errorf("cannot hash manifest file %q: %w", path, copyErr)
	}
	if closeErr != nil {
		hashErr = errors.Join(hashErr, fmt.Errorf("cannot close manifest file %q: %w", path, closeErr))
	}
	if hashErr != nil {
		return "", hashErr
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
