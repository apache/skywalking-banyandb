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

package export

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	pkgfs "github.com/apache/skywalking-banyandb/pkg/fs"
)

// localFS is the file system the sidx part metadata reader goes through.
var localFS = pkgfs.NewLocalFileSystem()

// readSegmentVersion returns the version recorded in <segDir>/metadata without judging
// its compatibility: the import side owns that gate. Storage creates the file before it
// writes it, so an empty file is a rollover in progress and reported as fs.ErrNotExist,
// which callers treat like a segment that is not there yet.
func readSegmentVersion(segDir string) (string, error) {
	path := filepath.Join(segDir, storage.SegmentMetadataFilename)
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	meta, err := storage.DecodeSegmentMetadata(raw)
	if errors.Is(err, storage.ErrEmptySegmentMetadata) {
		return "", fmt.Errorf("segment metadata %s is still being written: %w", path, fs.ErrNotExist)
	}
	if err != nil {
		return "", err
	}
	return meta.Version, nil
}

// dirBytes sums the sizes of every regular file below dir. A missing dir counts as 0
// so a segment without sidx/ or a shard that vanished mid-walk is not an error.
func dirBytes(dir string) (uint64, error) {
	var total uint64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			if errors.Is(infoErr, fs.ErrNotExist) {
				return nil
			}
			return infoErr
		}
		total += uint64(info.Size())
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	return total, err
}
