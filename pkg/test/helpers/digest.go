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

package helpers

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// DirectoriesDigest hashes each directory in order and counts their total file bytes.
func DirectoriesDigest(b testing.TB, roots []string) ([sha256.Size]byte, int64) {
	b.Helper()
	digest := sha256.New()
	var size int64
	for _, root := range roots {
		partDigest, partBytes := DirectoryDigest(b, root)
		_, _ = digest.Write(partDigest[:])
		size += partBytes
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, size
}

// DirectoryDigest hashes relative file names and contents in lexical path order.
func DirectoryDigest(b testing.TB, root string) ([sha256.Size]byte, int64) {
	b.Helper()
	digest := sha256.New()
	var size int64
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, pathErr error) error {
		if pathErr != nil {
			return pathErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, relativeErr := filepath.Rel(root, path)
		if relativeErr != nil {
			return relativeErr
		}
		HashValue(digest, []byte(relative))
		file, openErr := os.Open(path)
		if openErr != nil {
			return openErr
		}
		copied, copyErr := io.Copy(digest, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		size += copied
		return nil
	})
	if walkErr != nil {
		b.Fatal(walkErr)
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, size
}

// HashValue adds a length-prefixed value to a digest.
func HashValue(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}
