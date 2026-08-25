// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package nativeice

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenVisibleDocCount(t *testing.T) {
	directory := t.TempDir()
	segmentID := uint64(2)
	segmentPath := filepath.Join(directory, "000000000002.seg")
	segment := make([]byte, 76)
	footer := segment[len(segment)-60:]
	binary.BigEndian.PutUint64(footer[0:8], 2)
	binary.BigEndian.PutUint64(footer[8:16], 0)
	binary.BigEndian.PutUint64(footer[16:24], 16)
	binary.BigEndian.PutUint64(footer[24:32], 16)
	binary.BigEndian.PutUint32(footer[32:36], 1025)
	binary.BigEndian.PutUint32(footer[52:56], 3)
	if writeErr := os.WriteFile(segmentPath, segment, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	manifest := []byte{3, 1, 3, 'i', 'c', 'e', 0, 0, 0, 3, byte(segmentID)}
	metadata := make([]byte, 32)
	binary.BigEndian.PutUint64(metadata[0:8], uint64(len(segment)))
	binary.BigEndian.PutUint64(metadata[8:16], 2)
	manifest = append(manifest, metadata...)
	manifest = append(manifest, 0, 0, 0, 0, 0)
	if writeErr := os.WriteFile(filepath.Join(directory, "000000000001.snp"), manifest, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	reader, openErr := Open(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	count, countErr := reader.VisibleDocCount()
	if countErr != nil {
		t.Fatal(countErr)
	}
	if count != 2 {
		t.Fatalf("VisibleDocCount() = %d, want 2", count)
	}
}
