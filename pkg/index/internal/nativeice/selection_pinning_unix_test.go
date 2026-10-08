// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

//go:build !windows

package nativeice

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestVisitSelectedDocumentsUsesPinnedSegmentAfterUnlink(t *testing.T) {
	indexPath := filepath.Join(t.TempDir(), "index")
	copyErr := os.CopyFS(indexPath, os.DirFS(filepath.Join("..", "..", "testdata", "nidx01c", "sourceA")))
	if copyErr != nil {
		t.Fatal(copyErr)
	}
	reader, openErr := Open(indexPath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()

	segmentPaths, globErr := filepath.Glob(filepath.Join(indexPath, "*.seg"))
	if globErr != nil {
		t.Fatal(globErr)
	}
	if len(segmentPaths) != 1 {
		t.Fatalf("segment files = %v, want one", segmentPaths)
	}
	if removeErr := os.Remove(segmentPaths[0]); removeErr != nil {
		t.Fatal(removeErr)
	}

	visited := 0
	visitErr := reader.VisitSelectedDocuments(context.Background(), "_id", [][]byte{{0x01, 0x02, 0x03}}, func(StoredDocument) error {
		visited++
		return nil
	})
	if visitErr != nil {
		t.Fatal(visitErr)
	}
	if visited != 1 {
		t.Fatalf("visited documents = %d, want 1", visited)
	}
}
