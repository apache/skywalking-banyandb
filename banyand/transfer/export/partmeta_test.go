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
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	pkgfs "github.com/apache/skywalking-banyandb/pkg/fs"
)

// testPartReader is the PartReader a catalog's storage service provides: its own
// ParsePartMetadata over the local file system.
func testPartReader(catalog commonv1.Catalog) partReadFunc {
	lfs := pkgfs.NewLocalFileSystem()
	return func(partDir string) (queue.StreamingPartData, error) {
		switch catalog {
		case commonv1.Catalog_CATALOG_TRACE:
			return trace.ParsePartMetadata(lfs, partDir)
		case commonv1.Catalog_CATALOG_MEASURE:
			return measure.ParsePartMetadata(lfs, partDir)
		default:
			return stream.ParsePartMetadata(lfs, partDir)
		}
	}
}

func TestPartReaders_UseEachCatalogsReader(t *testing.T) {
	dir := t.TempDir()
	streamPart := filepath.Join(dir, "0000000000000001")
	writeFile(t, filepath.Join(streamPart, storage.PartMetadataFilename),
		[]byte(`{"compressedSizeBytes":10,"uncompressedSizeBytes":40,"totalCount":7,"blocksCount":1,"minTimestamp":100,"maxTimestamp":200}`))
	tracePart := filepath.Join(dir, "0000000000000002")
	writeFile(t, filepath.Join(tracePart, storage.PartMetadataFilename),
		[]byte(`{"compressedSizeBytes":11,"uncompressedSpanSizeBytes":41,"totalCount":8,"blocksCount":1,"minTimestamp":101,"maxTimestamp":201}`))
	measurePart := filepath.Join(dir, "0000000000000003")
	writeFile(t, filepath.Join(measurePart, storage.PartMetadataFilename),
		[]byte(`{"compressedSizeBytes":12,"uncompressedSizeBytes":42,"totalCount":9,"blocksCount":1,"minTimestamp":102,"maxTimestamp":202}`))

	got, err := testPartReader(commonv1.Catalog_CATALOG_STREAM)(streamPart)
	if err != nil {
		t.Fatal(err)
	}
	if got.CompressedSizeBytes != 10 || got.UncompressedSizeBytes != 40 || got.TotalCount != 7 || got.MinTimestamp != 100 || got.MaxTimestamp != 200 {
		t.Fatalf("unexpected stream metadata %+v", got)
	}
	got, err = testPartReader(commonv1.Catalog_CATALOG_TRACE)(tracePart)
	if err != nil {
		t.Fatal(err)
	}
	if got.UncompressedSizeBytes != 41 || got.TotalCount != 8 {
		t.Fatalf("trace span size must be mapped onto UncompressedSizeBytes, got %+v", got)
	}
	got, err = testPartReader(commonv1.Catalog_CATALOG_MEASURE)(measurePart)
	if err != nil {
		t.Fatal(err)
	}
	if got.CompressedSizeBytes != 12 || got.UncompressedSizeBytes != 42 || got.TotalCount != 9 || got.MinTimestamp != 102 || got.MaxTimestamp != 202 {
		t.Fatalf("unexpected measure metadata %+v", got)
	}
}

func TestPartReaders_MissingIsNotExist(t *testing.T) {
	for _, catalog := range []commonv1.Catalog{commonv1.Catalog_CATALOG_STREAM, commonv1.Catalog_CATALOG_MEASURE, commonv1.Catalog_CATALOG_TRACE} {
		if _, err := testPartReader(catalog)(filepath.Join(t.TempDir(), "gone")); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s: a missing metadata.json must match fs.ErrNotExist, got %v", catalog, err)
		}
	}
}

func TestReadSegmentVersion(t *testing.T) {
	seg := t.TempDir()
	if err := os.WriteFile(filepath.Join(seg, "metadata"), []byte(`{"version":"1.5.0"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	v, err := readSegmentVersion(seg)
	if err != nil || v != "1.5.0" {
		t.Fatalf("readSegmentVersion() = %q, %v", v, err)
	}
}

func TestDirBytes(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a"), make([]byte, 3), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "b"), make([]byte, 5), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := dirBytes(dir)
	if err != nil || got != 8 {
		t.Fatalf("dirBytes() = %d, %v; want 8", got, err)
	}
	got, err = dirBytes(filepath.Join(dir, "missing"))
	if err != nil || got != 0 {
		t.Fatalf("missing dir must count as 0, got %d, %v", got, err)
	}
}
