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

package csv

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	stdcsv "encoding/csv"
	"encoding/hex"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// exportedFile is one file rebuilt from its frames.
type exportedFile struct {
	started  *transferv1.FileStarted
	finished *transferv1.FileFinished
	content  []byte
}

// decodeFiles checks the frame order of the files of a unit ((file_started, chunk*,
// file_finished)*), that every chunk is at most chunkSize bytes and that bytes/crc32 match
// the content, and rebuilds the files.
func decodeFiles(t *testing.T, frames []*transferv1.ExportResponse) []*exportedFile {
	t.Helper()
	var files []*exportedFile
	var file *exportedFile
	for i, f := range frames {
		switch frame := f.GetFrame().(type) {
		case *transferv1.ExportResponse_FileStarted:
			require.Nil(t, file, "frame %d: nested file", i)
			file = &exportedFile{started: frame.FileStarted}
		case *transferv1.ExportResponse_Chunk:
			require.NotNil(t, file, "frame %d: chunk outside a file", i)
			require.LessOrEqual(t, len(frame.Chunk.GetContent()), chunkSize)
			require.NotEmpty(t, frame.Chunk.GetContent())
			file.content = append(file.content, frame.Chunk.GetContent()...)
		case *transferv1.ExportResponse_FileFinished:
			require.NotNil(t, file, "frame %d: file_finished outside a file", i)
			file.finished = frame.FileFinished
			require.Equal(t, uint64(len(file.content)), file.finished.GetBytes())
			require.Equal(t, crc32.ChecksumIEEE(file.content), file.finished.GetCrc32())
			for resource := range file.finished.GetRowCounts() {
				require.Contains(t, file.finished.GetSchemaModRevisions(), resource)
			}
			files = append(files, file)
			file = nil
		default:
			t.Fatalf("frame %d: unexpected %T", i, frame)
		}
	}
	require.Nil(t, file, "the frames ended inside a file")
	return files
}

// csvRows decodes a produced file into header -> cell maps, checking that every row has
// as many cells as the header.
func csvRows(t *testing.T, f *exportedFile) []map[string]string {
	t.Helper()
	var r io.Reader = bytes.NewReader(f.content)
	if strings.HasSuffix(f.started.GetRelativePath(), ".gz") {
		gz, err := gzip.NewReader(r)
		require.NoError(t, err)
		r = gz
	}
	reader := stdcsv.NewReader(r)
	reader.FieldsPerRecord = -1 // the #datatype line has one more cell than the header
	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.NotEmpty(t, records)
	header := records[0]
	if len(header) > 0 && header[0] == "#datatype" {
		header = records[1]
		records = records[1:]
	}
	var out []map[string]string
	for _, rec := range records[1:] {
		require.Len(t, rec, len(header))
		row := make(map[string]string, len(header))
		for i, h := range header {
			row[h] = rec[i]
		}
		out = append(out, row)
	}
	return out
}

// hashTree fingerprints every file under root, index directories included: producing a
// unit must not change a single byte of the snapshot.
func hashTree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		body, readErr := os.ReadFile(p)
		if readErr != nil {
			return readErr
		}
		sum := sha256.Sum256(body)
		rel, _ := filepath.Rel(root, p)
		out[rel] = hex.EncodeToString(sum[:])
		return nil
	}))
	return out
}
