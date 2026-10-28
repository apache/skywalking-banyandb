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
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// fakeExportStream is an in-process Export stream: Recv replays reqs then io.EOF, Send records.
type fakeExportStream struct {
	grpc.ServerStream
	ctx  context.Context
	reqs []*transferv1.ExportRequest
	sent []*transferv1.ExportResponse
}

func newFakeExportStream(reqs ...*transferv1.ExportRequest) *fakeExportStream {
	return &fakeExportStream{ctx: context.Background(), reqs: reqs}
}

func (f *fakeExportStream) Context() context.Context { return f.ctx }

func (f *fakeExportStream) Recv() (*transferv1.ExportRequest, error) {
	if len(f.reqs) == 0 {
		return nil, io.EOF
	}
	r := f.reqs[0]
	f.reqs = f.reqs[1:]
	return r, nil
}

func (f *fakeExportStream) Send(r *transferv1.ExportResponse) error {
	f.sent = append(f.sent, r)
	return nil
}

func initFrame(id string, format transferv1.Format, compress transferv1.Compress) *transferv1.ExportRequest {
	return &transferv1.ExportRequest{Frame: &transferv1.ExportRequest_Init_{Init: &transferv1.ExportRequest_Init{
		SessionId: id, TargetNode: "node-1", Format: format, Compress: compress,
	}}}
}

func segmentFrame(u *transferv1.SegmentUnit) *transferv1.ExportRequest {
	return &transferv1.ExportRequest{Frame: &transferv1.ExportRequest_Segment{Segment: u}}
}

func propertyFrame(group string, shards ...uint32) *transferv1.ExportRequest {
	return &transferv1.ExportRequest{Frame: &transferv1.ExportRequest_Property{Property: &transferv1.PropertyUnit{Group: group, ShardIds: shards}}}
}

// exportedFile is one file rebuilt from its frames.
type exportedFile struct {
	started   *transferv1.FileStarted
	finished  *transferv1.FileFinished
	content   []byte
	chunkRows []uint64
	chunks    int
}

// exportedUnit is one unit rebuilt from its frames.
type exportedUnit struct {
	started  *transferv1.UnitStarted
	warnings []string
	files    []*exportedFile
}

// decodeFrames checks the frame order of a whole stream (ready, then per unit:
// unit_started, (file_started, chunk*, file_finished)*, unit_finished), that every chunk
// is at most 1 MiB and that bytes/crc32 match the content, and rebuilds the units.
func decodeFrames(t *testing.T, frames []*transferv1.ExportResponse) []*exportedUnit {
	t.Helper()
	require.NotEmpty(t, frames)
	require.NotNil(t, frames[0].GetReady(), "the first frame must be ready")
	var units []*exportedUnit
	var unit *exportedUnit
	var file *exportedFile
	for i, f := range frames[1:] {
		switch frame := f.GetFrame().(type) {
		case *transferv1.ExportResponse_UnitStarted:
			require.Nil(t, unit, "frame %d: unit_started inside a unit", i)
			unit = &exportedUnit{started: frame.UnitStarted}
		case *transferv1.ExportResponse_FileStarted:
			require.NotNil(t, unit, "frame %d: file outside a unit", i)
			require.Nil(t, file, "frame %d: nested file", i)
			file = &exportedFile{started: frame.FileStarted}
		case *transferv1.ExportResponse_Chunk:
			require.NotNil(t, file, "frame %d: chunk outside a file", i)
			require.LessOrEqual(t, len(frame.Chunk.GetContent()), 1<<20)
			require.NotEmpty(t, frame.Chunk.GetContent())
			file.content = append(file.content, frame.Chunk.GetContent()...)
			if n := len(file.chunkRows); n > 0 {
				require.GreaterOrEqual(t, frame.Chunk.GetRows(), file.chunkRows[n-1], "frame %d: chunk rows only grow within a file", i)
			}
			file.chunkRows = append(file.chunkRows, frame.Chunk.GetRows())
			file.chunks++
		case *transferv1.ExportResponse_FileFinished:
			require.NotNil(t, file, "frame %d: file_finished outside a file", i)
			file.finished = frame.FileFinished
			require.Equal(t, uint64(len(file.content)), file.finished.GetBytes())
			require.Equal(t, crc32.ChecksumIEEE(file.content), file.finished.GetCrc32())
			var rows uint64
			for resource, n := range file.finished.GetRowCounts() {
				require.Contains(t, file.finished.GetSchemaModRevisions(), resource)
				rows += n
			}
			if n := len(file.chunkRows); n > 0 {
				require.LessOrEqual(t, file.chunkRows[n-1], rows, "frame %d: chunks count at most the file's rows", i)
			}
			unit.files = append(unit.files, file)
			file = nil
		case *transferv1.ExportResponse_UnitFinished:
			require.NotNil(t, unit, "frame %d: unit_finished outside a unit", i)
			require.Nil(t, file, "frame %d: unit_finished inside a file", i)
			unit.warnings = frame.UnitFinished.GetWarnings()
			units = append(units, unit)
			unit = nil
		default:
			t.Fatalf("frame %d: unexpected %T", i, frame)
		}
	}
	require.Nil(t, unit, "the stream ended inside a unit")
	return units
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
	reader := csv.NewReader(r)
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

// hashTree fingerprints every file under root but the lease, index directories included:
// the export must not change a single byte of the session snapshot.
func hashTree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() == LeaseFileName {
			return err // the lease is renewed by design
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
