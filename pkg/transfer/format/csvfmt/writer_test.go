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

package csvfmt

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/csv"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

type sinkFile struct {
	rowCounts map[string]uint64
	path      string
	data      []byte
	size      uint64
	crc       uint32
	seq       uint32
	finished  bool
}

type memSink struct {
	failOn   error
	files    []*sinkFile
	maxWrite int
}

func (s *memSink) StartFile(relativePath string, seq uint32) error {
	if len(s.files) > 0 && !s.files[len(s.files)-1].finished {
		return errors.New("previous file not finished")
	}
	s.files = append(s.files, &sinkFile{path: relativePath, seq: seq})
	return nil
}

func (s *memSink) Write(p []byte) error {
	if s.failOn != nil {
		return s.failOn
	}
	if len(p) > s.maxWrite {
		s.maxWrite = len(p)
	}
	f := s.files[len(s.files)-1]
	f.data = append(f.data, p...)
	return nil
}

func (s *memSink) FinishFile(size uint64, crc uint32, rowCounts map[string]uint64) error {
	f := s.files[len(s.files)-1]
	f.size, f.crc, f.rowCounts, f.finished = size, crc, rowCounts, true
	return nil
}

func testLayout(t *testing.T) *Layout {
	l, err := StreamLayout([]*databasev1.Stream{
		{Metadata: md("alpha"), TagFamilies: []*databasev1.TagFamilySpec{
			family("f", tag("msg", databasev1.TagType_TAG_TYPE_STRING), tag("n", databasev1.TagType_TAG_TYPE_INT)),
		}},
		{Metadata: md("beta"), TagFamilies: []*databasev1.TagFamilySpec{
			family("f", tag("msg", databasev1.TagType_TAG_TYPE_STRING), tag("blob", databasev1.TagType_TAG_TYPE_DATA_BINARY)),
		}},
	})
	require.NoError(t, err)
	return l
}

// rowFor returns a row of resource with msg set and the fixed columns filled.
func rowFor(t *testing.T, l *Layout, resource, msg string, i int) []string {
	row := l.NewRow()
	row[1] = EncodeInt(int64(i))
	row[2] = EncodeTimestamp(int64(i) * 1000)
	idx, ok := l.TagIndex(resource, "f", "msg")
	require.True(t, ok)
	row[idx] = EncodeString(msg)
	return row
}

func readCSV(t *testing.T, f *sinkFile, gz bool) [][]string {
	var r io.Reader = bytes.NewReader(f.data)
	if gz {
		zr, err := gzip.NewReader(r)
		require.NoError(t, err)
		r = zr
	}
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	records, err := cr.ReadAll()
	require.NoError(t, err)
	return records
}

func checkIntegrity(t *testing.T, f *sinkFile) {
	require.True(t, f.finished, f.path)
	assert.Equal(t, uint64(len(f.data)), f.size, f.path)
	assert.Equal(t, crc32.ChecksumIEEE(f.data), f.crc, f.path)
}

func TestWriterNoRowsNoFile(t *testing.T) {
	sink := &memSink{}
	w := NewWriter(sink, WriterOptions{Layout: testLayout(t), PathPrefix: "p", MaxFileSize: 10, Gzip: true})
	require.NoError(t, w.Close())
	assert.Empty(t, sink.files)
	assert.Zero(t, w.Rows())
	rows, largest := w.Oversized()
	assert.Zero(t, rows)
	assert.Zero(t, largest)
}

func TestWriterSingleFileNeverRolls(t *testing.T) {
	l := testLayout(t)
	sink := &memSink{}
	w := NewWriter(sink, WriterOptions{Layout: l, PathPrefix: "stream/g/seg-20260907/shard-0"})
	for i := 0; i < 1000; i++ {
		res := "alpha"
		if i%3 == 0 {
			res = "beta"
		}
		require.NoError(t, w.WriteRow(res, rowFor(t, l, res, fmt.Sprintf("message, \"quoted\"\nline %d", i), i)))
	}
	require.NoError(t, w.Close())
	require.Len(t, sink.files, 1)
	f := sink.files[0]
	assert.Equal(t, "stream/g/seg-20260907/shard-0-00001.csv", f.path)
	assert.Equal(t, uint32(1), f.seq)
	checkIntegrity(t, f)
	assert.Equal(t, map[string]uint64{"alpha": 666, "beta": 334}, f.rowCounts)
	assert.Equal(t, uint64(1000), w.Rows())

	records := readCSV(t, f, false)
	require.Len(t, records, 1001)
	assert.Equal(t, l.Header(), records[0])
	for i, rec := range records[1:] {
		require.Len(t, rec, len(l.Columns))
		v, _, err := DecodeCell(rec[3], ValueString)
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("message, \"quoted\"\nline %d", i), v)
	}
	// WriteRow after Close fails.
	require.Error(t, w.WriteRow("alpha", l.NewRow()))
}

func TestWriterRollsPlain(t *testing.T) {
	l := testLayout(t)
	sink := &memSink{}
	const limit = 300
	w := NewWriter(sink, WriterOptions{Layout: l, PathPrefix: "x/shard-1", MaxFileSize: limit})
	const total = 200
	for i := 0; i < total; i++ {
		require.NoError(t, w.WriteRow("alpha", rowFor(t, l, "alpha", strings.Repeat("v", i%17), i)))
	}
	require.NoError(t, w.Close())
	require.Greater(t, len(sink.files), 5)

	var rows uint64
	for i, f := range sink.files {
		assert.Equal(t, uint32(i+1), f.seq)
		assert.Equal(t, fmt.Sprintf("x/shard-1-%05d.csv", i+1), f.path)
		checkIntegrity(t, f)
		if i < len(sink.files)-1 {
			assert.GreaterOrEqual(t, f.size, uint64(limit), "non-final files reach the threshold")
		}
		records := readCSV(t, f, false)
		require.Equal(t, l.Header(), records[0], "header repeated in file %d", i+1)
		for _, rec := range records[1:] {
			require.Len(t, rec, len(l.Columns), "no split row")
			assert.Equal(t, "alpha", rec[0])
		}
		assert.Equal(t, uint64(len(records)-1), f.rowCounts["alpha"])
		rows += f.rowCounts["alpha"]
		// Removing the last row would bring the file under the threshold: rows are atomic and the roll happens right after.
		assert.True(t, bytes.HasSuffix(f.data, []byte("\n")))
	}
	assert.Equal(t, uint64(total), rows)
}

func TestWriterOversizedRowAlone(t *testing.T) {
	l := testLayout(t)
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprintf("gzip=%v", gz), func(t *testing.T) {
			sink := &memSink{}
			const limit = 4096
			w := NewWriter(sink, WriterOptions{Layout: l, PathPrefix: "p", MaxFileSize: limit, Gzip: gz})
			require.NoError(t, w.WriteRow("alpha", rowFor(t, l, "alpha", "small", 1)))
			big := l.NewRow()
			idx, _ := l.TagIndex("beta", "f", "blob")
			payload := make([]byte, 3*limit)
			_, err := rand.Read(payload)
			require.NoError(t, err)
			big[idx] = EncodeBinary(payload)
			require.NoError(t, w.WriteRow("beta", big))
			require.NoError(t, w.WriteRow("alpha", rowFor(t, l, "alpha", "after", 3)))
			require.NoError(t, w.Close())

			require.Len(t, sink.files, 3)
			rows, largest := w.Oversized()
			assert.Equal(t, uint64(1), rows, "only the big row is oversized")
			assert.Greater(t, largest, uint64(limit))
			assert.Equal(t, map[string]uint64{"alpha": 1}, sink.files[0].rowCounts)
			assert.Equal(t, map[string]uint64{"beta": 1}, sink.files[1].rowCounts)
			assert.Equal(t, map[string]uint64{"alpha": 1}, sink.files[2].rowCounts)
			for _, f := range sink.files {
				checkIntegrity(t, f)
				records := readCSV(t, f, gz)
				assert.Equal(t, l.Header(), records[0])
				require.Len(t, records, 2)
			}
			records := readCSV(t, sink.files[1], gz)
			v, _, err := DecodeCell(records[1][idx], ValueBinary)
			require.NoError(t, err)
			assert.Equal(t, payload, v)
		})
	}
}

func TestWriterGzipRolling(t *testing.T) {
	l := testLayout(t)
	sink := &memSink{}
	const limit = 64 << 10
	w := NewWriter(sink, WriterOptions{Layout: l, PathPrefix: "trace/g/seg-1/shard-0", MaxFileSize: limit, Gzip: true, Annotations: true})
	idx, _ := l.TagIndex("beta", "f", "blob")
	const total = 400
	for i := 0; i < total; i++ {
		row := rowFor(t, l, "beta", fmt.Sprintf("row %d", i), i)
		payload := make([]byte, 1024)
		_, err := rand.Read(payload)
		require.NoError(t, err)
		row[idx] = EncodeBinary(payload)
		require.NoError(t, w.WriteRow("beta", row))
	}
	require.NoError(t, w.Close())
	require.Greater(t, len(sink.files), 3)
	var rows int
	for i, f := range sink.files {
		assert.Equal(t, fmt.Sprintf("trace/g/seg-1/shard-0-%05d.csv.gz", i+1), f.path)
		checkIntegrity(t, f)
		records := readCSV(t, f, true)
		assert.Equal(t, l.DatatypeLine(), records[0])
		assert.Equal(t, l.Header(), records[1])
		for _, rec := range records[2:] {
			require.Len(t, rec, len(l.Columns))
		}
		rows += len(records) - 2
		assert.Equal(t, uint64(len(records)-2), f.rowCounts["beta"])
	}
	assert.Equal(t, total, rows)
	assert.LessOrEqual(t, sink.maxWrite, SinkWriteSize)
}

// Flush hands the sink every row written so far, the rows the compressor held included, and
// the file stays one valid gzip stream.
func TestWriterFlushHandsOverTheRowsWritten(t *testing.T) {
	l := testLayout(t)
	sink := &memSink{}
	w := NewWriter(sink, WriterOptions{Layout: l, PathPrefix: "stream/g/seg-1/shard-0", Gzip: true})
	require.NoError(t, w.Flush(), "nothing to flush before the first row")
	require.Empty(t, sink.files)
	for i := 0; i < 3; i++ {
		require.NoError(t, w.WriteRow("alpha", rowFor(t, l, "alpha", fmt.Sprintf("row %d", i), i)))
	}
	require.NoError(t, w.Flush())
	zr, err := gzip.NewReader(bytes.NewReader(sink.files[0].data))
	require.NoError(t, err)
	partial, err := io.ReadAll(zr)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF, "the gzip stream is not closed yet")
	for i := 0; i < 3; i++ {
		require.Contains(t, string(partial), fmt.Sprintf("row %d", i))
	}
	require.NoError(t, w.WriteRow("alpha", rowFor(t, l, "alpha", "row 3", 3)))
	require.NoError(t, w.Close())
	checkIntegrity(t, sink.files[0])
	require.Len(t, readCSV(t, sink.files[0], true), 5)
	require.NoError(t, w.Flush(), "nothing to flush after the last file")
}

func TestWriterSinkWriteBounded(t *testing.T) {
	l := testLayout(t)
	sink := &memSink{}
	w := NewWriter(sink, WriterOptions{Layout: l, PathPrefix: "p"})
	row := rowFor(t, l, "alpha", strings.Repeat("x", 3*SinkWriteSize+5), 1)
	require.NoError(t, w.WriteRow("alpha", row))
	require.NoError(t, w.Close())
	require.Len(t, sink.files, 1)
	checkIntegrity(t, sink.files[0])
	assert.Equal(t, SinkWriteSize, sink.maxWrite)
}

func TestWriterAnnotationsPlain(t *testing.T) {
	l := testLayout(t)
	sink := &memSink{}
	w := NewWriter(sink, WriterOptions{Layout: l, PathPrefix: "p", Annotations: true})
	require.NoError(t, w.WriteRow("alpha", rowFor(t, l, "alpha", "a", 1)))
	require.NoError(t, w.Close())
	require.Len(t, sink.files, 1)
	lines := strings.SplitN(string(sink.files[0].data), "\n", 3)
	assert.Equal(t, "#datatype,string,string,timestamp,string,long,base64Binary", lines[0])
	assert.Equal(t, "_name,_element_id,_timestamp,f.msg,f.n,f.blob", lines[1])
}

func TestWriterRejectsBadRows(t *testing.T) {
	l := testLayout(t)
	w := NewWriter(&memSink{}, WriterOptions{Layout: l, PathPrefix: "p"})
	require.Error(t, w.WriteRow("alpha", []string{"x"}))
	require.Error(t, w.WriteRow("gamma", l.NewRow()))
	require.NoError(t, w.Close())
}

func TestWriterSinkErrorSticks(t *testing.T) {
	l := testLayout(t)
	boom := errors.New("boom")
	sink := &memSink{failOn: boom}
	w := NewWriter(sink, WriterOptions{Layout: l, PathPrefix: "p"})
	require.NoError(t, w.WriteRow("alpha", rowFor(t, l, "alpha", "a", 1)), "bytes are buffered until the file is finished")
	err := w.Close()
	require.ErrorIs(t, err, boom)
	require.ErrorIs(t, w.WriteRow("alpha", l.NewRow()), errWriterClosed)
}
