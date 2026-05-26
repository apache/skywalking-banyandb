// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"bytes"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/internal/dump/measure"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// captureMeasureStdout redirects os.Stdout while f runs and returns what was written.
func captureMeasureStdout(t *testing.T, f func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()
	f()
	require.NoError(t, w.Close())
	os.Stdout = old
	return <-done
}

// expectedDataPointCount opens the part with the library to learn how many rows
// the CLI should emit, so the assertions are independent of the fixture details.
func expectedDataPointCount(t *testing.T, shardPath string, fileSystem fs.FileSystem) int {
	t.Helper()
	partIDs, err := dump.DiscoverPartIDs(shardPath)
	require.NoError(t, err)
	require.NotEmpty(t, partIDs)
	total := 0
	for _, id := range partIDs {
		reader, openErr := dumpmeasure.OpenPart(id, shardPath, fileSystem)
		require.NoError(t, openErr)
		total += int(reader.Metadata().TotalCount)
		require.NoError(t, reader.Close())
	}
	return total
}

func newMeasureFixture(t *testing.T) (shardPath, segmentPath string, total int) {
	tmpPath, defFn := test.Space(require.New(t))
	t.Cleanup(defFn)

	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := measure.BuildPartForDump(tmpPath, fileSystem, 12345, measure.StandardDumpRows())
	t.Cleanup(cleanup)

	shardPath = filepath.Dir(partPath)
	segmentPath = tmpPath // no sidx dir; series map load is best-effort
	total = expectedDataPointCount(t, shardPath, fileSystem)
	require.Greater(t, total, 0)
	return shardPath, segmentPath, total
}

// TestDumpMeasureShardText drives the full CLI text flow and verifies every row
// is emitted with correctly typed field values (covers the per-field-type fix).
func TestDumpMeasureShardText(t *testing.T) {
	shardPath, segmentPath, total := newMeasureFixture(t)

	out := captureMeasureStdout(t, func() {
		require.NoError(t, dumpMeasureShard(measureDumpOptions{
			shardPath:   shardPath,
			segmentPath: segmentPath,
		}))
	})

	// Every data point is rendered as a "Row N:" block.
	for i := 1; i <= total; i++ {
		assert.Contains(t, out, "Row "+strconv.Itoa(i)+":", "row %d should be printed", i)
	}
	assert.Contains(t, out, "Total rows: "+strconv.Itoa(total))

	// Mixed field types must render with their own type: the float64 field
	// formats as a float, which is only possible when FieldTypes is per-field
	// (regression guard for the per-field type bugfix).
	assert.Contains(t, out, "floatField: 3.14")
	assert.Contains(t, out, "intField: 1000")
	assert.Contains(t, out, "intField: 2000")
	// Tag values flow through too.
	assert.Contains(t, out, "test-value")
	assert.Contains(t, out, "tag1")
}

// TestDumpMeasureShardCSV drives the full CLI CSV flow and verifies the header
// plus one data record per data point.
func TestDumpMeasureShardCSV(t *testing.T) {
	shardPath, segmentPath, total := newMeasureFixture(t)

	out := captureMeasureStdout(t, func() {
		require.NoError(t, dumpMeasureShard(measureDumpOptions{
			shardPath:   shardPath,
			segmentPath: segmentPath,
			csvOutput:   true,
		}))
	})

	records, err := csv.NewReader(strings.NewReader(out)).ReadAll()
	require.NoError(t, err, "CLI must emit valid CSV")
	require.GreaterOrEqual(t, len(records), 1, "must have a header row")

	header := records[0]
	assert.Equal(t, []string{"PartID", "Timestamp", "Version", "SeriesID", "Series"}, header[:5],
		"fixed CSV header prefix must be preserved")
	// DiscoverColumns scans only the first part's first block (series 1 -> intField).
	// floatField lives in a later series block, so it is not a CSV column (this
	// preserves the historical CLI behavior). Text mode still renders it per-row.
	assert.Contains(t, header, "intField")
	assert.NotContains(t, header, "floatField")

	assert.Equal(t, total, len(records)-1, "one CSV data record per data point")
}

// TestDumpMeasureShardProjectionTags verifies --projection-tags restricts the
// rendered tags to the requested set.
func TestDumpMeasureShardProjectionTags(t *testing.T) {
	shardPath, segmentPath, _ := newMeasureFixture(t)

	out := captureMeasureStdout(t, func() {
		require.NoError(t, dumpMeasureShard(measureDumpOptions{
			shardPath:      shardPath,
			segmentPath:    segmentPath,
			projectionTags: "singleTag.strTag",
		}))
	})

	assert.Contains(t, out, "singleTag.strTag")
	// A tag that exists in the part but is not projected must not appear.
	assert.NotContains(t, out, "singleTag.strTag1")
}

// TestDumpMeasureShardNoParts verifies graceful handling of an empty shard dir.
func TestDumpMeasureShardNoParts(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	out := captureMeasureStdout(t, func() {
		require.NoError(t, dumpMeasureShard(measureDumpOptions{
			shardPath:   tmpPath,
			segmentPath: tmpPath,
		}))
	})
	assert.Contains(t, out, "No parts found in shard directory")
}

// TestDumpMeasureShardSeriesColumn verifies the "Series" column is resolved from
// part-level smeta.bin end-to-end (entity-aligned part so seriesID == hash(entity)).
func TestDumpMeasureShardSeriesColumn(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	entities := []string{"service=a", "service=b", "service=c"}
	partPath, _, cleanup := measure.BuildEntityPartWithSeriesMeta(tmpPath, fileSystem, 0x3039, entities)
	defer cleanup()
	shardPath := filepath.Dir(partPath)

	out := captureMeasureStdout(t, func() {
		require.NoError(t, dumpMeasureShard(measureDumpOptions{
			shardPath:   shardPath,
			segmentPath: tmpPath,
		}))
	})

	for _, e := range entities {
		assert.Contains(t, out, "Series: "+e, "Series column should resolve %q from smeta", e)
	}
}

// TestDumpMeasureShardSeriesColumnNoMeta verifies that without smeta.bin (and no
// sidx segment) the Series line is simply omitted — best-effort, non-fatal.
func TestDumpMeasureShardSeriesColumnNoMeta(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := measure.BuildPartForDump(tmpPath, fileSystem, 0x3039, measure.EntityDumpRows([]string{"service=a"}))
	defer cleanup()
	shardPath := filepath.Dir(partPath)

	out := captureMeasureStdout(t, func() {
		require.NoError(t, dumpMeasureShard(measureDumpOptions{
			shardPath:   shardPath,
			segmentPath: tmpPath,
		}))
	})
	assert.NotContains(t, out, "Series:", "no Series line when neither smeta nor sidx resolves the series")
}

// TestDumpMeasureShardCriteria verifies --criteria filtering end-to-end: with a
// single-tag-per-row part, an EQ filter on meta.name keeps only the matching row.
func TestDumpMeasureShardCriteria(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := measure.BuildEntityPartWithSeriesMeta(tmpPath, fileSystem, 0x3039, []string{"service=a", "service=b", "service=c"})
	defer cleanup()
	shardPath := filepath.Dir(partPath)

	criteria := `{"condition":{"name":"meta.name","op":"BINARY_OP_EQ","value":{"str":{"value":"service=a"}}}}`
	out := captureMeasureStdout(t, func() {
		require.NoError(t, dumpMeasureShard(measureDumpOptions{
			shardPath:    shardPath,
			segmentPath:  tmpPath,
			criteriaJSON: criteria,
		}))
	})

	assert.Contains(t, out, "Total rows: 1", "only the one matching row should pass the filter")
	assert.Contains(t, out, "service=a")
	assert.NotContains(t, out, "service=b", "non-matching rows must be filtered out")
}
