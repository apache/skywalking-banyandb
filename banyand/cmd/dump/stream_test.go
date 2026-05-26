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
	"encoding/csv"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/dump"
	dumpstream "github.com/apache/skywalking-banyandb/banyand/dump/stream"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func newStreamFixture(t *testing.T) (shardPath, segmentPath string, total int) {
	tmpPath, defFn := test.Space(require.New(t))
	t.Cleanup(defFn)

	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := stream.BuildPartForDump(tmpPath, fileSystem, 12345, stream.StandardDumpRows())
	t.Cleanup(cleanup)

	shardPath = filepath.Dir(partPath)
	segmentPath = tmpPath

	partIDs, err := dump.DiscoverPartIDs(shardPath)
	require.NoError(t, err)
	require.NotEmpty(t, partIDs)
	for _, id := range partIDs {
		reader, openErr := dumpstream.OpenPart(id, shardPath, fileSystem)
		require.NoError(t, openErr)
		total += int(reader.Metadata().TotalCount)
		require.NoError(t, reader.Close())
	}
	require.Greater(t, total, 0)
	return shardPath, segmentPath, total
}

// TestDumpStreamShardText drives the full CLI text flow.
func TestDumpStreamShardText(t *testing.T) {
	shardPath, segmentPath, total := newStreamFixture(t)

	out := captureMeasureStdout(t, func() {
		require.NoError(t, dumpStreamShard(streamDumpOptions{
			shardPath:   shardPath,
			segmentPath: segmentPath,
		}))
	})

	for i := 1; i <= total; i++ {
		assert.Contains(t, out, "Row "+strconv.Itoa(i)+":", "row %d should be printed", i)
	}
	assert.Contains(t, out, "Total rows: "+strconv.Itoa(total))
	assert.Contains(t, out, "ElementID:")
	assert.Contains(t, out, "Element Data: 0 bytes")
	assert.Contains(t, out, "test-value")
}

// TestDumpStreamShardCSV drives the full CLI CSV flow.
func TestDumpStreamShardCSV(t *testing.T) {
	shardPath, segmentPath, total := newStreamFixture(t)

	out := captureMeasureStdout(t, func() {
		require.NoError(t, dumpStreamShard(streamDumpOptions{
			shardPath:   shardPath,
			segmentPath: segmentPath,
			csvOutput:   true,
		}))
	})

	records, err := csv.NewReader(strings.NewReader(out)).ReadAll()
	require.NoError(t, err, "CLI must emit valid CSV")
	require.GreaterOrEqual(t, len(records), 1)
	assert.Equal(t, []string{"PartID", "ElementID", "Timestamp", "SeriesID", "Series", "ElementDataSize"}, records[0][:6],
		"fixed CSV header prefix must be preserved")
	assert.Equal(t, total, len(records)-1, "one CSV data record per element")
}

// TestDumpStreamShardSeriesColumn verifies the "Series" column resolves from smeta.
func TestDumpStreamShardSeriesColumn(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	entities := []string{"service=a", "service=b", "service=c"}
	partPath, _, cleanup := stream.BuildEntityPartWithSeriesMeta(tmpPath, fileSystem, 0x3039, entities)
	defer cleanup()
	shardPath := filepath.Dir(partPath)

	out := captureMeasureStdout(t, func() {
		require.NoError(t, dumpStreamShard(streamDumpOptions{
			shardPath:   shardPath,
			segmentPath: tmpPath,
		}))
	})
	for _, e := range entities {
		assert.Contains(t, out, "Series: "+e)
	}
}

// TestDumpStreamShardCriteria verifies --criteria filtering on a single-tag-per-row part.
func TestDumpStreamShardCriteria(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := stream.BuildEntityPartWithSeriesMeta(tmpPath, fileSystem, 0x3039, []string{"service=a", "service=b", "service=c"})
	defer cleanup()
	shardPath := filepath.Dir(partPath)

	criteria := `{"condition":{"name":"meta.name","op":"BINARY_OP_EQ","value":{"str":{"value":"service=a"}}}}`
	out := captureMeasureStdout(t, func() {
		require.NoError(t, dumpStreamShard(streamDumpOptions{
			shardPath:    shardPath,
			segmentPath:  tmpPath,
			criteriaJSON: criteria,
		}))
	})

	assert.Contains(t, out, "Total rows: 1", "only the one matching element should pass the filter")
	assert.Contains(t, out, "service=a")
	assert.NotContains(t, out, "service=b")
}

// TestDumpStreamShardNoParts verifies graceful handling of an empty shard dir.
func TestDumpStreamShardNoParts(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	out := captureMeasureStdout(t, func() {
		require.NoError(t, dumpStreamShard(streamDumpOptions{
			shardPath:   tmpPath,
			segmentPath: tmpPath,
		}))
	})
	assert.Contains(t, out, "No parts found in shard directory")
}
