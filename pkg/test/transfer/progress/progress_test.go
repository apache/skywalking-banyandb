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

package progress

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
)

func TestRead_AMissingProgressFileIsNil(t *testing.T) {
	path := filepath.Join(t.TempDir(), "none.json")
	p, err := ReadImport(path)
	require.NoError(t, err)
	assert.Nil(t, p)
	e, err := ReadExport(path)
	require.NoError(t, err)
	assert.Nil(t, e)
}

func TestStreamTraceRowsOnce_CSVCountsTheShardFilesNotTheParts(t *testing.T) {
	unit := manifest.Unit{Catalog: "STREAM", Group: "g", Stage: "1d", Segment: "s", Sources: []manifest.UnitSource{{
		Node: "n1",
		Shards: []manifest.Shard{{
			Shard: 0, TotalCount: 7,
			Parts: []manifest.Part{{ID: 1, TotalCount: 5}, {ID: 2, TotalCount: 6}},
		}},
	}}}
	p := &Import{ProgressView: importer.ProgressView{Plan: []importer.PlannedItemView{{Key: "stream/g/1d/s/shard-0", Sources: []string{"n1"}}}}}
	csv := &manifest.Manifest{Format: manifest.FormatCSV, Units: []manifest.Unit{unit}}
	assert.Equal(t, uint64(7), p.StreamTraceRowsOnce(csv), "a CSV shard pushes the rows of its files")
	native := &manifest.Manifest{Format: manifest.FormatNative, Units: []manifest.Unit{unit}}
	assert.Equal(t, uint64(11), p.StreamTraceRowsOnce(native), "a native shard pushes its parts")
}

func TestStreamTraceBoundViolations_ArePerGroup(t *testing.T) {
	unit := func(group string, rows uint64) manifest.Unit {
		return manifest.Unit{Catalog: "STREAM", Group: group, Stage: "1d", Segment: "s", Sources: []manifest.UnitSource{{
			Node: "n1", Shards: []manifest.Shard{{Shard: 0, TotalCount: rows, Parts: []manifest.Part{{ID: 1, TotalCount: rows}}}},
		}}}
	}
	m := &manifest.Manifest{Format: manifest.FormatNative, Units: []manifest.Unit{unit("a", 10), unit("b", 10)}}
	p := &Import{ProgressView: importer.ProgressView{
		Plan: []importer.PlannedItemView{{Key: "stream/a/1d/s/shard-0", Sources: []string{"n1"}}, {Key: "stream/b/1d/s/shard-0", Sources: []string{"n1"}}},
		// a lost 3 rows that b's duplicates would hide in the sums.
		Pushed:     []importer.RowCountView{{Catalog: "stream", Group: "a", Stage: "1d", Rows: 7}, {Catalog: "stream", Group: "b", Stage: "1d", Rows: 13}},
		Duplicates: []importer.RowCountView{{Catalog: "stream", Group: "b", Stage: "1d", Rows: 5}},
	}}
	assert.Equal(t, uint64(20), p.StreamTraceRowsOnce(m))
	assert.Equal(t, []string{"stream/a: pushed 7 < one import 10"}, p.StreamTraceBoundViolations(m))

	p.Pushed[0].Rows = 10
	assert.Empty(t, p.StreamTraceBoundViolations(m))
	m.Format = manifest.FormatCSV
	assert.Equal(t, []string{"stream/b: pushed 13 != one import 10 of a CSV artifact (5 possible duplicates)"}, p.StreamTraceBoundViolations(m))
}

func TestStreamTraceBoundViolations_IgnoreTheTargetStage(t *testing.T) {
	// A standalone target, or one without the source stage, stores the warm rows in hot: the
	// plan keys carry the source stage and the pushed counts the target stage.
	m := &manifest.Manifest{Format: manifest.FormatCSV, Units: []manifest.Unit{{
		Catalog: "STREAM", Group: "a", Stage: "warm", Segment: "s", Sources: []manifest.UnitSource{{
			Node: "n1", Shards: []manifest.Shard{{Shard: 0, TotalCount: 10}},
		}},
	}}}
	p := &Import{ProgressView: importer.ProgressView{
		Plan:   []importer.PlannedItemView{{Key: "stream/a/warm/s/shard-0", Sources: []string{"n1"}}},
		Pushed: []importer.RowCountView{{Catalog: "stream", Group: "a", Stage: "hot", Rows: 10}},
	}}
	assert.Empty(t, p.StreamTraceBoundViolations(m))
}

func TestReadUnlessGone_AFileRemovedWhileReadIsNil(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.json")
	require.NoError(t, os.WriteFile(path, []byte("{}"), 0o600))
	// The run finishing removes the progress file, then its side files: a read between the two
	// fails on the side file.
	finishing := func(p string) (*int, error) {
		require.NoError(t, os.Remove(p))
		return nil, errors.New("plan side file: no such file")
	}
	v, err := readUnlessGone(path, finishing)
	require.NoError(t, err)
	assert.Nil(t, v)

	require.NoError(t, os.WriteFile(path, []byte("{}"), 0o600))
	broken := func(string) (*int, error) { return nil, errors.New("unusable") }
	_, err = readUnlessGone(path, broken)
	require.Error(t, err, "a progress file that is still there and unusable is an error")
}
