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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// fakeStoredDocument is a stored document with fixed fields.
type fakeStoredDocument map[string][]byte

func (d fakeStoredDocument) VisitStoredFields(visit func(name string, value []byte) bool) error {
	for name, value := range d {
		if !visit(name, value) {
			return nil
		}
	}
	return nil
}

func newPropertyShardWriter(t *testing.T) (*propertyShardWriter, *fileWriter) {
	t.Helper()
	layout, err := csvfmt.PropertyLayout([]*databasev1.Property{{
		Metadata: &commonv1.Metadata{Name: "ui", Group: "g"},
		Tags:     []*databasev1.TagSpec{{Name: "k", Type: databasev1.TagType_TAG_TYPE_STRING}},
	}})
	require.NoError(t, err)
	run, _ := recordingRun(nil)
	fw := run.newWriter(layout, "property/g/shard-0", 0, 0, false, map[string]int64{"ui": 1})
	idx, ok := layout.TagIndex("ui", "", "k")
	require.True(t, ok)
	return &propertyShardWriter{
		run: run, fw: fw, layout: layout, shardDir: "/data/property/g/shard-0", fixed: fixedColumns(layout),
		resources: map[string]map[string]int{"ui": {"k": idx}}, cells: layout.NewRow(),
	}, fw
}

func propertySource(t *testing.T) []byte {
	t.Helper()
	b, err := protojson.Marshal(&propertyv1.Property{Metadata: &commonv1.Metadata{Name: "ui", Group: "g"}, Id: "p1"})
	require.NoError(t, err)
	return b
}

func TestPropertyCSV_ShortDeletedValueIsCorruption(t *testing.T) {
	for _, value := range [][]byte{{1}, {1, 2, 3, 4, 5, 6, 7}, make([]byte, 9), {}} {
		pw, _ := newPropertyShardWriter(t)
		err := pw.write(fakeStoredDocument{propertySourceField: propertySource(t), propertyDeleteField: value})
		require.Equal(t, codes.Internal, status.Code(err), err)
		require.Contains(t, err.Error(), "shard /data/property/g/shard-0")
		require.Contains(t, err.Error(), "property ui/p1")
		require.Equal(t, uint64(0), pw.fw.w.Rows())
	}
}

func TestPropertyCSV_DeleteTimeCells(t *testing.T) {
	pw, fw := newPropertyShardWriter(t)
	require.NoError(t, pw.write(fakeStoredDocument{propertySourceField: propertySource(t)}))
	require.Equal(t, csvfmt.EncodeTimestamp(0), pw.cells[pw.fixed[csvfmt.KindDeleteTime]], "a live property keeps _delete_time 0")
	require.NoError(t, pw.write(fakeStoredDocument{propertySourceField: propertySource(t), propertyDeleteField: convert.Int64ToBytes(42)}))
	require.Equal(t, csvfmt.EncodeTimestamp(42), pw.cells[pw.fixed[csvfmt.KindDeleteTime]])
	require.NoError(t, fw.close())
	require.Equal(t, uint64(2), fw.w.Rows())
}

// Every shard of a session's snapshot has a manifest, an empty shard included, so a shard
// without one lost its snapshot: the unit fails instead of exporting no property.
func TestPropertyCSV_MissingShardManifestFailsTheUnit(t *testing.T) {
	pw, fw := newPropertyShardWriter(t)
	err := writePropertyShard(pw.run, fw, t.TempDir(), pw.layout, pw.fixed, pw.resources)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), err)
	require.Equal(t, uint64(0), fw.w.Rows())
}
