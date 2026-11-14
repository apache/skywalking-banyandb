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

package nativefmt

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

func TestSyncFileName(t *testing.T) {
	const (
		stream   = commonv1.Catalog_CATALOG_STREAM
		measure  = commonv1.Catalog_CATALOG_MEASURE
		trace    = commonv1.Catalog_CATALOG_TRACE
		property = commonv1.Catalog_CATALOG_PROPERTY
	)
	cases := []struct {
		name     string
		partType string
		disk     string
		want     string
		catalog  commonv1.Catalog
		kind     Kind
		skip     bool
		wantErr  bool
	}{
		{name: "stream primary", catalog: stream, kind: KindPart, disk: "primary.bin", want: "primary"},
		{name: "stream meta", catalog: stream, kind: KindPart, disk: "meta.bin", want: "meta"},
		{name: "stream timestamps", catalog: stream, kind: KindPart, disk: "timestamps.bin", want: "timestamps"},
		{name: "stream tag type", catalog: stream, kind: KindPart, disk: "tag.type", want: "tag.type"},
		{name: "stream tag family", catalog: stream, kind: KindPart, disk: "default.tf", want: "tf:default"},
		{name: "stream tag metadata", catalog: stream, kind: KindPart, disk: "default.tfm", want: "tfm:default"},
		{name: "stream tag filter", catalog: stream, kind: KindPart, disk: "default.tff", want: "tff:default"},
		{name: "stream metadata skipped", catalog: stream, kind: KindPart, disk: "metadata.json", skip: true},
		{name: "stream series metadata skipped", catalog: stream, kind: KindPart, disk: "smeta.bin", skip: true},
		{name: "stream field values unknown", catalog: stream, kind: KindPart, disk: "fv.bin", wantErr: true},
		{name: "stream bare extension unknown", catalog: stream, kind: KindPart, disk: ".tf", wantErr: true},
		{name: "measure field values", catalog: measure, kind: KindPart, disk: "fv.bin", want: "fv"},
		{name: "measure tag family", catalog: measure, kind: KindPart, disk: "default.tf", want: "tf:default"},
		{name: "measure tag metadata", catalog: measure, kind: KindPart, disk: "default.tfm", want: "tfm:default"},
		{name: "measure has no tag filter", catalog: measure, kind: KindPart, disk: "default.tff", wantErr: true},
		{name: "measure secondary part refused", catalog: measure, kind: KindPart, partType: "idx", disk: "primary.bin", wantErr: true},
		{name: "trace spans", catalog: trace, kind: KindPart, disk: "spans.bin", want: "spans"},
		{name: "trace id filter", catalog: trace, kind: KindPart, disk: "traceID.filter", want: "traceID.filter"},
		{name: "trace tags", catalog: trace, kind: KindPart, disk: "service.t", want: "t:service"},
		{name: "trace tag metadata", catalog: trace, kind: KindPart, disk: "service.tm", want: "tm:service"},
		{name: "trace tag family unknown", catalog: trace, kind: KindPart, disk: "service.tf", wantErr: true},
		{name: "sidx primary", catalog: trace, kind: KindPart, partType: "dur", disk: "primary.bin", want: "primary"},
		{name: "sidx data", catalog: trace, kind: KindPart, partType: "dur", disk: "data.bin", want: "data"},
		{name: "sidx keys", catalog: trace, kind: KindPart, partType: "dur", disk: "keys.bin", want: "keys"},
		{name: "sidx meta", catalog: trace, kind: KindPart, partType: "dur", disk: "meta.bin", want: "meta"},
		{name: "sidx tag data", catalog: trace, kind: KindPart, partType: "dur", disk: "svc.td", want: "td:svc"},
		{name: "sidx tag metadata", catalog: trace, kind: KindPart, partType: "dur", disk: "svc.tm", want: "tm:svc"},
		{name: "sidx tag filter", catalog: trace, kind: KindPart, partType: "dur", disk: "svc.tf", want: "tf:svc"},
		{name: "sidx manifest skipped", catalog: trace, kind: KindPart, partType: "dur", disk: "manifest.json", skip: true},
		{name: "sidx spans unknown", catalog: trace, kind: KindPart, partType: "dur", disk: "spans.bin", wantErr: true},
		{name: "property has no parts", catalog: property, kind: KindPart, disk: "primary.bin", wantErr: true},
		{name: "series segment", catalog: measure, kind: KindSeriesIndex, disk: "000000000012.seg", want: "000000000012.seg"},
		{name: "series manifest skipped", catalog: measure, kind: KindSeriesIndex, disk: "000000000012.snp", skip: true},
		{name: "series unknown", catalog: measure, kind: KindSeriesIndex, disk: "lock", wantErr: true},
		{name: "element segment", catalog: stream, kind: KindElementIndex, disk: "000000000001.seg", want: "000000000001.seg"},
		{name: "element manifest skipped", catalog: stream, kind: KindElementIndex, disk: "000000000001.snp", skip: true},
		{name: "property passthrough", catalog: property, kind: KindProperty, disk: "000000000003.snp", want: "000000000003.snp"},
		{name: "property any file", catalog: property, kind: KindProperty, disk: "whatever", want: "whatever"},
		{name: "nested path refused", catalog: property, kind: KindProperty, disk: "a/b", wantErr: true},
		{name: "dot dot refused", catalog: property, kind: KindProperty, disk: "..", wantErr: true},
		{name: "empty refused", catalog: stream, kind: KindPart, disk: "", wantErr: true},
		{name: "unspecified kind", catalog: stream, kind: transferv1.ImportPartsRequest_KIND_UNSPECIFIED, disk: "primary.bin", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, skip, err := SyncFileName(c.catalog, c.kind, c.partType, c.disk)
			if c.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.skip, skip)
			assert.Equal(t, c.want, got)
		})
	}
}

func TestParsePartInfo(t *testing.T) {
	t.Run("stream core part", func(t *testing.T) {
		meta := `{"compressedSizeBytes":10,"uncompressedSizeBytes":20,"totalCount":3,"blocksCount":1,"minTimestamp":100,"maxTimestamp":200}`
		info, err := ParsePartInfo(7, "", []byte(meta), nil)
		require.NoError(t, err)
		assert.Equal(t, uint64(7), info.GetId())
		assert.Empty(t, info.GetPartType())
		assert.Equal(t, uint64(10), info.GetCompressedSizeBytes())
		assert.Equal(t, uint64(20), info.GetUncompressedSizeBytes())
		assert.Equal(t, uint64(3), info.GetTotalCount())
		assert.Equal(t, uint64(1), info.GetBlocksCount())
		assert.Equal(t, int64(100), info.GetMinTimestamp())
		assert.Equal(t, int64(200), info.GetMaxTimestamp())
	})
	t.Run("trace core part span size", func(t *testing.T) {
		meta := `{"compressedSizeBytes":10,"uncompressedSpanSizeBytes":40,"totalCount":2,"blocksCount":1,"minTimestamp":5,"maxTimestamp":9}`
		info, err := ParsePartInfo(1, "", []byte(meta), nil)
		require.NoError(t, err)
		assert.Equal(t, uint64(40), info.GetUncompressedSizeBytes())
	})
	t.Run("sidx falls back to core time range", func(t *testing.T) {
		core := &transferv1.ImportPartsRequest_PartInfo{MinTimestamp: 11, MaxTimestamp: 22}
		meta := `{"compressedSizeBytes":1,"uncompressedSizeBytes":2,"totalCount":3,"blocksCount":1,"minKey":-4,"maxKey":8,"id":1,"segmentID":5}`
		info, err := ParsePartInfo(1, "dur", []byte(meta), core)
		require.NoError(t, err)
		assert.Equal(t, "dur", info.GetPartType())
		assert.Equal(t, int64(11), info.GetMinTimestamp())
		assert.Equal(t, int64(22), info.GetMaxTimestamp())
		assert.Equal(t, int64(-4), info.GetMinKey())
		assert.Equal(t, int64(8), info.GetMaxKey())
	})
	t.Run("sidx keeps its own time range", func(t *testing.T) {
		core := &transferv1.ImportPartsRequest_PartInfo{MinTimestamp: 11, MaxTimestamp: 22}
		info, err := ParsePartInfo(1, "dur", []byte(`{"minTimestamp":12,"maxTimestamp":13}`), core)
		require.NoError(t, err)
		assert.Equal(t, int64(12), info.GetMinTimestamp())
		assert.Equal(t, int64(13), info.GetMaxTimestamp())
	})
	t.Run("core without min timestamp", func(t *testing.T) {
		_, err := ParsePartInfo(1, "", []byte(`{"totalCount":1}`), nil)
		require.Error(t, err)
	})
	t.Run("invalid json", func(t *testing.T) {
		_, err := ParsePartInfo(1, "", []byte(`{`), nil)
		require.Error(t, err)
	})
}

func TestSupports(t *testing.T) {
	require.NoError(t, Supports(commonv1.Catalog_CATALOG_STREAM, KindElementIndex))
	require.NoError(t, Supports(commonv1.Catalog_CATALOG_PROPERTY, KindProperty))
	assert.ErrorContains(t, Supports(commonv1.Catalog_CATALOG_MEASURE, KindElementIndex), "has no")
	assert.ErrorContains(t, Supports(commonv1.Catalog_CATALOG_UNSPECIFIED, KindPart), "no native payloads")
	_, _, err := SyncFileName(commonv1.Catalog_CATALOG_PROPERTY, KindSeriesIndex, "", "1.seg")
	assert.Error(t, err)
	_, _, err = SyncFileName(commonv1.Catalog_CATALOG_MEASURE, KindElementIndex, "", "1.seg")
	assert.Error(t, err)
	assert.True(t, Duplicable(commonv1.Catalog_CATALOG_TRACE, KindPart))
	assert.False(t, Duplicable(commonv1.Catalog_CATALOG_MEASURE, KindPart))
	assert.True(t, HasSecondaryParts(commonv1.Catalog_CATALOG_TRACE))
	assert.False(t, HasSecondaryParts(commonv1.Catalog_CATALOG_STREAM))
}

func TestTopic(t *testing.T) {
	got, err := Topic(commonv1.Catalog_CATALOG_STREAM, KindElementIndex)
	require.NoError(t, err)
	assert.Equal(t, data.TopicStreamElementIndexSync.String(), got)
	got, err = Topic(commonv1.Catalog_CATALOG_PROPERTY, KindProperty)
	require.NoError(t, err)
	assert.Equal(t, data.TopicPropertyPartSync.String(), got)
	_, err = Topic(commonv1.Catalog_CATALOG_MEASURE, KindElementIndex)
	assert.ErrorContains(t, err, "has no")
	_, err = Topic(commonv1.Catalog_CATALOG_UNSPECIFIED, KindPart)
	assert.ErrorContains(t, err, "no native payloads")
	assert.True(t, Segmented(commonv1.Catalog_CATALOG_TRACE))
	assert.False(t, Segmented(commonv1.Catalog_CATALOG_PROPERTY))
	assert.Equal(t, "shard-12", ShardDir(12))
}
