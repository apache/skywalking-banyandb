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

package inverted

import (
	"bytes"
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/inverted/internal/nativeice"
)

func TestNativePluginMergePreservesRepeatedDocValues(t *testing.T) {
	doc := &nidx02bDocument{fields: []nidx02bField{
		nidx02bKeywordField(nidx02bIdentifierField, []byte("id"), true, true, false),
		{name: "dv", value: []byte("first"), docValues: true},
		{name: "dv", value: []byte("second"), docValues: true},
	}}
	built, _, buildErr := nativeSegmentPluginNew([]segmentDocument{doc}, nidx02bNormCalc)
	require.NoError(t, buildErr)
	merger := nativeSegmentPluginMerge([]segmentValue{built}, []*roaringpkg.Bitmap{nil}, 0)
	var buffer bytes.Buffer
	_, writeErr := merger.WriteTo(&buffer, nil)
	require.NoError(t, writeErr)
	loaded, loadErr := nativeSegmentPluginLoad(newSegmentBytes(buffer.Bytes()))
	require.NoError(t, loadErr)
	require.Equal(t, []string{"dv=first", "dv=second"}, nidx02bDocValues(t, loaded, 0, "dv"))
}

func TestNativePluginStatsMergeIsMutableAndAllDeletedMergeIsEmpty(t *testing.T) {
	left := &nativePluginStats{total: 2, documents: 1, frequency: 3}
	right := &nativePluginStats{total: 4, documents: 2, frequency: 5}
	left.Merge(right)
	require.Equal(t, uint64(6), left.TotalDocumentCount())
	require.Equal(t, uint64(3), left.DocumentCount())
	require.Equal(t, uint64(8), left.SumTotalTermFrequency())

	built, _, buildErr := nativeSegmentPluginNew(nidx02bAnalyzedDocuments(), nidx02bNormCalc)
	require.NoError(t, buildErr)
	drops := roaringpkg.New()
	drops.AddRange(0, built.Count())
	merger := nativeSegmentPluginMerge([]segmentValue{built}, []*roaringpkg.Bitmap{drops}, 0)
	var buffer bytes.Buffer
	_, writeErr := merger.WriteTo(&buffer, nil)
	require.NoError(t, writeErr)
	loaded, loadErr := nativeSegmentPluginLoad(newSegmentBytes(buffer.Bytes()))
	require.NoError(t, loadErr)
	require.Equal(t, uint64(0), loaded.Count())
}

func TestNativePluginPreservesEncodedFrequencyAcrossLoadAndMerge(t *testing.T) {
	payload, encodeErr := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{{
		Identifier: []byte("id"),
		Fields:     []nativeice.EncodeField{{Name: "keyword", Index: true, Terms: []nativeice.EncodeTerm{{Value: []byte("term"), Frequency: 3}}}},
	}}})
	require.NoError(t, encodeErr)
	loaded, loadErr := nativeSegmentPluginLoad(newSegmentBytes(payload))
	require.NoError(t, loadErr)
	stats, statsErr := loaded.CollectionStats("keyword")
	require.NoError(t, statsErr)
	require.Equal(t, uint64(3), stats.SumTotalTermFrequency())
	merger := nativeSegmentPluginMerge([]segmentValue{loaded}, []*roaringpkg.Bitmap{nil}, 0)
	var buffer bytes.Buffer
	_, writeErr := merger.WriteTo(&buffer, nil)
	require.NoError(t, writeErr)
	merged, mergedErr := nativeSegmentPluginLoad(newSegmentBytes(buffer.Bytes()))
	require.NoError(t, mergedErr)
	mergedStats, mergedStatsErr := merged.CollectionStats("keyword")
	require.NoError(t, mergedStatsErr)
	require.Equal(t, uint64(3), mergedStats.SumTotalTermFrequency())
}

func TestNativePluginMergeDropsTheCorrectTermFrequency(t *testing.T) {
	payload, encodeErr := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{
		{Identifier: []byte("id-0"), Fields: []nativeice.EncodeField{{Name: "keyword", Index: true, Terms: []nativeice.EncodeTerm{{Value: []byte("term"), Frequency: 3}}}}},
		{Identifier: []byte("id-1"), Fields: []nativeice.EncodeField{{Name: "keyword", Index: true, Terms: []nativeice.EncodeTerm{{Value: []byte("term"), Frequency: 1}}}}},
	}})
	require.NoError(t, encodeErr)
	for _, testCase := range []struct {
		name     string
		drop     uint32
		expected uint64
	}{
		{name: "drop high frequency", drop: 0, expected: 1},
		{name: "drop low frequency", drop: 1, expected: 3},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			loaded, loadErr := nativeSegmentPluginLoad(newSegmentBytes(payload))
			require.NoError(t, loadErr)
			drops := roaringpkg.New()
			drops.Add(testCase.drop)
			merger := nativeSegmentPluginMerge([]segmentValue{loaded}, []*roaringpkg.Bitmap{drops}, 0)
			var buffer bytes.Buffer
			_, writeErr := merger.WriteTo(&buffer, nil)
			require.NoError(t, writeErr)
			merged, mergedErr := nativeSegmentPluginLoad(newSegmentBytes(buffer.Bytes()))
			require.NoError(t, mergedErr)
			stats, statsErr := merged.CollectionStats("keyword")
			require.NoError(t, statsErr)
			require.Equal(t, testCase.expected, stats.SumTotalTermFrequency())
		})
	}
}
