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

package measure

import (
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

type mockIterator struct {
	data []*comparableDataPoint
	idx  int
}

func (m *mockIterator) Next() bool {
	m.idx++
	return m.idx < len(m.data)
}

func (m *mockIterator) Val() *comparableDataPoint {
	return m.data[m.idx]
}

func (m *mockIterator) Close() error {
	return nil
}

func makeComparableDP(sid uint64, seconds, nanos int64, version int64, sortField byte) *comparableDataPoint {
	return &comparableDataPoint{
		InternalDataPoint: &measurev1.InternalDataPoint{
			DataPoint: &measurev1.DataPoint{
				Sid:       sid,
				Timestamp: &timestamppb.Timestamp{Seconds: seconds, Nanos: int32(nanos)},
				Version:   version,
			},
		},
		sortField: []byte{sortField},
	}
}

func TestSortedMIterator(t *testing.T) {
	testCases := []struct {
		name string
		data []*comparableDataPoint
		want []*measurev1.DataPoint
	}{
		{
			name: "empty data",
			data: []*comparableDataPoint{},
			want: []*measurev1.DataPoint{},
		},
		{
			name: "all data points are the same",
			data: []*comparableDataPoint{
				makeComparableDP(1, 1, 1, 1, 1),
				makeComparableDP(1, 1, 1, 1, 1),
			},
			want: []*measurev1.DataPoint{
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1},
			},
		},
		{
			name: "identical data points with different sort fields",
			data: []*comparableDataPoint{
				makeComparableDP(1, 1, 1, 1, 1),
				makeComparableDP(1, 1, 1, 1, 2),
			},
			want: []*measurev1.DataPoint{
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1},
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1},
			},
		},
		{
			name: "different data points with different sort fields",
			data: []*comparableDataPoint{
				makeComparableDP(1, 1, 1, 1, 1),
				makeComparableDP(1, 2, 2, 1, 2),
			},
			want: []*measurev1.DataPoint{
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1},
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 2, Nanos: 2}, Version: 1},
			},
		},
		{
			name: "identical data points with different versions",
			data: []*comparableDataPoint{
				makeComparableDP(1, 1, 1, 1, 1),
				makeComparableDP(1, 2, 2, 1, 1),
				makeComparableDP(1, 1, 1, 2, 1),
			},
			want: []*measurev1.DataPoint{
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 2},
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 2, Nanos: 2}, Version: 1},
			},
		},
		{
			name: "multiple sids with different versions",
			data: []*comparableDataPoint{
				makeComparableDP(1, 1, 1, 1, 1),
				makeComparableDP(2, 1, 1, 1, 1),
				makeComparableDP(1, 1, 1, 2, 1),
				makeComparableDP(1, 2, 2, 2, 2),
				makeComparableDP(1, 2, 2, 1, 2),
				makeComparableDP(2, 2, 2, 2, 2),
				makeComparableDP(1, 3, 3, 1, 3),
				makeComparableDP(1, 3, 3, 2, 3),
				makeComparableDP(3, 3, 3, 2, 3),
			},
			want: []*measurev1.DataPoint{
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 2},
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 2, Nanos: 2}, Version: 2},
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 3, Nanos: 3}, Version: 2},
				{Sid: 2, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1},
				{Sid: 2, Timestamp: &timestamppb.Timestamp{Seconds: 2, Nanos: 2}, Version: 2},
				{Sid: 3, Timestamp: &timestamppb.Timestamp{Seconds: 3, Nanos: 3}, Version: 2},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			iter := &sortedMIterator{
				Iterator: &mockIterator{data: tc.data, idx: -1},
			}

			iter.init()

			got := make([]*measurev1.DataPoint, 0)
			for iter.Next() {
				got = append(got, iter.Current()[0].GetDataPoint())
			}

			slices.SortFunc(got, func(a, b *measurev1.DataPoint) int {
				if diff := a.Sid - b.Sid; diff != 0 {
					return int(diff)
				}
				if diff := a.Timestamp.Seconds - b.Timestamp.Seconds; diff != 0 {
					return int(diff)
				}
				if diff := a.Timestamp.Nanos - b.Timestamp.Nanos; diff != 0 {
					return int(diff)
				}
				return int(a.Version - b.Version)
			})

			if diff := cmp.Diff(tc.want, got,
				protocmp.Transform(), protocmp.IgnoreUnknown()); diff != "" {
				t.Errorf("sortedMIterator mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func makeInternalDP(shardID uint32, tagValue string) *measurev1.InternalDataPoint {
	return &measurev1.InternalDataPoint{
		ShardId: shardID,
		DataPoint: &measurev1.DataPoint{
			TagFamilies: []*modelv1.TagFamily{
				{
					Name: "default",
					Tags: []*modelv1.Tag{
						{Key: "group_tag", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: tagValue}}}},
					},
				},
			},
		},
	}
}

func makeGroupByTagsRefs() [][]*logical.TagRef {
	return [][]*logical.TagRef{
		{
			{
				Tag:  &logical.Tag{},
				Spec: &logical.TagSpec{TagFamilyIdx: 0, TagIdx: 0, Spec: &databasev1.TagSpec{Type: databasev1.TagType_TAG_TYPE_STRING}},
			},
		},
	}
}

func TestDeduplicateAggregatedDataPointsWithShard(t *testing.T) {
	testCases := []struct {
		name            string
		dataPoints      []*measurev1.InternalDataPoint
		groupByTagsRefs [][]*logical.TagRef
		wantShardIDs    []uint32
		wantTagValues   []string
		wantLen         int
		expectErr       bool
	}{
		{
			name:            "empty groupByTagsRefs returns all data points",
			dataPoints:      []*measurev1.InternalDataPoint{makeInternalDP(1, "a"), makeInternalDP(2, "a")},
			groupByTagsRefs: nil,
			wantLen:         2,
			wantShardIDs:    []uint32{1, 2},
			wantTagValues:   []string{"a", "a"},
		},
		{
			name:            "deduplicate replicas of same shard with same group key",
			dataPoints:      []*measurev1.InternalDataPoint{makeInternalDP(1, "a"), makeInternalDP(1, "a")},
			groupByTagsRefs: makeGroupByTagsRefs(),
			wantLen:         1,
			wantShardIDs:    []uint32{1},
			wantTagValues:   []string{"a"},
		},
		{
			name:            "preserve data from different shards with same group key",
			dataPoints:      []*measurev1.InternalDataPoint{makeInternalDP(1, "a"), makeInternalDP(2, "a")},
			groupByTagsRefs: makeGroupByTagsRefs(),
			wantLen:         2,
			wantShardIDs:    []uint32{1, 2},
			wantTagValues:   []string{"a", "a"},
		},
		{
			name: "preserve data from same shard with different group keys",
			dataPoints: []*measurev1.InternalDataPoint{
				makeInternalDP(1, "a"),
				makeInternalDP(1, "b"),
			},
			groupByTagsRefs: makeGroupByTagsRefs(),
			wantLen:         2,
			wantShardIDs:    []uint32{1, 1},
			wantTagValues:   []string{"a", "b"},
		},
		{
			name: "complex scenario: multiple shards and group keys with replicas",
			dataPoints: []*measurev1.InternalDataPoint{
				makeInternalDP(1, "a"),
				makeInternalDP(1, "a"), // replica, should be deduplicated
				makeInternalDP(2, "a"), // different shard, keep
				makeInternalDP(1, "b"), // different group key, keep
				makeInternalDP(2, "b"), // different shard, keep
				makeInternalDP(2, "b"), // replica, should be deduplicated
			},
			groupByTagsRefs: makeGroupByTagsRefs(),
			wantLen:         4,
			wantShardIDs:    []uint32{1, 2, 1, 2},
			wantTagValues:   []string{"a", "a", "b", "b"},
		},
		{
			name:            "empty data points",
			dataPoints:      []*measurev1.InternalDataPoint{},
			groupByTagsRefs: makeGroupByTagsRefs(),
			wantLen:         0,
			wantShardIDs:    []uint32{},
			wantTagValues:   []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := deduplicateAggregatedDataPointsWithShard(tc.dataPoints, tc.groupByTagsRefs)
			if tc.expectErr {
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if len(got) != tc.wantLen {
				t.Errorf("got %d data points, want %d", len(got), tc.wantLen)
				return
			}

			for i, dp := range got {
				if dp.ShardId != tc.wantShardIDs[i] {
					t.Errorf("data point %d: got shard %d, want %d", i, dp.ShardId, tc.wantShardIDs[i])
				}
				tagValue := dp.DataPoint.GetTagFamilies()[0].GetTags()[0].GetValue().GetStr().GetValue()
				if tagValue != tc.wantTagValues[i] {
					t.Errorf("data point %d: got tag value %s, want %s", i, tagValue, tc.wantTagValues[i])
				}
			}
		})
	}
}
