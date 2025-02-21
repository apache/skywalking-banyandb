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

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
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
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1}, sortField: []byte{1}},
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1}, sortField: []byte{1}},
			},
			want: []*measurev1.DataPoint{
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1},
			},
		},
		{
			name: "identical data points with different sort fields",
			data: []*comparableDataPoint{
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1}, sortField: []byte{1}},
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1}, sortField: []byte{2}},
			},
			want: []*measurev1.DataPoint{
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1},
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1},
			},
		},
		{
			name: "different data points with different sort fields",
			data: []*comparableDataPoint{
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1}, sortField: []byte{1}},
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 2, Nanos: 2}, Version: 1}, sortField: []byte{2}},
			},
			want: []*measurev1.DataPoint{
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1},
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 2, Nanos: 2}, Version: 1},
			},
		},
		{
			name: "identical data points with different versions",
			data: []*comparableDataPoint{
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1}, sortField: []byte{1}},
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 2, Nanos: 2}, Version: 1}, sortField: []byte{1}},
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 2}, sortField: []byte{1}},
			},
			want: []*measurev1.DataPoint{
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 2},
				{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 2, Nanos: 2}, Version: 1},
			},
		},
		{
			name: "identical data points with different versions",
			data: []*comparableDataPoint{
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1}, sortField: []byte{1}},
				{DataPoint: &measurev1.DataPoint{Sid: 2, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 1}, sortField: []byte{1}},
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}, Version: 2}, sortField: []byte{1}},
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 2, Nanos: 2}, Version: 2}, sortField: []byte{2}},
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 2, Nanos: 2}, Version: 1}, sortField: []byte{2}},
				{DataPoint: &measurev1.DataPoint{Sid: 2, Timestamp: &timestamppb.Timestamp{Seconds: 2, Nanos: 2}, Version: 2}, sortField: []byte{2}},
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 3, Nanos: 3}, Version: 1}, sortField: []byte{3}},
				{DataPoint: &measurev1.DataPoint{Sid: 1, Timestamp: &timestamppb.Timestamp{Seconds: 3, Nanos: 3}, Version: 2}, sortField: []byte{3}},
				{DataPoint: &measurev1.DataPoint{Sid: 3, Timestamp: &timestamppb.Timestamp{Seconds: 3, Nanos: 3}, Version: 2}, sortField: []byte{3}},
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
				got = append(got, iter.Current()[0])
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
