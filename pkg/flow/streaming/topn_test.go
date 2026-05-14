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

package streaming

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type testCaseFloat64 struct {
	expected map[string][]*Tuple2[float64]
	name     string
	sort     TopNSort
}

func TestFlow_TopN_Aggregator_Float64(t *testing.T) {
	verifyFn := func(t *testing.T, input []flow.StreamRecord, tests []testCaseFloat64) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				require := require.New(t)
				config := &topNConfig{
					cacheSize: 3,
					sort:      tt.sort,
					l:         logger.GetLogger("test"),
					keyExtractor: func(record flow.StreamRecord) uint64 {
						return uint64(record.Data().(flow.Data)[0].(int))
					},
					groupKeyExtractor: func(record flow.StreamRecord) string {
						return record.Data().(flow.Data)[1].(string)
					},
				}
				sortKeyExtractor := func(record flow.StreamRecord) float64 {
					return float64(record.Data().(flow.Data)[2].(float64))
				}
				topN := newTopNAggregatorGroup(config, sortKeyExtractor)
				topN.Add(input)
				topN.leakCheck()
				snapshot := topN.Snapshot()
				require.Len(snapshot, 2)
				require.Contains(snapshot, "e2e-service-provider")
				require.Contains(snapshot, "e2e-service-consumer")
				if diff := cmp.Diff(tt.expected, snapshot); diff != "" {
					t.Errorf("Snapshot() mismatch (-want +got):\n%s", diff)
				}
			})
		}
	}

	t.Run("DESC", func(t *testing.T) {
		verifyFn(t,
			[]flow.StreamRecord{
				flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 100.5, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{2, "e2e-service-consumer", 99.3, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 98.7, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 97.1, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 97.0, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{6, "e2e-service-consumer", 96.2, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{7, "e2e-service-consumer", 98.0, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{8, "e2e-service-consumer", 95.5, []interface{}{"e2e-service-consumer"}}),
			},
			[]testCaseFloat64{
				{
					name: "DESC",
					sort: DESC,
					expected: map[string][]*Tuple2[float64]{
						"e2e-service-provider": {
							{V1: 100.5, V2: flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 100.5, []interface{}{"e2e-service-provider"}})},
							{V1: 98.7, V2: flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 98.7, []interface{}{"e2e-service-provider"}})},
							{V1: 97.0, V2: flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 97.0, []interface{}{"e2e-service-provider"}})},
						},
						"e2e-service-consumer": {
							{V1: 99.3, V2: flow.NewStreamRecordWithoutTS(flow.Data{2, "e2e-service-consumer", 99.3, []interface{}{"e2e-service-consumer"}})},
							{V1: 98.0, V2: flow.NewStreamRecordWithoutTS(flow.Data{7, "e2e-service-consumer", 98.0, []interface{}{"e2e-service-consumer"}})},
							{V1: 97.1, V2: flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 97.1, []interface{}{"e2e-service-consumer"}})},
						},
					},
				},
				{
					name: "ASC",
					sort: ASC,
					expected: map[string][]*Tuple2[float64]{
						"e2e-service-consumer": {
							{V1: 95.5, V2: flow.NewStreamRecordWithoutTS(flow.Data{8, "e2e-service-consumer", 95.5, []interface{}{"e2e-service-consumer"}})},
							{V1: 96.2, V2: flow.NewStreamRecordWithoutTS(flow.Data{6, "e2e-service-consumer", 96.2, []interface{}{"e2e-service-consumer"}})},
							{V1: 97.1, V2: flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 97.1, []interface{}{"e2e-service-consumer"}})},
						},
						"e2e-service-provider": {
							{V1: 97.0, V2: flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 97.0, []interface{}{"e2e-service-provider"}})},
							{V1: 98.7, V2: flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 98.7, []interface{}{"e2e-service-provider"}})},
							{V1: 100.5, V2: flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 100.5, []interface{}{"e2e-service-provider"}})},
						},
					},
				},
			},
		)
	})

	t.Run("update existing key", func(t *testing.T) {
		verifyFn(t,
			[]flow.StreamRecord{
				flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 100.5, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{2, "e2e-service-consumer", 99.3, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 98.7, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 101.2, []interface{}{"e2e-service-provider"}}),
			},
			[]testCaseFloat64{
				{
					name: "DESC",
					sort: DESC,
					expected: map[string][]*Tuple2[float64]{
						"e2e-service-provider": {
							{V1: 101.2, V2: flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 101.2, []interface{}{"e2e-service-provider"}})},
							{V1: 98.7, V2: flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 98.7, []interface{}{"e2e-service-provider"}})},
						},
						"e2e-service-consumer": {
							{V1: 99.3, V2: flow.NewStreamRecordWithoutTS(flow.Data{2, "e2e-service-consumer", 99.3, []interface{}{"e2e-service-consumer"}})},
						},
					},
				},
			},
		)
	})
}

type testCase struct {
	expected map[string][]*Tuple2[int64]
	name     string
	sort     TopNSort
}

func TestFlow_TopN_Aggregator(t *testing.T) {
	verifyFn := func(t *testing.T, input []flow.StreamRecord, tests []testCase) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				require := require.New(t)
				config := &topNConfig{
					cacheSize: 3,
					sort:      tt.sort,
					l:         logger.GetLogger("test"),
					keyExtractor: func(record flow.StreamRecord) uint64 {
						return uint64(record.Data().(flow.Data)[0].(int))
					},
					groupKeyExtractor: func(record flow.StreamRecord) string {
						return record.Data().(flow.Data)[1].(string)
					},
				}
				sortKeyExtractor := func(record flow.StreamRecord) int64 {
					return int64(record.Data().(flow.Data)[2].(int))
				}
				topN := newTopNAggregatorGroup(config, sortKeyExtractor)
				topN.Add(input)
				topN.leakCheck()
				snapshot := topN.Snapshot()
				require.Len(snapshot, 2)
				require.Contains(snapshot, "e2e-service-provider")
				require.Contains(snapshot, "e2e-service-consumer")
				if diff := cmp.Diff(tt.expected, snapshot); diff != "" {
					t.Errorf("Snapshot() mismatch (-want +got):\n%s", diff)
				}
			})
		}
	}

	t.Run("normal", func(t *testing.T) {
		verifyFn(t,
			[]flow.StreamRecord{
				flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{2, "e2e-service-consumer", 9900, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9800, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9700, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 9700, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{6, "e2e-service-consumer", 9600, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{7, "e2e-service-consumer", 9800, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{8, "e2e-service-consumer", 9500, []interface{}{"e2e-service-consumer"}}),
			},
			[]testCase{
				{
					name: "DESC",
					sort: DESC,
					expected: map[string][]*Tuple2[int64]{
						"e2e-service-provider": {
							{V1: int64(10000), V2: flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9800), V2: flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9800, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9700), V2: flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 9700, []interface{}{"e2e-service-provider"}})},
						},
						"e2e-service-consumer": {
							{V1: int64(9900), V2: flow.NewStreamRecordWithoutTS(flow.Data{2, "e2e-service-consumer", 9900, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9800), V2: flow.NewStreamRecordWithoutTS(flow.Data{7, "e2e-service-consumer", 9800, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9700), V2: flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9700, []interface{}{"e2e-service-consumer"}})},
						},
					},
				},
				{
					name: "DESC by default",
					sort: 0,
					expected: map[string][]*Tuple2[int64]{
						"e2e-service-provider": {
							{V1: int64(10000), V2: flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9800), V2: flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9800, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9700), V2: flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 9700, []interface{}{"e2e-service-provider"}})},
						},
						"e2e-service-consumer": {
							{V1: int64(9900), V2: flow.NewStreamRecordWithoutTS(flow.Data{2, "e2e-service-consumer", 9900, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9800), V2: flow.NewStreamRecordWithoutTS(flow.Data{7, "e2e-service-consumer", 9800, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9700), V2: flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9700, []interface{}{"e2e-service-consumer"}})},
						},
					},
				},
				{
					name: "ASC",
					sort: ASC,
					expected: map[string][]*Tuple2[int64]{
						"e2e-service-consumer": {
							{V1: int64(9500), V2: flow.NewStreamRecordWithoutTS(flow.Data{8, "e2e-service-consumer", 9500, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9600), V2: flow.NewStreamRecordWithoutTS(flow.Data{6, "e2e-service-consumer", 9600, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9700), V2: flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9700, []interface{}{"e2e-service-consumer"}})},
						},
						"e2e-service-provider": {
							{V1: int64(9700), V2: flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 9700, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9800), V2: flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9800, []interface{}{"e2e-service-provider"}})},
							{V1: int64(10000), V2: flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}})},
						},
					},
				},
			},
		)
	})
	t.Run("duplicated with different sort key", func(t *testing.T) {
		verifyFn(t,
			[]flow.StreamRecord{
				flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{2, "e2e-service-consumer", 9900, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9800, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9700, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 9700, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{6, "e2e-service-consumer", 9600, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{7, "e2e-service-consumer", 9800, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{8, "e2e-service-consumer", 9500, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9801, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9701, []interface{}{"e2e-service-consumer"}}),
			},
			[]testCase{
				{
					name: "DESC",
					sort: DESC,
					expected: map[string][]*Tuple2[int64]{
						"e2e-service-provider": {
							{V1: int64(10000), V2: flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9801), V2: flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9801, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9700), V2: flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 9700, []interface{}{"e2e-service-provider"}})},
						},
						"e2e-service-consumer": {
							{V1: int64(9900), V2: flow.NewStreamRecordWithoutTS(flow.Data{2, "e2e-service-consumer", 9900, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9800), V2: flow.NewStreamRecordWithoutTS(flow.Data{7, "e2e-service-consumer", 9800, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9701), V2: flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9701, []interface{}{"e2e-service-consumer"}})},
						},
					},
				},
				{
					name: "DESC by default",
					sort: 0,
					expected: map[string][]*Tuple2[int64]{
						"e2e-service-provider": {
							{V1: int64(10000), V2: flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9801), V2: flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9801, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9700), V2: flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 9700, []interface{}{"e2e-service-provider"}})},
						},
						"e2e-service-consumer": {
							{V1: int64(9900), V2: flow.NewStreamRecordWithoutTS(flow.Data{2, "e2e-service-consumer", 9900, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9800), V2: flow.NewStreamRecordWithoutTS(flow.Data{7, "e2e-service-consumer", 9800, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9701), V2: flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9701, []interface{}{"e2e-service-consumer"}})},
						},
					},
				},
				{
					name: "ASC",
					sort: ASC,
					expected: map[string][]*Tuple2[int64]{
						"e2e-service-consumer": {
							{V1: int64(9500), V2: flow.NewStreamRecordWithoutTS(flow.Data{8, "e2e-service-consumer", 9500, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9600), V2: flow.NewStreamRecordWithoutTS(flow.Data{6, "e2e-service-consumer", 9600, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9701), V2: flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9701, []interface{}{"e2e-service-consumer"}})},
						},
						"e2e-service-provider": {
							{V1: int64(9700), V2: flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 9700, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9801), V2: flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9801, []interface{}{"e2e-service-provider"}})},
							{V1: int64(10000), V2: flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}})},
						},
					},
				},
			},
		)
	})

	t.Run("duplicated with identical sort key", func(t *testing.T) {
		verifyFn(t,
			[]flow.StreamRecord{
				flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{2, "e2e-service-consumer", 9900, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9800, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9700, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 9800, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{6, "e2e-service-consumer", 9700, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{7, "e2e-service-consumer", 9800, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{8, "e2e-service-consumer", 9500, []interface{}{"e2e-service-consumer"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9801, []interface{}{"e2e-service-provider"}}),
				flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9701, []interface{}{"e2e-service-consumer"}}),
			},
			[]testCase{
				{
					name: "DESC",
					sort: DESC,
					expected: map[string][]*Tuple2[int64]{
						"e2e-service-provider": {
							{V1: int64(10000), V2: flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9801), V2: flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9801, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9800), V2: flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 9800, []interface{}{"e2e-service-provider"}})},
						},
						"e2e-service-consumer": {
							{V1: int64(9900), V2: flow.NewStreamRecordWithoutTS(flow.Data{2, "e2e-service-consumer", 9900, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9800), V2: flow.NewStreamRecordWithoutTS(flow.Data{7, "e2e-service-consumer", 9800, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9701), V2: flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9701, []interface{}{"e2e-service-consumer"}})},
						},
					},
				},
				{
					name: "DESC by default",
					sort: 0,
					expected: map[string][]*Tuple2[int64]{
						"e2e-service-provider": {
							{V1: int64(10000), V2: flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9801), V2: flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9801, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9800), V2: flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 9800, []interface{}{"e2e-service-provider"}})},
						},
						"e2e-service-consumer": {
							{V1: int64(9900), V2: flow.NewStreamRecordWithoutTS(flow.Data{2, "e2e-service-consumer", 9900, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9800), V2: flow.NewStreamRecordWithoutTS(flow.Data{7, "e2e-service-consumer", 9800, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9701), V2: flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9701, []interface{}{"e2e-service-consumer"}})},
						},
					},
				},
				{
					name: "ASC",
					sort: ASC,
					expected: map[string][]*Tuple2[int64]{
						"e2e-service-consumer": {
							{V1: int64(9500), V2: flow.NewStreamRecordWithoutTS(flow.Data{8, "e2e-service-consumer", 9500, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9700), V2: flow.NewStreamRecordWithoutTS(flow.Data{6, "e2e-service-consumer", 9700, []interface{}{"e2e-service-consumer"}})},
							{V1: int64(9701), V2: flow.NewStreamRecordWithoutTS(flow.Data{4, "e2e-service-consumer", 9701, []interface{}{"e2e-service-consumer"}})},
						},
						"e2e-service-provider": {
							{V1: int64(9800), V2: flow.NewStreamRecordWithoutTS(flow.Data{5, "e2e-service-provider", 9800, []interface{}{"e2e-service-provider"}})},
							{V1: int64(9801), V2: flow.NewStreamRecordWithoutTS(flow.Data{3, "e2e-service-provider", 9801, []interface{}{"e2e-service-provider"}})},
							{V1: int64(10000), V2: flow.NewStreamRecordWithoutTS(flow.Data{1, "e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}})},
						},
					},
				},
			},
		)
	})
}
