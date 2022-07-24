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

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/flow/api"
)

func TestFlow_TopN_Aggregator(t *testing.T) {
	input := []interface{}{
		// 1. string
		// 2. number
		// 3. slices of groupBy values
		api.Data{"e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}},
		api.Data{"e2e-service-consumer", 9900, []interface{}{"e2e-service-consumer"}},
		api.Data{"e2e-service-provider", 9800, []interface{}{"e2e-service-provider"}},
		api.Data{"e2e-service-consumer", 9700, []interface{}{"e2e-service-consumer"}},
		api.Data{"e2e-service-provider", 9700, []interface{}{"e2e-service-provider"}},
		api.Data{"e2e-service-consumer", 9600, []interface{}{"e2e-service-consumer"}},
		api.Data{"e2e-service-consumer", 9800, []interface{}{"e2e-service-consumer"}},
		api.Data{"e2e-service-consumer", 9500, []interface{}{"e2e-service-consumer"}},
	}
	tests := []struct {
		name     string
		sort     modelv1.Sort
		expected []*Tuple2
	}{
		{
			name: "DESC",
			sort: modelv1.Sort_SORT_DESC,
			expected: []*Tuple2{
				{int64(10000), api.Data{"e2e-service-provider", 10000, []interface{}{"e2e-service-provider"}}},
				{int64(9900), api.Data{"e2e-service-consumer", 9900, []interface{}{"e2e-service-consumer"}}},
				{int64(9800), api.Data{"e2e-service-provider", 9800, []interface{}{"e2e-service-provider"}}},
			},
		},
		{
			name: "ASC",
			sort: modelv1.Sort_SORT_ASC,
			expected: []*Tuple2{
				{int64(9500), api.Data{"e2e-service-consumer", 9500, []interface{}{"e2e-service-consumer"}}},
				{int64(9600), api.Data{"e2e-service-consumer", 9600, []interface{}{"e2e-service-consumer"}}},
				{int64(9700), api.Data{"e2e-service-consumer", 9700, []interface{}{"e2e-service-consumer"}}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			var comparator utils.Comparator
			if tt.sort == modelv1.Sort_SORT_DESC {
				comparator = func(a, b interface{}) int {
					return utils.Int64Comparator(b, a)
				}
			} else {
				comparator = utils.Int64Comparator
			}
			topN := &topNAggregator{
				cacheSize:  3,
				sort:       tt.sort,
				comparator: comparator,
				treeMap:    treemap.NewWith(comparator),
				sortKeyExtractor: func(elem interface{}) int64 {
					return int64(elem.(api.Data)[1].(int))
				},
			}
			for _, item := range input {
				topN.Add([]interface{}{item})
			}
			result := topN.GetResult()
			require.Len(result, 3)
			require.Equal(tt.expected, result)
		})
	}
}
