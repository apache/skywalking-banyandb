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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package measure_test

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

func verifyGeneratedAggregateGroups(ctx context.Context, query string, expected []byte) (*measurev1.QueryResponse, error) {
	text, params, extractErr := helpers.ExtractQL(query)
	if extractErr != nil {
		return nil, extractErr
	}
	want := &measurev1.QueryResponse{}
	if decodeErr := protojson.Unmarshal(expected, want); decodeErr != nil {
		return nil, fmt.Errorf("decode generated aggregate response: %w", decodeErr)
	}
	response, queryErr := bydbqlv1.NewBydbQLServiceClient(SharedContext.Connection).Query(ctx, &bydbqlv1.QueryRequest{Query: text, Params: params})
	if queryErr != nil {
		return nil, fmt.Errorf("query generated aggregate: %w", queryErr)
	}
	actual := response.GetMeasureResult()
	if actual == nil {
		return nil, fmt.Errorf("generated aggregate returned no measure result")
	}
	// The complete-input fixture has a unique non-null string zone key. Prove
	// that invariant on both sides, then compare in key order to avoid quadratic
	// multiset matching. This does not impose a SQL output-order requirement.
	for _, points := range [][]*measurev1.DataPoint{actual.GetDataPoints(), want.GetDataPoints()} {
		keys := make(map[string]bool, len(points))
		pointKeys := make(map[*measurev1.DataPoint]string, len(points))
		for _, point := range points {
			key, keyErr := generatedAggregateGroupKey(point)
			if keyErr != nil {
				return nil, keyErr
			}
			if keys[key] {
				return nil, fmt.Errorf("generated complete-input aggregate repeats key %q", key)
			}
			keys[key] = true
			pointKeys[point] = key
		}
		slices.SortFunc(points, func(left, right *measurev1.DataPoint) int {
			return strings.Compare(pointKeys[left], pointKeys[right])
		})
	}
	if compareErr := measuredata.CompareQLResult(actual, want, true); compareErr != nil {
		return nil, compareErr
	}
	return actual, nil
}

func generatedAggregateGroupKey(point *measurev1.DataPoint) (string, error) {
	for _, family := range point.GetTagFamilies() {
		if family.GetName() != "default" {
			continue
		}
		for _, tag := range family.GetTags() {
			if tag.GetKey() == "key" && tag.GetValue().GetStr() != nil {
				return tag.GetValue().GetStr().GetValue(), nil
			}
		}
	}
	return "", fmt.Errorf("generated complete-input aggregate lacks its non-null string key")
}
