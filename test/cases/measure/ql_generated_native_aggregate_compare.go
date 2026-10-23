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

func verifyGeneratedNativeAggregateResult(ctx context.Context, query string, expected []byte, groups uint64) (*measurev1.QueryResponse, error) {
	if groups <= 100 {
		return measuredata.VerifyQL(ctx, SharedContext.Connection, query, expected, false)
	}
	text, params, extractErr := helpers.ExtractQL(query)
	if extractErr != nil {
		return nil, extractErr
	}
	want := &measurev1.QueryResponse{}
	if decodeErr := protojson.Unmarshal(expected, want); decodeErr != nil {
		return nil, fmt.Errorf("decode generated native aggregate response: %w", decodeErr)
	}
	response, queryErr := bydbqlv1.NewBydbQLServiceClient(SharedContext.Connection).Query(ctx, &bydbqlv1.QueryRequest{Query: text, Params: params})
	if queryErr != nil {
		return nil, fmt.Errorf("query generated native aggregate: %w", queryErr)
	}
	actual := response.GetMeasureResult()
	if actual == nil {
		return nil, fmt.Errorf("generated native aggregate returned no measure result")
	}
	// The complete fixture projects the full non-null entity tuple. Validate its
	// uniqueness and sort both responses so result validation stays linearithmic.
	// This is independent of production key encoding and imposes no SQL ordering.
	for _, points := range [][]*measurev1.DataPoint{actual.GetDataPoints(), want.GetDataPoints()} {
		seen := make(map[string]bool, len(points))
		keys := make(map[*measurev1.DataPoint]string, len(points))
		for _, point := range points {
			key, keyErr := generatedNativeAggregateTuple(point)
			if keyErr != nil {
				return nil, keyErr
			}
			if seen[key] {
				return nil, fmt.Errorf("generated native aggregate repeats entity tuple %q", key)
			}
			seen[key], keys[point] = true, key
		}
		slices.SortFunc(points, func(left, right *measurev1.DataPoint) int {
			return strings.Compare(keys[left], keys[right])
		})
	}
	if compareErr := measuredata.CompareQLResult(actual, want, true); compareErr != nil {
		return nil, compareErr
	}
	return actual, nil
}

func generatedNativeAggregateTuple(point *measurev1.DataPoint) (string, error) {
	var host, region *string
	for _, family := range point.GetTagFamilies() {
		for _, tag := range family.GetTags() {
			if tag.GetValue().GetStr() == nil {
				continue
			}
			value := tag.GetValue().GetStr().GetValue()
			switch tag.GetKey() {
			case "host":
				host = &value
			case "region":
				region = &value
			}
		}
	}
	if host == nil || region == nil {
		return "", fmt.Errorf("generated native aggregate lacks its non-null host/region tuple")
	}
	return fmt.Sprintf("%q/%q", *host, *region), nil
}
