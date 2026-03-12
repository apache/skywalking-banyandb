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

package observability

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/meter/native"
)

// Point represents a simplified view of a datapoint in the _monitoring group.
type Point struct {
	Tags  map[string]string
	Value float64
}

// QueryObservabilityMeasure queries a measure from the _monitoring group and returns flattened datapoints.
// tagNames are additional tag names to project beyond the default entity tags (node_type, node_id, grpc_address, http_address).
// For example, cpu_state and memory_state need "kind"; disk needs "path" and "kind".
func QueryObservabilityMeasure(metricName string, tagNames ...string) ([]Point, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := measurev1.NewMeasureServiceClient(sharedContext.Connection)

	now := time.Now().Truncate(time.Second)
	resp, err := client.Query(ctx, &measurev1.QueryRequest{
		Groups: []string{native.ObservabilityGroupName},
		Name:   metricName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(now.Add(-10 * time.Minute)),
			End:   timestamppb.New(now.Add(1 * time.Minute)),
		},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{
					Name: "default",
					Tags: append([]string{"node_type", "node_id", "grpc_address", "http_address"}, tagNames...),
				},
			},
		},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{
			Names: []string{"value"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("query %s failed: %w", metricName, err)
	}

	var points []Point
	for _, dp := range resp.GetDataPoints() {
		if len(dp.TagFamilies) == 0 {
			continue
		}
		tags := make(map[string]string)
		for _, tagFamily := range dp.TagFamilies {
			for _, tag := range tagFamily.Tags {
				if tag == nil {
					continue
				}
				tagValue := tag.GetValue()
				if tagValue == nil {
					continue
				}
				str := tagValue.GetStr()
				if str == nil {
					continue
				}
				if tag.Key == "" {
					continue
				}
				tags[tag.Key] = str.Value
			}
		}
		if len(dp.Fields) == 0 {
			continue
		}
		field := dp.Fields[0]
		floatField := field.GetValue().GetFloat()
		if floatField == nil {
			continue
		}
		value := floatField.Value
		points = append(points, Point{
			Value: value,
			Tags:  tags,
		})
	}

	return points, nil
}
