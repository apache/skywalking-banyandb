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

package querybench

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func TestBuildScenarioQueryScanAllMatchesFixtureShape(t *testing.T) {
	req, buildErr := buildScenarioQuery(ScenarioScanAll, 1024, time.Unix(0, 0))
	if buildErr != nil {
		t.Fatalf("buildScenarioQuery() failed: %v", buildErr)
	}
	if req.GetName() != benchMeasureName || req.GetGroups()[0] != benchGroupName || req.GetLimit() != 1024 {
		t.Fatalf("unexpected scan-all request identity: %+v", req)
	}
	if len(req.GetTagProjection().GetTagFamilies()[0].GetTags()) != 2 || len(req.GetFieldProjection().GetNames()) != 2 {
		t.Fatalf("scan-all projection does not match all.yaml: %+v", req)
	}
}

func TestBuildScenarioQueryTopWithFilterMatchesFixtureShape(t *testing.T) {
	req, buildErr := buildScenarioQuery(ScenarioTopWithFilter, 1024, time.Unix(0, 0))
	if buildErr != nil {
		t.Fatalf("buildScenarioQuery() failed: %v", buildErr)
	}
	condition := req.GetCriteria().GetCondition()
	if condition.GetName() != benchTagID || condition.GetOp() != modelv1.Condition_BINARY_OP_NE || condition.GetValue().GetStr().GetValue() != "svc3" {
		t.Fatalf("top-with-filter condition does not match fixture: %+v", condition)
	}
	if req.GetAgg().GetFunction() != modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN || req.GetTop().GetNumber() != 2 || req.GetTop().GetFieldValueSort() != modelv1.Sort_SORT_DESC {
		t.Fatalf("top-with-filter agg/top does not match fixture: %+v", req)
	}
}

func TestHashResponseStable(t *testing.T) {
	dpA := &measurev1.DataPoint{Timestamp: timestamppb.New(time.Unix(1, 0))}
	dpB := &measurev1.DataPoint{Timestamp: timestamppb.New(time.Unix(2, 0))}
	respForward := &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{dpA, dpB}}
	respReverse := &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{dpB, dpA}}
	hashForward := hashResponse(respForward)
	hashReverse := hashResponse(respReverse)
	if hashForward != hashReverse {
		t.Fatalf("hashResponse must be order-independent: forward=%d reverse=%d", hashForward, hashReverse)
	}
	if hashForward == 0 {
		t.Fatalf("hashResponse must be non-zero for non-empty response")
	}
	respSingle := &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{dpA}}
	if hashResponse(respSingle) == hashForward {
		t.Fatalf("hashResponse must differ when point set differs")
	}
	emptyA := &measurev1.QueryResponse{}
	emptyB := &measurev1.QueryResponse{}
	if hashResponse(emptyA) != hashResponse(emptyB) {
		t.Fatalf("hashResponse must be deterministic for empty response")
	}
}
