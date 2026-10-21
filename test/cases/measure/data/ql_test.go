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

package data

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func qlTestRows() *measurev1.QueryResponse {
	return &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{
		{
			Timestamp: &timestamppb.Timestamp{Seconds: 100},
			Fields:    []*measurev1.DataPoint_Field{{Name: "v", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 2}}}}},
		},
		{
			Timestamp: &timestamppb.Timestamp{Seconds: 101},
			Fields:    []*measurev1.DataPoint_Field{{Name: "v", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 4}}}}},
		},
	}}
}

func TestCompareQLResultRejectsWrongResults(t *testing.T) {
	tests := []struct {
		mutate func(*measurev1.QueryResponse)
		name   string
	}{
		{name: "value", mutate: func(r *measurev1.QueryResponse) { r.DataPoints[0].Fields[0].Value.GetInt().Value = 3 }},
		{name: "type", mutate: func(r *measurev1.QueryResponse) {
			r.DataPoints[0].Fields[0].Value.Value = &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 2}}
		}},
		{name: "order", mutate: func(r *measurev1.QueryResponse) { r.DataPoints[0], r.DataPoints[1] = r.DataPoints[1], r.DataPoints[0] }},
		{name: "timestamp", mutate: func(r *measurev1.QueryResponse) { r.DataPoints[0].Timestamp.Seconds++ }},
		{name: "name", mutate: func(r *measurev1.QueryResponse) { r.DataPoints[0].Fields[0].Name = "hidden" }},
		{name: "null", mutate: func(r *measurev1.QueryResponse) { r.DataPoints[0].Fields[0].Value = &modelv1.FieldValue{} }},
		{name: "duplicate", mutate: func(r *measurev1.QueryResponse) {
			r.DataPoints[1] = proto.Clone(r.DataPoints[0]).(*measurev1.DataPoint)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := qlTestRows()
			got := proto.Clone(want).(*measurev1.QueryResponse)
			test.mutate(got)
			if err := CompareQLResult(got, want, true); err == nil {
				t.Fatal("incorrect result accepted")
			}
			if test.name != "order" {
				if err := CompareQLResult(got, want, false); err == nil {
					t.Fatal("incorrect multiset accepted")
				}
			}
		})
	}
}

func TestCompareQLResultMultisetAndIdentity(t *testing.T) {
	want := qlTestRows()
	got := proto.Clone(want).(*measurev1.QueryResponse)
	got.DataPoints[0], got.DataPoints[1] = got.DataPoints[1], got.DataPoints[0]
	got.DataPoints[0].Sid = 42
	got.DataPoints[0].Version = 99
	if err := CompareQLResult(got, want, false); err != nil {
		t.Fatal(err)
	}
	if want.DataPoints[0].Sid != 0 || got.DataPoints[0].Sid != 42 {
		t.Fatal("comparison mutated its inputs")
	}
}

func qlFloatRows(values ...float64) *measurev1.QueryResponse {
	response := &measurev1.QueryResponse{}
	for _, value := range values {
		response.DataPoints = append(response.DataPoints, &measurev1.DataPoint{
			Fields: []*measurev1.DataPoint_Field{{Name: "f", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: value}}}}},
		})
	}
	return response
}

func TestCompareQLResultFloatToleranceAtZero(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		for _, value := range []float64{-2e-12, -5e-13, 0, 5e-13, 2e-12} {
			t.Run(fmt.Sprintf("ordered=%t/value=%g", ordered, value), func(t *testing.T) {
				want, got := qlFloatRows(0), qlFloatRows(value)
				withinTolerance := value >= -1e-12 && value <= 1e-12
				if err := CompareQLResult(got, want, ordered); (err == nil) != withinTolerance {
					t.Fatalf("within tolerance=%t, comparison error=%v", withinTolerance, err)
				}
				if err := CompareQLResult(want, got, ordered); (err == nil) != withinTolerance {
					t.Fatalf("reverse comparison: within tolerance=%t, error=%v", withinTolerance, err)
				}
			})
		}
	}
}

func TestCompareQLResultFloatMultisetPermutations(t *testing.T) {
	for _, expected := range [][]float64{{1 + 0.75e-12, 1}, {1, 1 + 0.75e-12}} {
		for _, actual := range [][]float64{{1, 1 + 1.5e-12}, {1 + 1.5e-12, 1}} {
			if err := CompareQLResult(qlFloatRows(actual...), qlFloatRows(expected...), false); err != nil {
				t.Fatalf("valid matching rejected: expected=%v actual=%v: %v", expected, actual, err)
			}
		}
	}
	if err := CompareQLResult(qlFloatRows(1, 1+1.5e-12), qlFloatRows(1, 1), false); err == nil {
		t.Fatal("a matching must not reuse the same actual row")
	}
}

type qlFixtureConn struct {
	grpc.ClientConnInterface
	response *bydbqlv1.QueryResponse
	err      error
	request  *bydbqlv1.QueryRequest
}

func (c *qlFixtureConn) Invoke(_ context.Context, method string, request, reply interface{}, _ ...grpc.CallOption) error {
	if method != bydbqlv1.BydbQLService_Query_FullMethodName {
		return status.Error(codes.Unimplemented, "unexpected method")
	}
	c.request = proto.Clone(request.(*bydbqlv1.QueryRequest)).(*bydbqlv1.QueryRequest)
	if c.err != nil {
		return c.err
	}
	proto.Merge(reply.(*bydbqlv1.QueryResponse), c.response)
	return nil
}

func TestVerifyQLOnlyFixture(t *testing.T) {
	conn := &qlFixtureConn{response: &bydbqlv1.QueryResponse{Result: &bydbqlv1.QueryResponse_MeasureResult{MeasureResult: &measurev1.QueryResponse{}}}}
	content := "#!params: [{\"str\":{\"value\":\"a\"}}]\nSELECT host FROM MEASURE qlb IN metrics WHERE host = ?"
	_, err := VerifyQL(context.Background(), conn, content, []byte("dataPoints: []"), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(conn.request.Params) != 1 || conn.request.Params[0].GetStr().GetValue() != "a" {
		t.Fatal("fixture parameters were not passed to RPC")
	}
	conn.response = &bydbqlv1.QueryResponse{}
	if _, err = VerifyQL(context.Background(), conn, "SELECT host", []byte("{}"), false); err == nil {
		t.Fatal("missing measure variant accepted")
	}
	conn.err = status.Error(codes.InvalidArgument, "bad query")
	if err = VerifyQLError(context.Background(), conn, "invalid", codes.InvalidArgument); err != nil {
		t.Fatal(err)
	}
	if err = VerifyQLError(context.Background(), conn, "invalid", codes.NotFound); err == nil {
		t.Fatal("wrong status accepted")
	}
	conn.err = nil
	conn.response = &bydbqlv1.QueryResponse{}
	if err = VerifyQLError(context.Background(), conn, "invalid", codes.InvalidArgument); err == nil {
		t.Fatal("success accepted as error")
	}
}
