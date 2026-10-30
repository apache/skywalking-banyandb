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
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
)

func TestQLBReadinessDifference(t *testing.T) {
	_, expected := qlbT17RawChunk(0, 64, 0, time.Unix(1700000000, 0))
	for _, tc := range []struct {
		name   string
		mutate func(*measurev1.QueryResponse)
		want   string
	}{
		{name: "unordered_storage_identity", mutate: func(r *measurev1.QueryResponse) {
			slices.Reverse(r.DataPoints)
			for _, point := range r.DataPoints {
				point.Sid = 42
				point.Version = 99
			}
		}, want: "no differing tuple"},
		{name: "field_corruption", mutate: func(r *measurev1.QueryResponse) { r.DataPoints[1].Fields[1].Value = intFieldValue(1776) }, want: "first differing tuple=1"},
		{name: "timestamp_corruption", mutate: func(r *measurev1.QueryResponse) { r.DataPoints[1].Timestamp.Nanos++ }, want: "first differing tuple=1"},
		{name: "missing_tuple", mutate: func(r *measurev1.QueryResponse) { r.DataPoints = r.DataPoints[:3] }, want: "missing tuple=3"},
		{name: "unexpected_tuple", mutate: func(r *measurev1.QueryResponse) {
			point := proto.Clone(r.DataPoints[3]).(*measurev1.DataPoint)
			point.Timestamp.Nanos++
			r.DataPoints = append(r.DataPoints, point)
		}, want: "unexpected tuple=4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := proto.Clone(expected).(*measurev1.QueryResponse)
			tc.mutate(actual)
			difference := qlbReadinessDifference(expected, actual)
			if !strings.Contains(difference, tc.want) {
				t.Fatalf("diagnostic %q does not contain %q", difference, tc.want)
			}
		})
	}
	if difference := qlbReadinessDifference(expected, nil); !strings.Contains(difference, "missing tuple=0") {
		t.Fatalf("missing response diagnostic: %s", difference)
	}
}
