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

package plan

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// The scan limit is offset plus limit, and only for an index-mode scan ordered by time that nothing but the Limit
// operator follows.
func TestIndexModeScanLimit(t *testing.T) {
	indexMode := &databasev1.Measure{IndexMode: true}
	normal := &databasev1.Measure{}
	timeOrder := &index.OrderBy{Type: index.OrderByTypeTime}
	indexOrder := &index.OrderBy{Type: index.OrderByTypeIndex}
	req := func(mutate func(*measurev1.QueryRequest)) *measurev1.QueryRequest {
		r := &measurev1.QueryRequest{Groups: []string{"g"}, Offset: 5, Limit: 20}
		if mutate != nil {
			mutate(r)
		}
		return r
	}
	plainScan := &Scan{}
	for _, tc := range []struct {
		measure *databasev1.Measure
		order   *index.OrderBy
		scan    *Scan
		req     *measurev1.QueryRequest
		name    string
		want    int
	}{
		{name: "offset plus limit", measure: indexMode, order: timeOrder, scan: plainScan, req: req(nil), want: 25},
		{name: "default limit when unset", measure: indexMode, order: timeOrder, scan: plainScan, req: req(func(r *measurev1.QueryRequest) { r.Limit = 0 }), want: 105},
		{name: "unbounded limit", measure: indexMode, order: timeOrder, scan: plainScan, req: req(func(r *measurev1.QueryRequest) { r.Limit = math.MaxUint32 })},
		{name: "normal measure", measure: normal, order: timeOrder, scan: plainScan, req: req(nil)},
		{name: "no order", measure: indexMode, scan: plainScan, req: req(nil)},
		{name: "index order", measure: indexMode, order: indexOrder, scan: plainScan, req: req(nil), want: 25},
		{name: "index order normal measure", measure: normal, order: indexOrder, scan: plainScan, req: req(nil)},
		{
			name: "index order several groups", measure: indexMode, order: indexOrder, scan: plainScan,
			req: req(func(r *measurev1.QueryRequest) { r.Groups = []string{"a", "b"} }),
		},
		{
			name: "index order top n", measure: indexMode, order: indexOrder, scan: plainScan,
			req: req(func(r *measurev1.QueryRequest) { r.Top = &measurev1.QueryRequest_Top{} }),
		},
		{name: "several groups", measure: indexMode, order: timeOrder, scan: plainScan, req: req(func(r *measurev1.QueryRequest) { r.Groups = []string{"a", "b"} })},
		{name: "top n", measure: indexMode, order: timeOrder, scan: plainScan, req: req(func(r *measurev1.QueryRequest) { r.Top = &measurev1.QueryRequest_Top{} })},
		{
			name: "aggregation", measure: indexMode, order: timeOrder, req: req(nil),
			scan: &Scan{Params: ScanParams{Agg: &model.MeasureAgg{}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, indexModeScanLimit(tc.req, tc.measure, tc.order, tc.scan))
		})
	}
}
