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

package bydbql_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

func TestMeasureScalarCountCanScanSeries(t *testing.T) {
	execution := typedExecution(t, "SELECT COUNT(DISTINCT host) AS n FROM g.m "+
		"WHERE time >= ? AND time < ? AND v::FIELD > 0 AND host NOT IN (?)",
		strParam("1970-01-01T00:00:00Z"), strParam("1970-01-01T00:00:06Z"),
		&modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"absent"}}}})
	require.True(t, execution.Plan.CanScanSeries())
}

func TestMeasureSortedCaseCountCanScanSeries(t *testing.T) {
	query := "SELECT CASE WHEN host IN (?) THEN 'selected' ELSE 'other' END AS category, COUNT(DISTINCT rid) AS n " +
		"FROM g.m WHERE v::FIELD > 0 GROUP BY 1 ORDER BY category ASC LIMIT 1 OFFSET 1"
	statement, err := bydbql.Prepare(query)
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	size := template.EstimatedSize()
	for _, value := range []string{"a", "b"} {
		parameter := &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{value, "absent"}}}}
		execution, bindErr := template.BindParams([]*modelv1.TagValue{parameter}, time.Unix(0, 0))
		require.NoError(t, bindErr)
		require.True(t, execution.Plan.CanScanSeries())
	}
	require.Equal(t, size, template.EstimatedSize())
}

func TestMeasureCountScanPreservesOrderSensitivePlans(t *testing.T) {
	for _, test := range []struct {
		name, query string
		eligible    bool
	}{
		{"multiple_counts", "SELECT COUNT(v) AS n, COUNT(DISTINCT host) AS hosts FROM g.m", true},
		{"computed_count", "SELECT COUNT(CASE WHEN v > 0 THEN host ELSE NULL END) AS n FROM g.m", true},
		{"group_alias", "SELECT host AS node, COUNT(v) AS n FROM g.m GROUP BY host ORDER BY node DESC", true},
		{"group_ordinal", "SELECT host, COUNT(v) AS n FROM g.m GROUP BY host ORDER BY 1", true},
		{"all_group_keys", "SELECT host, rid, COUNT(v) AS n FROM g.m GROUP BY host, rid ORDER BY rid, host LIMIT 1 OFFSET 1", true},
		{"having", "SELECT host AS node, COUNT(v) AS n FROM g.m GROUP BY host HAVING COUNT(v) > 0 ORDER BY node", true},
		{"hidden_group_sort", "SELECT COUNT(v) AS n FROM g.m GROUP BY host ORDER BY host", true},
		{"raw", "SELECT host FROM g.m", false},
		{"ordered_raw", "SELECT host FROM g.m ORDER BY time DESC", false},
		{"legacy", "SELECT COUNT(v) FROM MEASURE m IN g", false},
		{"sum", "SELECT COUNT(v) AS n, SUM(v) AS total FROM g.m", false},
		{"average", "SELECT COUNT(v) AS n, AVG(v) AS mean FROM g.m", false},
		{"integer_extrema", "SELECT COUNT(v) AS n, MIN(v) AS lo, MAX(v) AS hi FROM g.m", false},
		{"float_extrema", "SELECT COUNT(f) AS n, MIN(f) AS lo, MAX(f) AS hi FROM g.m", false},
		{"hidden_having_sum", "SELECT host, COUNT(v) AS n FROM g.m GROUP BY host HAVING SUM(v) > 0 ORDER BY host", false},
		{"hidden_sort_sum", "SELECT host, COUNT(v) AS n FROM g.m GROUP BY host ORDER BY SUM(v), host", false},
		{"unordered_groups", "SELECT host, COUNT(v) AS n FROM g.m GROUP BY host LIMIT 1 OFFSET 1", false},
		{"incomplete_group_order", "SELECT host, rid, COUNT(v) AS n FROM g.m GROUP BY host, rid ORDER BY host LIMIT 1", false},
		{"tied_count_page", "SELECT host, COUNT(v) AS n FROM g.m GROUP BY host ORDER BY n LIMIT 1 OFFSET 1", false},
		{"distinct", "SELECT DISTINCT COUNT(v) AS n FROM g.m GROUP BY host ORDER BY n", false},
		{"bucket", "SELECT TIME_BUCKET('2s') AS bucket, COUNT(v) AS n FROM g.m GROUP BY 1 ORDER BY bucket", false},
		{"fallible_filter", "SELECT COUNT(DISTINCT host) AS n FROM g.m WHERE 1 / v > 0", false},
		{"fallible_input", "SELECT COUNT(1 / v) AS n FROM g.m", false},
		{"fallible_group", "SELECT CAST(host AS INT) AS key, COUNT(v) AS n FROM g.m GROUP BY 1 ORDER BY key", false},
		{"fallible_output", "SELECT host, COUNT(v) + 1 AS n FROM g.m GROUP BY host ORDER BY host", false},
		{"fallible_sort", "SELECT host, COUNT(v) AS n FROM g.m GROUP BY host ORDER BY 1 / COUNT(v), host", false},
		{"unproven_group_mapping", "SELECT host, COUNT(v) AS n FROM g.m GROUP BY host ORDER BY COALESCE(host, 'unknown')", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			execution := typedExecution(t, test.query)
			require.Equal(t, test.eligible, execution.Plan.CanScanSeries())
		})
	}
}

func TestMeasureNativeAggregationDoesNotChangeScanOrder(t *testing.T) {
	execution := nativeAggregateExecution(t, "SELECT COUNT(v) AS n, COUNT(f) AS nf FROM g.m "+
		"WHERE host = 'a' AND time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:06Z'", nativeAggregateSchema())
	require.NotNil(t, execution.Plan.NativeAggregate)
	require.False(t, execution.Plan.CanScanSeries())
	var absent *ql.RawPlan
	require.False(t, absent.CanScanSeries())
}
