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

package main

import (
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
)

func TestTTLTableRowsCountsOneRowPerCatalogGroupStage(t *testing.T) {
	measure := func(group, segment string) importer.WorkItem {
		return importer.WorkItem{Catalog: commonv1.Catalog_CATALOG_MEASURE, Group: group, Stage: "hot", Segment: segment}
	}
	plan := &importer.Plan{
		Entries: []importer.Entry{
			// Two expired units of one group and stage share a table row.
			{Item: measure("sw_metricsHour", "2026101800"), TargetStage: "hot", TTLExpired: true},
			{Item: measure("sw_metricsHour", "2026101900"), TargetStage: "hot", TTLExpired: true},
			// A row with one expired unit among fresh ones is marked.
			{Item: measure("sw_metricsDay", "20261010"), TargetStage: "hot", TTLExpired: true},
			{Item: measure("sw_metricsDay", "20261020"), TargetStage: "hot"},
			// Another stage of the same group is its own row.
			{Item: measure("sw_metricsHour", "2026101000"), TargetStage: "warm", TTLExpired: true},
			// Fresh rows are not counted.
			{Item: measure("sw_metricsMinute", "20261020"), TargetStage: "hot"},
		},
		Rejected: []importer.Rejection{
			{Item: measure("sw_metricsDay", "20261001"), TargetStage: "warm", Gate: importer.GateTTL},
			{Item: measure("sw_records", "20261020"), TargetStage: "hot", Gate: importer.GateSchema},
		},
	}
	if got, want := ttlTableRows(plan), 4; got != want {
		t.Fatalf("ttlTableRows = %d, want %d", got, want)
	}
}

func TestTTLTableRowsEmptyPlan(t *testing.T) {
	if got := ttlTableRows(&importer.Plan{}); got != 0 {
		t.Fatalf("ttlTableRows = %d, want 0", got)
	}
}
