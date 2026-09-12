// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package db

import (
	"context"
	"math"
	"testing"
	"time"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// Property queries with an oversized Limit previously sized shard/db result slices from
// that value. The capacity hint must stay bounded even if such a limit reaches allocation.
func TestQueryCapacityCapsOversizedLimit(t *testing.T) {
	if got := queryCapacity(math.MaxInt32); got != queryCapacityHint {
		t.Fatalf("queryCapacity(MaxInt32)=%d, want %d", got, queryCapacityHint)
	}
	if got := queryCapacityUint(math.MaxUint32); got != queryCapacityHint {
		t.Fatalf("queryCapacityUint(MaxUint32)=%d, want %d", got, queryCapacityHint)
	}
	if got := queryCapacity(16); got != 16 {
		t.Fatalf("queryCapacity(16)=%d, want 16", got)
	}
	if got := queryCapacityUint(16); got != 16 {
		t.Fatalf("queryCapacityUint(16)=%d, want 16", got)
	}
}

func TestQueryCapacityAllocationStaysBounded(t *testing.T) {
	// Use a modest oversized limit so a guard regression fails the assertion without OOM.
	const oversized = 4096
	data := make([]*queryProperty, 0, queryCapacity(oversized))
	if cap(data) != queryCapacityHint {
		t.Fatalf("shard slice capacity %d, want %d", cap(data), queryCapacityHint)
	}
	result := make([]QueriedProperty, 0, queryCapacityUint(oversized))
	if cap(result) != queryCapacityHint {
		t.Fatalf("db slice capacity %d, want %d", cap(result), queryCapacityHint)
	}
}

func TestOrderedPropertyQueryCapacityBounded(t *testing.T) {
	const oversizedLimit uint32 = 4096
	dataDir, dataCleanup, err := test.NewSpace()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dataCleanup)

	db, openErr := OpenDB(context.Background(), Config{
		Location:               dataDir,
		MetricsScopeName:       "property_query_capacity_test",
		FlushInterval:          time.Hour,
		ExpireToDeleteDuration: time.Hour,
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	prop := generateProperty("cap-id", time.Now().UnixNano(), 1)
	if updateErr := db.Update(ctx, 0, GetPropertyID(prop), prop); updateErr != nil {
		t.Fatal(updateErr)
	}

	req := &propertyv1.QueryRequest{
		Groups: []string{testPropertyGroup},
		Limit:  oversizedLimit,
		OrderBy: &propertyv1.QueryOrder{
			TagName: "tag1",
			Sort:    modelv1.Sort_SORT_ASC,
		},
	}
	result, queryErr := db.Query(ctx, req)
	if queryErr != nil {
		t.Fatal(queryErr)
	}
	if len(result) == 0 {
		t.Fatal("expected at least one ordered property result")
	}
	if cap(result) != queryCapacityHint {
		t.Fatalf("database.Query ordered merge capacity %d, want %d", cap(result), queryCapacityHint)
	}

	sd, loadErr := db.(*database).loadShard(ctx, testPropertyGroup, 0)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	iq, buildErr := inverted.BuildPropertyQuery(req, groupField, entityID)
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	shardResults, searchErr := sd.search(ctx, iq, req.OrderBy, int(req.Limit))
	if searchErr != nil {
		t.Fatal(searchErr)
	}
	if len(shardResults) == 0 {
		t.Fatal("expected ordered shard.search to return the inserted property")
	}
	if cap(shardResults) != queryCapacityHint {
		t.Fatalf("shard.search ordered capacity %d, want %d", cap(shardResults), queryCapacityHint)
	}
}
