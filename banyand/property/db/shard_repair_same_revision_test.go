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

package db

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

const sameRevisionTestID = "same-rev-id"

// repairInput is one Repair call: a property revision with its delete time (0 means live).
type repairInput struct {
	revision   int64
	deleteTime int64
}

func openSameRevisionTestDB(t *testing.T) Database {
	t.Helper()
	dataDir, dataDefer, err := test.NewSpace()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dataDefer)
	snapshotDir, snapshotDefer, err := test.NewSpace()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(snapshotDefer)
	db, err := OpenDB(context.Background(), Config{
		Location:               dataDir,
		MetricsScopeName:       "property_test",
		FlushInterval:          3 * time.Second,
		ExpireToDeleteDuration: time.Hour,
		Repair: RepairConfig{
			Enabled:            true,
			Location:           snapshotDir,
			BuildTreeCron:      "@every 10m",
			QuickBuildTreeTime: 10 * time.Second,
			TreeSlotCount:      32,
		},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func applyRepairInputs(ctx context.Context, t *testing.T, db Database, inputs ...repairInput) {
	t.Helper()
	for _, in := range inputs {
		property := generateProperty(sameRevisionTestID, in.revision, 1)
		if err := db.Repair(ctx, GetPropertyID(property), 0, property, in.deleteTime); err != nil {
			t.Fatal(err)
		}
	}
}

func querySortedByRevision(ctx context.Context, t *testing.T, db Database) []QueriedProperty {
	t.Helper()
	resp := queryDB(ctx, t, db, sameRevisionTestID)
	sort.Slice(resp, func(i, j int) bool {
		return resp[i].Timestamp() < resp[j].Timestamp()
	})
	return resp
}

func assertSingleDoc(t *testing.T, resp []QueriedProperty, wantDeleteTime int64) {
	t.Helper()
	if len(resp) != 1 {
		t.Fatalf("expect 1 document, got %d", len(resp))
	}
	if resp[0].DeleteTime() != wantDeleteTime {
		t.Fatalf("expect deleteTime %d, got %d", wantDeleteTime, resp[0].DeleteTime())
	}
}

func TestRepairSameRevision(t *testing.T) {
	const revision int64 = 5
	base := time.Now().UnixNano()
	t1 := base - 2000
	t2 := base - 1000
	tests := []struct {
		name           string
		local          repairInput
		incoming       repairInput
		wantDeleteTime int64
	}{
		{
			name:           "local tombstone keeps against incoming live",
			local:          repairInput{revision: revision, deleteTime: t1},
			incoming:       repairInput{revision: revision},
			wantDeleteTime: t1,
		},
		{
			name:           "incoming tombstone replaces local live with its own deleteTime",
			local:          repairInput{revision: revision},
			incoming:       repairInput{revision: revision, deleteTime: t1},
			wantDeleteTime: t1,
		},
		{
			name:           "incoming smaller tombstone wins",
			local:          repairInput{revision: revision, deleteTime: t2},
			incoming:       repairInput{revision: revision, deleteTime: t1},
			wantDeleteTime: t1,
		},
		{
			name:           "local smaller tombstone wins",
			local:          repairInput{revision: revision, deleteTime: t1},
			incoming:       repairInput{revision: revision, deleteTime: t2},
			wantDeleteTime: t1,
		},
		{
			name:           "identical tombstone is a no-op",
			local:          repairInput{revision: revision, deleteTime: t1},
			incoming:       repairInput{revision: revision, deleteTime: t1},
			wantDeleteTime: t1,
		},
		{
			name:     "identical live is a no-op",
			local:    repairInput{revision: revision},
			incoming: repairInput{revision: revision},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db := openSameRevisionTestDB(t)
			applyRepairInputs(ctx, t, db, tt.local, tt.incoming)
			assertSingleDoc(t, querySortedByRevision(ctx, t, db), tt.wantDeleteTime)
		})
	}
}

// TestRepairSameRevisionOrderIndependent applies the same inputs to a single replica in every
// order and checks the end state does not depend on it; the gossip suite covers the exchange
// between replicas.
func TestRepairSameRevisionOrderIndependent(t *testing.T) {
	const revision int64 = 5
	base := time.Now().UnixNano()
	t1 := base - 2000
	t2 := base - 1000
	scenarios := []struct {
		name           string
		inputs         []repairInput
		wantDeleteTime int64
	}{
		{
			name:           "live live tombstone",
			inputs:         []repairInput{{revision: revision}, {revision: revision}, {revision: revision, deleteTime: t1}},
			wantDeleteTime: t1,
		},
		{
			name:           "live tombstone-t1 tombstone-t2",
			inputs:         []repairInput{{revision: revision}, {revision: revision, deleteTime: t1}, {revision: revision, deleteTime: t2}},
			wantDeleteTime: t1,
		},
	}
	orders := [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
	for _, sc := range scenarios {
		for _, order := range orders {
			t.Run(fmt.Sprintf("%s/order-%d%d%d", sc.name, order[0], order[1], order[2]), func(t *testing.T) {
				ctx := context.Background()
				db := openSameRevisionTestDB(t)
				for _, idx := range order {
					applyRepairInputs(ctx, t, db, sc.inputs[idx])
				}
				assertSingleDoc(t, querySortedByRevision(ctx, t, db), sc.wantDeleteTime)
			})
		}
	}
}

func TestRepairAscendingHistoryReplayKeepsTombstoneTime(t *testing.T) {
	ctx := context.Background()
	db := openSameRevisionTestDB(t)
	tombstoneTime := time.Now().UnixNano() - int64(time.Minute) + 100
	applyRepairInputs(ctx, t, db,
		repairInput{revision: 1, deleteTime: tombstoneTime},
		repairInput{revision: 2},
	)
	resp := querySortedByRevision(ctx, t, db)
	if len(resp) != 2 {
		t.Fatalf("expect 2 documents, got %d", len(resp))
	}
	if resp[0].Timestamp() != 1 || resp[0].DeleteTime() != tombstoneTime {
		t.Fatalf("expect revision 1 to keep deleteTime %d, got revision %d deleteTime %d",
			tombstoneTime, resp[0].Timestamp(), resp[0].DeleteTime())
	}
	if resp[1].Timestamp() != 2 || resp[1].DeleteTime() != 0 {
		t.Fatalf("expect revision 2 to be live, got revision %d deleteTime %d", resp[1].Timestamp(), resp[1].DeleteTime())
	}
}

func TestRepairSameRevisionBeyondHundredRevisions(t *testing.T) {
	ctx := context.Background()
	db := openSameRevisionTestDB(t)
	const latestRevision int64 = 150
	for revision := int64(1); revision < latestRevision; revision++ {
		applyRepairInputs(ctx, t, db, repairInput{revision: revision})
	}
	tombstoneTime := time.Now().UnixNano() - int64(time.Minute)
	applyRepairInputs(ctx, t, db, repairInput{revision: latestRevision, deleteTime: tombstoneTime})

	live := generateProperty(sameRevisionTestID, latestRevision, 1)
	if err := db.Repair(ctx, GetPropertyID(live), 0, live, 0); err != nil {
		t.Fatal(err)
	}
	resp := querySortedByRevision(ctx, t, db)
	if len(resp) != int(latestRevision) {
		t.Fatalf("expect %d documents, got %d", latestRevision, len(resp))
	}
	for _, doc := range resp {
		if doc.DeleteTime() <= 0 {
			t.Fatalf("expect every revision to be deleted, revision %d is live", doc.Timestamp())
		}
	}
	latest := resp[len(resp)-1]
	if latest.Timestamp() != latestRevision || latest.DeleteTime() != tombstoneTime {
		t.Fatalf("expect revision %d to keep deleteTime %d, got revision %d deleteTime %d",
			latestRevision, tombstoneTime, latest.Timestamp(), latest.DeleteTime())
	}
}

func entityRequest() *propertyv1.QueryRequest {
	return &propertyv1.QueryRequest{Groups: []string{testPropertyGroup}, Name: testPropertyName, Ids: []string{sameRevisionTestID}}
}

func storeRevision(ctx context.Context, t *testing.T, sd *shard, revision, deleteTime int64) {
	t.Helper()
	property := generateProperty(sameRevisionTestID, revision, 1)
	doc, err := sd.buildUpdateDocument(GetPropertyID(property), property, deleteTime)
	if err != nil {
		t.Fatal(err)
	}
	if err = sd.updateDocuments(ctx, index.Documents{*doc}); err != nil {
		t.Fatal(err)
	}
}

func repairRevision(ctx context.Context, t *testing.T, sd *shard, revision int64) (bool, *queryProperty) {
	t.Helper()
	property := generateProperty(sameRevisionTestID, revision, 1)
	updated, newer, err := sd.repair(ctx, GetPropertyID(property), property, 0)
	if err != nil {
		t.Fatal(err)
	}
	return updated, newer
}

// liveRevisions returns the revisions of every stored document that is not deleted.
func liveRevisions(ctx context.Context, t *testing.T, sd *shard) (live []int64, total int) {
	t.Helper()
	all, err := sd.nativeStore.query(ctx, entityRequest(), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range all {
		if p.deleteTime <= 0 {
			live = append(live, p.timestamp)
		}
	}
	sort.Slice(live, func(i, j int) bool { return live[i] < live[j] })
	return live, len(all)
}

const (
	hundredPlusTombstones           = 120
	hundredPlusNewestRevision int64 = 2 * (hundredPlusTombstones + 1)
)

// newHundredPlusTombstonesShard stores more than 100 tombstoned revisions of one entity, then its
// newest revision, live, last.
func newHundredPlusTombstonesShard(t *testing.T) *shard {
	t.Helper()
	sd := newRealMemoryShard(t)
	ctx := context.Background()
	tombstoneTime := time.Now().UnixNano() - int64(time.Minute)
	for i := int64(1); i <= hundredPlusTombstones; i++ {
		storeRevision(ctx, t, sd, 2*i, tombstoneTime)
	}
	storeRevision(ctx, t, sd, hundredPlusNewestRevision, 0)
	return sd
}

func TestRepairBeyondHundredTombstones(t *testing.T) {
	t.Run("older revision is rejected", func(t *testing.T) {
		sd := newHundredPlusTombstonesShard(t)
		ctx := context.Background()
		updated, newer := repairRevision(ctx, t, sd, 101)
		if updated || newer == nil || newer.timestamp != hundredPlusNewestRevision || newer.deleteTime != 0 {
			t.Fatalf("expect rejection by live revision %d, got updated %v newer %+v", hundredPlusNewestRevision, updated, newer)
		}
		live, total := liveRevisions(ctx, t, sd)
		if total != hundredPlusTombstones+1 || len(live) != 1 || live[0] != hundredPlusNewestRevision {
			t.Fatalf("expect store unchanged, got %d documents with live %v", total, live)
		}
	})

	t.Run("newer revision tombstones the live document", func(t *testing.T) {
		sd := newHundredPlusTombstonesShard(t)
		ctx := context.Background()
		if updated, newer := repairRevision(ctx, t, sd, hundredPlusNewestRevision+1); !updated || newer != nil {
			t.Fatalf("expect repair to apply, got updated %v newer %+v", updated, newer)
		}
		live, total := liveRevisions(ctx, t, sd)
		if total != hundredPlusTombstones+2 || len(live) != 1 || live[0] != hundredPlusNewestRevision+1 {
			t.Fatalf("expect only revision %d live among %d documents, got %d documents with live %v",
				hundredPlusNewestRevision+1, hundredPlusTombstones+2, total, live)
		}
	})
}

func TestRepairTombstonesEveryLiveRevisionInOneRead(t *testing.T) {
	ctx := context.Background()
	sd := newRealMemoryShard(t)
	// Two live revisions simulate an earlier write whose removal of the older one failed.
	storeRevision(ctx, t, sd, 1, 0)
	storeRevision(ctx, t, sd, 2, 0)
	var reads int
	sd.testBeforeNativeRead = func() { reads++ }

	if updated, _ := repairRevision(ctx, t, sd, 3); !updated || reads != 1 {
		t.Fatalf("expect repair to apply with 1 native read, got updated %v with %d reads", updated, reads)
	}
	live, total := liveRevisions(ctx, t, sd)
	if total != 3 || len(live) != 1 || live[0] != 3 {
		t.Fatalf("expect only revision 3 live among 3 documents, got %d documents with live %v", total, live)
	}

	reads = 0
	if updated, newer := repairRevision(ctx, t, sd, 2); updated || newer == nil || newer.timestamp != 3 || reads != 1 {
		t.Fatalf("expect rejection with 1 native read, got updated %v newer %+v with %d reads", updated, newer, reads)
	}
}

func TestEntityRevisionsReturnsDeletedNewestAndLiveOlder(t *testing.T) {
	ctx := context.Background()
	sd := newRealMemoryShard(t)
	tombstoneTime := time.Now().UnixNano() - int64(time.Minute)
	storeRevision(ctx, t, sd, 1, tombstoneTime)
	storeRevision(ctx, t, sd, 2, 0)
	storeRevision(ctx, t, sd, 3, tombstoneTime)

	newest, live, err := sd.entityRevisions(ctx, entityRequest())
	if err != nil {
		t.Fatal(err)
	}
	if newest == nil || newest.timestamp != 3 || newest.deleteTime != tombstoneTime || len(newest.source) == 0 {
		t.Fatalf("expect deleted revision 3 with source as newest, got %+v", newest)
	}
	if len(live) != 1 || live[0].timestamp != 2 || live[0].deleteTime != 0 || len(live[0].source) == 0 {
		t.Fatalf("expect live revision 2 with source, got %+v", live)
	}

	empty := entityRequest()
	empty.Ids = []string{"missing"}
	if newest, live, err = sd.entityRevisions(ctx, empty); err != nil || newest != nil || len(live) != 0 {
		t.Fatalf("expect no revisions for a missing entity, got newest %+v live %+v err %v", newest, live, err)
	}
}
