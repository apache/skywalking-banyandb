// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. The ASF licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type blockingQuery struct{}

func (blockingQuery) String() string { return "" }

type blockingStore struct {
	index.SeriesStore
	entered        chan struct{}
	updateEntered  chan struct{}
	release        chan struct{}
	closed         chan struct{}
	usedAfterClose chan struct{}
}

func (s *blockingStore) BuildQuery([]index.SeriesMatcher, index.Query, *timestamp.TimeRange) (index.Query, error) {
	return blockingQuery{}, nil
}

func (s *blockingStore) Search(context.Context, []index.FieldKey, index.Query, int) ([]index.SeriesDocument, error) {
	close(s.entered)
	<-s.release
	select {
	case <-s.closed:
		close(s.usedAfterClose)
	default:
	}
	return nil, nil
}

func (s *blockingStore) UpdateSeriesBatch(index.Batch) error {
	entered := s.updateEntered
	if entered == nil {
		entered = s.entered
	}
	close(entered)
	<-s.release
	select {
	case <-s.closed:
		close(s.usedAfterClose)
	default:
	}
	return nil
}

func (s *blockingStore) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	return nil
}

func TestDeleteAndDropCoordinateShardLifetime(t *testing.T) {
	store := &blockingStore{
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
		closed:         make(chan struct{}),
		usedAfterClose: make(chan struct{}),
	}
	shardList := []*shard{{store: store, id: 0}}
	group := &groupShards{location: t.TempDir()}
	group.shards.Store(&shardList)
	var db database
	db.lfs = fs.NewLocalFileSystem()
	db.groups.Store("g", group)
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- db.Delete(context.Background(), [][]byte{[]byte("id")}, time.Now())
	}()
	<-store.entered
	dropDone := make(chan error, 1)
	go func() { dropDone <- db.Drop("g") }()
	select {
	case <-dropDone:
		t.Fatal("Drop closed a shard while Delete was using it")
	case <-time.After(10 * time.Millisecond):
	}
	close(store.release)
	require.NoError(t, <-deleteDone)
	require.NoError(t, <-dropDone)
	select {
	case <-store.usedAfterClose:
		t.Fatal("Delete used a shard after Drop closed it")
	default:
	}
}

func TestUpdateAndDropCoordinateShardLifetime(t *testing.T) {
	store := &blockingStore{
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
		closed:         make(chan struct{}),
		usedAfterClose: make(chan struct{}),
	}
	shardList := []*shard{{store: store, id: 0, repairState: &repair{}}}
	group := &groupShards{location: t.TempDir()}
	group.shards.Store(&shardList)
	var db database
	db.lfs = fs.NewLocalFileSystem()
	db.groups.Store("g", group)
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- db.Update(context.Background(), 0, []byte("id"), &propertyv1.Property{
			Metadata: &commonv1.Metadata{Group: "g"},
			Id:       "id",
		})
	}()
	<-store.entered
	dropDone := make(chan error, 1)
	go func() { dropDone <- db.Drop("g") }()
	select {
	case <-dropDone:
		t.Fatal("Drop closed a shard while Update was using it")
	case <-time.After(10 * time.Millisecond):
	}
	close(store.release)
	require.NoError(t, <-updateDone)
	require.NoError(t, <-dropDone)
	select {
	case <-store.usedAfterClose:
		t.Fatal("Update used a shard after Drop closed it")
	default:
	}
}

func TestRepairAndDropCoordinateShardLifetime(t *testing.T) {
	store := &blockingStore{
		entered:        make(chan struct{}),
		updateEntered:  make(chan struct{}),
		release:        make(chan struct{}),
		closed:         make(chan struct{}),
		usedAfterClose: make(chan struct{}),
	}
	shardList := []*shard{{store: store, id: 0, repairState: &repair{}}}
	group := &groupShards{location: t.TempDir()}
	group.shards.Store(&shardList)
	var db database
	db.lfs = fs.NewLocalFileSystem()
	db.groups.Store("g", group)
	repairDone := make(chan error, 1)
	go func() {
		repairDone <- db.Repair(context.Background(), []byte("id"), 0, &propertyv1.Property{
			Metadata: &commonv1.Metadata{Group: "g"},
			Id:       "id",
		}, 0)
	}()
	<-store.entered
	dropDone := make(chan error, 1)
	go func() { dropDone <- db.Drop("g") }()
	select {
	case <-dropDone:
		t.Fatal("Drop closed a shard while Repair was using it")
	case <-time.After(10 * time.Millisecond):
	}
	close(store.release)
	require.NoError(t, <-repairDone)
	require.NoError(t, <-dropDone)
	select {
	case <-store.usedAfterClose:
		t.Fatal("Repair used a shard after Drop closed it")
	default:
	}
}
