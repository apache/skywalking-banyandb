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

package storage

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/posting"
	"github.com/apache/skywalking-banyandb/pkg/posting/roaring"
)

func TestDB_Create_Directory(t *testing.T) {
	ctrl := gomock.NewController(t)
	p := NewMockPlugin(ctrl)
	p.EXPECT().Meta().Return(PluginMeta{
		ID:          "sw",
		Group:       "default",
		ShardNumber: 1,
		KVSpecs: []KVSpec{
			{
				Name:          "test",
				Type:          KVTypeTimeSeries,
				CompressLevel: 3,
			},
		},
	}).AnyTimes()
	p.EXPECT().Init(gomock.Any(), gomock.Any()).AnyTimes()
	tempDir, _ := setUp(t, p)
	defer removeDir(tempDir)
	shardPath := fmt.Sprintf(shardTemplate, tempDir+"/sw", 0)
	validateDirectory(t, shardPath)
	now := time.Now()
	segPath := fmt.Sprintf(segTemplate, shardPath, now.Format(segFormat))
	validateDirectory(t, segPath)
	validateDirectory(t, fmt.Sprintf(blockTemplate, segPath, now.Format(blockFormat)))
}

func TestDB_Store(t *testing.T) {
	is := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	now := uint64(time.Now().UnixNano())
	var ap WritePoint
	var repo StoreRepo
	p := mockPlugin(ctrl, func(r StoreRepo, get GetWritePoint) {
		ap = get(now)
		repo = r
	})

	tempDir, db := setUp(t, p)
	defer func() {
		db.GracefulStop()
		removeDir(tempDir)
	}()

	is.NoError(ap.Writer(0, "normal").Put([]byte("key1"), []byte{12}))
	val, err := repo.Reader(0, "normal", now, now).Get([]byte("key1"))
	is.NoError(err)
	is.Equal([]byte{12}, val)

	is.NoError(ap.TimeSeriesWriter(1, "time-series").Put([]byte("key11"), []byte{33}, 1))
	val, err = repo.TimeSeriesReader(1, "time-series", now, now).Get([]byte("key11"), 1)
	is.NoError(err)
	is.Equal([]byte{33}, val)
	vals, allErr := repo.TimeSeriesReader(1, "time-series", now, now).GetAll([]byte("key11"))
	is.NoError(allErr)
	is.Equal([][]byte{{33}}, vals)

	index := repo.Index(1, "index")
	is.NoError(index.Handover(mockMemtable([]uint64{1, 2}, []uint64{3, 6})))
	list, err := index.Seek(convert.Int64ToBytes(0), 2)
	is.NoError(err)
	is.Equal(2, list.Len())
	is.True(list.Contains(common.ChunkID(1)))
	is.True(list.Contains(common.ChunkID(2)))
	list, err = index.Seek(convert.Int64ToBytes(1), 2)
	is.NoError(err)
	is.Equal(2, list.Len())
	is.True(list.Contains(common.ChunkID(3)))
	is.True(list.Contains(common.ChunkID(6)))

	is.NoError(index.Handover(mockMemtable([]uint64{11, 14})))
	list, err = index.Seek(convert.Int64ToBytes(0), 2)
	is.NoError(err)
	is.Equal(4, list.Len())
	is.True(list.Contains(common.ChunkID(1)))
	is.True(list.Contains(common.ChunkID(2)))
	is.True(list.Contains(common.ChunkID(11)))
	is.True(list.Contains(common.ChunkID(14)))
}

func TestDB_FlushCallback(t *testing.T) {
	is := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	now := uint64(time.Now().UnixNano())
	var ap WritePoint
	//var repo StoreRepo
	p := NewMockPlugin(ctrl)
	latch := make(chan struct{})
	closed := false
	p.EXPECT().Meta().Return(PluginMeta{
		ID:          "sw",
		Group:       "default",
		ShardNumber: 2,
		KVSpecs: []KVSpec{
			{
				Name:       "normal",
				Type:       KVTypeNormal,
				BufferSize: 10 << 20,
				FlushCallback: func() {
					if closed {
						return
					}
					close(latch)
					closed = true
				},
			},
		},
	}).AnyTimes()
	p.EXPECT().Init(gomock.Any(), gomock.Any()).Do(func(r StoreRepo, wp GetWritePoint) {
		ap = wp(now)
	}).AnyTimes()

	tempDir, db := setUp(t, p)
	defer func() {
		db.GracefulStop()
		removeDir(tempDir)
	}()
	for i := 0; i < 5000; i++ {
		key := make([]byte, i)
		_ = ap.Writer(0, "normal").Put(key, []byte{1})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	select {
	case <-latch:
	case <-ctx.Done():
		is.Fail("timeout")
	}
}

var _ kv.Iterator = (*iter)(nil)

type iter struct {
	data map[int]posting.List
	p    int
}

func (i *iter) Next() {
	i.p++
}

func (i *iter) Rewind() {
	i.p = 0
}

func (i *iter) Seek(key []byte) {
	panic("implement me")
}

func (i *iter) Key() []byte {
	return convert.Int64ToBytes(int64(i.p))
}

func (i *iter) Val() posting.List {
	return i.data[i.p]
}

func (i *iter) Valid() bool {
	_, ok := i.data[i.p]
	return ok
}

func (i *iter) Close() error {
	return nil
}

func mockMemtable(data ...[]uint64) kv.Iterator {
	it := &iter{
		data: make(map[int]posting.List),
	}
	for i, d := range data {
		it.data[i] = roaring.NewPostingListWithInitialData(d...)
	}
	return it
}

func mockPlugin(ctrl *gomock.Controller, f func(repo StoreRepo, get GetWritePoint)) Plugin {
	p := NewMockPlugin(ctrl)
	p.EXPECT().Meta().Return(PluginMeta{
		ID:          "sw",
		Group:       "default",
		ShardNumber: 2,
		KVSpecs: []KVSpec{
			{
				Name: "normal",
				Type: KVTypeNormal,
			},
			{
				Name:          "time-series",
				Type:          KVTypeTimeSeries,
				CompressLevel: 3,
			},
			{
				Name: "index",
				Type: KVTypeIndex,
			},
		},
	}).AnyTimes()
	p.EXPECT().Init(gomock.Any(), gomock.Any()).Do(func(r StoreRepo, wp GetWritePoint) {
		f(r, wp)
	}).AnyTimes()
	return p
}

func setUp(t *testing.T, p Plugin) (tempDir string, db Database) {
	require.NoError(t, logger.Init(logger.Logging{
		Env:   "dev",
		Level: "debug",
	}))
	db, err := NewDB(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, db)

	var tempDirErr error
	tempDir, tempDirErr = ioutil.TempDir("", "banyandb-test-*")
	require.Nil(t, tempDirErr)

	require.NoError(t, db.FlagSet().Parse([]string{"--root-path", tempDir}))
	if p != nil {
		db.Register(p)
	}
	require.NoError(t, db.PreRun())
	go func() {
		require.NoError(t, db.Serve())
	}()
	return tempDir, db
}

func validateDirectory(t *testing.T, dir string) {
	info, err := os.Stat(dir)
	assert.False(t, os.IsNotExist(err), "Directory does not exist: %v", dir)
	assert.NoError(t, err, "Directory error: %v", dir)
	assert.True(t, info.IsDir(), "Directory is a file, not a directory: %#v\n", dir)
}

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		fmt.Printf("Error while removing dir: %v\n", err)
	}
}
