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

	"github.com/apache/skywalking-banyandb/pkg/logger"
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

	assert.NoError(t, ap.Writer(0, "normal").Put([]byte("key1"), []byte{12}))
	val, err := repo.Reader(0, "normal", now, now).Get([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte{12}, val)

	assert.NoError(t, ap.TimeSeriesWriter(1, "time-series").Put([]byte("key11"), []byte{33}, 1))
	val, err = repo.TimeSeriesReader(1, "time-series", now, now).Get([]byte("key11"), 1)
	assert.NoError(t, err)
	assert.Equal(t, []byte{33}, val)
	vals, allErr := repo.TimeSeriesReader(1, "time-series", now, now).GetAll([]byte("key11"))
	assert.NoError(t, allErr)
	assert.Equal(t, [][]byte{{33}}, vals)
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
