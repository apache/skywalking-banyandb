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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3/y"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestDB_Create_Directory(t *testing.T) {
	tempDir, _ := setUp(t, nil)
	defer removeDir(tempDir)
	shardPath := fmt.Sprintf(shardTemplate, tempDir, 0)
	validateDirectory(t, shardPath)
	now := time.Now()
	segPath := fmt.Sprintf(segTemplate, shardPath, now.Format(segFormat))
	validateDirectory(t, segPath)
	validateDirectory(t, fmt.Sprintf(blockTemplate, segPath, now.Format(blockFormat)))
}

func TestDB_Store(t *testing.T) {
	p := new(mockPlugin)
	tempDir, db := setUp(t, p)
	defer func() {
		db.GracefulStop()
		removeDir(tempDir)
	}()
	ap := p.ApFunc()
	s := ap.Store("normal")
	assert.NoError(t, s.Put([]byte("key1"), []byte{12}))
	val, err := s.Get([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte{12}, val)

	s = ap.Store("auto-gen")
	key, addErr := s.Add([]byte{11})
	assert.NoError(t, addErr)
	val, err = s.Get(convert.Uint64ToBytes(key))
	assert.NoError(t, err)
	assert.Equal(t, []byte{11}, val)

	tss := ap.TimeSeriesStore("time-series")
	assert.NoError(t, tss.Put([]byte("key11"), []byte{33}, 1))
	val, err = tss.Get([]byte("key11"), 1)
	assert.NoError(t, err)
	assert.Equal(t, []byte{33}, val)
	vals, allErr := tss.GetAll([]byte("key11"))
	assert.NoError(t, allErr)
	assert.Equal(t, [][]byte{{33}}, vals)
}

func setUp(t *testing.T, p *mockPlugin) (tempDir string, db Database) {
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

	require.NoError(t, db.FlagSet().Parse([]string{"--root-path", tempDir, "--shards", "1"}))
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

var _ Plugin = (*mockPlugin)(nil)

type mockPlugin struct {
	ApFunc GetAccessPoint
}

func (m *mockPlugin) ID() string {
	return "foo"
}

func (m *mockPlugin) Init() []KVSpec {
	return []KVSpec{
		{
			Name: "normal",
			Type: KVTypeNormal,
		},
		{
			Name:       "auto-gen",
			Type:       KVTypeNormal,
			AutoGenKey: true,
		},
		{
			Name:           "time-series",
			Type:           KVTypeTimeSeries,
			TimeSeriesHook: &mockHook{},
		},
	}
}

func (m *mockPlugin) Start(point GetAccessPoint) {
	m.ApFunc = point
}

var _ kv.Hook = (*mockHook)(nil)

type mockHook struct {
}

func (m *mockHook) Reduce(left bytes.Buffer, right y.ValueStruct) bytes.Buffer {
	return left
}

func (m *mockHook) Extract(raw []byte, ts uint64) ([]byte, error) {
	return raw, nil
}

func (m *mockHook) Split(raw []byte) ([][]byte, error) {
	return [][]byte{raw}, nil
}
