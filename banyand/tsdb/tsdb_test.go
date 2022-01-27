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

package tsdb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func TestOpenDatabase(t *testing.T) {
	tester := assert.New(t)
	req := require.New(t)
	tempDir, deferFunc := test.Space(req)
	openDatabase(req, tempDir)
	defer deferFunc()
	verifyDatabaseStructure(tester, tempDir)
}

func TestReOpenDatabase(t *testing.T) {
	tester := assert.New(t)
	req := require.New(t)
	tempDir, deferFunc := test.Space(req)
	defer deferFunc()
	db := openDatabase(req, tempDir)
	req.NoError(db.Close())
	verifyDatabaseStructure(tester, tempDir)
	db = openDatabase(req, tempDir)
	req.NoError(db.Close())
	verifyDatabaseStructure(tester, tempDir)
}

func verifyDatabaseStructure(tester *assert.Assertions, tempDir string) {
	shardPath := fmt.Sprintf(shardTemplate, tempDir, 0)
	validateDirectory(tester, shardPath)
	seriesPath := fmt.Sprintf(seriesTemplate, shardPath)
	validateDirectory(tester, seriesPath)
	now := time.Now()
	segPath := fmt.Sprintf(segTemplate, shardPath, now.Format(segDayFormat))
	validateDirectory(tester, segPath)
	validateDirectory(tester, fmt.Sprintf(blockTemplate, segPath, now.Format(blockHourFormat)))
}

func openDatabase(t *require.Assertions, path string) (db Database) {
	t.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "warn",
	}))
	db, err := OpenDatabase(
		context.WithValue(context.Background(), logger.ContextKey, logger.GetLogger("test")),
		DatabaseOpts{
			Location: path,
			ShardNum: 1,
			EncodingMethod: EncodingMethod{
				EncoderPool: encoding.NewPlainEncoderPool(0),
				DecoderPool: encoding.NewPlainDecoderPool(0),
			},
		})
	t.NoError(err)
	t.NotNil(db)
	return db
}

func validateDirectory(t *assert.Assertions, dir string) {
	info, err := os.Stat(dir)
	t.False(os.IsNotExist(err), "Directory does not exist: %v", dir)
	t.NoError(err, "Directory error: %v", dir)
	t.True(info.IsDir(), "Directory is a file, not a directory: %#v\n", dir)
}
