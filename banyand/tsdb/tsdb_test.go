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
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestOpenDatabase(t *testing.T) {
	tester := assert.New(t)
	tempDir, _ := setUp(tester)
	defer removeDir(tempDir)
	shardPath := fmt.Sprintf(shardTemplate, tempDir, 0)
	validateDirectory(tester, shardPath)
	seriesPath := fmt.Sprintf(seriesTemplate, shardPath)
	validateDirectory(tester, seriesPath)
	now := time.Now()
	segPath := fmt.Sprintf(segTemplate, seriesPath, now.Format(segFormat))
	validateDirectory(tester, segPath)
	validateDirectory(tester, fmt.Sprintf(blockTemplate, segPath, now.Format(blockFormat)))
}

func setUp(t *assert.Assertions) (tempDir string, db Database) {
	t.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "debug",
	}))
	var tempDirErr error
	tempDir, tempDirErr = ioutil.TempDir("", "banyandb-test-*")
	t.Nil(tempDirErr)
	db, err := OpenDatabase(
		context.WithValue(context.Background(), logger.ContextKey, logger.GetLogger("test")),
		DatabaseOpts{
			Location: tempDir,
			ShardNum: 1,
		})
	t.NoError(err)
	t.NotNil(db)
	return tempDir, db
}

func validateDirectory(t *assert.Assertions, dir string) {
	info, err := os.Stat(dir)
	t.False(os.IsNotExist(err), "Directory does not exist: %v", dir)
	t.NoError(err, "Directory error: %v", dir)
	t.True(info.IsDir(), "Directory is a file, not a directory: %#v\n", dir)
}

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		fmt.Printf("Error while removing dir: %v\n", err)
	}
}
