// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package measure

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// Test_partFlush_atomicMetadata_noTmpLeftover asserts that after a successful
// part flush, no <file>.tmp sibling survives anywhere under the part directory.
// This is the post-condition of mustWriteMetadata going through
// fileSystem.WriteAtomic.
func Test_partFlush_atomicMetadata_noTmpLeftover(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	epoch := uint64(1)

	mp := generateMemPart()
	defer releaseMemPart(mp)
	mp.mustInitFromDataPoints(dps)
	mp.mustFlush(fileSystem, partPath(tmpPath, epoch))

	require.NoError(t, filepath.Walk(tmpPath, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		assert.Falsef(t, strings.HasSuffix(p, ".tmp"), "leftover tmp at %s", p)
		return nil
	}))
}

// Test_mustOpenFilePart_removesLeftoverTmp asserts that mustOpenFilePart
// removes a stray <file>.tmp sibling whose matching final exists.
func Test_mustOpenFilePart_removesLeftoverTmp(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	epoch := uint64(1)

	mp := generateMemPart()
	mp.mustInitFromDataPoints(dps)
	mp.mustFlush(fileSystem, partPath(tmpPath, epoch))
	releaseMemPart(mp)

	pp := partPath(tmpPath, epoch)
	stray := filepath.Join(pp, "metadata.json.tmp")
	require.NoError(t, os.WriteFile(stray, []byte("stale-leftover"), 0o600))

	p := mustOpenFilePart(epoch, tmpPath, fileSystem)
	defer p.close()

	_, statErr := os.Stat(stray)
	assert.Truef(t, os.IsNotExist(statErr), "expected stray .tmp removed, got err=%v", statErr)
}
