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

package sidx

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// minimalTestElements builds a tiny elements collection sufficient to flush a
// non-empty memPart to disk.
func minimalTestElements(t *testing.T) []testElement {
	t.Helper()
	return []testElement{
		{seriesID: common.SeriesID(1), userKey: 1, data: []byte("a")},
		{seriesID: common.SeriesID(1), userKey: 2, data: []byte("b")},
	}
}

// Test_partFlush_atomicManifest_noTmpLeftover asserts that after a successful
// part flush, no <file>.tmp sibling survives anywhere under the part directory.
// Sidx writes manifest.json (not metadata.json), via fs.MustFlushAtomic.
func Test_partFlush_atomicManifest_noTmpLeftover(t *testing.T) {
	tempDir := t.TempDir()
	testFS := fs.NewLocalFileSystem()

	elements := createTestElements(minimalTestElements(t))
	defer releaseElements(elements)

	mp := GenerateMemPart()
	defer ReleaseMemPart(mp)
	mp.mustInitFromElements(elements)

	partDir := filepath.Join(tempDir, "part_1")
	mp.mustFlush(testFS, partDir)

	require.NoError(t, filepath.Walk(tempDir, func(p string, info os.FileInfo, err error) error {
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

// Test_mustOpenPart_removesLeftoverTmp asserts that mustOpenPart removes a
// stray <file>.tmp sibling whose matching final exists.
func Test_mustOpenPart_removesLeftoverTmp(t *testing.T) {
	tempDir := t.TempDir()
	testFS := fs.NewLocalFileSystem()

	elements := createTestElements(minimalTestElements(t))
	defer releaseElements(elements)

	mp := GenerateMemPart()
	mp.mustInitFromElements(elements)
	partDir := filepath.Join(tempDir, "part_1")
	mp.mustFlush(testFS, partDir)
	ReleaseMemPart(mp)

	stray := filepath.Join(partDir, "manifest.json.tmp")
	require.NoError(t, os.WriteFile(stray, []byte("stale-leftover"), 0o600))

	p := mustOpenPart(1, partDir, testFS)
	defer p.close()

	_, statErr := os.Stat(stray)
	assert.Truef(t, os.IsNotExist(statErr), "expected stray .tmp removed, got err=%v", statErr)
}
