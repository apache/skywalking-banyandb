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

package trace

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// Test_partFlush_atomicMetadata_noTmpLeftover asserts that after a successful
// part flush, no <file>.tmp sibling survives anywhere under the part directory.
// This is the post-condition of mustWriteMetadata, mustWriteTraceIDFilter, and
// mustWriteTagType all going through fileSystem.WriteAtomic.
func Test_partFlush_atomicMetadata_noTmpLeftover(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	epoch := uint64(1)

	mp := generateMemPart()
	defer releaseMemPart(mp)
	mp.mustInitFromTraces(ts)
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
// removes a stray <file>.tmp sibling whose matching final exists. This is the
// post-rename cleanup hook into fs.CleanupLeftoverTmp. Trace writes three
// metadata files via WriteAtomic — metadata.json, traceID.filter, tag.type —
// so all three .tmp shapes must be cleaned up on open.
func Test_mustOpenFilePart_removesLeftoverTmp(t *testing.T) {
	cases := []string{"metadata.json.tmp", "traceID.filter.tmp", "tag.type.tmp"}
	for _, strayName := range cases {
		t.Run(strayName, func(t *testing.T) {
			tmpPath, defFn := test.Space(require.New(t))
			defer defFn()
			fileSystem := fs.NewLocalFileSystem()
			epoch := uint64(1)

			mp := generateMemPart()
			mp.mustInitFromTraces(ts)
			mp.mustFlush(fileSystem, partPath(tmpPath, epoch))
			releaseMemPart(mp)

			pp := partPath(tmpPath, epoch)
			// Skip the case where the matching final file doesn't exist on
			// disk for this fixture — without a healthy <file>, the cleanup
			// helper is correct to leave the .tmp in place.
			final := strings.TrimSuffix(strayName, ".tmp")
			if _, err := os.Stat(filepath.Join(pp, final)); err != nil {
				t.Skipf("fixture did not produce %s; cleanup behavior covered by other cases", final)
			}
			stray := filepath.Join(pp, strayName)
			require.NoError(t, os.WriteFile(stray, []byte("stale-leftover"), 0o600))

			p := mustOpenFilePart(epoch, tmpPath, fileSystem)
			defer p.close()

			_, statErr := os.Stat(stray)
			assert.Truef(t, os.IsNotExist(statErr), "expected %s removed, got err=%v", strayName, statErr)
		})
	}
}

// Test_merger_tornSpansBin_stillPanics is the reproducer described in the body
// of apache/skywalking#13862. It forges the production-shape on-disk state —
// a part directory whose metadata.json claims N blocks but whose spans.bin is
// truncated — and feeds it to mergeParts. The expected outcome under the
// merge-durability fix is *unchanged* from before the fix: the merger panics
// with "offset must be equal to bytesRead", because that is BanyanDB's
// canonical fail-fast contract for a corrupt source part.
//
// The fix prevents torn parts from being CREATED via a clean-crash path
// (atomic metadata commit makes that impossible). It does NOT prevent the
// panic when an externally-corrupt part is forced onto disk (rsync, manual
// file copy, host disk failure, pre-fix-deployment leftovers). This test
// guards against accidental panic suppression by future refactors.
func Test_merger_tornSpansBin_stillPanics(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	// Build two healthy parts with multiple blocks.
	mp1 := generateMemPart()
	mp1.mustInitFromTraces(ts)
	mp1.mustFlush(fileSystem, partPath(tmpPath, 1))
	releaseMemPart(mp1)

	mp2 := generateMemPart()
	mp2.mustInitFromTraces(ts)
	mp2.mustFlush(fileSystem, partPath(tmpPath, 2))
	releaseMemPart(mp2)

	// Forge the torn shape: truncate part 1's spans.bin so primary.bin's
	// block-metadata claims more bytes than spans.bin actually contains.
	spansPath := filepath.Join(partPath(tmpPath, 1), "spans.bin")
	stat, err := os.Stat(spansPath)
	require.NoError(t, err)
	require.Greater(t, stat.Size(), int64(0), "test fixture must produce non-empty spans.bin")
	tornAt := stat.Size() - 1
	require.NoError(t, os.Truncate(spansPath, tornAt))

	// mustOpenFilePart succeeds because metadata.json is intact — the
	// truncation is only in spans.bin.
	p1 := mustOpenFilePart(1, tmpPath, fileSystem)
	p1.partMetadata.ID = 1
	p2 := mustOpenFilePart(2, tmpPath, fileSystem)
	p2.partMetadata.ID = 2

	tst := &tsTable{pm: protector.Nop{}, fileSystem: fileSystem, root: tmpPath}
	closeCh := make(chan struct{})
	defer close(closeCh)

	defer func() {
		r := recover()
		// Release fds even on the panic path. mustOpenFilePart opens
		// many underlying files; without explicit close here the panic
		// would leak them for the lifetime of the test process.
		p1.close()
		p2.close()
		require.NotNilf(t, r, "merger must panic on torn spans.bin (canonical fail-fast contract)")
		// Accept any panic from the merger's read path. The exact message
		// depends on which block boundary the truncation hit; valid shapes:
		//   "offset N must be equal to bytesRead M"
		//   "cannot read full data: ..."
		//   "cannot read data: ..."
		//   "cannot decode spans: ..."
		got := fmt.Sprint(r)
		t.Logf("captured panic: %s", got)
		valid := strings.Contains(got, "offset") ||
			strings.Contains(got, "cannot read") ||
			strings.Contains(got, "cannot decode") ||
			strings.Contains(got, "EOF")
		assert.Truef(t, valid, "panic message did not match canonical fail-fast shapes: %s", got)
	}()

	_, _ = tst.mergeParts(fileSystem, closeCh, []*partWrapper{
		newPartWrapper(nil, p1),
		newPartWrapper(nil, p2),
	}, 99, tmpPath)

	t.Fatal("mergeParts returned without panicking — torn-part fail-fast contract is broken")
}
