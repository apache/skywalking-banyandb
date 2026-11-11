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

package stream

import (
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// TestFlushOfClosingTableLeavesNoParts pins that a flush the closing table will never introduce leaves no part on
// disk. Left behind, a part directory collides with the next flush of the same memory part.
func TestFlushOfClosingTableLeavesNoParts(t *testing.T) {
	for name, closeAfter := range map[string]time.Duration{
		"closed before the flush":           0,
		"closed while waiting to introduce": 200 * time.Millisecond,
	} {
		t.Run(name, func(t *testing.T) {
			tmpPath, defFn := test.Space(require.New(t))
			defer defFn()

			mp := generateMemPart()
			mp.mustInitFromElements(esTS1)
			pw := newPartWrapper(mp, openMemPart(mp))
			pw.p.partMetadata.ID = 7
			defer pw.decRef()

			tst := &tsTable{
				pm: protector.Nop{}, fileSystem: fs.NewLocalFileSystem(), root: tmpPath,
				l: logger.GetLogger("test"), loopCloser: run.NewCloser(1),
			}
			closeLoops := func() {
				tst.loopCloser.Done()
				tst.loopCloser.CloseThenWait()
			}
			if closeAfter == 0 {
				closeLoops()
			} else {
				time.AfterFunc(closeAfter, closeLoops)
			}
			// No introducer reads the channel, so the flush can only end on the closed table.
			tst.flush(&snapshot{parts: []*partWrapper{pw}}, make(chan *flusherIntroduction))

			entries, readErr := os.ReadDir(tmpPath)
			require.NoError(t, readErr)
			partDir := regexp.MustCompile("^[0-9a-f]{16}$")
			for _, entry := range entries {
				require.False(t, partDir.MatchString(entry.Name()), "flush left part %s behind", entry.Name())
			}
		})
	}
}
