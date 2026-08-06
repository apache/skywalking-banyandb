// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func TestMergeControl_TriggerAndWaitForIdle(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, cleanup := test.Space(require.New(t))
	t.Cleanup(cleanup)

	tableRoot := filepath.Join(tmpPath, "table")
	fileSystem.MkdirPanicIfExist(tableRoot, 0o755)
	tst, tableErr := newTSTable(
		fileSystem,
		tableRoot,
		common.Position{Database: "merge-control"},
		logger.GetLogger("merge-control"),
		timestamp.TimeRange{},
		option{
			flushTimeout: 0,
			mergePolicy:  newDefaultMergePolicyForTesting(),
			protector:    protector.Nop{},
		},
		nil,
	)
	require.NoError(t, tableErr)
	t.Cleanup(func() { require.NoError(t, tst.Close()) })
	tst.observePartID(3)

	for partID := uint64(1); partID <= 3; partID++ {
		traceSet := &traces{
			traceIDs:   []string{fmt.Sprintf("trace-%d", partID)},
			timestamps: []int64{int64(partID)},
			spanIDs:    []string{"span"},
			spans:      [][]byte{[]byte("payload")},
			tags:       [][]*tagValue{{}},
		}
		memPart := generateMemPart()
		memPart.mustInitFromTraces(traceSet)
		memPart.mustFlush(fileSystem, partPath(tableRoot, partID))
		releaseMemPart(memPart)
		tst.mustAddFilePart(partID, nil)
	}

	require.NoError(t, tst.triggerMerge())
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, tst.waitForMergeIdle(waitCtx))

	current := tst.currentSnapshot()
	require.NotNil(t, current)
	defer current.decRef()
	require.Len(t, current.parts, 1, "idle is reached only after all policy-selectable work has completed")
	require.Nil(t, current.parts[0].mp)
	require.Greater(t, current.parts[0].ID(), uint64(3))

	tst.inFlightMu.RLock()
	require.Empty(t, tst.inFlight)
	tst.inFlightMu.RUnlock()
}
