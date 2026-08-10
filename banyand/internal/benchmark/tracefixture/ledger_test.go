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

package tracefixture

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogicalLedgerSnapshotExcludesDroppedTraces(t *testing.T) {
	var retainedRow, droppedRow [sha256.Size]byte
	retainedRow[0] = 1
	droppedRow[0] = 2
	snapshot := &LogicalLedgerSnapshot{ledgers: map[string]ledgerHashes{
		"core":       {"retained": {retainedRow}, "dropped": {droppedRow}},
		"latency":    {"retained": {retainedRow}, "dropped": {droppedRow}},
		"start_time": {"retained": {retainedRow}, "dropped": {droppedRow}},
	}}

	selection := snapshot.Excluding(map[string]struct{}{"dropped": {}})

	require.Equal(t, map[string]uint64{"core": 1, "latency": 1, "start_time": 1}, selection.Rows)
	require.Len(t, selection.Checksums, 3)
	require.NotEqual(t, snapshot.Excluding(nil).Checksums["core"], selection.Checksums["core"])
}
