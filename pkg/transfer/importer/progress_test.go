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

package importer

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

// progressEntry is a measure work item of the hot stage.
func progressEntry(group string, shard uint32, rows uint64) Entry {
	return Entry{TargetStage: "hot", Item: WorkItem{
		Catalog: commonv1.Catalog_CATALOG_MEASURE, Group: group, Stage: "hot", Segment: "20261026", Shard: shard,
		Sources: []WorkSource{{Node: "n1", Rows: rows}},
	}}
}

func TestImportProgressPrintsItemAndGroupLines(t *testing.T) {
	var out bytes.Buffer
	entries := []Entry{progressEntry("sw_metric", 0, 30), progressEntry("sw_metric", 1, 20), progressEntry("other", 0, 5)}
	ip := newImportProgress(output.NewProgress(&out, nil, "group(s) importing"), entries)
	items := []*itemProgress{{}, {}}
	for i := range items {
		ip.itemStarted(&entries[i], items[i])
	}
	ip.acked(&entries[0], items[0], 30)
	ip.itemDone(&entries[0], items[0])
	ip.acked(&entries[1], items[1], 20)
	ip.itemDone(&entries[1], items[1])
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if assert.Len(t, lines, 3, out.String()) {
		assert.Contains(t, lines[0], "measure sw_metric/hot  [1/2] seg=20261026 shard=0  30 rows")
		assert.Contains(t, lines[1], "measure sw_metric/hot  [2/2] seg=20261026 shard=1  20 rows")
		assert.Contains(t, lines[2], "✓ measure sw_metric/hot  done: 2 work item(s), 50 rows in")
	}
}

func TestImportProgressLiveLineCountsTheGroup(t *testing.T) {
	var out bytes.Buffer
	entries := []Entry{progressEntry("sw_metric", 0, 30), progressEntry("sw_metric", 1, 20)}
	ip := newImportProgress(output.NewProgress(&out, func() (int, int) { return 200, 50 }, "group(s) importing"), entries)
	item := &itemProgress{}
	ip.itemStarted(&entries[0], item)
	ip.acked(&entries[0], item, 12)
	g := ip.groups[entryStreamKey(&entries[0])]
	line := g.LiveLine(g.start.Add(2 * time.Second))
	assert.Contains(t, line, "measure sw_metric/hot  12 rows written, 38 left  6.0 rows/s  2.0s")
	assert.Contains(t, out.String(), "measure sw_metric/hot", "the group's live line is drawn once its first item starts")
	// A rate below one row a second, as a low node rate limit shared by many groups gives, is not 0.
	assert.Contains(t, g.LiveLine(g.start.Add(32*time.Second)), "12 rows written, 38 left  0.4 rows/s  32.0s")
	// Rows acknowledged beyond the planned ones, a replay counted twice, never make the rows left negative.
	ip.acked(&entries[0], item, 60)
	assert.Contains(t, g.LiveLine(g.start.Add(4*time.Second)), "72 rows written, 0 left")
}

func TestImportProgressNilPrintsNothing(t *testing.T) {
	var ip *importProgress
	e := progressEntry("g", 0, 1)
	item := &itemProgress{}
	assert.NotPanics(t, func() {
		ip.itemStarted(&e, item)
		ip.acked(&e, item, 1)
		ip.itemDone(&e, item)
	})
}
