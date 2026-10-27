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

package exporter

import (
	"fmt"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

// nodeProgress is the progress of one node's export. doneUnits, files, rows and bytes add up
// the finished units, the unit* fields the finished files of the current unit, and the file*
// fields the open file.
type nodeProgress struct {
	start     time.Time
	unitStart time.Time
	fileStart time.Time
	p         *output.Progress
	// width is the node column's width, the longest node id.
	unit      *plannedUnit
	node      string
	file      string
	where     string // the open file's shard, "" before the unit's first file
	bytes     uint64
	unitBytes uint64
	files     uint64
	rows      uint64
	doneUnits int
	unitFiles int
	unitRows  uint64
	units     int
	fileRows  uint64
	fileBytes uint64
	width     int
	shards    uint32
	parts     uint32
}

// nodeWidth is the width of a node column holding ids.
func nodeWidth(ids []string) int {
	w := 0
	for _, id := range ids {
		w = max(w, len(id))
	}
	return w
}

// newNodeProgress registers a node that exports units units, in a node column width wide,
// and shows its live line.
func newNodeProgress(p *output.Progress, id string, units, width int) *nodeProgress {
	n := &nodeProgress{p: p, node: id, units: units, width: width, start: p.Now()}
	p.Add(n)
	return n
}

// unitStarted opens the node's next unit.
func (n *nodeProgress) unitStarted(u *plannedUnit, shards, parts uint32) {
	n.p.Update(func() string {
		n.unit, n.shards, n.parts = u, shards, parts
		n.unitStart = n.p.Now()
		n.unitFiles, n.unitRows, n.unitBytes = 0, 0, 0
		if n.p.Terminal() {
			return ""
		}
		layout := "no shards"
		if shards > 0 {
			layout = fmt.Sprintf("%d shard(s)%s", shards, partsSuffix(parts))
		}
		return fmt.Sprintf("  %-*s  [%d/%d] %s started: %s\n", n.width, n.node, n.doneUnits+1, n.units, u.displayName(), layout)
	})
}

// partsSuffix is ", <n> part(s)", or "" for a unit without parts.
func partsSuffix(parts uint32) string {
	if parts == 0 {
		return ""
	}
	return fmt.Sprintf(", %d part(s)", parts)
}

// fileStarted opens a file of the current unit; shardIndex is the shard's position in the
// unit.
func (n *nodeProgress) fileStarted(rel string, shardID, shardIndex uint32, segmentLevel bool) {
	n.p.Update(func() string {
		n.file = rel
		n.fileStart = n.p.Now()
		n.where = "segment index"
		if !segmentLevel {
			n.where = fmt.Sprintf("shard %d (%d/%d%s)", shardID, shardIndex+1, n.shards, partsSuffix(n.parts))
		}
		return ""
	})
}

// chunk counts a received chunk of size bytes; rows is what the node has written into the
// file so far.
func (n *nodeProgress) chunk(size int, rows uint64) {
	n.p.Update(func() string {
		n.fileBytes += uint64(size)
		n.fileRows = rows
		return ""
	})
}

// fileFinished closes the open file with its final rows per resource and size.
func (n *nodeProgress) fileFinished(rowCounts map[string]uint64, size uint64) {
	var rows uint64
	for _, c := range rowCounts {
		rows += c
	}
	n.p.Update(func() string {
		n.unitFiles++
		n.unitRows += rows
		n.unitBytes += size
		fixed := fmt.Sprintf("  %-*s  %s  %d rows  %s  %s\n", n.width, n.node, n.file, rows, output.HumanBytes(size),
			output.FormatElapsed(n.p.Now().Sub(n.fileStart)))
		n.file, n.where, n.fileRows, n.fileBytes = "", "", 0, 0
		return fixed
	})
}

// unitFinished closes the current unit, which took elapsed.
func (n *nodeProgress) unitFinished(elapsed time.Duration) {
	n.p.Update(func() string {
		n.doneUnits++
		n.files += uint64(n.unitFiles)
		n.rows += n.unitRows
		n.bytes += n.unitBytes
		fixed := fmt.Sprintf("✓ %-*s  [%d/%d] %s  %d file(s)  %d rows  %s  %s\n", n.width, n.node, n.doneUnits, n.units, n.unit.displayName(),
			n.unitFiles, n.unitRows, output.HumanBytes(n.unitBytes), output.FormatElapsed(elapsed))
		n.unit = nil
		return fixed
	})
}

// finish removes the live line; a node that exported every unit gets its done line. Only the
// node's worker changes its fields, so it reads them unlocked.
func (n *nodeProgress) finish(ok bool) {
	fixed := ""
	if ok {
		fixed = fmt.Sprintf("✓ %-*s  done: %d unit(s), %d file(s), %d rows, %s in %s\n", n.width, n.node, n.doneUnits, n.files, n.rows,
			output.HumanBytes(n.bytes), output.FormatElapsed(n.p.Now().Sub(n.start)))
	}
	n.p.Remove(n, fixed)
}

// LiveLine is the node's line in the live area; the progress calls it locked.
func (n *nodeProgress) LiveLine(now time.Time) string {
	spin := n.p.Spinner()
	if n.unit == nil {
		return fmt.Sprintf("%s %-*s  waiting for the next unit", spin, n.width, n.node)
	}
	where := n.where
	if where == "" {
		where = "starting"
	}
	bytes := n.unitBytes + n.fileBytes
	elapsed := now.Sub(n.unitStart)
	var rate uint64
	if elapsed > 0 {
		rate = uint64(float64(bytes) / elapsed.Seconds())
	}
	return fmt.Sprintf("%s %-*s  [%d/%d] %s  %s  %d rows  %s  %s/s  %s", spin, n.width, n.node, n.doneUnits+1, n.units, n.unit.displayName(), where,
		n.unitRows+n.fileRows, output.HumanBytes(bytes), output.HumanBytes(rate), output.FormatElapsed(elapsed))
}
