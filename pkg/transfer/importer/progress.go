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
	"fmt"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

// importProgress prints the progress of a push per (catalog, group, target stage): on a
// terminal a live line for every one being imported, and a fixed line for every work item and
// every group done. A nil importProgress prints nothing.
type importProgress struct {
	p      *output.Progress
	groups map[streamKey]*groupProgress
	nameW  int
}

// groupProgress is the import of one (catalog, group, target stage).
type groupProgress struct {
	start time.Time
	ip    *importProgress
	name  string
	rows  uint64
	// writtenRows are the rows the data nodes acknowledged.
	writtenRows uint64
	items       int
	doneItems   int
}

func newImportProgress(p *output.Progress, entries []Entry) *importProgress {
	ip := &importProgress{p: p, groups: map[streamKey]*groupProgress{}}
	for i := range entries {
		e := &entries[i]
		k := entryStreamKey(e)
		g, ok := ip.groups[k]
		if !ok {
			name := e.Item.CatalogName() + " " + e.Item.Group
			if e.TargetStage != "" {
				name += "/" + e.TargetStage
			}
			g = &groupProgress{ip: ip, name: name}
			ip.groups[k] = g
			ip.nameW = max(ip.nameW, len(name))
		}
		g.items++
		g.rows += e.Item.Rows()
	}
	return ip
}

// itemStarted starts the clock of the item, whose state its worker owns, and shows the live
// line of its group once the group's first item starts.
func (ip *importProgress) itemStarted(e *Entry, item *itemProgress) {
	if ip == nil {
		return
	}
	item.start = ip.p.Now()
	g := ip.groups[entryStreamKey(e)]
	first := false
	ip.p.Update(func() string {
		if g.start.IsZero() {
			g.start, first = item.start, true
		}
		return ""
	})
	if first {
		ip.p.Add(g)
	}
}

// acked counts the rows of an acknowledged batch of the item as written.
func (ip *importProgress) acked(e *Entry, item *itemProgress, rows int) {
	if ip == nil {
		return
	}
	item.rows += uint64(rows)
	ip.p.Update(func() string {
		ip.groups[entryStreamKey(e)].writtenRows += uint64(rows)
		return ""
	})
}

// itemDone prints the item's done line and, after the last item of its group, the group's.
func (ip *importProgress) itemDone(e *Entry, item *itemProgress) {
	if ip == nil {
		return
	}
	g := ip.groups[entryStreamKey(e)]
	var groupLine string
	ip.p.Update(func() string {
		now := ip.p.Now()
		g.doneItems++
		if g.doneItems == g.items {
			groupLine = fmt.Sprintf("✓ %-*s  done: %d work item(s), %d rows in %s\n", ip.nameW, g.name, g.items, g.writtenRows,
				output.FormatElapsed(now.Sub(g.start)))
		}
		return fmt.Sprintf("  %-*s  [%d/%d] %s  %d rows  %s\n", ip.nameW, g.name, g.doneItems, g.items, e.Item.position(),
			item.rows, output.FormatElapsed(now.Sub(item.start)))
	})
	if groupLine != "" {
		ip.p.Remove(g, groupLine)
	}
}

// LiveLine is the group's line in the live area. The caller holds the progress locked.
func (g *groupProgress) LiveLine(now time.Time) string {
	elapsed := now.Sub(g.start)
	var rate float64
	if elapsed > 0 {
		rate = float64(g.writtenRows) / elapsed.Seconds()
	}
	return fmt.Sprintf("%s %-*s  %d rows written, %d left  %s rows/s  %s", g.ip.p.Spinner(), g.ip.nameW, g.name,
		g.writtenRows, g.rows-min(g.writtenRows, g.rows), output.HumanRate(rate), output.FormatElapsed(elapsed))
}
