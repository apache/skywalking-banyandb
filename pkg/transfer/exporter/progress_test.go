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
	"bytes"
	"strings"
	"testing"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

// fakeClock is a settable now for a progress.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
func newFakeClock() *fakeClock               { return &fakeClock{t: time.Unix(1_800_000_000, 0)} }

// eraseLine is what the progress writes to erase one live line.
const eraseLine = "\x1b[1A\x1b[2K"

// testWidth is the node column of the nodes the tests export.
var testWidth = nodeWidth([]string{"data-hot1:17912", "data-a:1"})

// testProgress is a progress on out with a fake clock. The redraw ticker is not started, so
// a test drives redraws itself and owns the output.
func testProgress(size func() (int, int)) (*output.Progress, *bytes.Buffer, *fakeClock) {
	var out bytes.Buffer
	clock := newFakeClock()
	p := output.NewProgress(&out, size, "node(s) exporting")
	p.SetClock(clock.now)
	return p, &out, clock
}

// nodeOf shows the live line of a node exporting units units.
func nodeOf(p *output.Progress, id string, units int) *nodeProgress {
	return newNodeProgress(p, id, units, testWidth)
}

// terminalOf is a terminal of the given size.
func terminalOf(width, height int) func() (int, int) {
	return func() (int, int) { return width, height }
}

func segmentUnitOf(catalog commonv1.Catalog, group, suffix string) *plannedUnit {
	return &plannedUnit{segment: &transferv1.SegmentInventory{Unit: &transferv1.SegmentUnit{Catalog: catalog, Group: group, SegmentSuffix: suffix}}}
}

// Without a terminal every event that ends something prints a fixed line, and a unit start
// prints one too, so a log shows where a long export is.
func TestProgress_FixedLinesWithoutATerminal(t *testing.T) {
	p, out, clock := testProgress(nil)
	p.Printf("export session %s created", "feedfeed")
	n := nodeOf(p, "data-hot1:17912", 2)
	n.unitStarted(segmentUnitOf(commonv1.Catalog_CATALOG_MEASURE, "sw_metricsDay", "20261004"), 1, 2)
	n.fileStarted("measure/sw_metricsDay/seg-20261004/shard-0-00001.csv.gz", 0, 0, false)
	n.chunk(1<<20, 100)
	clock.advance(300 * time.Millisecond)
	n.fileFinished(map[string]uint64{"a": 100, "b": 80}, 4403)
	n.unitFinished(400 * time.Millisecond)
	n.unitStarted(&plannedUnit{property: &transferv1.PropertyInventory{Group: "sw_property"}}, 1, 0)
	n.fileStarted("property/sw_property/shard-0-00001.csv.gz", 0, 0, false)
	clock.advance(90 * time.Second)
	n.fileFinished(map[string]uint64{"ui": 136}, 82534)
	n.unitFinished(90 * time.Second)
	n.finish(true)
	idx := nodeOf(p, "data-a:1", 1)
	idx.unitStarted(segmentUnitOf(commonv1.Catalog_CATALOG_MEASURE, "sw_metadata", "20261004"), 0, 0)
	idx.finish(false)
	p.Close()

	want := "export session feedfeed created\n" +
		"  data-hot1:17912  [1/2] measure/sw_metricsDay seg-20261004 started: 1 shard(s), 2 part(s)\n" +
		"  data-hot1:17912  measure/sw_metricsDay/seg-20261004/shard-0-00001.csv.gz  180 rows  4.3 KiB  0.3s\n" +
		"✓ data-hot1:17912  [1/2] measure/sw_metricsDay seg-20261004  1 file(s)  180 rows  4.3 KiB  0.4s\n" +
		"  data-hot1:17912  [2/2] property/sw_property started: 1 shard(s)\n" +
		"  data-hot1:17912  property/sw_property/shard-0-00001.csv.gz  136 rows  80.6 KiB  1m30s\n" +
		"✓ data-hot1:17912  [2/2] property/sw_property  1 file(s)  136 rows  80.6 KiB  1m30s\n" +
		"✓ data-hot1:17912  done: 2 unit(s), 2 file(s), 316 rows, 84.9 KiB in 1m30s\n" +
		"  data-a:1         [1/1] measure/sw_metadata seg-20261004 started: no shards\n"
	if out.String() != want {
		t.Fatalf("got\n%s\nwant\n%s", out.String(), want)
	}
}

// On a terminal every node has a live line that is erased and drawn again on each redraw;
// fixed lines go above it. Erasing starts at the beginning of the line, whatever the terminal
// echoed after the live lines.
func TestProgress_LiveLinesOnATerminal(t *testing.T) {
	p, out, clock := testProgress(terminalOf(200, 50))
	n := nodeOf(p, "data-hot1:17912", 3)
	if want := "\r⠋ data-hot1:17912  waiting for the next unit\n"; out.String() != want {
		t.Fatalf("got %q, want %q", out.String(), want)
	}
	n.doneUnits = 1
	n.unitStarted(segmentUnitOf(commonv1.Catalog_CATALOG_TRACE, "sw_trace", "20261016"), 2, 6)
	n.fileStarted("trace/sw_trace/seg-20261016/shard-1-00001.csv.gz", 1, 1, false)
	clock.advance(2 * time.Second)
	n.chunk(1<<20, 3000)
	n.chunk(1<<20, 6000)
	out.Reset()
	p.Redraw()
	want := "\r" + eraseLine + "⠙ data-hot1:17912  [2/3] trace/sw_trace seg-20261016  shard 1 (2/2, 6 part(s))  6000 rows  2.0 MiB  1.0 MiB/s  2.0s\n"
	if out.String() != want {
		t.Fatalf("got %q, want %q", out.String(), want)
	}

	// A fixed line erases the live line, prints, and draws the live line again below it.
	out.Reset()
	n.fileFinished(map[string]uint64{"t": 6500}, 2<<20)
	got := out.String()
	if !strings.HasPrefix(got, "\r"+eraseLine+"  data-hot1:17912  trace/sw_trace/seg-20261016/shard-1-00001.csv.gz  6500 rows  2.0 MiB  2.0s\n⠙ ") ||
		!strings.Contains(got, "[2/3] trace/sw_trace seg-20261016  starting  6500 rows  2.0 MiB") {
		t.Fatalf("got %q", got)
	}

	// A finished unit's line is redrawn as waiting, not as the unit after the last.
	out.Reset()
	n.unitFinished(2 * time.Second)
	if got := out.String(); !strings.Contains(got, "✓ data-hot1:17912  [2/3] trace/sw_trace seg-20261016  1 file(s)  6500 rows  2.0 MiB  2.0s\n") ||
		!strings.HasSuffix(got, "  waiting for the next unit\n") {
		t.Fatalf("got %q", got)
	}

	// The segment-level file of an index-mode unit shows no shard.
	n.unitStarted(segmentUnitOf(commonv1.Catalog_CATALOG_MEASURE, "sw_metadata", "20261004"), 0, 0)
	n.fileStarted("measure/sw_metadata/seg-20261004/segment-00001.csv.gz", 0, 0, true)
	out.Reset()
	p.Redraw()
	if !strings.Contains(out.String(), "[3/3] measure/sw_metadata seg-20261004  segment index  0 rows  0 B") {
		t.Fatalf("got %q", out.String())
	}

	// Closing erases the live lines, so the summary starts on a clean line.
	out.Reset()
	p.Close()
	if out.String() != "\r"+eraseLine {
		t.Fatalf("got %q", out.String())
	}
}

// A live line never wraps: it is cut to the terminal width, and an unknown width counts as
// 80 columns.
func TestProgress_LiveLineFitsTheTerminal(t *testing.T) {
	p, out, _ := testProgress(terminalOf(20, 50))
	nodeOf(p, "data-hot1:17912", 1)
	if want := "\r⠋ data-hot1:17912  \n"; out.String() != want {
		t.Fatalf("got %q, want %q", out.String(), want)
	}

	unknown, out, _ := testProgress(terminalOf(0, 0))
	newNodeProgress(unknown, strings.Repeat("n", 100), 1, 100)
	if got := strings.TrimSuffix(strings.TrimPrefix(out.String(), "\r"), "\n"); len([]rune(got)) != 80-1 {
		t.Fatalf("got %d runes: %q", len([]rune(got)), got)
	}
}

// A node that fails loses its live line without a done line; the other nodes' lines stay.
func TestProgress_FailedNodeLeavesNoLine(t *testing.T) {
	p, out, _ := testProgress(terminalOf(200, 50))
	a := nodeOf(p, "data-a:1", 1)
	nodeOf(p, "data-hot1:17912", 1)
	a.unitStarted(segmentUnitOf(commonv1.Catalog_CATALOG_STREAM, "s", "20261019"), 1, 0)
	out.Reset()
	a.finish(false)
	got := out.String()
	if !strings.HasPrefix(got, "\r"+strings.Repeat(eraseLine, 2)) || strings.Contains(got, "data-a:1") || strings.Contains(got, "done") ||
		strings.Count(got, "\n") != 1 {
		t.Fatalf("got %q", got)
	}
}

// A node that does not report rows in its chunks shows the rows of its finished files.
func TestProgress_ChunksWithoutRows(t *testing.T) {
	p, _, _ := testProgress(terminalOf(200, 50))
	n := nodeOf(p, "data-hot1:17912", 1)
	n.unitStarted(segmentUnitOf(commonv1.Catalog_CATALOG_STREAM, "sw_records", "20261019"), 2, 0)
	n.fileStarted("stream/sw_records/seg-20261019/shard-0-00001.csv.gz", 0, 0, false)
	n.fileFinished(map[string]uint64{"r": 5}, 600)
	n.fileStarted("stream/sw_records/seg-20261019/shard-1-00001.csv.gz", 1, 1, false)
	n.chunk(100, 0)
	if got := n.LiveLine(p.Now()); !strings.Contains(got, "shard 1 (2/2)  5 rows  700 B") {
		t.Fatalf("got %q", got)
	}
}

// The rate is the unit's average, so chunks that arrive seconds apart under a low byte limit
// show the limit instead of alternating between a burst and zero.
func TestProgress_RateIsTheUnitAverage(t *testing.T) {
	p, _, clock := testProgress(terminalOf(200, 50))
	n := nodeOf(p, "n:1", 1)
	n.unitStarted(segmentUnitOf(commonv1.Catalog_CATALOG_STREAM, "s", "20261019"), 1, 0)
	n.fileStarted("stream/s/seg-20261019/shard-0-00001.csv.gz", 0, 0, false)
	if got := n.LiveLine(clock.now()); !strings.Contains(got, "  0 B/s  ") {
		t.Fatalf("rate at the start: %q", got)
	}
	for i := 0; i < 4; i++ {
		clock.advance(16 * time.Second)
		n.chunk(1<<20, 0)
		clock.advance(time.Second)
	}
	// 4 MiB over 68 s, though each chunk arrived in one go.
	if got := n.LiveLine(clock.now()); !strings.Contains(got, "  60.2 KiB/s  ") {
		t.Fatalf("got %q", got)
	}
}
