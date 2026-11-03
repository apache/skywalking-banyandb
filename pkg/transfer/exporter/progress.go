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
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	// progressRedraw is how often the live lines are redrawn on a terminal.
	progressRedraw = 200 * time.Millisecond
	// eraseLine moves the cursor up one line and clears it.
	eraseLine = "\x1b[1A\x1b[2K"
	// defaultWidth stands in for a terminal whose size is unknown.
	defaultWidth = 80
)

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// progress prints what the node workers report: a fixed line for every finished file, unit
// and node and, on a terminal, one live line per exporting node that is redrawn in place.
// Without a terminal it prints the fixed lines only, plus one when a unit starts.
type progress struct {
	w     io.Writer
	size  func() (width, height int) // nil when w is not a terminal; 0 for an unknown dimension
	now   func() time.Time
	stop  chan struct{}
	done  chan struct{}
	live  []*nodeProgress
	mu    sync.Mutex
	nodeW int
	drawn int
	frame int
}

// newProgress prints to w; size is nil when w is not a terminal.
func newProgress(w io.Writer, size func() (width, height int)) *progress {
	return &progress{w: w, size: size, now: time.Now}
}

// terminal reports whether live lines are drawn.
func (p *progress) terminal() bool { return p.size != nil }

// setNodes sizes the node column for the given node ids.
func (p *progress) setNodes(ids []string) {
	for _, id := range ids {
		p.nodeW = max(p.nodeW, len(id))
	}
}

// start begins redrawing the live lines every progressRedraw on a terminal, until close or
// the end of ctx.
func (p *progress) start(ctx context.Context) {
	if !p.terminal() {
		return
	}
	p.stop, p.done = make(chan struct{}), make(chan struct{})
	run.Go(ctx, "export-progress-redraw", logger.GetLogger("exporter"), func(ctx context.Context) {
		defer close(p.done)
		ticker := time.NewTicker(progressRedraw)
		defer ticker.Stop()
		for {
			select {
			case <-p.stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.redraw()
			}
		}
	})
}

// close stops the redraws and erases the live lines left by workers that did not finish.
func (p *progress) close() {
	if p.stop != nil {
		close(p.stop)
		<-p.done
		p.stop = nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.live = nil
	p.write("")
}

// printf prints a fixed line.
func (p *progress) printf(format string, args ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.write(fmt.Sprintf(format, args...) + "\n")
}

func (p *progress) redraw() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.frame++
	p.write("")
}

// write erases the live lines, prints fixed and draws the live lines again, in one Write
// so the terminal never shows a half-drawn screen. The caller holds mu.
func (p *progress) write(fixed string) {
	if !p.terminal() {
		if fixed != "" {
			_, _ = io.WriteString(p.w, fixed)
		}
		return
	}
	var b strings.Builder
	b.WriteString(p.erase())
	b.WriteString(fixed)
	width, height := p.size()
	if width <= 0 {
		width = defaultWidth
	}
	// Erasing moves up one row per drawn line, which only works while every drawn line is
	// still on screen: keep the live area below the terminal height and on one row per line.
	live := p.live
	more := 0
	if height > 1 && len(live) > height-1 {
		live, more = live[:height-2], len(live)-(height-2)
	}
	now := p.now()
	for _, n := range live {
		b.WriteString(truncate(n.liveLine(now), width))
		b.WriteByte('\n')
	}
	if more > 0 {
		b.WriteString(truncate(fmt.Sprintf("  ... %d more node(s) exporting", more), width))
		b.WriteByte('\n')
	}
	p.drawn = len(live)
	if more > 0 {
		p.drawn++
	}
	_, _ = io.WriteString(p.w, b.String())
}

// erase returns the sequence that clears the drawn live lines. It starts at the beginning of
// the line, as the terminal may have echoed something (^C) after the live lines.
func (p *progress) erase() string {
	return "\r" + strings.Repeat(eraseLine, p.drawn)
}

// truncate cuts s to fit a terminal width columns wide, so a long line never wraps and
// breaks the erasing of the live lines.
func truncate(s string, width int) string {
	if width <= 1 || utf8.RuneCountInString(s) < width {
		return s
	}
	runes := []rune(s)
	return string(runes[:width-1])
}

// wrap returns a writer that prints through the progress, so that lines written to another
// stream of the same terminal (warnings on stderr) do not tear the live lines.
func (p *progress) wrap(w io.Writer) io.Writer {
	return writerFunc(func(b []byte) (int, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if !p.terminal() {
			return w.Write(b)
		}
		_, _ = io.WriteString(p.w, p.erase())
		p.drawn = 0
		n, err := w.Write(b)
		p.write("")
		return n, err
	})
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(b []byte) (int, error) { return f(b) }

// nodeProgress is the progress of one node's export. doneUnits, files, rows and bytes add up
// the finished units, the unit* fields the finished files of the current unit, and the file*
// fields the open file.
type nodeProgress struct {
	start     time.Time
	unitStart time.Time
	fileStart time.Time
	p         *progress
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
	shards    uint32
	parts     uint32
}

// node registers a node that exports units units and shows its live line.
func (p *progress) node(id string, units int) *nodeProgress {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := &nodeProgress{p: p, node: id, units: units, start: p.now()}
	p.live = append(p.live, n)
	p.write("")
	return n
}

// unitStarted opens the node's next unit.
func (n *nodeProgress) unitStarted(u *plannedUnit, shards, parts uint32) {
	n.p.mu.Lock()
	defer n.p.mu.Unlock()
	n.unit, n.shards, n.parts = u, shards, parts
	n.unitStart = n.p.now()
	n.unitFiles, n.unitRows, n.unitBytes = 0, 0, 0
	fixed := ""
	if !n.p.terminal() {
		layout := "no shards"
		if shards > 0 {
			layout = fmt.Sprintf("%d shard(s)%s", shards, partsSuffix(parts))
		}
		fixed = fmt.Sprintf("  %-*s  [%d/%d] %s started: %s\n", n.p.nodeW, n.node, n.doneUnits+1, n.units, u.displayName(), layout)
	}
	n.p.write(fixed)
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
	n.p.mu.Lock()
	defer n.p.mu.Unlock()
	n.file = rel
	n.fileStart = n.p.now()
	n.where = "segment index"
	if !segmentLevel {
		n.where = fmt.Sprintf("shard %d (%d/%d%s)", shardID, shardIndex+1, n.shards, partsSuffix(n.parts))
	}
}

// chunk counts a received chunk of size bytes; rows is what the node has written into the
// file so far.
func (n *nodeProgress) chunk(size int, rows uint64) {
	n.p.mu.Lock()
	defer n.p.mu.Unlock()
	n.fileBytes += uint64(size)
	n.fileRows = rows
}

// fileFinished closes the open file with its final rows per resource and size.
func (n *nodeProgress) fileFinished(rowCounts map[string]uint64, size uint64) {
	var rows uint64
	for _, c := range rowCounts {
		rows += c
	}
	n.p.mu.Lock()
	defer n.p.mu.Unlock()
	n.unitFiles++
	n.unitRows += rows
	n.unitBytes += size
	fixed := fmt.Sprintf("  %-*s  %s  %d rows  %s  %s\n", n.p.nodeW, n.node, n.file, rows, humanBytes(size), formatDuration(n.p.now().Sub(n.fileStart)))
	n.file, n.where, n.fileRows, n.fileBytes = "", "", 0, 0
	n.p.write(fixed)
}

// unitFinished closes the current unit, which took elapsed.
func (n *nodeProgress) unitFinished(elapsed time.Duration) {
	n.p.mu.Lock()
	defer n.p.mu.Unlock()
	n.doneUnits++
	n.files += uint64(n.unitFiles)
	n.rows += n.unitRows
	n.bytes += n.unitBytes
	fixed := fmt.Sprintf("✓ %-*s  [%d/%d] %s  %d file(s)  %d rows  %s  %s\n", n.p.nodeW, n.node, n.doneUnits, n.units, n.unit.displayName(),
		n.unitFiles, n.unitRows, humanBytes(n.unitBytes), formatDuration(elapsed))
	n.unit = nil
	n.p.write(fixed)
}

// finish removes the live line; a node that exported every unit gets its done line.
func (n *nodeProgress) finish(ok bool) {
	n.p.mu.Lock()
	defer n.p.mu.Unlock()
	for i, live := range n.p.live {
		if live == n {
			n.p.live = append(n.p.live[:i], n.p.live[i+1:]...)
			break
		}
	}
	fixed := ""
	if ok {
		fixed = fmt.Sprintf("✓ %-*s  done: %d unit(s), %d file(s), %d rows, %s in %s\n", n.p.nodeW, n.node, n.doneUnits, n.files, n.rows,
			humanBytes(n.bytes), formatDuration(n.p.now().Sub(n.start)))
	}
	n.p.write(fixed)
}

// liveLine is the node's line in the live area. The caller holds mu.
func (n *nodeProgress) liveLine(now time.Time) string {
	spin := spinnerFrames[n.p.frame%len(spinnerFrames)]
	if n.unit == nil {
		return fmt.Sprintf("%s %-*s  waiting for the next unit", spin, n.p.nodeW, n.node)
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
	return fmt.Sprintf("%s %-*s  [%d/%d] %s  %s  %d rows  %s  %s/s  %s", spin, n.p.nodeW, n.node, n.doneUnits+1, n.units, n.unit.displayName(), where,
		n.unitRows+n.fileRows, humanBytes(bytes), humanBytes(rate), formatDuration(elapsed))
}

// formatDuration spells a duration as 0.3s below a minute and as 2m5s from then on.
func formatDuration(d time.Duration) string {
	if d = d.Round(100 * time.Millisecond); d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return d.Round(time.Second).String()
}
