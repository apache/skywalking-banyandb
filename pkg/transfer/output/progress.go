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

package output

import (
	"context"
	"fmt"
	"io"
	"slices"
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

// LiveLine is one line of the live area of a Progress, redrawn in place.
type LiveLine interface {
	// LiveLine renders the line at now; it is called with the Progress locked.
	LiveLine(now time.Time) string
}

// Progress prints the progress of a transfer: fixed lines and, on a terminal, a live area of
// one line per worker in flight that is redrawn in place. Without a terminal it prints the
// fixed lines only.
type Progress struct {
	w    io.Writer
	size func() (width, height int) // nil when w is not a terminal; 0 for an unknown dimension
	now  func() time.Time
	stop chan struct{}
	done chan struct{}
	// more names the live lines that do not fit the terminal, as in "... 3 more <more>".
	more  string
	live  []LiveLine
	mu    sync.Mutex
	drawn int
	frame int
}

// NewProgress prints to w; size is nil when w is not a terminal. more names the live lines, as
// in "node(s) exporting".
func NewProgress(w io.Writer, size func() (width, height int), more string) *Progress {
	return &Progress{w: w, size: size, now: time.Now, more: more}
}

// Terminal reports whether live lines are drawn.
func (p *Progress) Terminal() bool { return p.size != nil }

// SetClock replaces the clock the progress reads its times from, for tests.
func (p *Progress) SetClock(now func() time.Time) { p.now = now }

// Now is the time of the progress clock.
func (p *Progress) Now() time.Time { return p.now() }

// Spinner is the current frame of the spinner a live line starts with; call it from LiveLine.
func (p *Progress) Spinner() string { return spinnerFrames[p.frame%len(spinnerFrames)] }

// Add shows a live line.
func (p *Progress) Add(l LiveLine) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.live = append(p.live, l)
	p.write("")
}

// Remove drops a live line and prints fixed, a line or "".
func (p *Progress) Remove(l LiveLine, fixed string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if i := slices.Index(p.live, l); i >= 0 {
		p.live = slices.Delete(p.live, i, i+1)
	}
	p.write(fixed)
}

// Update runs change with the progress locked, so that it may change what the live lines
// show, and prints the fixed line it returns, if any; the live lines are redrawn by the ticker.
func (p *Progress) Update(change func() (fixed string)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if fixed := change(); fixed != "" {
		p.write(fixed)
	}
}

// Start begins redrawing the live lines every progressRedraw on a terminal, until close or
// the end of ctx.
func (p *Progress) Start(ctx context.Context) {
	if !p.Terminal() {
		return
	}
	p.stop, p.done = make(chan struct{}), make(chan struct{})
	run.Go(ctx, "transfer-progress-redraw", logger.GetLogger("transfer-progress"), func(ctx context.Context) {
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
				p.Redraw()
			}
		}
	})
}

// Close stops the redraws and erases the live lines left by workers that did not finish.
func (p *Progress) Close() {
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

// Printf prints a fixed line.
func (p *Progress) Printf(format string, args ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.write(fmt.Sprintf(format, args...) + "\n")
}

// Redraw draws the live lines again with the next spinner frame, as the redraw ticker does.
func (p *Progress) Redraw() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.frame++
	p.write("")
}

// write erases the live lines, prints fixed and draws the live lines again, in one Write
// so the terminal never shows a half-drawn screen. The caller holds mu.
func (p *Progress) write(fixed string) {
	if !p.Terminal() {
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
		b.WriteString(truncate(n.LiveLine(now), width))
		b.WriteByte('\n')
	}
	if more > 0 {
		b.WriteString(truncate(fmt.Sprintf("  ... %d more %s", more, p.more), width))
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
func (p *Progress) erase() string {
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

// Wrap returns a writer that prints through the progress, so that lines written to another
// stream of the same terminal (warnings on stderr) do not tear the live lines.
func (p *Progress) Wrap(w io.Writer) io.Writer {
	return writerFunc(func(b []byte) (int, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if !p.Terminal() {
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

// FormatElapsed spells a duration as 0.3s below a minute and as 2m5s from then on.
func FormatElapsed(d time.Duration) string {
	if d = d.Round(100 * time.Millisecond); d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return d.Round(time.Second).String()
}
