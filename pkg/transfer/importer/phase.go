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
	"context"
	"fmt"
	"io"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

// phaseWaiter shows, on a terminal, a spinner line while the import waits on a phase that
// prints nothing, and erases it when the phase ends. Without a terminal it shows nothing.
type phaseWaiter struct {
	log      io.Writer
	terminal func() (width, height int)
}

// run runs fn under a live line naming label and the time spent so far.
func (w phaseWaiter) run(ctx context.Context, label string, fn func() error) error {
	if w.terminal == nil {
		return fn()
	}
	p := output.NewProgress(w.log, w.terminal, "")
	p.Start(ctx)
	defer p.Close()
	p.Add(&phaseLine{p: p, label: label, start: p.Now()})
	return fn()
}

// phaseLine is the live line of a phase.
type phaseLine struct {
	start time.Time
	p     *output.Progress
	label string
}

// LiveLine renders the spinner, the phase and its elapsed time.
func (l *phaseLine) LiveLine(now time.Time) string {
	return fmt.Sprintf("%s %s (%s)", l.p.Spinner(), l.label, now.Sub(l.start).Truncate(100*time.Millisecond))
}
