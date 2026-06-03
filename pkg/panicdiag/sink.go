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

package panicdiag

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// DefaultSinkQueueDepth is the default ArtifactSink queue capacity.
const DefaultSinkQueueDepth = 64

// ArtifactJob carries the inputs for one panic artifact write.
type ArtifactJob struct {
	// Record is the panic record to serialize.
	Record *PanicRecord

	// StateDump is the value returned by StateDumper.
	StateDump any

	// StateErr is recorded instead of writing StateDump when non-nil.
	StateErr error

	// Logger receives worker errors.
	Logger *logger.Logger

	// Done is closed after the job finishes.
	Done chan<- struct{}

	// RootDir is the artifact root. Empty means no-op.
	RootDir string

	// StateLimit caps the size of the deep state dump. Zero means no cap.
	StateLimit int64

	// HasStateDump is true when recovery attempted a state dump.
	HasStateDump bool
}

// ArtifactSink writes panic artifacts through a bounded async queue.
// Submit is non-blocking, and Submit/Close are synchronized to avoid sending
// on a closed queue.
type ArtifactSink struct {
	queue   chan ArtifactJob
	drained chan struct{}
	started atomic.Bool
	mu      sync.Mutex
	closed  bool
}

// NewArtifactSink creates a sink. Non-positive depth uses the default.
func NewArtifactSink(queueDepth int) *ArtifactSink {
	if queueDepth <= 0 {
		queueDepth = DefaultSinkQueueDepth
	}
	return &ArtifactSink{
		queue:   make(chan ArtifactJob, queueDepth),
		drained: make(chan struct{}),
	}
}

// Start launches the worker goroutine. It is idempotent.
func (s *ArtifactSink) Start() {
	if !s.started.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer close(s.drained)
		for job := range s.queue {
			runArtifactJob(job)
		}
	}()
}

// Submit enqueues a job without blocking. It returns false when full or closed.
func (s *ArtifactSink) Submit(job ArtifactJob) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	select {
	case s.queue <- job:
		return true
	default:
		return false
	}
}

// QueueDepth returns the current queue length.
func (s *ArtifactSink) QueueDepth() int {
	return len(s.queue)
}

// Close stops submissions and waits for queued jobs to drain.
func (s *ArtifactSink) Close(ctx context.Context) error {
	s.mu.Lock()
	first := !s.closed
	if first {
		s.closed = true
		close(s.queue)
	}
	s.mu.Unlock()

	// An unstarted worker has nothing to drain.
	if !s.started.Load() {
		return nil
	}
	select {
	case <-s.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runArtifactJob writes one artifact and always closes job.Done when present.
func runArtifactJob(job ArtifactJob) {
	if job.Done != nil {
		defer close(job.Done)
	}
	defer func() {
		if rec := recover(); rec != nil {
			fmt.Fprintf(os.Stderr,
				"panicdiag: artifact-write panic (suppressed): %v\n%s\n",
				rec, debug.Stack())
		}
	}()

	if job.RootDir == "" || job.Record == nil {
		return
	}
	log := job.Logger
	if log == nil {
		log = logger.GetLogger("panicdiag")
	}

	aw := NewArtifactWriter(job.RootDir)
	artifactDir, err := aw.MkdirArtifact(job.Record)
	if err != nil {
		log.Error().Err(err).Str("component", job.Record.Component).
			Msg("sink: failed to create panic artifact dir")
		return
	}
	if job.HasStateDump {
		if job.StateErr != nil {
			job.Record.StateDump = &StateDumpStatus{Error: job.StateErr.Error()}
		} else {
			truncated, dumpPath, writeDumpErr := aw.WriteStateDump(artifactDir, job.StateDump, job.StateLimit)
			dumpStatus := &StateDumpStatus{Truncated: truncated}
			if writeDumpErr != nil {
				dumpStatus.Error = writeDumpErr.Error()
			} else {
				dumpStatus.Path = dumpPath
			}
			job.Record.StateDump = dumpStatus
		}
	}
	if writeErr := aw.WriteRecord(artifactDir, job.Record); writeErr != nil {
		log.Error().Err(writeErr).Str("component", job.Record.Component).
			Msg("sink: failed to write panic record")
		return
	}
	aw.pruneArtifacts()
}

// writeArtifactSync writes an artifact inline when no sink is registered.
func writeArtifactSync(job ArtifactJob) {
	runArtifactJob(job)
}
