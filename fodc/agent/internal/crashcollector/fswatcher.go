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

package crashcollector

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

const (
	periodicRescanInterval = 30 * time.Second
	fsNotifyRescanDelay    = 100 * time.Millisecond
	fsSourcePrefix         = "file://"
)

// CrashAnalysis holds the result of analyzing a crash artifact directory.
type CrashAnalysis struct {
	MissingFiles []string
	Complete     bool
}

// DirectoryWatcher watches a local crash artifact directory for new collections
// using filesystem notifications (inotify on Linux, kqueue on macOS, FSEvents on macOS).
// This implements the FS Watcher / Crash Analyzer component from the Core Components and Workflow.
type DirectoryWatcher struct {
	cancel   context.CancelFunc
	seenKeys map[string]struct{}
	records  *flightrecorder.RingBuffer[CollectionRecord]
	log      *logger.Logger
	dir      string
	mu       sync.RWMutex
	wg       sync.WaitGroup
}

// NewDirectoryWatcher returns a new DirectoryWatcher that monitors dir for crash artifacts.
func NewDirectoryWatcher(log *logger.Logger, dir string, cfg Config) *DirectoryWatcher {
	bufferSize := defaultDiagnosisBufferSize
	if cfg.CapacitySizeBytes > 0 {
		bufferSize = computeCapacity(cfg.CapacitySizeBytes)
	}
	rb := flightrecorder.NewRingBuffer[CollectionRecord]()
	rb.SetCapacity(bufferSize)
	return &DirectoryWatcher{
		log:      log,
		dir:      dir,
		records:  rb,
		seenKeys: make(map[string]struct{}),
	}
}

// Start starts the directory watcher and returns a stop function.
func (w *DirectoryWatcher) Start(ctx context.Context) func() {
	if w == nil || w.dir == "" {
		return nil
	}
	watchCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	readyCh := make(chan struct{})
	w.wg.Add(1)
	panicdiag.GoWithRecovery(watchCtx, panicdiag.RecoveryOptions{
		Component: "crash-fswatcher",
		Logger:    w.log,
	}, nil, func(_ *context.Context) {
		defer w.wg.Done()
		w.watch(watchCtx, readyCh)
	})
	select {
	case <-watchCtx.Done():
	case <-readyCh:
	}
	return func() {
		cancel()
		w.wg.Wait()
	}
}

func (w *DirectoryWatcher) watch(ctx context.Context, readyCh chan<- struct{}) {
	fsWatcher, watchErr := fsnotify.NewWatcher()
	if watchErr != nil {
		close(readyCh)
		w.log.Warn().Err(watchErr).Str("dir", w.dir).Msg("Failed to create filesystem watcher, using periodic scan")
		w.periodicScan(ctx)
		return
	}
	defer func() {
		_ = fsWatcher.Close()
	}()

	if addErr := fsWatcher.Add(w.dir); addErr != nil {
		close(readyCh)
		w.log.Warn().Err(addErr).Str("dir", w.dir).Msg("Failed to watch crash directory, using periodic scan")
		w.periodicScan(ctx)
		return
	}

	w.Scan()
	close(readyCh)

	ticker := time.NewTicker(periodicRescanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-fsWatcher.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Create) || event.Has(fsnotify.Write) {
				w.Scan()
				select {
				case <-ctx.Done():
					return
				case <-time.After(fsNotifyRescanDelay):
					w.Scan()
				}
			}
		case watcherErr, ok := <-fsWatcher.Errors:
			if !ok {
				return
			}
			w.log.Warn().Err(watcherErr).Msg("Filesystem watcher error")
		case <-ticker.C:
			w.Scan()
		}
	}
}

func (w *DirectoryWatcher) periodicScan(ctx context.Context) {
	ticker := time.NewTicker(periodicRescanInterval)
	defer ticker.Stop()
	w.Scan()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.Scan()
		}
	}
}

// Scan scans the watched directory for new crash artifact collections.
func (w *DirectoryWatcher) Scan() {
	collections, scanErr := panicdiag.ListCollections(w.dir)
	if scanErr != nil {
		w.log.Warn().Err(scanErr).Str("dir", w.dir).Msg("Failed to scan crash directory")
		return
	}
	for _, collection := range collections {
		w.analyzeAndStore(collection)
	}
}

func (w *DirectoryWatcher) analyzeAndStore(collection panicdiag.Collection) {
	key := fsSourcePrefix + w.dir + "::" + collection.ArtifactDir
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, exists := w.seenKeys[key]; exists {
		return
	}

	analysis := analyzeCrashArtifact(&collection)
	if !analysis.Complete {
		w.log.Warn().
			Str("artifactDir", collection.ArtifactDir).
			Strs("missingFiles", analysis.MissingFiles).
			Msg("Incomplete crash artifact detected")
		return
	}
	if collection.Record != nil {
		w.log.Info().
			Str("artifactDir", collection.ArtifactDir).
			Str("component", collection.Record.Component).
			Str("panicValue", collection.Record.PanicValue).
			Bool("recovered", collection.Record.Recovered).
			Msg("Crash artifact detected")
	}

	w.records.Add(CollectionRecord{
		FetchedAt:      time.Now().UTC(),
		SourceEndpoint: fsSourcePrefix + w.dir,
		Collection:     collection,
	})
	w.records.FinalizeLastVisible()
	w.seenKeys[key] = struct{}{}
}

var requiredArtifactFiles = []string{"panic.json"}

func analyzeCrashArtifact(collection *panicdiag.Collection) CrashAnalysis {
	fileSet := make(map[string]struct{}, len(collection.Files))
	for _, file := range collection.Files {
		fileSet[file] = struct{}{}
	}
	var missing []string
	for _, required := range requiredArtifactFiles {
		if _, exists := fileSet[required]; !exists {
			missing = append(missing, required)
		}
	}
	return CrashAnalysis{
		Complete:     len(missing) == 0,
		MissingFiles: missing,
	}
}

// ListCollections returns all crash collections detected by the watcher.
func (w *DirectoryWatcher) ListCollections() []CollectionRecord {
	if w == nil {
		return nil
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.records.GetAllValues()
}

// MarshalCollections marshals all detected crash collections as JSON.
func (w *DirectoryWatcher) MarshalCollections() ([]byte, error) {
	data, err := json.Marshal(w.ListCollections())
	if err != nil {
		return nil, fmt.Errorf("marshal collections: %w", err)
	}
	return data, nil
}
