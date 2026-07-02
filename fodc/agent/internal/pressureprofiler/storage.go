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

package pressureprofiler

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// metaFilename marks a complete capture event; a directory is only listed and
// counted once this file exists (written atomically last).
const metaFilename = "meta.json"

// ProfileInfo describes one pprof profile inside a capture event (metadata only).
type ProfileInfo struct {
	Type      string `json:"type"`
	Filename  string `json:"filename"`
	Filepath  string `json:"filepath"`
	Format    string `json:"format"`
	SizeBytes int64  `json:"size_bytes"`
}

// ProfileRecord is the metadata of one capture event, reconstructed from its meta.json. It
// intentionally omits pod name and role: those are carried in meta.json for the file to be
// self-describing, but over the wire the proxy enriches them from the registered AgentIdentity.
type ProfileRecord struct {
	CapturedAt       time.Time
	ProfileID        string
	SourceEndpoint   string
	Profiles         []ProfileInfo
	RSSBytes         uint64
	CgroupLimitBytes uint64
	ThresholdBytes   uint64
	TriggerPercent   uint32
}

// meta is the on-disk meta.json schema for one capture event.
type meta struct {
	CapturedAt       string        `json:"capturedAt"`
	PodName          string        `json:"podName"`
	Role             string        `json:"role"`
	SourceEndpoint   string        `json:"sourceEndpoint"`
	Profiles         []ProfileInfo `json:"profiles"`
	RSSBytes         uint64        `json:"rssBytes"`
	CgroupLimitBytes uint64        `json:"cgroupLimitBytes"`
	ThresholdBytes   uint64        `json:"thresholdBytes"`
	TriggerPercent   uint32        `json:"triggerPercent"`
}

// store owns the on-disk layout under a private directory: one sub-directory per
// capture event (named by a UTC nanosecond timestamp), each holding the pprof
// binaries plus a meta.json. A single mutex serializes the directory-entry
// operations (finalize, evict, list, open); long file reads run lock-free because
// an already-open fd survives a concurrent unlink.
type store struct {
	log          *logger.Logger
	dir          string
	maxArtifacts int
	maxDiskBytes int64
	mu           sync.Mutex
}

func newStore(dir string, maxArtifacts int, maxDiskBytes int64, log *logger.Logger) *store {
	return &store{dir: dir, maxArtifacts: maxArtifacts, maxDiskBytes: maxDiskBytes, log: log}
}

// selfCheck verifies the storage directory can be created and written, surfacing a
// misconfigured/read-only volume at startup instead of at the moment of capture.
func (s *store) selfCheck() error {
	if err := os.MkdirAll(s.dir, 0o750); err != nil {
		return fmt.Errorf("cannot create pressure-profiler dir %q: %w", s.dir, err)
	}
	probe := filepath.Join(s.dir, ".write-probe")
	if err := os.WriteFile(probe, []byte("ok"), 0o600); err != nil {
		return fmt.Errorf("pressure-profiler dir %q is not writable: %w", s.dir, err)
	}
	if err := os.Remove(probe); err != nil {
		return fmt.Errorf("pressure-profiler dir %q probe cleanup failed: %w", s.dir, err)
	}
	return nil
}

// eventDir returns the directory path for a capture event without creating it.
func (s *store) eventDir(profileID string) string {
	return filepath.Join(s.dir, profileID)
}

// finalize atomically writes meta.json for an already-populated event directory and
// then enforces the retention bounds. Profile binaries are streamed into the unique
// event directory before this call without holding the lock; the directory only
// becomes visible to list/evict once meta.json exists.
func (s *store) finalize(profileID string, m meta) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := s.eventDir(profileID)
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal meta.json: %w", err)
	}
	tmp := filepath.Join(dir, metaFilename+".tmp")
	if err = os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("failed to write meta.json: %w", err)
	}
	if err = os.Rename(tmp, filepath.Join(dir, metaFilename)); err != nil {
		return fmt.Errorf("failed to finalize meta.json: %w", err)
	}
	s.evictLocked()
	return nil
}

// list returns the metadata of every complete capture event, newest sort handled by callers.
func (s *store) list() []ProfileRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scanLocked()
}

// open validates that requestedPath resolves inside the storage directory (the path
// is round-tripped through the proxy and therefore untrusted) and returns an open
// file handle. The caller streams and closes it; a concurrent evict that unlinks the
// directory entry does not disturb an already-open fd.
func (s *store) open(requestedPath string) (*os.File, error) {
	clean := filepath.Clean(requestedPath)
	base := filepath.Clean(s.dir)
	if clean != base && !strings.HasPrefix(clean, base+string(os.PathSeparator)) {
		return nil, fmt.Errorf("path %q escapes pressure-profiler dir", requestedPath)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Open(clean) //nolint:gosec // path validated to be within s.dir above
	if err != nil {
		return nil, fmt.Errorf("failed to open profile %q: %w", requestedPath, err)
	}
	return f, nil
}

// diskSize is the on-disk size of an event: the sum of its profile file sizes.
func (r ProfileRecord) diskSize() int64 {
	var n int64
	for _, p := range r.Profiles {
		n += p.SizeBytes
	}
	return n
}

// scanLocked reads every complete event directory into a record. Callers must hold s.mu.
func (s *store) scanLocked() []ProfileRecord {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}
	var records []ProfileRecord
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		profileID := entry.Name()
		m, readErr := readMeta(filepath.Join(s.dir, profileID, metaFilename))
		if readErr != nil {
			continue
		}
		capturedAt, _ := time.Parse(time.RFC3339Nano, m.CapturedAt)
		records = append(records, ProfileRecord{
			ProfileID:        profileID,
			CapturedAt:       capturedAt,
			SourceEndpoint:   m.SourceEndpoint,
			RSSBytes:         m.RSSBytes,
			CgroupLimitBytes: m.CgroupLimitBytes,
			TriggerPercent:   m.TriggerPercent,
			ThresholdBytes:   m.ThresholdBytes,
			Profiles:         m.Profiles,
		})
	}
	return records
}

// evictLocked keeps the highest-RSS events within both the artifact-count and the
// total-disk bounds, deleting the lowest-RSS events first; it always keeps at least
// one event. Callers must hold s.mu.
func (s *store) evictLocked() {
	records := s.scanLocked()
	if len(records) <= 1 {
		return
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].RSSBytes != records[j].RSSBytes {
			return records[i].RSSBytes > records[j].RSSBytes
		}
		return records[i].CapturedAt.After(records[j].CapturedAt)
	})

	var kept int
	var diskUsed int64
	var evicted []string
	for idx, rec := range records {
		size := rec.diskSize()
		overCount := s.maxArtifacts > 0 && kept >= s.maxArtifacts
		overDisk := s.maxDiskBytes > 0 && diskUsed+size > s.maxDiskBytes
		if idx > 0 && (overCount || overDisk) {
			if err := os.RemoveAll(s.eventDir(rec.ProfileID)); err != nil {
				if s.log != nil {
					s.log.Warn().Err(err).Str("profile_id", rec.ProfileID).Msg("failed to evict pressure profile event")
				}
				continue
			}
			evicted = append(evicted, rec.ProfileID)
			continue
		}
		kept++
		diskUsed += size
	}
	if len(evicted) > 0 && s.log != nil {
		s.log.Info().
			Strs("evicted", evicted).
			Int("evicted_count", len(evicted)).
			Int("kept_count", kept).
			Int64("disk_used_bytes", diskUsed).
			Int("max_artifacts", s.maxArtifacts).
			Int64("max_disk_bytes", s.maxDiskBytes).
			Msg("enforced pressure profile retention")
	}
}

func readMeta(path string) (meta, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is built from the store's own directory listing
	if err != nil {
		return meta{}, err
	}
	var m meta
	if err = json.Unmarshal(data, &m); err != nil {
		return meta{}, err
	}
	return m, nil
}
