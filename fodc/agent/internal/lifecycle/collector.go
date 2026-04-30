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

// Package lifecycle provides lifecycle data collection functionality for FODC agent.
package lifecycle

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/internal/timeouts"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	// DefaultReportDir is the default directory for lifecycle report files.
	DefaultReportDir  = "/tmp/lifecycle-reports"
	maxReportFiles    = 5
	maxReportFileSize = 5 * 1024 * 1024 // 5MB
)

// Collector collects lifecycle data from local files.
type Collector struct {
	lastCollectTime   time.Time
	log               *logger.Logger
	nowFunc           func() time.Time
	currentData       *fodcv1.LifecycleData
	grpcAddr          string
	reportDir         string
	cacheTTL          time.Duration
	mu                sync.RWMutex
	grpcUnimplemented atomic.Bool
}

// NewCollector creates a new lifecycle data collector.
func NewCollector(log *logger.Logger, grpcAddr, reportDir string, cacheTTL time.Duration) *Collector {
	if reportDir == "" {
		reportDir = DefaultReportDir
	}
	return &Collector{
		log:       log,
		grpcAddr:  grpcAddr,
		reportDir: reportDir,
		cacheTTL:  cacheTTL,
		nowFunc:   time.Now,
	}
}

// Collect returns lifecycle data, serving the cache when fresh. A transient InspectAll
// failure is propagated to the caller (the cache is left untouched so the next call
// retries) so that callers can distinguish a real failure from a healthy liaison that
// happens to have zero groups.
func (c *Collector) Collect(ctx context.Context) (*fodcv1.LifecycleData, error) {
	c.mu.RLock()
	if c.currentData != nil && c.cacheTTL > 0 && c.nowFunc().Sub(c.lastCollectTime) < c.cacheTTL {
		data := c.currentData
		c.mu.RUnlock()
		return data, nil
	}
	c.mu.RUnlock()

	reports := c.readReportFiles()
	groups, err := c.collectGroups(ctx)
	if err != nil {
		if c.log != nil {
			c.log.Warn().Err(err).Int("reports", len(reports)).
				Msg("InspectAll failed; cache untouched, propagating error to caller")
		}
		return nil, err
	}

	data := &fodcv1.LifecycleData{
		Reports: reports,
		Groups:  groups,
	}
	c.mu.Lock()
	c.currentData = data
	c.lastCollectTime = c.nowFunc()
	c.mu.Unlock()
	return data, nil
}

// collectGroups invokes InspectAll on the local liaison.
//
// Return contract:
//   - (nil, nil): no RPC was issued (grpcAddr empty, or InspectAll already known to be Unimplemented).
//     Callers treat this as a successful collection with no groups.
//   - (non-nil slice, nil): RPC succeeded. The slice is empty (non-nil) when the server returned no groups.
//   - (nil, err): a transient error occurred (DeadlineExceeded, Unavailable, dial failure, etc.).
//     Callers must NOT cache this outcome.
func (c *Collector) collectGroups(ctx context.Context) ([]*fodcv1.GroupLifecycleInfo, error) {
	if c.grpcAddr == "" || c.grpcUnimplemented.Load() {
		return nil, nil
	}
	conn, err := grpc.NewClient(c.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", c.grpcAddr, err)
	}
	defer func() {
		_ = conn.Close()
	}()

	client := fodcv1.NewGroupLifecycleServiceClient(conn)
	reqCtx, cancel := context.WithTimeout(ctx, timeouts.AgentInspectAll)
	defer cancel()
	resp, err := client.InspectAll(reqCtx, &fodcv1.InspectAllRequest{})
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			c.grpcUnimplemented.Store(true)
			if c.log != nil {
				c.log.Info().Str("addr", c.grpcAddr).Msg("GroupLifecycleService.InspectAll is not implemented, skipping future calls")
			}
			return nil, nil
		}
		return nil, fmt.Errorf("InspectAll on %s: %w", c.grpcAddr, err)
	}
	got := resp.GetGroups()
	if got == nil {
		return []*fodcv1.GroupLifecycleInfo{}, nil
	}
	return got, nil
}

func (c *Collector) readReportFiles() []*fodcv1.LifecycleReport {
	entries, err := os.ReadDir(c.reportDir)
	if err != nil {
		if !os.IsNotExist(err) && c.log != nil {
			c.log.Warn().Err(err).Msg("Error reading lifecycle reports directory")
		}
		return nil
	}

	var jsonFiles []os.DirEntry
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			jsonFiles = append(jsonFiles, entry)
		}
	}

	sort.Slice(jsonFiles, func(i, j int) bool {
		return jsonFiles[i].Name() > jsonFiles[j].Name()
	})

	if len(jsonFiles) > maxReportFiles {
		jsonFiles = jsonFiles[:maxReportFiles]
	}

	reports := make([]*fodcv1.LifecycleReport, 0, len(jsonFiles))
	for _, entry := range jsonFiles {
		info, statErr := entry.Info()
		if statErr != nil {
			if c.log != nil {
				c.log.Warn().Err(statErr).Str("file", entry.Name()).Msg("Error getting report file info")
			}
			continue
		}
		if info.Size() > maxReportFileSize {
			if c.log != nil {
				c.log.Warn().Str("file", entry.Name()).Int64("size", info.Size()).Int64("max", maxReportFileSize).
					Msg("Report file exceeds max size, skipping")
			}
			continue
		}
		filePath := filepath.Join(c.reportDir, entry.Name())
		data, readErr := os.ReadFile(filePath)
		if readErr != nil {
			if c.log != nil {
				c.log.Warn().Err(readErr).Str("file", entry.Name()).Msg("Error reading report file")
			}
			continue
		}
		reports = append(reports, &fodcv1.LifecycleReport{
			Filename:   entry.Name(),
			ReportJson: string(data),
		})
	}

	return reports
}
