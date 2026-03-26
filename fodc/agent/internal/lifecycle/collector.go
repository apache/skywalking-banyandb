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
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	// DefaultReportDir is the default directory for lifecycle report files.
	DefaultReportDir = "/tmp/lifecycle-reports"
	maxReportFiles   = 5
)

const grpcTimeout = 10 * time.Second

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

// Collect collects lifecycle data from local files.
// Data is cached for cacheTTL duration; after expiry the next call refreshes the cache.
func (c *Collector) Collect(ctx context.Context) (*fodcv1.LifecycleData, error) {
	c.mu.RLock()
	if c.currentData != nil && c.cacheTTL > 0 && c.nowFunc().Sub(c.lastCollectTime) < c.cacheTTL {
		data := c.currentData
		c.mu.RUnlock()
		return data, nil
	}
	c.mu.RUnlock()

	data := &fodcv1.LifecycleData{
		Reports: c.readReportFiles(),
		Groups:  c.collectGroups(ctx),
	}
	c.mu.Lock()
	c.currentData = data
	c.lastCollectTime = c.nowFunc()
	c.mu.Unlock()
	return data, nil
}

func (c *Collector) collectGroups(ctx context.Context) []*fodcv1.GroupLifecycleInfo {
	if c.grpcAddr == "" || c.grpcUnimplemented.Load() {
		return nil
	}
	conn, err := grpc.NewClient(c.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		if c.log != nil {
			c.log.Warn().Err(err).Str("addr", c.grpcAddr).Msg("Failed to create gRPC connection for InspectAll")
		}
		return nil
	}
	defer func() {
		_ = conn.Close()
	}()

	client := fodcv1.NewGroupLifecycleServiceClient(conn)
	reqCtx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()
	resp, err := client.InspectAll(reqCtx, &fodcv1.InspectAllRequest{})
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			c.grpcUnimplemented.Store(true)
			if c.log != nil {
				c.log.Info().Str("addr", c.grpcAddr).Msg("GroupLifecycleService.InspectAll is not implemented, skipping future calls")
			}
			return nil
		}
		if c.log != nil {
			c.log.Warn().Err(err).Str("addr", c.grpcAddr).Msg("InspectAll failed, skipping groups")
		}
		return nil
	}
	return resp.GetGroups()
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
