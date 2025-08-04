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

package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/robfig/cron/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type service interface {
	run.Config
	run.Service
}

var _ service = (*lifecycleService)(nil)

type lifecycleService struct {
	metadata          metadata.Repo
	omr               observability.MetricsRegistry
	pm                protector.Memory
	l                 *logger.Logger
	sch               *timestamp.Scheduler
	measureRoot       string
	streamRoot        string
	progressFilePath  string
	reportDir         string
	schedule          string
	cert              string
	gRPCAddr          string
	maxExecutionTimes int
	enableTLS         bool
	insecure          bool
	chunkSize         run.Bytes
}

// NewService creates a new lifecycle service.
func NewService(meta metadata.Repo) run.Unit {
	ls := &lifecycleService{
		metadata: meta,
		omr:      observability.BypassRegistry,
	}
	ls.pm = protector.NewMemory(ls.omr)
	return ls
}

func (l *lifecycleService) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet(l.Name())
	flagS.StringSliceVar(&common.FlagNodeLabels, "node-labels", nil, "the node labels. e.g. key1=value1,key2=value2")
	flagS.StringVar(&l.gRPCAddr, "grpc-addr", "127.0.0.1:17912", "gRPC address of the data node")
	flagS.BoolVar(&l.enableTLS, "enable-tls", false, "Enable TLS for gRPC connection")
	flagS.BoolVar(&l.insecure, "insecure", false, "Skip server certificate verification")
	flagS.StringVar(&l.cert, "cert", "", "Path to the gRPC server certificate")
	flagS.StringVar(&l.streamRoot, "stream-root-path", "/tmp", "Root directory for stream catalog")
	flagS.StringVar(&l.measureRoot, "measure-root-path", "/tmp", "Root directory for measure catalog")
	flagS.StringVar(&l.progressFilePath, "progress-file", "/tmp/lifecycle-progress.json", "Path to store progress for crash recovery")
	flagS.StringVar(&l.reportDir, "report-dir", "/tmp/lifecycle-reports", "Directory to store migration reports")
	flagS.StringVar(
		&l.schedule,
		"schedule",
		"",
		"Schedule expression for periodic backup. Options: @yearly, @monthly, @weekly, @daily, @hourly or @every <duration>",
	)
	flagS.IntVar(&l.maxExecutionTimes, "max-execution-times", 0, "Maximum number of times to execute the lifecycle migration. 0 means no limit.")
	l.chunkSize = run.Bytes(1024 * 1024)
	flagS.VarP(&l.chunkSize, "chunk-size", "", "Chunk size in bytes for streaming data during migration (default: 1MB)")
	return flagS
}

func (l *lifecycleService) Validate() error {
	return nil
}

func (l *lifecycleService) GracefulStop() {
	if l.sch != nil {
		l.sch.Close()
	}
}

func (l *lifecycleService) Name() string {
	return "lifecycle"
}

func (l *lifecycleService) Serve() run.StopNotify {
	l.l = logger.GetLogger("lifecycle")
	done := make(chan struct{})
	if l.schedule == "" {
		defer close(done)
		l.l.Info().Msg("starting lifecycle migration without schedule")
		if err := l.action(); err != nil {
			logger.Panicf("failed to run lifecycle migration: %v", err)
		}
		return done
	}
	l.l.Info().Msgf("lifecycle migration will run with schedule: %s", l.schedule)
	clockInstance := clock.New()
	l.sch = timestamp.NewScheduler(l.l, clockInstance)
	var executionCount int
	err := l.sch.Register("lifecycle", cron.Descriptor, l.schedule, func(triggerTime time.Time, _ *logger.Logger) bool {
		l.l.Info().Msgf("lifecycle migration triggered at %s", triggerTime)
		if err := l.action(); err != nil {
			l.l.Error().Err(err).Msg("failed to run lifecycle migration action")
		}
		executionCount++
		if l.maxExecutionTimes > 0 && executionCount >= l.maxExecutionTimes {
			l.l.Info().Msgf("lifecycle migration reached max execution times: %d, stopping scheduler", l.maxExecutionTimes)
			close(done)
			return false
		}
		return true
	})
	if err != nil {
		l.l.Error().Err(err).Msg("failed to register lifecycle migration schedule")
		return done
	}
	return done
}

func (l *lifecycleService) action() error {
	ctx := context.Background()
	progress := LoadProgress(l.progressFilePath, l.l)
	progress.ClearErrors()

	groups, err := l.getGroupsToProcess(ctx, progress)
	if err != nil {
		l.l.Error().Err(err).Msg("failed to get groups to process")
		return err
	}

	l.l.Info().Msgf("starting migration for %d groups: %v", len(groups), getGroupNames(groups))

	if len(groups) == 0 {
		l.l.Info().Msg("no groups to process, all groups already completed")
		progress.Remove(l.progressFilePath, l.l)
		return err
	}

	// Pass progress to getSnapshots
	streamDir, measureDir, err := l.getSnapshots(groups, progress)
	if err != nil {
		l.l.Error().Err(err).Msg("failed to get snapshots")
		return err
	}
	if streamDir == "" && measureDir == "" {
		l.l.Warn().Msg("no snapshots found, skipping lifecycle migration")
		l.generateReport(progress)
		return nil
	}
	l.l.Info().
		Str("stream_snapshot", streamDir).
		Str("measure_snapshot", measureDir).
		Msg("created snapshots")
	progress.Save(l.progressFilePath, l.l)

	nodes, err := l.metadata.NodeRegistry().ListNode(ctx, databasev1.Role_ROLE_DATA)
	if err != nil {
		l.l.Error().Err(err).Msg("failed to list data nodes")
		return err
	}
	labels := common.ParseNodeFlags()

	allGroupsCompleted := true
	for _, g := range groups {
		switch g.Catalog {
		case commonv1.Catalog_CATALOG_STREAM:
			if streamDir == "" {
				l.l.Warn().Msgf("stream snapshot directory is not available, skipping group: %s", g.Metadata.Name)
				progress.MarkGroupCompleted(g.Metadata.Name)
				continue
			}
			l.processStreamGroup(ctx, g, streamDir, nodes, labels, progress)
		case commonv1.Catalog_CATALOG_MEASURE:
			if measureDir == "" {
				l.l.Warn().Msgf("measure snapshot directory is not available, skipping group: %s", g.Metadata.Name)
				progress.MarkGroupCompleted(g.Metadata.Name)
				continue
			}
			l.processMeasureGroup(ctx, g, measureDir, nodes, labels, progress)
		default:
			l.l.Info().Msgf("group catalog: %s doesn't support lifecycle management", g.Catalog)
		}
		progress.Save(l.progressFilePath, l.l)
	}

	// Only remove progress file if ALL groups are fully completed
	if allGroupsCompleted && progress.AllGroupsFullyCompleted(groups) {
		progress.Remove(l.progressFilePath, l.l)
		l.l.Info().Msg("lifecycle migration completed successfully")
		l.generateReport(progress)
		return nil
	}
	l.l.Info().Msg("lifecycle migration partially completed, progress file retained")
	return fmt.Errorf("lifecycle migration partially completed, progress file retained; %d groups not fully completed", len(groups)-len(progress.CompletedGroups))
}

// generateReport gathers detailed counts & errors from Progress, writes comprehensive JSON file per run, and keeps only 5 latest.
func (l *lifecycleService) generateReport(p *Progress) {
	reportDir := l.reportDir
	if err := os.MkdirAll(reportDir, 0o755); err != nil {
		l.l.Error().Err(err).Msgf("failed to create report dir %s", reportDir)
		return
	}

	// Build comprehensive migration report
	report := l.buildMigrationReport(p)

	// write file
	fname := time.Now().Format("20060102_150405") + ".json"
	fpath := filepath.Join(reportDir, fname)
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		l.l.Error().Err(err).Msg("failed to marshal migration report")
		return
	}
	if err = os.WriteFile(fpath, data, 0o600); err != nil {
		l.l.Error().Err(err).Msgf("failed to write report file %s", fpath)
		return
	}
	l.l.Info().Msgf("wrote comprehensive migration report %s", fpath)

	// rotate: keep only 5 latest
	l.rotateReportFiles(reportDir)
}

// buildMigrationReport creates a comprehensive migration report from progress data.
func (l *lifecycleService) buildMigrationReport(p *Progress) map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	report := map[string]interface{}{
		"generated_at":   now,
		"report_version": "2.0",
		"summary":        l.buildSummaryStats(p),
		"errors":         l.buildErrorSummary(p),
		"snapshot_info": map[string]interface{}{
			"stream_dir":  p.SnapshotStreamDir,
			"measure_dir": p.SnapshotMeasureDir,
		},
	}

	return report
}

// buildSummaryStats creates overall migration statistics.
func (l *lifecycleService) buildSummaryStats(p *Progress) map[string]interface{} {
	totalGroups := len(p.CompletedGroups) + len(p.DeletedStreamGroups) + len(p.DeletedMeasureGroups)
	completedGroups := len(p.CompletedGroups)

	// Calculate total parts and series across all groups
	totalStreamParts, completedStreamParts := l.calculateTotalCounts(p.StreamPartCounts, p.StreamPartProgress)
	totalStreamSeries, completedStreamSeries := l.calculateTotalCounts(p.StreamSeriesCounts, p.StreamSeriesProgress)
	totalStreamElementIndex, completedStreamElementIndex := l.calculateTotalCounts(p.StreamElementIndexCounts, p.StreamElementIndexProgress)
	totalMeasureParts, completedMeasureParts := l.calculateTotalCounts(p.MeasurePartCounts, p.MeasurePartProgress)
	totalMeasureSeries, completedMeasureSeries := l.calculateTotalCounts(p.MeasureSeriesCounts, p.MeasureSeriesProgress)

	// Calculate error counts
	streamPartErrors := l.countErrors(p.StreamPartErrors)
	streamSeriesErrors := l.countErrors(p.StreamSeriesErrors)
	streamElementIndexErrors := l.countErrors(p.StreamElementIndexErrors)
	measurePartErrors := l.countErrors(p.MeasurePartErrors)
	measureSeriesErrors := l.countErrors(p.MeasureSeriesErrors)

	return map[string]interface{}{
		"migration_status": map[string]interface{}{
			"total_groups":     totalGroups,
			"completed_groups": completedGroups,
			"completion_rate":  l.calculatePercentage(completedGroups, totalGroups),
		},
		"stream_migration": map[string]interface{}{
			"parts": map[string]interface{}{
				"total":           totalStreamParts,
				"completed":       completedStreamParts,
				"errors":          streamPartErrors,
				"completion_rate": l.calculatePercentage(completedStreamParts, totalStreamParts),
			},
			"series": map[string]interface{}{
				"total":           totalStreamSeries,
				"completed":       completedStreamSeries,
				"errors":          streamSeriesErrors,
				"completion_rate": l.calculatePercentage(completedStreamSeries, totalStreamSeries),
			},
			"element_index": map[string]interface{}{
				"total":           totalStreamElementIndex,
				"completed":       completedStreamElementIndex,
				"errors":          streamElementIndexErrors,
				"completion_rate": l.calculatePercentage(completedStreamElementIndex, totalStreamElementIndex),
			},
		},
		"measure_migration": map[string]interface{}{
			"parts": map[string]interface{}{
				"total":           totalMeasureParts,
				"completed":       completedMeasureParts,
				"errors":          measurePartErrors,
				"completion_rate": l.calculatePercentage(completedMeasureParts, totalMeasureParts),
			},
			"series": map[string]interface{}{
				"total":           totalMeasureSeries,
				"completed":       completedMeasureSeries,
				"errors":          measureSeriesErrors,
				"completion_rate": l.calculatePercentage(completedMeasureSeries, totalMeasureSeries),
			},
		},
	}
}

// buildErrorSummary creates detailed error information.
func (l *lifecycleService) buildErrorSummary(p *Progress) map[string]interface{} {
	errors := map[string]interface{}{
		"stream_parts":         l.buildErrorDetails(p.StreamPartErrors),
		"stream_series":        l.buildErrorDetails(p.StreamSeriesErrors),
		"stream_element_index": l.buildErrorDetails(p.StreamElementIndexErrors),
		"measure_parts":        l.buildErrorDetails(p.MeasurePartErrors),
		"measure_series":       l.buildErrorDetails(p.MeasureSeriesErrors),
	}

	return errors
}

// Helper functions.
func (l *lifecycleService) calculateTotalCounts(counts, progress map[string]int) (int, int) {
	totalCount, totalProgress := 0, 0
	for _, count := range counts {
		totalCount += count
	}
	for _, prog := range progress {
		totalProgress += prog
	}
	return totalCount, totalProgress
}

func (l *lifecycleService) countErrors(errorMaps interface{}) int {
	switch v := errorMaps.(type) {
	// New four-level structure: map[group]map[segmentID]map[shardID]map[partID]error
	case map[string]map[string]map[common.ShardID]map[uint64]string:
		total := 0
		for _, segments := range v {
			for _, shards := range segments {
				for _, parts := range shards {
					total += len(parts)
				}
			}
		}
		return total
	// New three-level structure: map[group]map[segmentID]map[shardID]error
	case map[string]map[string]map[common.ShardID]string:
		total := 0
		for _, segments := range v {
			for _, shards := range segments {
				total += len(shards)
			}
		}
		return total
	// Legacy two-level structure: map[group]map[partID]error
	case map[string]map[uint64]string:
		total := 0
		for _, groupErrors := range v {
			total += len(groupErrors)
		}
		return total
	// Legacy two-level structure: map[group]map[shardID]error
	case map[string]map[string]string:
		total := 0
		for _, groupErrors := range v {
			total += len(groupErrors)
		}
		return total
	default:
		return 0
	}
}

func (l *lifecycleService) calculatePercentage(completed, total int) float64 {
	if total == 0 {
		return 0.0
	}
	return float64(completed) / float64(total) * 100.0
}

func (l *lifecycleService) buildErrorDetails(errorMaps interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	switch v := errorMaps.(type) {
	// New four-level structure: map[group]map[segmentID]map[shardID]map[partID]error
	case map[string]map[string]map[common.ShardID]map[uint64]string:
		for group, segments := range v {
			groupDetails := make(map[string]interface{})
			for segmentID, shards := range segments {
				segmentDetails := make(map[string]interface{})
				for shardID, parts := range shards {
					if len(parts) > 0 {
						segmentDetails[fmt.Sprintf("shard_%d", shardID)] = parts
					}
				}
				if len(segmentDetails) > 0 {
					groupDetails[segmentID] = segmentDetails
				}
			}
			if len(groupDetails) > 0 {
				result[group] = groupDetails
			}
		}
	// New three-level structure: map[group]map[segmentID]map[shardID]error
	case map[string]map[string]map[common.ShardID]string:
		for group, segments := range v {
			groupDetails := make(map[string]interface{})
			for segmentID, shards := range segments {
				if len(shards) > 0 {
					// Convert ShardID keys to strings for JSON serialization
					shardDetails := make(map[string]string)
					for shardID, errorMsg := range shards {
						shardDetails[fmt.Sprintf("shard_%d", shardID)] = errorMsg
					}
					groupDetails[segmentID] = shardDetails
				}
			}
			if len(groupDetails) > 0 {
				result[group] = groupDetails
			}
		}
	// Legacy two-level structure: map[group]map[partID]error
	case map[string]map[uint64]string:
		for group, groupErrors := range v {
			if len(groupErrors) > 0 {
				result[group] = groupErrors
			}
		}
	// Legacy two-level structure: map[group]map[shardID]error
	case map[string]map[string]string:
		for group, groupErrors := range v {
			if len(groupErrors) > 0 {
				result[group] = groupErrors
			}
		}
	}

	return result
}

// rotateReportFiles keeps only the 5 most recent report files.
func (l *lifecycleService) rotateReportFiles(reportDir string) {
	entries, err := os.ReadDir(reportDir)
	if err != nil {
		return
	}

	type fileInfo struct {
		t    time.Time
		name string
	}

	var list []fileInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		list = append(list, fileInfo{name: e.Name(), t: info.ModTime()})
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].t.After(list[j].t)
	})

	for i := 5; i < len(list); i++ {
		_ = os.Remove(filepath.Join(reportDir, list[i].name))
	}

	if len(list) > 5 {
		l.l.Info().Msgf("rotated %d old migration reports", len(list)-5)
	}
}

func getGroupNames(groups []*commonv1.Group) []string {
	names := make([]string, 0, len(groups))
	for _, g := range groups {
		names = append(names, g.Metadata.Name)
	}
	return names
}

func (l *lifecycleService) getGroupsToProcess(ctx context.Context, progress *Progress) ([]*commonv1.Group, error) {
	gg, err := l.metadata.GroupRegistry().ListGroup(ctx)
	if err != nil {
		l.l.Error().Err(err).Msg("failed to list groups")
		return nil, err
	}

	groups := make([]*commonv1.Group, 0, len(gg))
	for _, g := range gg {
		if g.ResourceOpts == nil {
			continue
		}
		if len(g.ResourceOpts.Stages) == 0 {
			continue
		}
		if progress.IsGroupCompleted(g.Metadata.Name) {
			l.l.Debug().Msgf("skipping already completed group: %s", g.Metadata.Name)
			continue
		}
		groups = append(groups, g)
	}

	return groups, nil
}

func (l *lifecycleService) processStreamGroup(ctx context.Context, g *commonv1.Group,
	streamDir string, nodes []*databasev1.Node, labels map[string]string, progress *Progress,
) {
	tr := l.getRemovalSegmentsTimeRange(g)
	if tr.Start.IsZero() && tr.End.IsZero() {
		l.l.Info().Msgf("no removal segments time range for group %s, skipping stream migration", g.Metadata.Name)
		progress.MarkGroupCompleted(g.Metadata.Name)
		return
	}

	err := l.processStreamGroupFileBased(ctx, g, streamDir, tr, nodes, labels, progress)
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to migrate stream group %s using file-based approach", g.Metadata.Name)
		return
	}

	l.l.Info().Msgf("deleting expired stream segments for group: %s", g.Metadata.Name)
	l.deleteExpiredStreamSegments(ctx, g, tr, progress)
	progress.MarkGroupCompleted(g.Metadata.Name)
}

// processStreamGroupFileBased uses file-based migration instead of element-based queries.
func (l *lifecycleService) processStreamGroupFileBased(_ context.Context, g *commonv1.Group,
	streamDir string, tr *timestamp.TimeRange, nodes []*databasev1.Node, labels map[string]string, progress *Progress,
) error {
	if progress.IsStreamGroupDeleted(g.Metadata.Name) {
		l.l.Info().Msgf("skipping already completed file-based migration for group: %s", g.Metadata.Name)
		return nil
	}

	l.l.Info().Msgf("starting file-based stream migration for group: %s", g.Metadata.Name)

	// Use the file-based migration with existing visitor pattern
	err := migrateStreamWithFileBasedAndProgress(
		filepath.Join(streamDir, g.Metadata.Name), // Use snapshot directory as source
		*tr,              // Time range for segments to migrate
		g,                // Group configuration
		labels,           // Node labels
		nodes,            // Target nodes
		l.metadata,       // Metadata repository
		l.l,              // Logger
		progress,         // Progress tracking
		int(l.chunkSize), // Chunk size for streaming
	)
	if err != nil {
		return fmt.Errorf("file-based stream migration failed: %w", err)
	}

	l.l.Info().Msgf("completed file-based stream migration for group: %s", g.Metadata.Name)
	return nil
}

// getRemovalSegmentsTimeRange calculates the time range for segments that should be migrated
// based on the group's TTL configuration, similar to storage.segmentController.getExpiredSegmentsTimeRange.
func (l *lifecycleService) getRemovalSegmentsTimeRange(g *commonv1.Group) *timestamp.TimeRange {
	if g.ResourceOpts == nil || g.ResourceOpts.Ttl == nil {
		l.l.Debug().Msgf("no TTL configured for group %s", g.Metadata.Name)
		return &timestamp.TimeRange{} // Return empty time range
	}

	// Convert TTL to storage.IntervalRule
	ttl := storage.MustToIntervalRule(g.ResourceOpts.Ttl)

	// Calculate deadline based on TTL (same logic as segmentController.getExpiredSegmentsTimeRange)
	deadline := time.Now().Local().Add(-l.calculateTTLDuration(ttl))

	// Create time range for segments before the deadline
	timeRange := &timestamp.TimeRange{
		Start:        time.Time{}, // Will be set to earliest segment start time
		End:          deadline,    // All segments before this time should be migrated
		IncludeStart: true,
		IncludeEnd:   false,
	}

	l.l.Info().
		Str("group", g.Metadata.Name).
		Time("deadline", deadline).
		Str("ttl", fmt.Sprintf("%d %s", g.ResourceOpts.Ttl.Num, g.ResourceOpts.Ttl.Unit.String())).
		Msg("calculated removal segments time range based on TTL")

	return timeRange
}

// calculateTTLDuration calculates the duration for a TTL interval rule.
// This implements the same logic as storage.IntervalRule.estimatedDuration().
func (l *lifecycleService) calculateTTLDuration(ttl storage.IntervalRule) time.Duration {
	switch ttl.Unit {
	case storage.HOUR:
		return time.Hour * time.Duration(ttl.Num)
	case storage.DAY:
		return 24 * time.Hour * time.Duration(ttl.Num)
	default:
		l.l.Warn().Msgf("unknown TTL unit %v, defaulting to 1 day", ttl.Unit)
		return 24 * time.Hour
	}
}

func (l *lifecycleService) deleteExpiredStreamSegments(ctx context.Context, g *commonv1.Group, tr *timestamp.TimeRange, progress *Progress) {
	if progress.IsStreamGroupDeleted(g.Metadata.Name) {
		l.l.Info().Msgf("skipping already deleted stream group segments: %s", g.Metadata.Name)
		return
	}

	resp, err := snapshot.Conn(l.gRPCAddr, l.enableTLS, l.insecure, l.cert, func(conn *grpc.ClientConn) (*streamv1.DeleteExpiredSegmentsResponse, error) {
		client := streamv1.NewStreamServiceClient(conn)
		return client.DeleteExpiredSegments(ctx, &streamv1.DeleteExpiredSegmentsRequest{
			Group: g.Metadata.Name,
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(tr.Start),
				End:   timestamppb.New(tr.End),
			},
		})
	})
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to delete expired segments in group %s", g.Metadata.Name)
		return
	}

	l.l.Info().Msgf("deleted %d expired segments in group %s", resp.Deleted, g.Metadata.Name)
	progress.MarkStreamGroupDeleted(g.Metadata.Name)
	progress.Save(l.progressFilePath, l.l)
}

func (l *lifecycleService) processMeasureGroup(ctx context.Context, g *commonv1.Group, measureDir string,
	nodes []*databasev1.Node, labels map[string]string, progress *Progress,
) {
	tr := l.getRemovalSegmentsTimeRange(g)
	if tr.Start.IsZero() && tr.End.IsZero() {
		l.l.Info().Msgf("no removal segments time range for group %s, skipping measure migration", g.Metadata.Name)
		progress.MarkGroupCompleted(g.Metadata.Name)
		progress.Save(l.progressFilePath, l.l)
		return
	}

	// Try file-based migration first
	if err := l.processMeasureGroupFileBased(ctx, g, measureDir, tr, nodes, labels, progress); err != nil {
		l.l.Error().Err(err).Msgf("failed to migrate measure group %s using file-based approach", g.Metadata.Name)
		return
	}

	l.l.Info().Msgf("deleting expired measure segments for group: %s", g.Metadata.Name)
	l.deleteExpiredMeasureSegments(ctx, g, tr, progress)
	progress.MarkGroupCompleted(g.Metadata.Name)
	progress.Save(l.progressFilePath, l.l)
}

// processMeasureGroupFileBased uses file-based migration instead of query-based migration.
func (l *lifecycleService) processMeasureGroupFileBased(_ context.Context, g *commonv1.Group,
	measureDir string, tr *timestamp.TimeRange, nodes []*databasev1.Node, labels map[string]string, progress *Progress,
) error {
	if progress.IsMeasureGroupDeleted(g.Metadata.Name) {
		l.l.Info().Msgf("skipping already completed file-based measure migration for group: %s", g.Metadata.Name)
		return nil
	}

	l.l.Info().Msgf("starting file-based measure migration for group: %s", g.Metadata.Name)

	// Use the file-based migration with existing visitor pattern
	err := migrateMeasureWithFileBasedAndProgress(
		filepath.Join(measureDir, g.Metadata.Name), // Use snapshot directory as source
		*tr,              // Time range for segments to migrate
		g,                // Group configuration
		labels,           // Node labels
		nodes,            // Target nodes
		l.metadata,       // Metadata repository
		l.l,              // Logger
		progress,         // Progress tracking
		int(l.chunkSize), // Chunk size for streaming
	)
	if err != nil {
		return fmt.Errorf("file-based measure migration failed: %w", err)
	}

	l.l.Info().Msgf("completed file-based measure migration for group: %s", g.Metadata.Name)
	return nil
}

func (l *lifecycleService) deleteExpiredMeasureSegments(ctx context.Context, g *commonv1.Group, tr *timestamp.TimeRange, progress *Progress) {
	if progress.IsMeasureGroupDeleted(g.Metadata.Name) {
		l.l.Info().Msgf("skipping already deleted measure group segments: %s", g.Metadata.Name)
		return
	}

	resp, err := snapshot.Conn(l.gRPCAddr, l.enableTLS, l.insecure, l.cert, func(conn *grpc.ClientConn) (*measurev1.DeleteExpiredSegmentsResponse, error) {
		client := measurev1.NewMeasureServiceClient(conn)
		return client.DeleteExpiredSegments(ctx, &measurev1.DeleteExpiredSegmentsRequest{
			Group: g.Metadata.Name,
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(tr.Start),
				End:   timestamppb.New(tr.End),
			},
		})
	})
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to delete expired segments in group %s", g.Metadata.Name)
		return
	}

	l.l.Info().Msgf("deleted %d expired segments in group %s", resp.Deleted, g.Metadata.Name)
	progress.MarkMeasureGroupDeleted(g.Metadata.Name)
	progress.Save(l.progressFilePath, l.l)
}
