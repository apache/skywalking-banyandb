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
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
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
	measureSVC, err := l.setupQuerySvc(ctx, measureDir)
	if measureSVC != nil {
		defer measureSVC.GracefulStop()
	}
	if err != nil {
		l.l.Error().Err(err).Msg("failed to setup query service")
		return err
	}

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
			if measureSVC == nil {
				l.l.Error().Msgf("measure service is not available, skipping group: %s", g.Metadata.Name)
				progress.MarkMeasureError(g.Metadata.Name, "", fmt.Sprintf("measure service unavailable for group %s", g.Metadata.Name))
				allGroupsCompleted = false
				continue
			}
			l.processMeasureGroup(ctx, g, measureSVC, nodes, labels, progress)
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

// generateReport gathers counts & errors from Progress, writes one JSON file per run, and keeps only 5 latest.
func (l *lifecycleService) generateReport(p *Progress) {
	type grp struct {
		MeasureCounts map[string]int    `json:"measure_counts"`
		MeasureErrors map[string]string `json:"measure_errors"`
		Name          string            `json:"name"`
	}
	reportDir := l.reportDir
	if err := os.MkdirAll(reportDir, 0o755); err != nil {
		l.l.Error().Err(err).Msgf("failed to create report dir %s", reportDir)
		return
	}

	// build report
	var groups []grp
	for name := range p.CompletedGroups {
		groups = append(groups, grp{
			Name:          name,
			MeasureCounts: p.MeasureCounts[name],
			MeasureErrors: p.MeasureErrors[name],
		})
	}
	// also include groups that had only measures
	for name := range p.CompletedMeasures {
		if _, seen := p.CompletedGroups[name]; seen {
			continue
		}
		groups = append(groups, grp{
			Name:          name,
			MeasureCounts: p.MeasureCounts[name],
			MeasureErrors: p.MeasureErrors[name],
		})
	}
	payload := struct {
		GeneratedAt time.Time `json:"generated_at"`
		Groups      []grp     `json:"groups"`
	}{
		GeneratedAt: time.Now(),
		Groups:      groups,
	}

	// write file
	fname := time.Now().Format("20060102_150405") + ".json"
	fpath := filepath.Join(reportDir, fname)
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		l.l.Error().Err(err).Msg("failed to marshal migration report")
		return
	}
	if err = os.WriteFile(fpath, data, 0o600); err != nil {
		l.l.Error().Err(err).Msgf("failed to write report file %s", fpath)
		return
	}
	l.l.Info().Msgf("wrote migration report %s", fpath)

	// rotate: keep only 5 latest
	entries, err := os.ReadDir(reportDir)
	if err != nil {
		return
	}
	type fi struct {
		t    time.Time
		name string
	}
	var list []fi
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		list = append(list, fi{name: e.Name(), t: info.ModTime()})
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].t.After(list[j].t)
	})
	for i := 5; i < len(list); i++ {
		_ = os.Remove(filepath.Join(reportDir, list[i].name))
	}
	l.l.Info().Msg("rotated old migration reports")
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

func (l *lifecycleService) processMeasureGroup(ctx context.Context, g *commonv1.Group, measureSVC measure.Service,
	nodes []*databasev1.Node, labels map[string]string, progress *Progress,
) {
	shardNum, replicas, _, selector, client, err := parseGroup(g, labels, nodes, l.l, l.metadata)
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to parse group %s", g.Metadata.Name)
		return
	}
	defer client.GracefulStop()

	mm, err := l.metadata.MeasureRegistry().ListMeasure(ctx, schema.ListOpt{Group: g.Metadata.Name})
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to list measures in group %s", g.Metadata.Name)
		return
	}

	tr := measureSVC.GetRemovalSegmentsTimeRange(g.Metadata.Name)
	if tr.Start.IsZero() && tr.End.IsZero() {
		l.l.Info().Msgf("no removal segments time range for group %s, skipping measure migration", g.Metadata.Name)
		progress.MarkGroupCompleted(g.Metadata.Name)
		progress.Save(l.progressFilePath, l.l)
		return
	}

	l.processMeasures(ctx, g, mm, measureSVC, tr, shardNum, replicas, selector, client, progress)

	allMeasuresDone := true
	for _, m := range mm {
		if !progress.IsMeasureCompleted(g.Metadata.Name, m.Metadata.Name) {
			allMeasuresDone = false
			break
		}
	}
	if allMeasuresDone {
		l.l.Info().Msgf("deleting expired measure segments for group: %s", g.Metadata.Name)
		l.deleteExpiredMeasureSegments(ctx, g, tr, progress)
		progress.MarkGroupCompleted(g.Metadata.Name)
		progress.Save(l.progressFilePath, l.l)
	} else {
		l.l.Info().Msgf("skipping delete expired measure segments for group %s: some measures not fully migrated", g.Metadata.Name)
	}
}

func (l *lifecycleService) processMeasures(
	ctx context.Context,
	g *commonv1.Group,
	measures []*databasev1.Measure,
	measureSVC measure.Service,
	tr *timestamp.TimeRange,
	shardNum uint32,
	replicas uint32,
	selector node.Selector,
	client queue.Client,
	progress *Progress,
) {
	for _, m := range measures {
		if progress.IsMeasureCompleted(g.Metadata.Name, m.Metadata.Name) {
			l.l.Debug().Msgf("skipping already completed measure: %s/%s", g.Metadata.Name, m.Metadata.Name)
			continue
		}
		sum, err := l.processSingleMeasure(ctx, m, measureSVC, tr, shardNum, replicas, selector, client)
		if err != nil {
			progress.MarkMeasureError(g.Metadata.Name, m.Metadata.Name, err.Error())
		} else {
			if sum < 1 {
				l.l.Debug().Msgf("no elements migrated for measure %s", m.Metadata.Name)
			} else {
				l.l.Info().Msgf("migrated %d elements in measure %s", sum, m.Metadata.Name)
			}
			progress.MarkMeasureCompleted(g.Metadata.Name, m.Metadata.Name, sum)
		}
		progress.Save(l.progressFilePath, l.l)
	}
}

func (l *lifecycleService) processSingleMeasure(ctx context.Context, m *databasev1.Measure,
	measureSVC measure.Service, tr *timestamp.TimeRange, shardNum uint32, replicas uint32, selector node.Selector, client queue.Client,
) (int, error) {
	q, err := measureSVC.Measure(m.Metadata)
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to get measure %s", m.Metadata.Name)
		return 0, err
	}

	tagProjection := make([]model.TagProjection, len(m.TagFamilies))
	for i, tf := range m.TagFamilies {
		tagProjection[i] = model.TagProjection{
			Family: tf.Name,
			Names:  make([]string, len(tf.Tags)),
		}
		for j, t := range tf.Tags {
			tagProjection[i].Names[j] = t.Name
		}
	}
	fieldProjection := make([]string, len(m.Fields))
	for i, f := range m.Fields {
		fieldProjection[i] = f.Name
	}
	entity := make([]*modelv1.TagValue, len(m.Entity.TagNames))
	for idx := range m.Entity.TagNames {
		entity[idx] = pbv1.AnyTagValue
	}

	result, err := q.Query(ctx, model.MeasureQueryOptions{
		Name:            m.Metadata.Name,
		TagProjection:   tagProjection,
		FieldProjection: fieldProjection,
		Entities:        [][]*modelv1.TagValue{entity},
		TimeRange:       tr,
	})
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to query measure %s", m.Metadata.Name)
		return 0, err
	}

	return migrateMeasure(ctx, m, result, shardNum, replicas, selector, client, l.l), nil
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
