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
	"math"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
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
	metadata         metadata.Repo
	omr              observability.MetricsRegistry
	l                *logger.Logger
	gRPCAddr         string
	cert             string
	streamRoot       string
	measureRoot      string
	progressFilePath string
	enableTLS        bool
	insecure         bool
}

// NewService creates a new lifecycle service.
func NewService(meta metadata.Repo, omr observability.MetricsRegistry) run.Unit {
	return &lifecycleService{
		metadata: meta,
		omr:      omr,
	}
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
	return flagS
}

func (l *lifecycleService) Validate() error {
	return nil
}

func (l *lifecycleService) GracefulStop() {
}

func (l *lifecycleService) Name() string {
	return "lifecycle"
}

func (l *lifecycleService) Serve() run.StopNotify {
	l.l = logger.GetLogger("lifecycle")
	ctx := context.Background()
	done := make(chan struct{})
	close(done)

	progress := LoadProgress(l.progressFilePath, l.l)

	groups, err := l.getGroupsToProcess(ctx, progress)
	if err != nil {
		l.l.Error().Err(err).Msg("failed to get groups to process")
		return done
	}

	if len(groups) == 0 {
		l.l.Info().Msg("no groups to process, all groups already completed")
		progress.Remove(l.progressFilePath, l.l)
		return done
	}

	streamDir, measureDir, err := l.getSnapshots(groups)
	if err != nil {
		l.l.Error().Err(err).Msg("failed to get snapshots")
		return done
	}
	streamSVC, measureSVC, err := l.setupQuerySvc(ctx, streamDir, measureDir)
	if err != nil {
		l.l.Error().Err(err).Msg("failed to setup query service")
		return done
	}
	if streamSVC != nil {
		defer streamSVC.GracefulStop()
	}
	if measureSVC != nil {
		defer measureSVC.GracefulStop()
	}
	nodes, err := l.metadata.NodeRegistry().ListNode(ctx, databasev1.Role_ROLE_DATA)
	if err != nil {
		l.l.Error().Err(err).Msg("failed to list data nodes")
		return done
	}
	labels := common.ParseNodeFlags()

	for _, g := range groups {
		switch g.Catalog {
		case commonv1.Catalog_CATALOG_STREAM:
			if streamSVC == nil {
				l.l.Error().Msgf("stream service is not available, skipping group: %s", g.Metadata.Name)
				continue
			}
			l.processStreamGroup(ctx, g, streamSVC, nodes, labels, progress)
		case commonv1.Catalog_CATALOG_MEASURE:
			if measureSVC == nil {
				l.l.Error().Msgf("measure service is not available, skipping group: %s", g.Metadata.Name)
				continue
			}
			l.processMeasureGroup(ctx, g, measureSVC, nodes, labels, progress)
		default:
			l.l.Info().Msgf("group catalog: %s doesn't support lifecycle management", g.Catalog)
		}

		progress.MarkGroupCompleted(g.Metadata.Name)
		progress.Save(l.progressFilePath, l.l)
	}

	progress.Remove(l.progressFilePath, l.l)
	l.l.Info().Msg("lifecycle migration completed successfully")
	return done
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
			l.l.Info().Msgf("skipping already completed group: %s", g.Metadata.Name)
			continue
		}
		groups = append(groups, g)
	}

	return groups, nil
}

func (l *lifecycleService) processStreamGroup(ctx context.Context, g *commonv1.Group, streamSVC stream.Service,
	nodes []*databasev1.Node, labels map[string]string, progress *Progress,
) {
	shardNum, replicas, selector, client, err := parseGroup(ctx, g, labels, nodes, l.l, l.metadata)
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to parse group %s", g.Metadata.Name)
		return
	}
	defer client.GracefulStop()

	ss, err := l.metadata.StreamRegistry().ListStream(ctx, schema.ListOpt{Group: g.Metadata.Name})
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to list streams in group %s", g.Metadata.Name)
		return
	}

	tr := streamSVC.GetRemovalSegmentsTimeRange(g.Metadata.Name)

	l.processStreams(ctx, g, ss, streamSVC, tr, shardNum, replicas, selector, client, progress)

	l.deleteExpiredStreamSegments(ctx, g, tr, progress)
}

func (l *lifecycleService) processStreams(ctx context.Context, g *commonv1.Group, streams []*databasev1.Stream,
	streamSVC stream.Service, tr *timestamp.TimeRange, shardNum uint32, replicas uint32, selector node.Selector, client queue.Client, progress *Progress,
) {
	for _, s := range streams {
		if progress.IsStreamCompleted(g.Metadata.Name, s.Metadata.Name) {
			l.l.Info().Msgf("skipping already completed stream: %s/%s", g.Metadata.Name, s.Metadata.Name)
			continue
		}

		if sum, err := l.processSingleStream(ctx, s, streamSVC, tr, shardNum, replicas, selector, client); err == nil {
			l.l.Info().Msgf("migrated %d elements in stream %s", sum, s.Metadata.Name)
		}

		progress.MarkStreamCompleted(g.Metadata.Name, s.Metadata.Name)
		progress.Save(l.progressFilePath, l.l)
	}
}

func (l *lifecycleService) processSingleStream(ctx context.Context, s *databasev1.Stream,
	streamSVC stream.Service, tr *timestamp.TimeRange, shardNum uint32, replicas uint32, selector node.Selector, client queue.Client,
) (int, error) {
	q, err := streamSVC.Stream(s.Metadata)
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to get stream %s", s.Metadata.Name)
		return 0, err
	}

	tagProjection := make([]model.TagProjection, len(s.TagFamilies))
	entity := make([]*modelv1.TagValue, len(s.Entity.TagNames))
	for idx := range s.Entity.TagNames {
		entity[idx] = pbv1.AnyTagValue
	}
	for i, tf := range s.TagFamilies {
		tagProjection[i] = model.TagProjection{
			Family: tf.Name,
			Names:  make([]string, len(tf.Tags)),
		}
		for j, t := range tf.Tags {
			tagProjection[i].Names[j] = t.Name
		}
	}

	result, err := q.Query(ctx, model.StreamQueryOptions{
		Name:           s.Metadata.Name,
		TagProjection:  tagProjection,
		Entities:       [][]*modelv1.TagValue{entity},
		TimeRange:      tr,
		MaxElementSize: math.MaxInt,
	})
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to query stream %s", s.Metadata.Name)
		return 0, err
	}
	return migrateStream(ctx, s, result, shardNum, replicas, selector, client, l.l), nil
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
	shardNum, replicas, selector, client, err := parseGroup(ctx, g, labels, nodes, l.l, l.metadata)
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

	l.processMeasures(ctx, g, mm, measureSVC, tr, shardNum, replicas, selector, client, progress)

	l.deleteExpiredMeasureSegments(ctx, g, tr, progress)
}

func (l *lifecycleService) processMeasures(ctx context.Context, g *commonv1.Group, measures []*databasev1.Measure,
	measureSVC measure.Service, tr *timestamp.TimeRange, shardNum uint32, replicas uint32, selector node.Selector, client queue.Client, progress *Progress,
) {
	for _, m := range measures {
		if progress.IsMeasureCompleted(g.Metadata.Name, m.Metadata.Name) {
			l.l.Info().Msgf("skipping already completed measure: %s/%s", g.Metadata.Name, m.Metadata.Name)
			continue
		}

		if sum, err := l.processSingleMeasure(ctx, m, measureSVC, tr, shardNum, replicas, selector, client); err == nil {
			l.l.Info().Msgf("migrated %d elements in measure %s", sum, m.Metadata.Name)
		}

		progress.MarkMeasureCompleted(g.Metadata.Name, m.Metadata.Name)
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
