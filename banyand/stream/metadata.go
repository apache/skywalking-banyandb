// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/internal/wqueue"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/idgen"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/schema/registry"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// streamRegistryKinds is the kind set the stream schemaRepo registers with
// the per-node NodeRepoRegistry; mirrors the stream variant in
// banyand/measure/metadata.go.
const streamRegistryKinds = schema.KindGroup | schema.KindStream | schema.KindIndexRule | schema.KindIndexRuleBinding

var metadataScope = streamScope.SubScope("metadata")

// SchemaService allows querying schema information.
type SchemaService interface {
	Query
	Close()
}
type schemaRepo struct {
	resourceSchema.Repository
	l        *logger.Logger
	metadata metadata.Repo
	idGen    *idgen.Generator
	path     string
	nodeID   string
	role     databasev1.Role
}

func newSchemaRepo(path string, svc *standalone, nodeLabels map[string]string, nodeID string) schemaRepo {
	sr := schemaRepo{
		l:        svc.l,
		path:     path,
		metadata: svc.metadata,
		nodeID:   nodeID,
		idGen:    idgen.NewGenerator(nodeID, svc.l),
		role:     databasev1.Role_ROLE_DATA,
		Repository: resourceSchema.NewRepository(
			svc.metadata,
			svc.l,
			newSupplier(path, svc, nodeLabels),
			resourceSchema.NewMetrics(svc.omr.With(metadataScope)),
		),
	}
	sr.start()
	sr.registerWithNodeRepo()
	return sr
}

func newLiaisonSchemaRepo(path string, svc *liaison, streamDataNodeRegistry grpc.NodeRegistry, nodeID string) schemaRepo {
	sr := schemaRepo{
		l:        svc.l,
		path:     path,
		metadata: svc.metadata,
		nodeID:   nodeID,
		idGen:    idgen.NewGenerator(nodeID, svc.l),
		role:     databasev1.Role_ROLE_LIAISON,
		Repository: resourceSchema.NewRepository(
			svc.metadata,
			svc.l,
			newQueueSupplier(path, svc, streamDataNodeRegistry),
			resourceSchema.NewMetrics(svc.omr.With(metadataScope)),
		),
	}
	sr.start()
	sr.registerWithNodeRepo()
	return sr
}

func (sr *schemaRepo) start() {
	sr.Watcher()
	sr.metadata.
		RegisterHandler("stream", schema.KindGroup|schema.KindStream|schema.KindIndexRuleBinding|schema.KindIndexRule,
			sr)
}

// registerWithNodeRepo joins this schemaRepo to the per-node aggregator so the
// cluster barrier and NodeSchemaStatusService route Group/Stream/IndexRule/
// IndexRuleBinding lookups through the same cache the executor consults via
// LoadGroup / LoadResource.
func (sr *schemaRepo) registerWithNodeRepo() {
	metaSvc, ok := sr.metadata.(metadata.Service)
	if !ok {
		return
	}
	registry.MaybeRegister(metaSvc.NodeRepoRegistry(), streamRegistryKinds, sr.Repository)
}

func (sr *schemaRepo) Stream(metadata *commonv1.Metadata) (Stream, error) {
	sm, ok := sr.loadStream(metadata)
	if !ok {
		return nil, errors.WithStack(ErrStreamNotExist)
	}
	return sm, nil
}

func (sr *schemaRepo) GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange {
	g, ok := sr.LoadGroup(group)
	if !ok {
		return nil
	}
	db := g.SupplyTSDB()
	if db == nil {
		return nil
	}
	return db.(storage.TSDB[*tsTable, option]).GetExpiredSegmentsTimeRange()
}

func (sr *schemaRepo) OnInit(kinds []schema.Kind) (bool, []int64) {
	if len(kinds) != 4 {
		logger.Panicf("invalid kinds: %v", kinds)
		return false, nil
	}
	_, revs := sr.Repository.Init(schema.KindStream)
	return true, revs
}

func (sr *schemaRepo) OnAddOrUpdate(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindGroup:
		g := metadata.Spec.(*commonv1.Group)
		if g.Catalog != commonv1.Catalog_CATALOG_STREAM {
			return
		}
		if err := validate.GroupForNonProperty(g); err != nil {
			sr.l.Warn().Err(err).Msg("group is ignored")
			return
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventAddOrUpdate,
			Kind:     resourceSchema.EventKindGroup,
			Metadata: g,
		})
	case schema.KindStream:
		if err := validate.Stream(metadata.Spec.(*databasev1.Stream)); err != nil {
			sr.l.Warn().Err(err).Msg("stream is ignored")
			return
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventAddOrUpdate,
			Kind:     resourceSchema.EventKindResource,
			Metadata: metadata.Spec.(*databasev1.Stream),
		})
	case schema.KindIndexRuleBinding:
		if irb, ok := metadata.Spec.(*databasev1.IndexRuleBinding); ok {
			if err := validate.IndexRuleBinding(irb); err != nil {
				sr.l.Warn().Err(err).Msg("index rule binding is ignored")
				return
			}
			if irb.GetSubject().Catalog == commonv1.Catalog_CATALOG_STREAM {
				sr.SendMetadataEvent(resourceSchema.MetadataEvent{
					Typ:      resourceSchema.EventAddOrUpdate,
					Kind:     resourceSchema.EventKindIndexRuleBinding,
					Metadata: irb,
				})
			}
		}
	case schema.KindIndexRule:
		if ir, ok := metadata.Spec.(*databasev1.IndexRule); ok {
			if err := validate.IndexRule(metadata.Spec.(*databasev1.IndexRule)); err != nil {
				sr.l.Warn().Err(err).Msg("index rule is ignored")
				return
			}
			sr.SendMetadataEvent(resourceSchema.MetadataEvent{
				Typ:      resourceSchema.EventAddOrUpdate,
				Kind:     resourceSchema.EventKindIndexRule,
				Metadata: ir,
			})
		}
	default:
	}
}

func (sr *schemaRepo) OnDelete(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindGroup:
		g := metadata.Spec.(*commonv1.Group)
		if g.Catalog != commonv1.Catalog_CATALOG_STREAM {
			return
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventDelete,
			Kind:     resourceSchema.EventKindGroup,
			Metadata: g,
		})
	case schema.KindStream:
		streamSpec := metadata.Spec.(*databasev1.Stream)
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:            resourceSchema.EventDelete,
			Kind:           resourceSchema.EventKindResource,
			DeleteRevision: streamSpec.GetMetadata().GetModRevision(),
			Metadata:       streamSpec,
		})
	case schema.KindIndexRuleBinding:
		if binding, ok := metadata.Spec.(*databasev1.IndexRuleBinding); ok {
			if binding.GetSubject().Catalog == commonv1.Catalog_CATALOG_STREAM {
				sr.SendMetadataEvent(resourceSchema.MetadataEvent{
					Typ:      resourceSchema.EventDelete,
					Kind:     resourceSchema.EventKindIndexRuleBinding,
					Metadata: metadata.Spec.(*databasev1.IndexRuleBinding),
				})
			}
		}
	case schema.KindIndexRule:
		if rule, ok := metadata.Spec.(*databasev1.IndexRule); ok {
			sr.SendMetadataEvent(resourceSchema.MetadataEvent{
				Typ:      resourceSchema.EventDelete,
				Kind:     resourceSchema.EventKindIndexRule,
				Metadata: rule,
			})
		}
	default:
	}
}

func (sr *schemaRepo) loadStream(metadata *commonv1.Metadata) (*stream, bool) {
	r, ok := sr.LoadResource(metadata)
	if !ok {
		return nil, false
	}
	s, ok := r.Delegated().(*stream)
	return s, ok
}

func (sr *schemaRepo) loadTSDB(groupName string) (storage.TSDB[*tsTable, option], error) {
	if sr == nil {
		return nil, fmt.Errorf("schemaRepo is nil")
	}
	g, ok := sr.LoadGroup(groupName)
	if !ok {
		return nil, fmt.Errorf("group %s not found", groupName)
	}
	db := g.SupplyTSDB()
	if db == nil {
		return nil, fmt.Errorf("group %s not found", groupName)
	}
	return db.(storage.TSDB[*tsTable, option]), nil
}

// CollectDataInfo collects data info for a specific group.
func (sr *schemaRepo) CollectDataInfo(ctx context.Context, group string) (*databasev1.DataInfo, error) {
	if sr.nodeID == "" {
		return nil, fmt.Errorf("node ID is empty")
	}
	node, nodeErr := sr.metadata.NodeRegistry().GetNode(ctx, sr.nodeID)
	if nodeErr != nil {
		return nil, fmt.Errorf("failed to get current node info: %w", nodeErr)
	}
	tsdb, tsdbErr := sr.loadTSDB(group)
	if tsdbErr != nil {
		return nil, tsdbErr
	}
	if tsdb == nil {
		return nil, nil
	}
	segments, segmentsErr := tsdb.SelectSegments(timestamp.TimeRange{
		Start: time.Unix(0, 0),
		End:   time.Unix(0, timestamp.MaxNanoTime),
	}, false)
	if segmentsErr != nil {
		return nil, segmentsErr
	}
	var segmentInfoList []*databasev1.SegmentInfo
	var totalDataSize int64
	for _, segment := range segments {
		timeRange := segment.GetTimeRange()
		tables, _ := segment.Tables()
		var shardInfoList []*databasev1.ShardInfo
		if len(tables) > 0 {
			for shardIdx, table := range tables {
				shardInfo := sr.collectShardInfo(table, uint32(shardIdx))
				shardInfoList = append(shardInfoList, shardInfo)
				totalDataSize += shardInfo.DataSizeBytes
			}
		} else {
			// No live shard tables (a closed segment, or a brand-new open one
			// with no data yet): read shard part stats from disk without
			// reopening. The per-shard InvertedIndexInfo is reported empty here:
			// reading it would reopen the shard's bluge index (the exclusive-lock
			// churn this change exists to avoid), so it is populated only for
			// open segments.
			closedShards, closedSize := storage.CollectClosedShardInfo(segment.Location())
			shardInfoList = closedShards
			totalDataSize += closedSize
		}
		seriesIndexInfo := sr.collectSeriesIndexInfo(segment)
		totalDataSize += seriesIndexInfo.DataSizeBytes
		segmentInfo := &databasev1.SegmentInfo{
			SegmentId:       fmt.Sprintf("%d-%d", timeRange.Start.UnixNano(), timeRange.End.UnixNano()),
			TimeRangeStart:  timeRange.Start.Format(time.RFC3339Nano),
			TimeRangeEnd:    timeRange.End.Format(time.RFC3339Nano),
			ShardInfo:       shardInfoList,
			SeriesIndexInfo: seriesIndexInfo,
		}
		segmentInfoList = append(segmentInfoList, segmentInfo)
		segment.DecRef()
	}
	dataInfo := &databasev1.DataInfo{
		Node:          node,
		SegmentInfo:   segmentInfoList,
		DataSizeBytes: totalDataSize,
	}
	return dataInfo, nil
}

func (sr *schemaRepo) collectSeriesIndexInfo(segment storage.Segment[*tsTable, option]) *databasev1.SeriesIndexInfo {
	// SeriesIndexStats reads from the live index when the segment is open and
	// from disk (read-only) when it is closed, so inspecting a cold segment
	// never reopens its writable index.
	dataCount, dataSizeBytes := segment.SeriesIndexStats()
	return &databasev1.SeriesIndexInfo{
		DataCount:     dataCount,
		DataSizeBytes: dataSizeBytes,
	}
}

func (sr *schemaRepo) collectShardInfo(table any, shardID uint32) *databasev1.ShardInfo {
	tst, ok := table.(*tsTable)
	if !ok {
		return &databasev1.ShardInfo{
			ShardId:       shardID,
			DataCount:     0,
			DataSizeBytes: 0,
			PartCount:     0,
		}
	}
	snapshot := tst.currentSnapshot()
	if snapshot == nil {
		return &databasev1.ShardInfo{
			ShardId:       shardID,
			DataCount:     0,
			DataSizeBytes: 0,
			PartCount:     0,
		}
	}
	defer snapshot.decRef()
	var totalCount, compressedSize, uncompressedSize, partCount, filePartCount uint64
	for _, pw := range snapshot.parts {
		if pw.p != nil {
			totalCount += pw.p.partMetadata.TotalCount
			compressedSize += pw.p.partMetadata.CompressedSizeBytes
			uncompressedSize += pw.p.partMetadata.UncompressedSizeBytes
			partCount++
			filePartCount++
		} else if pw.mp != nil {
			totalCount += pw.mp.partMetadata.TotalCount
			compressedSize += pw.mp.partMetadata.CompressedSizeBytes
			uncompressedSize += pw.mp.partMetadata.UncompressedSizeBytes
			partCount++
		}
	}
	invertedIndexInfo := sr.collectInvertedIndexInfo(tst)
	return &databasev1.ShardInfo{
		ShardId:           shardID,
		DataCount:         int64(totalCount),
		DataSizeBytes:     int64(compressedSize),
		PartCount:         int64(partCount),
		InvertedIndexInfo: invertedIndexInfo,
		SidxInfo:          &databasev1.SIDXInfo{},
		FilePartCount:     int64(filePartCount),
	}
}

func (sr *schemaRepo) collectInvertedIndexInfo(tst *tsTable) *databasev1.InvertedIndexInfo {
	if tst.index == nil {
		return &databasev1.InvertedIndexInfo{
			DataCount:     0,
			DataSizeBytes: 0,
		}
	}
	dataCount, dataSizeBytes := tst.index.store.Stats()
	return &databasev1.InvertedIndexInfo{
		DataCount:     dataCount,
		DataSizeBytes: dataSizeBytes,
	}
}

func (sr *schemaRepo) collectPendingWriteInfo(groupName string) (int64, error) {
	if sr == nil || sr.Repository == nil {
		return 0, fmt.Errorf("schema repository is not initialized")
	}
	if sr.role == databasev1.Role_ROLE_LIAISON {
		queue, queueErr := sr.loadQueue(groupName)
		if queueErr != nil {
			return 0, fmt.Errorf("failed to load queue: %w", queueErr)
		}
		if queue == nil {
			return 0, nil
		}
		var pendingWriteCount int64
		for _, sq := range queue.SubQueues() {
			if sq != nil {
				pendingWriteCount += sq.getPendingDataCount()
			}
		}
		return pendingWriteCount, nil
	}
	// Standalone mode
	tsdb, tsdbErr := sr.loadTSDB(groupName)
	if tsdbErr != nil {
		return 0, fmt.Errorf("failed to load TSDB: %w", tsdbErr)
	}
	if tsdb == nil {
		return 0, fmt.Errorf("TSDB is nil for group %s", groupName)
	}
	segments, segmentsErr := tsdb.SelectSegments(timestamp.TimeRange{
		Start: time.Unix(0, 0),
		End:   time.Unix(0, timestamp.MaxNanoTime),
	}, false)
	if segmentsErr != nil {
		return 0, fmt.Errorf("failed to select segments: %w", segmentsErr)
	}
	var pendingWriteCount int64
	for _, segment := range segments {
		tables, _ := segment.Tables()
		for _, tst := range tables {
			pendingWriteCount += tst.getPendingDataCount()
		}
		segment.DecRef()
	}
	return pendingWriteCount, nil
}

func (sr *schemaRepo) collectPendingSyncInfo(groupName string) (partCount int64, totalSizeBytes int64, err error) {
	if sr == nil || sr.Repository == nil {
		return 0, 0, fmt.Errorf("schema repository is not initialized")
	}
	// Only liaison nodes collect pending sync info
	if sr.role != databasev1.Role_ROLE_LIAISON {
		return 0, 0, nil
	}
	queue, queueErr := sr.loadQueue(groupName)
	if queueErr != nil {
		return 0, 0, fmt.Errorf("failed to load queue: %w", queueErr)
	}
	if queue == nil {
		return 0, 0, nil
	}
	for _, sq := range queue.SubQueues() {
		if sq != nil {
			snapshot := sq.currentSnapshot()
			if snapshot != nil {
				for _, pw := range snapshot.parts {
					if pw.mp == nil && pw.p != nil && pw.p.partMetadata.TotalCount > 0 {
						partCount++
						totalSizeBytes += int64(pw.p.partMetadata.CompressedSizeBytes)
					}
				}
				snapshot.decRef()
			}
		}
	}
	return partCount, totalSizeBytes, nil
}

func (sr *schemaRepo) loadQueue(groupName string) (*wqueue.Queue[*tsTable, option], error) {
	g, ok := sr.LoadGroup(groupName)
	if !ok {
		return nil, fmt.Errorf("group %s not found", groupName)
	}
	db := g.SupplyTSDB()
	if db == nil {
		return nil, fmt.Errorf("queue for group %s not found", groupName)
	}
	return db.(*wqueue.Queue[*tsTable, option]), nil
}

var _ resourceSchema.ResourceSupplier = (*supplier)(nil)

type supplier struct {
	metadata   metadata.Repo
	omr        observability.MetricsRegistry
	pm         protector.Memory
	l          *logger.Logger
	schemaRepo *schemaRepo
	nodeLabels map[string]string
	path       string
	option     option
}

func newSupplier(path string, svc *standalone, nodeLabels map[string]string) *supplier {
	if svc.pm == nil {
		svc.l.Panic().Msg("CRITICAL: svc.pm is nil in newSupplier")
	}
	opt := svc.option
	opt.protector = svc.pm

	if opt.protector == nil {
		svc.l.Panic().Msg("CRITICAL: opt.protector is still nil after assignment")
	}

	return &supplier{
		metadata:   svc.metadata,
		l:          svc.l,
		option:     opt,
		omr:        svc.omr,
		pm:         svc.pm,
		path:       path,
		schemaRepo: &svc.schemaRepo,
		nodeLabels: nodeLabels,
	}
}

func (s *supplier) OpenResource(spec resourceSchema.Resource) (resourceSchema.IndexListener, error) {
	streamSchema := spec.Schema().(*databasev1.Stream)
	return openStream(streamSpec{
		schema: streamSchema,
	}, s.l, s.pm, s.schemaRepo), nil
}

func (s *supplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.metadata.StreamRegistry().GetStream(ctx, md)
}

func (s *supplier) OpenDB(groupSchema *commonv1.Group) (resourceSchema.DB, error) {
	name := groupSchema.Metadata.Name
	p := common.Position{
		Module:   "stream",
		Database: name,
	}
	ro := groupSchema.ResourceOpts
	if ro == nil {
		return nil, fmt.Errorf("no resource opts in group %s", name)
	}
	res, err := pub.ResolveStage(s.l, name, ro, s.nodeLabels)
	if err != nil {
		return nil, err
	}
	group := groupSchema.Metadata.Name
	opts := storage.TSDBOpts[*tsTable, option]{
		ShardNum:                       res.ResourceOpts.ShardNum,
		Location:                       path.Join(s.path, group),
		TSTableCreator:                 newTSTable,
		TableMetrics:                   s.newMetrics(p),
		SegmentInterval:                storage.MustToIntervalRule(res.ResourceOpts.SegmentInterval),
		TTL:                            storage.MustToIntervalRule(res.ResourceOpts.Ttl),
		Option:                         s.option,
		SeriesIndexFlushTimeoutSeconds: s.option.flushTimeout.Nanoseconds() / int64(time.Second),
		SeriesIndexCacheMaxBytes:       int(s.option.seriesCacheMaxSize),
		StorageMetricsFactory:          s.omr.With(storageScope.ConstLabels(meter.ToLabelPairs(common.DBLabelNames(), p.DBLabelValues()))),
		SegmentIdleTimeout:             res.SegmentIdleTimeout,
		DisableRetention:               res.DisableRetention,
		DisableRotation:                res.DisableRotation,
		MemoryLimit:                    s.pm.GetLimit(),
	}
	return storage.OpenTSDB(
		common.SetPosition(context.Background(), func(_ common.Position) common.Position {
			return p
		}),
		opts, nil, group,
	)
}

// ResolveResourceOpts returns the stage-resolved ResourceOpts so a group UpdateOptions
// applies the matched stage's interval/ttl/shardNum instead of the group default.
func (s *supplier) ResolveResourceOpts(groupSchema *commonv1.Group) *commonv1.ResourceOpts {
	return pub.ResolveResourceOptsForUpdate(s.l, groupSchema, s.nodeLabels)
}

var _ resourceSchema.ResourceSupplier = (*queueSupplier)(nil)

type queueSupplier struct {
	metadata               metadata.Repo
	omr                    observability.MetricsRegistry
	pm                     protector.Memory
	streamDataNodeRegistry grpc.NodeRegistry
	l                      *logger.Logger
	schemaRepo             *schemaRepo
	path                   string
	option                 option
}

func newQueueSupplier(path string, svc *liaison, streamDataNodeRegistry grpc.NodeRegistry) *queueSupplier {
	if svc.pm == nil {
		svc.l.Panic().Msg("CRITICAL: svc.pm is nil in newSupplier")
	}
	opt := svc.option
	opt.protector = svc.pm

	if opt.protector == nil {
		svc.l.Panic().Msg("CRITICAL: opt.protector is still nil after assignment")
	}

	return &queueSupplier{
		metadata:               svc.metadata,
		l:                      svc.l,
		option:                 opt,
		omr:                    svc.omr,
		pm:                     svc.pm,
		path:                   path,
		schemaRepo:             &svc.schemaRepo,
		streamDataNodeRegistry: streamDataNodeRegistry,
	}
}

func (s *queueSupplier) OpenResource(spec resourceSchema.Resource) (resourceSchema.IndexListener, error) {
	streamSchema := spec.Schema().(*databasev1.Stream)
	return openStream(streamSpec{
		schema: streamSchema,
	}, s.l, s.pm, s.schemaRepo), nil
}

func (s *queueSupplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.metadata.StreamRegistry().GetStream(ctx, md)
}

// ResolveResourceOpts returns the group opts unchanged: the liaison write queue has no
// lifecycle stages to resolve.
func (s *queueSupplier) ResolveResourceOpts(groupSchema *commonv1.Group) *commonv1.ResourceOpts {
	return groupSchema.GetResourceOpts()
}

func (s *queueSupplier) OpenDB(groupSchema *commonv1.Group) (resourceSchema.DB, error) {
	name := groupSchema.Metadata.Name
	p := common.Position{
		Module:   "stream",
		Database: name,
	}
	ro := groupSchema.ResourceOpts
	if ro == nil {
		return nil, fmt.Errorf("no resource opts in group %s", name)
	}
	shardNum := ro.ShardNum
	group := groupSchema.Metadata.Name
	metrics, metricsFactory := s.newMetrics(p)
	opts := wqueue.Opts[*tsTable, option]{
		Group:           group,
		ShardNum:        shardNum,
		SegmentInterval: storage.MustToIntervalRule(ro.SegmentInterval),
		Location:        path.Join(s.path, group),
		Option:          s.option,
		Metrics:         metrics,
		MetricsFactory:  metricsFactory,
		SubQueueCreator: newWriteQueue,
		GetNodes: func(shardID common.ShardID) []string {
			copies := ro.Replicas + 1
			nodes, err := s.streamDataNodeRegistry.LocateAll(group, uint32(shardID), int(copies))
			if err != nil {
				s.l.Error().Err(err).Str("group", group).Uint32("shard", uint32(shardID)).Msg("failed to locate nodes")
				return nil
			}
			return nodes
		},
	}
	return wqueue.Open(
		common.SetPosition(context.Background(), func(_ common.Position) common.Position {
			return p
		}),
		opts, group,
	)
}
