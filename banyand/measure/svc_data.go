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

package measure

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter/native"
	"github.com/apache/skywalking-banyandb/pkg/run"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	serviceName = "measure"
)

var _ Service = (*dataSVC)(nil)

type dataSVC struct {
	lfs                 fs.FileSystem
	c                   storage.Cache
	pipeline            queue.Server
	omr                 observability.MetricsRegistry
	metadata            metadata.Repo
	pm                  protector.Memory
	metricPipeline      queue.Server
	schemaRepo          *schemaRepo
	l                   *logger.Logger
	cm                  *cacheMetrics
	root                string
	dataPath            string
	snapshotDir         string
	option              option
	cc                  storage.CacheConfig
	maxDiskUsagePercent int
	maxFileSnapshotNum  int
}

func (s *dataSVC) Measure(metadata *commonv1.Metadata) (Measure, error) {
	sm, ok := s.schemaRepo.loadMeasure(metadata)
	if !ok {
		return nil, errors.WithStack(ErrMeasureNotExist)
	}
	return sm, nil
}

func (s *dataSVC) LoadGroup(name string) (resourceSchema.Group, bool) {
	return s.schemaRepo.LoadGroup(name)
}

func (s *dataSVC) GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange {
	return s.schemaRepo.GetRemovalSegmentsTimeRange(group)
}

func (s *dataSVC) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("storage")
	flagS.StringVar(&s.root, "measure-root-path", "/tmp", "the root path of measure")
	flagS.StringVar(&s.dataPath, "measure-data-path", "", "the data directory path of measure. If not set, <measure-root-path>/measure/data will be used")
	flagS.DurationVar(&s.option.flushTimeout, "measure-flush-timeout", defaultFlushTimeout, "the memory data timeout of measure")
	s.option.mergePolicy = newDefaultMergePolicy()
	flagS.VarP(&s.option.mergePolicy.maxFanOutSize, "measure-max-fan-out-size", "", "the upper bound of a single file size after merge of measure")
	s.option.seriesCacheMaxSize = run.Bytes(32 << 20)
	flagS.VarP(&s.option.seriesCacheMaxSize, "measure-series-cache-max-size", "", "the max size of series cache in each group")
	flagS.IntVar(&s.maxDiskUsagePercent, "measure-max-disk-usage-percent", 95, "the maximum disk usage percentage allowed")
	flagS.IntVar(&s.maxFileSnapshotNum, "measure-max-file-snapshot-num", 10, "the maximum number of file snapshots allowed")
	s.cc.MaxCacheSize = run.Bytes(100 * 1024 * 1024)
	flagS.VarP(&s.cc.MaxCacheSize, "service-cache-max-size", "", "maximum service cache size (e.g., 100M)")
	flagS.DurationVar(&s.cc.CleanupInterval, "service-cache-cleanup-interval", 30*time.Second, "service cache cleanup interval")
	flagS.DurationVar(&s.cc.IdleTimeout, "service-cache-idle-timeout", 2*time.Minute, "service cache entry idle timeout")
	return flagS
}

func (s *dataSVC) Validate() error {
	if s.root == "" {
		return errEmptyRootPath
	}
	if s.maxDiskUsagePercent < 0 {
		return errors.New("measure-max-disk-usage-percent must be greater than or equal to 0")
	}
	if s.maxDiskUsagePercent > 100 {
		return errors.New("measure-max-disk-usage-percent must be less than or equal to 100")
	}
	if s.cc.MaxCacheSize < 0 {
		return errors.New("service-cache-max-size must be greater than or equal to 0")
	}
	if s.cc.CleanupInterval <= 0 {
		return errors.New("service-cache-cleanup-interval must be greater than 0")
	}
	if s.cc.IdleTimeout <= 0 {
		return errors.New("service-cache-idle-timeout must be greater than 0")
	}
	return nil
}

func (s *dataSVC) Name() string {
	return serviceName
}

func (s *dataSVC) Role() databasev1.Role {
	return databasev1.Role_ROLE_DATA
}

func (s *dataSVC) PreRun(ctx context.Context) error {
	s.l = logger.GetLogger(s.Name())
	s.l.Info().Msg("memory protector is initialized in PreRun")
	s.lfs = fs.NewLocalFileSystemWithLoggerAndLimit(s.l, s.pm.GetLimit())
	path := path.Join(s.root, s.Name())
	s.snapshotDir = filepath.Join(path, storage.SnapshotsDir)
	observability.UpdatePath(path)
	if s.dataPath == "" {
		s.dataPath = filepath.Join(path, storage.DataDir)
	}
	if !strings.HasPrefix(filepath.VolumeName(s.dataPath), filepath.VolumeName(path)) {
		observability.UpdatePath(s.dataPath)
	}
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return errors.New("node id is empty")
	}
	s.c = storage.NewServiceCacheWithConfig(s.cc)
	node := val.(common.Node)
	s.schemaRepo = newDataSchemaRepo(s.dataPath, s, node.Labels)

	s.cm = newCacheMetrics(s.omr)
	observability.MetricsCollector.Register("measure_cache", s.collectCacheMetrics)

	if s.pipeline == nil {
		return nil
	}

	if err := s.createDataNativeObservabilityGroup(ctx); err != nil {
		return err
	}

	if err := s.pipeline.Subscribe(data.TopicSnapshot, &dataSnapshotListener{s: s}); err != nil {
		return err
	}

	if err := s.pipeline.Subscribe(data.TopicMeasureDeleteExpiredSegments, &dataDeleteStreamSegmentsListener{s: s}); err != nil {
		return err
	}

	s.pipeline.RegisterChunkedSyncHandler(data.TopicMeasurePartSync, setUpChunkedSyncCallback(s.l, s.schemaRepo))
	s.pipeline.RegisterChunkedSyncHandler(data.TopicMeasureSeriesSync, setUpSyncSeriesCallback(s.l, s.schemaRepo))
	err := s.pipeline.Subscribe(data.TopicMeasureSeriesIndexInsert, setUpIndexCallback(s.l, s.schemaRepo, data.TopicMeasureSeriesIndexInsert))
	if err != nil {
		return err
	}
	err = s.pipeline.Subscribe(data.TopicMeasureSeriesIndexUpdate, setUpIndexCallback(s.l, s.schemaRepo, data.TopicMeasureSeriesIndexUpdate))
	if err != nil {
		return err
	}

	writeListener := setUpWriteCallback(s.l, s.schemaRepo, s.maxDiskUsagePercent)
	err = s.pipeline.Subscribe(data.TopicMeasureWrite, writeListener)
	if err != nil {
		return err
	}
	// only subscribe metricPipeline for data node
	if s.metricPipeline != nil {
		err = s.metricPipeline.Subscribe(data.TopicMeasureWrite, writeListener)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *dataSVC) Serve() run.StopNotify {
	return s.schemaRepo.StopCh()
}

func (s *dataSVC) GracefulStop() {
	observability.MetricsCollector.Unregister("measure_cache")
	s.schemaRepo.Close()
	s.c.Close()
}

func (s *dataSVC) collectCacheMetrics() {
	if s.cm == nil || s.c == nil {
		return
	}

	requests := s.c.Requests()
	misses := s.c.Misses()
	length := s.c.Entries()
	size := s.c.Size()
	var hitRatio float64
	if requests > 0 {
		hitRatio = float64(requests-misses) / float64(requests)
	}

	s.cm.requests.Set(float64(requests))
	s.cm.misses.Set(float64(misses))
	s.cm.hitRatio.Set(hitRatio)
	s.cm.entries.Set(float64(length))
	s.cm.size.Set(float64(size))
}

func (s *dataSVC) createDataNativeObservabilityGroup(ctx context.Context) error {
	if !s.omr.NativeEnabled() {
		return nil
	}
	g := &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name: native.ObservabilityGroupName,
		},
		Catalog: commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 1,
			SegmentInterval: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  1,
			},
			Ttl: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  1,
			},
		},
	}
	if err := s.metadata.GroupRegistry().CreateGroup(ctx, g); err != nil &&
		!errors.Is(err, schema.ErrGRPCAlreadyExists) {
		return errors.WithMessage(err, "failed to create native observability group")
	}
	return nil
}

func (s *dataSVC) takeGroupSnapshot(dstDir string, groupName string) error {
	group, ok := s.schemaRepo.LoadGroup(groupName)
	if !ok {
		return errors.Errorf("group %s not found", groupName)
	}
	db := group.SupplyTSDB()
	if db == nil {
		return errors.Errorf("group %s has no tsdb", group.GetSchema().Metadata.Name)
	}
	tsdb := db.(storage.TSDB[*tsTable, option])
	if err := tsdb.TakeFileSnapshot(dstDir); err != nil {
		return errors.WithMessagef(err, "snapshot %s fail to take file snapshot for group %s", dstDir, group.GetSchema().Metadata.Name)
	}
	return nil
}

// NewDataSVC returns a new data service.
func NewDataSVC(metadata metadata.Repo, pipeline queue.Server, metricPipeline queue.Server, omr observability.MetricsRegistry, pm protector.Memory) (Service, error) {
	return &dataSVC{
		metadata:       metadata,
		pipeline:       pipeline,
		metricPipeline: metricPipeline,
		omr:            omr,
		pm:             pm,
	}, nil
}

// NewReadonlyDataSVC returns a new readonly data service.
func NewReadonlyDataSVC(metadata metadata.Repo, omr observability.MetricsRegistry, pm protector.Memory) (Service, error) {
	return &dataSVC{
		metadata: metadata,
		omr:      omr,
		pm:       pm,
	}, nil
}

func newDataSchemaRepo(path string, svc *dataSVC, nodeLabels map[string]string) *schemaRepo {
	sr := &schemaRepo{
		path:     path,
		l:        svc.l,
		metadata: svc.metadata,
	}
	sr.Repository = resourceSchema.NewRepository(
		svc.metadata,
		svc.l,
		newDataSupplier(path, svc, sr, nodeLabels),
		resourceSchema.NewMetrics(svc.omr.With(metadataScope)),
	)
	sr.start()
	return sr
}

func newDataSupplier(path string, svc *dataSVC, sr *schemaRepo, nodeLabels map[string]string) *supplier {
	if svc.pm == nil {
		svc.l.Panic().Msg("CRITICAL: svc.pm is nil in newSupplier")
	}
	opt := svc.option
	opt.protector = svc.pm

	if opt.protector == nil {
		svc.l.Panic().Msg("CRITICAL: opt.protector is still nil after assignment")
	}
	return &supplier{
		path:       path,
		metadata:   svc.metadata,
		l:          svc.l,
		c:          svc.c,
		option:     opt,
		omr:        svc.omr,
		pm:         svc.pm,
		schemaRepo: sr,
		nodeLabels: nodeLabels,
	}
}

type dataSnapshotListener struct {
	*bus.UnImplementedHealthyListener
	s           *dataSVC
	snapshotSeq uint64
	snapshotMux sync.Mutex
}

func (d *dataSnapshotListener) Rev(ctx context.Context, message bus.Message) bus.Message {
	groups := message.Data().([]*databasev1.SnapshotRequest_Group)
	var gg []resourceSchema.Group
	if len(groups) == 0 {
		gg = d.s.schemaRepo.LoadAllGroups()
	} else {
		for _, g := range groups {
			if g.Catalog != commonv1.Catalog_CATALOG_MEASURE {
				continue
			}
			group, ok := d.s.schemaRepo.LoadGroup(g.Group)
			if !ok {
				continue
			}
			gg = append(gg, group)
		}
	}
	if len(gg) == 0 {
		return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), nil)
	}
	d.snapshotMux.Lock()
	defer d.snapshotMux.Unlock()
	storage.DeleteStaleSnapshots(d.s.snapshotDir, d.s.maxFileSnapshotNum, d.s.lfs)
	sn := d.snapshotName()
	var err error
	for _, g := range gg {
		select {
		case <-ctx.Done():
			return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), nil)
		default:
		}
		if errGroup := d.s.takeGroupSnapshot(filepath.Join(d.s.snapshotDir, sn, g.GetSchema().Metadata.Name), g.GetSchema().Metadata.Name); err != nil {
			d.s.l.Error().Err(errGroup).Str("group", g.GetSchema().Metadata.Name).Msg("fail to take group snapshot")
			err = multierr.Append(err, errGroup)
			continue
		}
	}
	snp := &databasev1.Snapshot{
		Name:    sn,
		Catalog: commonv1.Catalog_CATALOG_MEASURE,
	}
	if err != nil {
		snp.Error = err.Error()
	}
	return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), snp)
}

func (d *dataSnapshotListener) snapshotName() string {
	d.snapshotSeq++
	return fmt.Sprintf("%s-%08X", time.Now().UTC().Format("20060102150405"), d.snapshotSeq)
}

type dataDeleteStreamSegmentsListener struct {
	*bus.UnImplementedHealthyListener
	s *dataSVC
}

func (d *dataDeleteStreamSegmentsListener) Rev(_ context.Context, message bus.Message) bus.Message {
	req := message.Data().(*measurev1.DeleteExpiredSegmentsRequest)
	if req == nil {
		return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), int64(0))
	}

	db, err := d.s.schemaRepo.loadTSDB(req.Group)
	if err != nil {
		d.s.l.Error().Err(err).Str("group", req.Group).Msg("failed to load tsdb")
		return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), int64(0))
	}
	deleted := db.DeleteExpiredSegments(timestamp.NewSectionTimeRange(req.TimeRange.Begin.AsTime(), req.TimeRange.End.AsTime()))
	return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), deleted)
}
