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

package property

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/spf13/pflag"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/gossip"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	defaultFlushTimeout = 5 * time.Second
)

var (
	errEmptyRootPath         = errors.New("root path is empty")
	_                Service = (*service)(nil)
)

type service struct {
	metadata                 metadata.Repo
	pipeline                 queue.Server
	gossipMessenger          gossip.Messenger
	omr                      observability.MetricsRegistry
	lfs                      fs.FileSystem
	pm                       protector.Memory
	close                    chan struct{}
	db                       *database
	l                        *logger.Logger
	nodeID                   string
	root                     string
	snapshotDir              string
	repairDir                string
	repairBuildTreeCron      string
	repairTriggerCron        string
	flushTimeout             time.Duration
	expireTimeout            time.Duration
	repairQuickBuildTreeTime time.Duration
	repairTreeSlotCount      int
	maxDiskUsagePercent      int
	maxFileSnapshotNum       int
	repairEnabled            bool
}

func (s *service) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("storage")
	flagS.StringVar(&s.root, "property-root-path", "/tmp", "the root path of database")
	flagS.DurationVar(&s.flushTimeout, "property-flush-timeout", defaultFlushTimeout, "the memory data timeout of measure")
	flagS.IntVar(&s.maxDiskUsagePercent, "property-max-disk-usage-percent", 95, "the maximum disk usage percentage allowed")
	flagS.IntVar(&s.maxFileSnapshotNum, "property-max-file-snapshot-num", 2, "the maximum number of file snapshots allowed")
	flagS.DurationVar(&s.expireTimeout, "property-expire-delete-timeout", time.Hour*24*7, "the duration of the expired data needs to be deleted")
	flagS.IntVar(&s.repairTreeSlotCount, "property-repair-tree-slot-count", 32, "the slot count of the repair tree")
	flagS.StringVar(&s.repairBuildTreeCron, "property-repair-build-tree-cron", "@every 1h", "the cron expression for repairing the build tree")
	flagS.DurationVar(&s.repairQuickBuildTreeTime, "property-repair-quick-build-tree-time", time.Minute*10,
		"the duration of the quick build tree after operate the property")
	flagS.StringVar(&s.repairTriggerCron, "property-repair-trigger-cron", "* 2 * * *", "the cron expression for background repairing the property data")
	flagS.BoolVar(&s.repairEnabled, "property-repair-enabled", false, "whether to enable the background property repair")
	s.gossipMessenger.FlagSet().VisitAll(func(f *pflag.Flag) {
		flagS.AddFlag(f)
	})
	return flagS
}

func (s *service) Validate() error {
	if s.root == "" {
		return errEmptyRootPath
	}
	if s.maxDiskUsagePercent < 0 {
		return errors.New("property-max-disk-usage-percent must be greater than or equal to 0")
	}
	if s.maxDiskUsagePercent > 100 {
		return errors.New("property-max-disk-usage-percent must be less than or equal to 100")
	}
	_, err := cron.ParseStandard(s.repairBuildTreeCron)
	if err != nil {
		return errors.New("property-repair-build-tree-cron is not a valid cron expression")
	}
	if err = s.gossipMessenger.Validate(); err != nil {
		return fmt.Errorf("gossip vilidate failure: %w", err)
	}
	return nil
}

func (s *service) Name() string {
	return "property"
}

func (s *service) Role() databasev1.Role {
	return databasev1.Role_ROLE_DATA
}

func (s *service) PreRun(ctx context.Context) error {
	s.l = logger.GetLogger(s.Name())
	s.lfs = fs.NewLocalFileSystemWithLoggerAndLimit(s.l, s.pm.GetLimit())
	path := path.Join(s.root, s.Name())
	s.snapshotDir = filepath.Join(path, storage.SnapshotsDir)
	s.repairDir = filepath.Join(path, storage.RepairDir)
	observability.UpdatePath(path)
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return errors.New("node id is empty")
	}
	node := val.(common.Node)
	s.nodeID = node.NodeID

	var err error
	snapshotLis := &snapshotListener{s: s}
	s.db, err = openDB(ctx, filepath.Join(path, storage.DataDir), s.flushTimeout, s.expireTimeout, s.repairTreeSlotCount, s.omr, s.lfs,
		s.repairEnabled, s.repairDir, s.repairBuildTreeCron, s.repairQuickBuildTreeTime, s.repairTriggerCron, s.gossipMessenger, s.metadata,
		func(ctx context.Context) (string, error) {
			res := snapshotLis.Rev(ctx,
				bus.NewMessage(bus.MessageID(time.Now().UnixNano()), []*databasev1.SnapshotRequest_Group{}))
			snpMsg := res.Data().(*databasev1.Snapshot)
			if snpMsg.Error != "" {
				return "", errors.New(snpMsg.Error)
			}
			return filepath.Join(snapshotLis.s.snapshotDir, snpMsg.Name, storage.DataDir), nil
		})
	if err != nil {
		return err
	}

	// if the gossip address is empty or repair scheduler is not start, it means that the gossip is not enabled.
	if node.PropertyGossipGrpcAddress == "" || s.db.repairScheduler == nil {
		s.gossipMessenger = nil
	}
	if s.gossipMessenger != nil {
		if err = s.gossipMessenger.PreRun(ctx); err != nil {
			return err
		}
		s.gossipMessenger.RegisterServices(s.db.repairScheduler.registerServerToGossip())
		s.db.repairScheduler.registerClientToGossip(s.gossipMessenger)
	}
	return multierr.Combine(
		s.pipeline.Subscribe(data.TopicPropertyUpdate, &updateListener{s: s, path: path, maxDiskUsagePercent: s.maxDiskUsagePercent}),
		s.pipeline.Subscribe(data.TopicPropertyDelete, &deleteListener{s: s}),
		s.pipeline.Subscribe(data.TopicPropertyQuery, &queryListener{s: s}),
		s.pipeline.Subscribe(data.TopicSnapshot, snapshotLis),
		s.pipeline.Subscribe(data.TopicPropertyRepair, &repairListener{s: s}),
	)
}

func (s *service) Serve() run.StopNotify {
	if s.gossipMessenger != nil {
		s.gossipMessenger.Serve(s.close)
	}
	return s.close
}

func (s *service) GracefulStop() {
	if s.gossipMessenger != nil {
		s.gossipMessenger.GracefulStop()
	}
	close(s.close)
	err := s.db.close()
	if err != nil {
		s.l.Err(err).Msg("Fail to close the property module")
	}
}

func (s *service) GetGossIPGrpcPort() *uint32 {
	return s.gossipMessenger.GetServerPort()
}

// NewService returns a new service.
func NewService(metadata metadata.Repo, pipeline queue.Server, omr observability.MetricsRegistry, pm protector.Memory) (Service, error) {
	return &service{
		metadata: metadata,
		pipeline: pipeline,
		omr:      omr,
		db:       &database{},
		pm:       pm,
		close:    make(chan struct{}),

		gossipMessenger: gossip.NewMessenger(omr, metadata),
	}, nil
}
