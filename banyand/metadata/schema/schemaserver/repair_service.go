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

package schemaserver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/gossip"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// RepairService is the independent schema repair service.
type RepairService interface {
	run.PreRunner
	run.Config
	run.Service
	GetGossipPort() *uint32
}

type repairService struct {
	schema.UnimplementedOnInitHandler
	registerGossip func(gossip.Messenger)
	metadata       metadata.Repo
	messenger      gossip.Messenger
	scheduler      *timestamp.Scheduler
	closer         *run.Closer
	l              *logger.Logger
	metaNodes      map[string]struct{}
	repairCron     string
	mu             sync.RWMutex
}

// NewGossipService creates a new schema repair service.
func NewGossipService(registerGossip func(gossip.Messenger), metadata metadata.Repo,
	pipelineClient queue.Client, omr observability.MetricsRegistry,
) RepairService {
	return &repairService{
		registerGossip: registerGossip,
		metadata:       metadata,
		closer:         run.NewCloser(1),
		metaNodes:      make(map[string]struct{}),
		messenger: gossip.NewMessenger(
			"schema-property",
			func(n *databasev1.Node) string { return n.PropertySchemaGossipGrpcAddress },
			omr, metadata, pipelineClient, 17933,
		),
	}
}

func (r *repairService) Name() string {
	return "schema-repair"
}

func (r *repairService) Role() databasev1.Role {
	return databasev1.Role_ROLE_META
}

func (r *repairService) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("schema-property-repair")
	flagS.StringVar(&r.repairCron, "schema-property-repair-trigger-cron", "@every 10m", "the cron expression for schema repair gossip")
	flagS.AddFlagSet(r.messenger.FlagSet().FlagSet)
	return flagS
}

func (r *repairService) Validate() error {
	_, cronErr := cron.ParseStandard(r.repairCron)
	if cronErr != nil {
		return fmt.Errorf("schema-property-repair-trigger-cron is not a valid cron expression: %w", cronErr)
	}
	return r.messenger.Validate()
}

func (r *repairService) PreRun(ctx context.Context) error {
	r.l = logger.GetLogger("schema-repair")
	if preRunErr := r.messenger.PreRun(ctx); preRunErr != nil {
		return preRunErr
	}
	r.registerGossip(r.messenger)
	r.metadata.RegisterHandler("schema-repair-nodes", schema.KindNode, r)
	r.scheduler = timestamp.NewScheduler(r.l, timestamp.NewClock())
	registerErr := r.scheduler.Register("schema-property-repair-trigger",
		cron.Minute|cron.Hour|cron.Dom|cron.Month|cron.Dow|cron.Descriptor,
		r.repairCron, func(_ time.Time, l *logger.Logger) bool {
			l.Debug().Msg("starting schema repair gossip")
			if repairErr := r.doRepairGossip(); repairErr != nil {
				l.Err(repairErr).Msg("schema repair gossip failed")
			}
			return true
		})
	if registerErr != nil {
		return fmt.Errorf("failed to register schema repair cron task: %w", registerErr)
	}
	return nil
}

func (r *repairService) Serve() run.StopNotify {
	r.messenger.Serve(r.closer)
	return r.closer.CloseNotify()
}

func (r *repairService) GracefulStop() {
	if r.scheduler != nil {
		r.scheduler.Close()
	}
	r.messenger.GracefulStop()
	r.closer.CloseThenWait()
}

// GetGossipPort returns the gossip gRPC port.
func (r *repairService) GetGossipPort() *uint32 {
	return r.messenger.GetServerPort()
}

// OnAddOrUpdate tracks META nodes that have a schema repair gossip address.
func (r *repairService) OnAddOrUpdate(md schema.Metadata) {
	if md.Kind != schema.KindNode {
		return
	}
	node, ok := md.Spec.(*databasev1.Node)
	if !ok {
		return
	}
	if node.PropertySchemaGossipGrpcAddress == "" {
		return
	}
	containsMetaRole := false
	for _, role := range node.Roles {
		if role == databasev1.Role_ROLE_META {
			containsMetaRole = true
			break
		}
	}
	if !containsMetaRole {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metaNodes[node.Metadata.GetName()] = struct{}{}
}

// OnDelete removes nodes from tracking.
func (r *repairService) OnDelete(md schema.Metadata) {
	if md.Kind != schema.KindNode {
		return
	}
	node, ok := md.Spec.(*databasev1.Node)
	if !ok {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.metaNodes, node.Metadata.GetName())
}

func (r *repairService) doRepairGossip() error {
	r.mu.RLock()
	nodes := make([]string, 0, len(r.metaNodes))
	for name := range r.metaNodes {
		nodes = append(nodes, name)
	}
	r.mu.RUnlock()
	if len(nodes) < 2 {
		r.l.Debug().Msg("schema repair gossip is skipped because there are " +
			"less than 2 meta nodes with schema repair gossip address")
		return nil
	}
	return r.messenger.Propagation(nodes, schemaGroup, 0)
}
