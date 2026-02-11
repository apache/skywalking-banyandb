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
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/robfig/cron/v3"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/property/gossip"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type repairScheduler struct {
	metadata        metadata.Repo
	gossipMessenger gossip.Messenger
	scheduler       *timestamp.Scheduler
}

func newRepairScheduler(ctx context.Context, l *logger.Logger, cronExp string, metadata metadata.Repo, messenger gossip.Messenger) (*repairScheduler, error) {
	r := &repairScheduler{
		metadata:        metadata,
		gossipMessenger: messenger,
		scheduler:       timestamp.NewScheduler(l, timestamp.NewClock()),
	}
	err := r.scheduler.Register("trigger", cron.Minute|cron.Hour|cron.Dom|cron.Month|cron.Dow|cron.Descriptor,
		cronExp, func(time.Time, *logger.Logger) bool {
			l.Debug().Msgf("starting background repair gossip")
			group, shardNum, nodes, gossipErr := r.doRepairGossip(ctx)
			if gossipErr != nil {
				l.Err(gossipErr).Msg("failed to repair gossip")
				return true
			}
			l.Info().Str("group", group).Uint32("shardNum", shardNum).Strs("nodes", nodes).Msg("background repair gossip scheduled")
			return true
		})
	if err != nil {
		return nil, fmt.Errorf("failed to add repair trigger cron task: %w", err)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to add repair build tree cron task: %w", err)
	}
	return r, nil
}

func (r *repairScheduler) close() {
	r.scheduler.Close()
}

func (r *repairScheduler) doRepairGossip(ctx context.Context) (string, uint32, []string, error) {
	group, shardNum, err := r.randomSelectGroup(ctx)
	if err != nil {
		return "", 0, nil, fmt.Errorf("selecting random group failure: %w", err)
	}

	nodes, err := r.gossipMessenger.LocateNodes(group.Metadata.Name, shardNum, uint32(r.copiesCount(group)))
	if err != nil {
		return "", 0, nil, fmt.Errorf("locating nodes for group %s, shard %d failure: %w", group.Metadata.Name, shardNum, err)
	}
	return group.Metadata.Name, shardNum, nodes, r.gossipMessenger.Propagation(nodes, group.Metadata.Name, shardNum)
}

func (r *repairScheduler) randomSelectGroup(ctx context.Context) (*commonv1.Group, uint32, error) {
	allGroups, err := r.metadata.GroupRegistry().ListGroup(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("listing groups failure: %w", err)
	}

	groups := make([]*commonv1.Group, 0)
	for _, group := range allGroups {
		if group.Catalog != commonv1.Catalog_CATALOG_PROPERTY {
			continue
		}
		// if the group don't have copies, skip it
		if r.copiesCount(group) < 2 {
			continue
		}
		groups = append(groups, group)
	}

	if len(groups) == 0 {
		return nil, 0, fmt.Errorf("no groups found with enough copies for repair")
	}
	// #nosec G404 -- not security-critical, just for random selection
	group := groups[rand.Int64()%int64(len(groups))]
	// #nosec G404 -- not security-critical, just for random selection
	return group, rand.Uint32() % group.ResourceOpts.ShardNum, nil
}

func (r *repairScheduler) copiesCount(group *commonv1.Group) int {
	return int(group.ResourceOpts.Replicas + 1)
}
