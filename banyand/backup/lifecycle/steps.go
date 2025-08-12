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
	"fmt"
	"os"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
)

func (l *lifecycleService) getSnapshots(groups []*commonv1.Group, p *Progress) (streamDir string, measureDir string, err error) {
	// If we already have snapshot dirs in Progress, reuse them
	if p.SnapshotStreamDir != "" || p.SnapshotMeasureDir != "" {
		return p.SnapshotStreamDir, p.SnapshotMeasureDir, nil
	}

	snapshotGroups := make([]*databasev1.SnapshotRequest_Group, 0, len(groups))
	for _, group := range groups {
		snapshotGroups = append(snapshotGroups, &databasev1.SnapshotRequest_Group{
			Group:   group.Metadata.Name,
			Catalog: group.Catalog,
		})
	}
	snn, err := snapshot.Get(l.gRPCAddr, l.enableTLS, l.insecure, l.cert, snapshotGroups...)
	if err != nil {
		return "", "", err
	}
	for _, snp := range snn {
		snapshotDir, errDir := snapshot.Dir(snp, l.streamRoot, l.measureRoot, "")
		if errDir != nil {
			l.l.Error().Err(errDir).Msgf("Failed to get snapshot directory for %s", snp.Name)
			continue
		}
		if _, err := os.Stat(snapshotDir); os.IsNotExist(err) {
			l.l.Error().Err(err).Msgf("Snapshot directory %s does not exist", snapshotDir)
			continue
		}
		if snp.Catalog == commonv1.Catalog_CATALOG_STREAM {
			streamDir = snapshotDir
		}
		if snp.Catalog == commonv1.Catalog_CATALOG_MEASURE {
			measureDir = snapshotDir
		}
	}
	// Save the new snapshot paths into Progress
	p.SnapshotStreamDir = streamDir
	p.SnapshotMeasureDir = measureDir
	return streamDir, measureDir, nil
}

func parseGroup(g *commonv1.Group, nodeLabels map[string]string, nodes []*databasev1.Node,
	l *logger.Logger, metadata metadata.Repo,
) (uint32, uint32, *commonv1.IntervalRule, node.Selector, queue.Client, error) {
	ro := g.ResourceOpts
	if ro == nil {
		return 0, 0, nil, nil, nil, fmt.Errorf("no resource opts in group %s", g.Metadata.Name)
	}
	if len(ro.Stages) == 0 {
		return 0, 0, nil, nil, nil, fmt.Errorf("no stages in group %s", g.Metadata.Name)
	}
	var nst *commonv1.LifecycleStage
	for i, st := range ro.Stages {
		selector, err := pub.ParseLabelSelector(st.NodeSelector)
		if err != nil {
			return 0, 0, nil, nil, nil, errors.WithMessagef(err, "failed to parse node selector %s", st.NodeSelector)
		}
		if !selector.Matches(nodeLabels) {
			continue
		}
		if i+1 >= len(ro.Stages) {
			l.Info().Msgf("no next stage for group %s at stage %s", g.Metadata.Name, st.Name)
			return 0, 0, nil, nil, nil, nil
		}
		nst = ro.Stages[i+1]
		l.Info().Msgf("migrating group %s at stage %s to stage %s", g.Metadata.Name, st.Name, nst.Name)
		break
	}
	if nst == nil {
		nst = ro.Stages[0]
	}
	nsl, err := pub.ParseLabelSelector(nst.NodeSelector)
	if err != nil {
		return 0, 0, nil, nil, nil, errors.WithMessagef(err, "failed to parse node selector %s", nst.NodeSelector)
	}
	nodeSel := node.NewRoundRobinSelector("", metadata)
	if ok, _ := nodeSel.OnInit([]schema.Kind{schema.KindGroup}); !ok {
		return 0, 0, nil, nil, nil, fmt.Errorf("failed to initialize node selector for group %s", g.Metadata.Name)
	}
	client := pub.NewWithoutMetadata()
	if g.Catalog == commonv1.Catalog_CATALOG_STREAM {
		_ = grpc.NewClusterNodeRegistry(data.TopicStreamWrite, client, nodeSel)
	} else {
		_ = grpc.NewClusterNodeRegistry(data.TopicMeasureWrite, client, nodeSel)
	}

	var existed bool
	for _, n := range nodes {
		if n.Labels == nil {
			continue
		}
		if nsl.Matches(n.Labels) {
			existed = true
			client.OnAddOrUpdate(schema.Metadata{
				TypeMeta: schema.TypeMeta{
					Kind: schema.KindNode,
				},
				Spec: n,
			})
		}
	}
	if !existed {
		return 0, 0, nil, nil, nil, errors.New("no nodes matched")
	}
	return nst.ShardNum, nst.Replicas, nst.Ttl, nodeSel, client, nil
}

type fileInfo struct {
	file fs.File
	name string
}
