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
	"encoding/base64"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func (l *lifecycleService) getSnapshots(groups []*commonv1.Group) (streamDir string, measureDir string, err error) {
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
			logger.Warningf("Failed to get snapshot directory for %s: %v", snp.Name, errDir)
			continue
		}
		if snp.Catalog == commonv1.Catalog_CATALOG_STREAM {
			streamDir = snapshotDir
		}
		if snp.Catalog == commonv1.Catalog_CATALOG_MEASURE {
			measureDir = snapshotDir
		}
	}
	return streamDir, measureDir, nil
}

func (l *lifecycleService) setupQuerySvc(ctx context.Context, streamDir, measureDir string) (stream.Service, measure.Service, error) {
	pm := protector.NewMemory(l.omr)
	streamSVC, err := stream.NewReadonlyService(l.metadata, l.omr, pm)
	if err != nil {
		return nil, nil, err
	}
	if err = streamSVC.FlagSet().Parse([]string{"--stream-root-path", streamDir}); err != nil {
		return nil, nil, err
	}
	if err = streamSVC.PreRun(ctx); err != nil {
		return nil, nil, err
	}
	defer streamSVC.GracefulStop()
	measureSVC, err := measure.NewReadonlyService(l.metadata, l.omr, pm)
	if err != nil {
		return nil, nil, err
	}
	if err = measureSVC.FlagSet().Parse([]string{"--measure-root-path", measureDir}); err != nil {
		return nil, nil, err
	}
	if err = measureSVC.PreRun(ctx); err != nil {
		return nil, nil, err
	}
	defer measureSVC.GracefulStop()
	return streamSVC, measureSVC, nil
}

func parseGroup(g *commonv1.Group, nodeLabels map[string]string, nodes []*databasev1.Node, l *logger.Logger) (uint32, node.Selector, queue.Client, error) {
	ro := g.ResourceOpts
	if ro == nil {
		return 0, nil, nil, fmt.Errorf("no resource opts in group %s", g.Metadata.Name)
	}
	if len(ro.Stages) == 0 {
		return 0, nil, nil, fmt.Errorf("no stages in group %s", g.Metadata.Name)
	}
	var nst *commonv1.LifecycleStage
	for i, st := range ro.Stages {
		selector, err := pub.ParseLabelSelector(st.NodeSelector)
		if err != nil {
			return 0, nil, nil, errors.WithMessagef(err, "failed to parse node selector %s", st.NodeSelector)
		}
		if !selector.Matches(nodeLabels) {
			continue
		}
		if i+1 >= len(ro.Stages) {
			l.Info().Msgf("no next stage for group %s at stage %s", g.Metadata.Name, st.Name)
			return 0, nil, nil, nil
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
		return 0, nil, nil, errors.WithMessagef(err, "failed to parse node selector %s", nst.NodeSelector)
	}
	nodeSel := node.NewRoundRobinSelector("", nil)
	client := pub.New(nil)

	var existed bool
	for _, n := range nodes {
		if n.Labels == nil {
			continue
		}
		if nsl.Matches(n.Labels) {
			existed = true
			nodeSel.AddNode(n)
			client.OnAddOrUpdate(schema.Metadata{
				TypeMeta: schema.TypeMeta{
					Kind: schema.KindNode,
				},
				Spec: n,
			})
		}
	}
	if !existed {
		return 0, nil, nil, errors.New("no nodes matched")
	}
	return nst.ShardNum, nodeSel, client, nil
}

func migrateStream(ctx context.Context, s *databasev1.Stream, result model.StreamQueryResult,
	shardNum uint32, selector node.Selector, client queue.Client, l *logger.Logger,
) {
	defer result.Release()

	entityLocator := partition.NewEntityLocator(s.TagFamilies, s.Entity, 0)

	batch := client.NewBatchPublisher(30 * time.Second)
	defer batch.Close()
	for sr := result.Pull(ctx); sr != nil; sr = result.Pull(ctx) {
		for i := range sr.ElementIDs {
			writeEntity := &streamv1.WriteRequest{
				Metadata: s.Metadata,
				Element:  &streamv1.ElementValue{},
			}
			ev := writeEntity.Element
			ev.ElementId = base64.StdEncoding.EncodeToString(convert.Uint64ToBytes(sr.ElementIDs[i]))
			ev.Timestamp = timestamppb.New(time.Unix(0, sr.Timestamps[i]))
			for _, tf := range sr.TagFamilies {
				tfw := &modelv1.TagFamilyForWrite{}
				for _, tag := range tf.Tags {
					tfw.Tags = append(tfw.Tags, tag.Values[i])
				}
				ev.TagFamilies = append(ev.TagFamilies, tfw)
			}
			entity, tagValues, shardID, err := entityLocator.Locate(s.Metadata.Name, ev.TagFamilies, shardNum)
			if err != nil {
				l.Error().Err(err).Msg("failed to locate entity")
				continue
			}
			nodeID, err := selector.Pick(s.Metadata.Group, s.Metadata.Name, uint32(shardID))
			if err != nil {
				l.Error().Err(err).Msg("failed to pick node")
				continue
			}
			iwr := &streamv1.InternalWriteRequest{
				Request:      writeEntity,
				ShardId:      uint32(shardID),
				SeriesHash:   pbv1.HashEntity(entity),
				EntityValues: tagValues[1:].Encode(),
			}
			message := bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), nodeID, iwr)
			_, err = batch.Publish(ctx, data.TopicStreamWrite, message)
			if err != nil {
				l.Error().Err(err).Msg("failed to publish message")
			}
		}
	}
}

func migrateMeasure(ctx context.Context, m *databasev1.Measure, result model.MeasureQueryResult,
	shardNum uint32, selector node.Selector, client queue.Client, l *logger.Logger,
) {
	defer result.Release()

	entityLocator := partition.NewEntityLocator(m.TagFamilies, m.Entity, 0)

	batch := client.NewBatchPublisher(30 * time.Second)
	defer batch.Close()
	for mr := result.Pull(); mr != nil; mr = result.Pull() {
		for i := range mr.Timestamps {
			writeRequest := &measurev1.WriteRequest{
				Metadata: m.Metadata,
				DataPoint: &measurev1.DataPointValue{
					Timestamp: timestamppb.New(time.Unix(0, mr.Timestamps[i])),
				},
				MessageId: uint64(time.Now().UnixNano()),
			}

			for _, tf := range mr.TagFamilies {
				tfWrite := &modelv1.TagFamilyForWrite{}
				for _, tag := range tf.Tags {
					tfWrite.Tags = append(tfWrite.Tags, tag.Values[i])
				}
				writeRequest.DataPoint.TagFamilies = append(writeRequest.DataPoint.TagFamilies, tfWrite)
			}

			for _, field := range mr.Fields {
				writeRequest.DataPoint.Fields = append(writeRequest.DataPoint.Fields, field.Values[i])
			}

			entity, tagValues, shardID, err := entityLocator.Locate(m.Metadata.Name, writeRequest.DataPoint.TagFamilies, shardNum)
			if err != nil {
				l.Error().Err(err).Msg("failed to locate entity")
				continue
			}

			nodeID, err := selector.Pick(m.Metadata.Group, m.Metadata.Name, uint32(shardID))
			if err != nil {
				l.Error().Err(err).Msg("failed to pick node")
				continue
			}

			iwr := &measurev1.InternalWriteRequest{
				Request:      writeRequest,
				ShardId:      uint32(shardID),
				SeriesHash:   pbv1.HashEntity(entity),
				EntityValues: tagValues[1:].Encode(),
			}

			message := bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), nodeID, iwr)
			_, err = batch.Publish(ctx, data.TopicMeasureWrite, message)
			if err != nil {
				l.Error().Err(err).Msg("failed to publish message")
			}
		}
	}
}
