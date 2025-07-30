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
	"io"

	"github.com/pkg/errors"
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/property/gossip"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

var (
	gossipMerkelTreeReadPageSize int64 = 10
	gossipShardQueryDatabaseSize       = 100
)

type repairGossipBase struct {
	scheduler *repairScheduler
}

func (b *repairGossipBase) getTreeReader(ctx context.Context, group string, shardID uint32) (repairTreeReader, bool, error) {
	s, err := b.scheduler.db.loadShard(ctx, common.ShardID(shardID))
	if err != nil {
		return nil, false, fmt.Errorf("failed to load shard %d: %w", shardID, err)
	}
	tree, err := s.repairState.treeReader(group)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get tree reader for group %s: %w", group, err)
	}
	if tree == nil {
		// if the tree is nil, but the state file exist, means the tree(group) is empty
		stateExist, err := s.repairState.stateFileExist()
		if err != nil {
			return nil, false, fmt.Errorf("failed to check state file existence for group %s: %w", group, err)
		}
		// if the tree is nil, it means the tree is no data
		return &emptyRepairTreeReader{}, stateExist, nil
	}
	return tree, true, nil
}

func (b *repairGossipBase) sendTreeSummary(
	reader repairTreeReader,
	group string,
	shardID uint32,
	stream grpclib.BidiStreamingClient[propertyv1.RepairRequest, propertyv1.RepairResponse],
) (map[int32]*repairTreeNode, error) {
	root, err := reader.read(nil, 1, false)
	if err != nil {
		return nil, fmt.Errorf("failed to read tree root: %w", err)
	}
	if len(root) == 0 {
		return nil, fmt.Errorf("tree root is empty for group %s", group)
	}

	err = stream.Send(&propertyv1.RepairRequest{
		Data: &propertyv1.RepairRequest_TreeRoot{
			TreeRoot: &propertyv1.TreeRoot{
				Group:   group,
				ShardId: shardID,
				RootSha: root[0].shaValue,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send tree root for group %s: %w", group, err)
	}

	slotsNodes := make(map[int32]*repairTreeNode)
	var slots []*repairTreeNode
	for {
		slots, err = reader.read(root[0], gossipMerkelTreeReadPageSize, false)
		if err != nil {
			return nil, fmt.Errorf("failed to read slots for group %s: %w", group, err)
		}
		if len(slots) == 0 {
			break
		}

		slotReq := &propertyv1.TreeSlots{
			SlotSha: make([]*propertyv1.TreeSlotSHA, 0, len(slots)),
		}
		for _, s := range slots {
			slotsNodes[s.slotInx] = s
			slotReq.SlotSha = append(slotReq.SlotSha, &propertyv1.TreeSlotSHA{
				Slot:  s.slotInx,
				Value: s.shaValue,
			})
		}

		err = stream.Send(&propertyv1.RepairRequest{
			Data: &propertyv1.RepairRequest_TreeSlots{
				TreeSlots: slotReq,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to send tree slots for group %s: %w", group, err)
		}
	}

	// send the empty slots list means there are no more slots to send
	err = stream.Send(&propertyv1.RepairRequest{
		Data: &propertyv1.RepairRequest_TreeSlots{
			TreeSlots: &propertyv1.TreeSlots{
				SlotSha: []*propertyv1.TreeSlotSHA{},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send empty tree slots for group %s: %w", group, err)
	}

	return slotsNodes, nil
}

func (b *repairGossipBase) queryProperty(ctx context.Context, syncShard *shard, leafNodeEntity string) (*queryProperty, *propertyv1.Property, error) {
	g, n, entity, err := syncShard.repairState.parseLeafNodeEntity(leafNodeEntity)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse leaf node entity %s: %w", leafNodeEntity, err)
	}
	searchQuery, err := inverted.BuildPropertyQueryFromEntity(groupField, g, n, entityID, entity)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build query from leaf node entity %s: %w", leafNodeEntity, err)
	}
	queriedProperties, err := syncShard.search(ctx, searchQuery, gossipShardQueryDatabaseSize)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to search properties for leaf node entity %s: %w", leafNodeEntity, err)
	}
	var latestProperty *queryProperty
	for _, queried := range queriedProperties {
		if latestProperty == nil || queried.timestamp > latestProperty.timestamp {
			latestProperty = queried
		}
	}
	if latestProperty == nil {
		return nil, nil, nil
	}
	var p propertyv1.Property
	err = protojson.Unmarshal(latestProperty.source, &p)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal property from leaf node entity %s: %w", leafNodeEntity, err)
	}
	return latestProperty, &p, nil
}

type repairGossipClient struct {
	repairGossipBase
}

func newRepairGossipClient(s *repairScheduler) *repairGossipClient {
	return &repairGossipClient{
		repairGossipBase: repairGossipBase{
			scheduler: s,
		},
	}
}

func (r *repairGossipClient) Rev(ctx context.Context, nextNode *grpclib.ClientConn, request *propertyv1.PropagationRequest) error {
	client := propertyv1.NewRepairServiceClient(nextNode)
	var hasPropertyUpdated bool
	defer func() {
		if hasPropertyUpdated {
			err := r.scheduler.buildingTree([]common.ShardID{common.ShardID(request.ShardId)}, request.Group, true)
			if err != nil {
				r.scheduler.l.Warn().Err(err).Msgf("failed to rebuild tree for group %s, shard %d", request.Group, request.ShardId)
			}
		}
	}()
	reader, found, err := r.getTreeReader(ctx, request.Group, request.ShardId)
	if err != nil {
		return errors.Wrapf(gossip.ErrAbortPropagation, "failed to get tree reader on client side: %v", err)
	}
	if !found {
		return errors.Wrapf(gossip.ErrAbortPropagation, "tree for group %s, shard %d not found on client side", request.Group, request.ShardId)
	}
	defer reader.close()

	stream, err := client.Repair(ctx)
	if err != nil {
		return fmt.Errorf("failed to create repair stream: %w", err)
	}
	defer func() {
		_ = stream.CloseSend()
	}()

	// step 1: send merkle tree data
	clientSlotNodes, err := r.sendTreeSummary(reader, request.Group, request.ShardId, stream)
	if err != nil {
		// if the tree summary cannot be built, we should abort the propagation
		return errors.Wrapf(gossip.ErrAbortPropagation, "failed to query/send tree summary on client side: %v", err)
	}

	syncShard, err := r.scheduler.db.loadShard(ctx, common.ShardID(request.ShardId))
	if err != nil {
		return errors.Wrapf(gossip.ErrAbortPropagation, "shard %d load failure on client side: %v", request.ShardId, err)
	}
	firstTreeSummaryResp := true

	for {
		recvResp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("failed to keep receive tree summary from server: %w", err)
		}

		switch resp := recvResp.Data.(type) {
		case *propertyv1.RepairResponse_DifferTreeSummary:
			// step 2: check with the server for different leaf nodes
			if !resp.DifferTreeSummary.TreeFound {
				// if the tree is not found, we should abort the propagation
				return errors.Wrapf(gossip.ErrAbortPropagation, "tree for group %s not found on server side", request.Group)
			}
			// there no different nodes, we can skip repair
			if firstTreeSummaryResp && len(resp.DifferTreeSummary.Nodes) == 0 {
				return nil
			}
			r.handleDifferSummaryFromServer(ctx, stream, resp.DifferTreeSummary, reader, syncShard, clientSlotNodes)
			firstTreeSummaryResp = false
		case *propertyv1.RepairResponse_PropertySync:
			// step 3: keep receiving messages from the server
			// if the server still sending different nodes, we should keep reading them
			// if the server sends a PropertySync, we should repair the property and send the newer property back to the server if needed
			sync := resp.PropertySync
			updated, newer, err := syncShard.repair(ctx, sync.Id, sync.Property, sync.DeleteTime)
			if err != nil {
				r.scheduler.l.Warn().Err(err).Msgf("failed to repair property %s", sync.Id)
				r.scheduler.metrics.totalRepairFailedCount.Inc(1, request.Group, fmt.Sprintf("%d", request.ShardId))
			}
			if updated {
				r.scheduler.l.Debug().Msgf("successfully repaired property %s", sync.Id)
				r.scheduler.metrics.totalRepairSuccessCount.Inc(1, request.Group, fmt.Sprintf("%d", request.ShardId))
				hasPropertyUpdated = true
				continue
			}
			// if the property hasn't been updated, and the newer property is not nil,
			// which means the property is newer than the server side,
			if !updated && newer != nil {
				var p propertyv1.Property
				err = protojson.Unmarshal(newer.source, &p)
				if err != nil {
					r.scheduler.l.Warn().Err(err).Msgf("failed to unmarshal property from db by entity %s", newer.id)
					continue
				}
				// send the newer property to the server
				err = stream.Send(&propertyv1.RepairRequest{
					Data: &propertyv1.RepairRequest_PropertySync{
						PropertySync: &propertyv1.PropertySync{
							Id:         newer.id,
							Property:   &p,
							DeleteTime: newer.deleteTime,
						},
					},
				})
				if err != nil {
					r.scheduler.l.Warn().Err(err).Msgf("failed to send newer property sync response to server, entity: %s", newer.id)
				}
			}
		default:
			r.scheduler.l.Warn().Msgf("unexpected response type: %T, expected DifferTreeSummary or PropertySync", resp)
		}
	}
}

func (r *repairGossipClient) sendPropertyMissing(stream grpclib.BidiStreamingClient[propertyv1.RepairRequest, propertyv1.RepairResponse], entity string) {
	err := stream.Send(&propertyv1.RepairRequest{
		Data: &propertyv1.RepairRequest_PropertyMissing{
			PropertyMissing: &propertyv1.PropertyMissing{
				Entity: entity,
			},
		},
	})
	if err != nil {
		r.scheduler.l.Warn().Err(err).Msgf("failed to send property missing response to client, entity: %s", entity)
	}
}

func (r *repairGossipClient) handleDifferSummaryFromServer(
	ctx context.Context,
	stream grpclib.BidiStreamingClient[propertyv1.RepairRequest, propertyv1.RepairResponse],
	differTreeSummary *propertyv1.DifferTreeSummary,
	reader repairTreeReader,
	syncShard *shard,
	clientSlotNodes map[int32]*repairTreeNode,
) {
	// if their no more different nodes, means the client side could be send the no more property sync request to notify the server
	if len(differTreeSummary.Nodes) == 0 {
		err := stream.Send(&propertyv1.RepairRequest{
			Data: &propertyv1.RepairRequest_NoMorePropertySync{
				NoMorePropertySync: &propertyv1.NoMorePropertySync{},
			},
		})
		if err != nil {
			r.scheduler.l.Warn().Err(err).Msgf("failed to send no more property sync request to server")
			return
		}
	}
	select {
	case <-ctx.Done():
		r.scheduler.l.Warn().Msgf("context done while handling differ summary from server")
		return
	default:
		break
	}
	// keep reading the tree summary until there are no more different nodes
	for _, node := range differTreeSummary.Nodes {
		// if the repair node doesn't exist in the server side, then should send all the real property data to server
		if !node.Exists {
			clientSlotNode, exist := clientSlotNodes[node.SlotIndex]
			if !exist {
				r.scheduler.l.Warn().Msgf("client slot %d not exist", node.SlotIndex)
				continue
			}
			// read the leaf nodes from the client side
			for {
				leafNodes, err := reader.read(clientSlotNode, gossipMerkelTreeReadPageSize, false)
				if err != nil {
					r.scheduler.l.Warn().Err(err).Msgf("failed to read leaf nodes from client side")
					break
				}
				if len(leafNodes) == 0 {
					break
				}
				// reading the real property data from the leaf nodes and sending to the server
				for _, leafNode := range leafNodes {
					property, p, err := r.queryProperty(ctx, syncShard, leafNode.entity)
					if err != nil {
						r.scheduler.l.Warn().Err(err).Msgf("failed to query property for leaf node entity %s", leafNode.entity)
						continue
					}
					if property != nil {
						// send the property to the server
						err = stream.Send(&propertyv1.RepairRequest{
							Data: &propertyv1.RepairRequest_PropertySync{
								PropertySync: &propertyv1.PropertySync{
									Id:         property.id,
									Property:   p,
									DeleteTime: property.deleteTime,
								},
							},
						})
						if err != nil {
							r.scheduler.l.Warn().Err(err).Msgf("failed to send property sync response to client, entity: %s", leafNode.entity)
						}
					}
				}
			}
		}

		slotNodes, slotNodesExist := clientSlotNodes[node.SlotIndex]
		// if slot not exists in client side, then the client should ask the server for the property data of leaf nodes
		if !slotNodesExist {
			r.sendPropertyMissing(stream, node.Entity)
			continue
		}
		// check the leaf node if exist in the client side or not
		clientLeafNode, clientLeafNodeExist, err := r.findExistingLeafNode(reader, slotNodes, node.Entity)
		if err != nil {
			r.scheduler.l.Warn().Err(err).Msgf("failed to find existing leaf node for entity %s", node.Entity)
			continue
		}
		if !clientLeafNodeExist {
			r.sendPropertyMissing(stream, node.Entity)
			continue
		}
		// if the client leaf node SHA is the same as the server leaf node SHA, then we can skip it
		if clientLeafNode.shaValue == node.Sha {
			continue
		}
		property, p, err := r.queryProperty(ctx, syncShard, clientLeafNode.entity)
		if err != nil {
			r.scheduler.l.Warn().Err(err).Msgf("failed to query property for leaf node entity %s", clientLeafNode.entity)
			continue
		}
		if property == nil {
			continue
		}
		// send the property to the server
		err = stream.Send(&propertyv1.RepairRequest{
			Data: &propertyv1.RepairRequest_PropertySync{
				PropertySync: &propertyv1.PropertySync{
					Id:         GetPropertyID(p),
					Property:   p,
					DeleteTime: property.deleteTime,
				},
			},
		})
		if err != nil {
			r.scheduler.l.Warn().Err(err).Msgf("failed to send property sync request to server, entity: %s", clientLeafNode.entity)
			continue
		}
	}
}

func (r *repairGossipClient) findExistingLeafNode(reader repairTreeReader, parent *repairTreeNode, entity string) (*repairTreeNode, bool, error) {
	isFirstRead := true
	// if not found in the cache, read from the tree
	for {
		leafNodes, err := reader.read(parent, gossipMerkelTreeReadPageSize, isFirstRead)
		if err != nil {
			return nil, false, fmt.Errorf("failed to read tree for entity %s: %w", entity, err)
		}
		if len(leafNodes) == 0 {
			return nil, false, nil
		}
		for _, leafNode := range leafNodes {
			if leafNode.entity == entity {
				// if the leaf node is found, cache it and return
				return leafNode, true, nil
			}
		}
		isFirstRead = false
	}
}

type repairGossipServer struct {
	propertyv1.UnimplementedRepairServiceServer
	repairGossipBase
}

func newRepairGossipServer(s *repairScheduler) *repairGossipServer {
	return &repairGossipServer{
		repairGossipBase: repairGossipBase{
			scheduler: s,
		},
	}
}

func (r *repairGossipServer) Repair(s grpclib.BidiStreamingServer[propertyv1.RepairRequest, propertyv1.RepairResponse]) error {
	summary, err := r.combineTreeSummary(s)
	if err != nil {
		return fmt.Errorf("failed to receive tree summary request: %w", err)
	}
	group := summary.group
	shardID := summary.shardID
	var hasPropertyUpdated bool
	defer func() {
		if hasPropertyUpdated {
			err = r.scheduler.buildingTree([]common.ShardID{common.ShardID(shardID)}, group, true)
			if err != nil {
				r.scheduler.l.Warn().Err(err).Msgf("failed to build tree for group %s", group)
			}
		}
	}()

	reader, found, err := r.getTreeReader(s.Context(), group, shardID)
	if err != nil {
		r.scheduler.l.Err(err).Msgf("failed to read tree on server side")
		return r.sendTreeFound(s, false)
	}
	if !found {
		r.scheduler.l.Warn().Msgf("tree for group %s, shard %d not found on server side", group, shardID)
		return r.sendTreeFound(s, false)
	}
	defer reader.close()
	rootVal, err := reader.read(nil, 1, true)
	if err != nil {
		r.scheduler.l.Err(err).Msgf("failed to read tree root on server side")
		return r.sendTreeFound(s, false)
	}
	if len(rootVal) == 0 {
		r.scheduler.l.Warn().Msgf("tree root not found for group %s, shard %d", group, shardID)
		return r.sendTreeFound(s, false)
	}
	if summary.rootSHA == rootVal[0].shaValue {
		// if the root SHA is the same, we can skip repair
		return r.sendTreeFound(s, true)
	}
	clientSlots := make(map[int32]string)
	for _, clientSlot := range summary.slots {
		clientSlots[clientSlot.index] = clientSlot.shaValue
	}
	serverSlotNodes, err := reader.read(rootVal[0], int64(r.scheduler.treeSlotCount), false)
	if err != nil {
		r.scheduler.l.Warn().Err(err).Msgf("failed to read slot nodes on server side")
		return r.sendTreeFound(s, false)
	}
	// client missing slots or server slots with different SHA values
	clientMismatchSlots := make([]*repairTreeNode, 0)
	// server missing slots
	serverMissingSlots := make([]int32, 0)
	for _, serverSlot := range serverSlotNodes {
		sameSlotSha, exist := clientSlots[serverSlot.slotInx]
		// if the slot not exists in the client or the SHA values are different,
		// then, server side should send all the leaf nodes of the slot
		if !exist || sameSlotSha != serverSlot.shaValue {
			clientMismatchSlots = append(clientMismatchSlots, serverSlot)
		}
		// remove the slot from the client slots map,
		// so reduced client slots only contains the slots that are not in the server
		delete(clientSlots, serverSlot.slotInx)
	}
	for slot := range clientSlots {
		serverMissingSlots = append(serverMissingSlots, slot)
	}
	sent, err := r.sendDifferSlots(reader, clientMismatchSlots, serverMissingSlots, s)
	if err != nil {
		r.scheduler.l.Warn().Err(err).Msgf("failed to send different slots to client")
	}
	// send the tree and no more different slots needs to be sent
	err = r.sendTreeFound(s, true)
	if !sent {
		return err
	} else if err != nil {
		// should keep the message receiving loop
		r.scheduler.l.Warn().Msgf("sent no difference slot to client failure, error: %v", err)
	}
	for {
		missingOrSyncRequest, err := s.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("failed to receive missing or sync request: %w", err)
		}
		syncShard, err := r.scheduler.db.loadShard(s.Context(), common.ShardID(shardID))
		if err != nil {
			return fmt.Errorf("shard %d load failure on server side: %w", shardID, err)
		}
		switch req := missingOrSyncRequest.Data.(type) {
		case *propertyv1.RepairRequest_PropertyMissing:
			r.processPropertyMissing(s.Context(), syncShard, req.PropertyMissing, s)
		case *propertyv1.RepairRequest_PropertySync:
			if r.processPropertySync(s.Context(), syncShard, req.PropertySync, s, group) {
				hasPropertyUpdated = true
			}
		case *propertyv1.RepairRequest_NoMorePropertySync:
			// if the client has no more property sync, the server side should stop the sync
			return nil
		}
	}
}

func (r *repairGossipServer) combineTreeSummary(s grpclib.BidiStreamingServer[propertyv1.RepairRequest, propertyv1.RepairResponse]) (*repairTreeSummary, error) {
	summary := &repairTreeSummary{}
	for {
		recvData, err := s.Recv()
		if err != nil {
			r.scheduler.l.Warn().Err(err).Msgf("failed to receive tree summary from client")
			return nil, err
		}

		switch data := recvData.Data.(type) {
		case *propertyv1.RepairRequest_TreeRoot:
			summary.group = data.TreeRoot.Group
			summary.shardID = data.TreeRoot.ShardId
			summary.rootSHA = data.TreeRoot.RootSha
		case *propertyv1.RepairRequest_TreeSlots:
			if len(data.TreeSlots.SlotSha) == 0 {
				return summary, nil
			}
			for _, slot := range data.TreeSlots.SlotSha {
				summary.slots = append(summary.slots, &repairTreeSummarySlot{
					index:    slot.Slot,
					shaValue: slot.Value,
				})
			}
		default:
			r.scheduler.l.Warn().Msgf("unexpected data type: %T, expected TreeRoot or TreeSlots", data)
		}
	}
}

func (r *repairGossipServer) processPropertySync(
	ctx context.Context,
	syncShard *shard,
	sync *propertyv1.PropertySync,
	s grpclib.BidiStreamingServer[propertyv1.RepairRequest, propertyv1.RepairResponse],
	group string,
) bool {
	updated, newer, err := syncShard.repair(ctx, sync.Id, sync.Property, sync.DeleteTime)
	if err != nil {
		r.scheduler.l.Warn().Err(err).Msgf("failed to repair property %s from client side", sync.Id)
		r.scheduler.metrics.totalRepairFailedCount.Inc(1, group, fmt.Sprintf("%d", syncShard.id))
		return false
	}
	if updated {
		r.scheduler.metrics.totalRepairSuccessCount.Inc(1, group, fmt.Sprintf("%d", syncShard.id))
	}
	if !updated && newer != nil {
		// if the property hasn't been updated, and the newer property is not nil,
		// which means the property is newer than the client side,
		var p propertyv1.Property
		err = protojson.Unmarshal(newer.source, &p)
		if err != nil {
			r.scheduler.l.Warn().Err(err).Msgf("failed to unmarshal property from db by entity %s", newer.id)
			return false
		}
		// send the newer property to the client
		err = s.Send(&propertyv1.RepairResponse{
			Data: &propertyv1.RepairResponse_PropertySync{
				PropertySync: &propertyv1.PropertySync{
					Id:         newer.id,
					Property:   &p,
					DeleteTime: newer.deleteTime,
				},
			},
		})
		if err != nil {
			r.scheduler.l.Warn().Err(err).Msgf("failed to send newer property sync response to client, entity: %s", newer.id)
			return false
		}
	}
	return updated
}

func (r *repairGossipServer) processPropertyMissing(
	ctx context.Context,
	syncShard *shard,
	missing *propertyv1.PropertyMissing,
	s grpclib.BidiStreamingServer[propertyv1.RepairRequest, propertyv1.RepairResponse],
) {
	property, data, err := r.queryProperty(ctx, syncShard, missing.Entity)
	if err != nil {
		r.scheduler.l.Warn().Err(err).Msgf("failed to query client missing property from server side: %s", missing.Entity)
		return
	}
	if property == nil {
		return
	}
	err = s.Send(&propertyv1.RepairResponse{
		Data: &propertyv1.RepairResponse_PropertySync{
			PropertySync: &propertyv1.PropertySync{
				Id:         property.id,
				Property:   data,
				DeleteTime: property.deleteTime,
			},
		},
	})
	if err != nil {
		r.scheduler.l.Warn().Err(err).Msgf("failed to send property sync response to client, entity: %s", missing.Entity)
		return
	}
}

func (r *repairGossipServer) sendDifferSlots(
	reader repairTreeReader,
	clientMismatchSlots []*repairTreeNode,
	serverMissingSlots []int32,
	s grpclib.BidiStreamingServer[propertyv1.RepairRequest, propertyv1.RepairResponse],
) (hasSent bool, err error) {
	var leafNodes []*repairTreeNode

	// send server mismatch slots to the client
	for _, node := range clientMismatchSlots {
		for {
			leafNodes, err = reader.read(node, gossipMerkelTreeReadPageSize, false)
			if err != nil {
				return hasSent, fmt.Errorf("failed to read leaf nodes for slot %d: %w", node.slotInx, err)
			}
			// if there are no more leaf nodes, we can skip this slot
			if len(leafNodes) == 0 {
				break
			}
			mismatchLeafNodes := make([]*propertyv1.TreeLeafNode, 0, len(leafNodes))
			for _, leafNode := range leafNodes {
				mismatchLeafNodes = append(mismatchLeafNodes, &propertyv1.TreeLeafNode{
					SlotIndex: node.slotInx,
					Exists:    true,
					Entity:    leafNode.entity,
					Sha:       leafNode.shaValue,
				})
			}
			// send the leaf nodes to the client
			err = s.Send(&propertyv1.RepairResponse{
				Data: &propertyv1.RepairResponse_DifferTreeSummary{
					DifferTreeSummary: &propertyv1.DifferTreeSummary{
						TreeFound: true,
						Nodes:     mismatchLeafNodes,
					},
				},
			})
			if err != nil {
				r.scheduler.l.Warn().Err(err).
					Msgf("failed to send leaf nodes for slot %d", node.slotInx)
			} else {
				hasSent = true
			}
		}
	}

	// send server missing slots to the client
	missingSlots := make([]*propertyv1.TreeLeafNode, 0, len(serverMissingSlots))
	for _, missingSlot := range serverMissingSlots {
		missingSlots = append(missingSlots, &propertyv1.TreeLeafNode{
			SlotIndex: missingSlot,
			Exists:    false,
		})
	}
	if len(missingSlots) > 0 {
		// send the missing slots to the client
		err = s.Send(&propertyv1.RepairResponse{
			Data: &propertyv1.RepairResponse_DifferTreeSummary{
				DifferTreeSummary: &propertyv1.DifferTreeSummary{
					TreeFound: true,
					Nodes:     missingSlots,
				},
			},
		})
		if err != nil {
			r.scheduler.l.Warn().Err(err).
				Msgf("failed to send missing slots")
		} else {
			hasSent = true
		}
	}
	return hasSent, nil
}

func (r *repairGossipServer) sendTreeFound(s grpclib.BidiStreamingServer[propertyv1.RepairRequest, propertyv1.RepairResponse], found bool) error {
	return s.Send(&propertyv1.RepairResponse{
		Data: &propertyv1.RepairResponse_DifferTreeSummary{
			DifferTreeSummary: &propertyv1.DifferTreeSummary{
				TreeFound: found,
			},
		},
	})
}

type repairTreeSummary struct {
	group   string
	rootSHA string
	slots   []*repairTreeSummarySlot
	shardID uint32
}

type repairTreeSummarySlot struct {
	shaValue string
	index    int32
}

type emptyRepairTreeReader struct{}

func (e *emptyRepairTreeReader) read(n *repairTreeNode, _ int64, _ bool) ([]*repairTreeNode, error) {
	if n == nil {
		return []*repairTreeNode{{tp: repairTreeNodeTypeRoot}}, nil
	}
	return nil, nil
}

func (e *emptyRepairTreeReader) close() error {
	return nil
}
