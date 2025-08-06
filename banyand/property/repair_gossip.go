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
	"strings"

	"github.com/pkg/errors"
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/property/gossip"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

var (
	gossipMerkleTreeReadPageSize int64 = 10
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
) (root *repairTreeNode, rootMatches bool, err error) {
	roots, err := reader.read(nil, 1, false)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read tree root: %w", err)
	}
	if len(roots) == 0 {
		return nil, false, fmt.Errorf("tree root is empty for group %s", group)
	}

	err = stream.Send(&propertyv1.RepairRequest{
		Data: &propertyv1.RepairRequest_TreeRoot{
			TreeRoot: &propertyv1.TreeRoot{
				Group:   group,
				ShardId: shardID,
				RootSha: roots[0].shaValue,
			},
		},
	})
	if err != nil {
		return nil, false, fmt.Errorf("failed to send tree root for group %s: %w", group, err)
	}

	recv, err := stream.Recv()
	if err != nil {
		return nil, false, fmt.Errorf("failed to receive tree summary response for group %s: %w", group, err)
	}
	rootCompare, ok := recv.Data.(*propertyv1.RepairResponse_RootCompare)
	if !ok {
		return nil, false, fmt.Errorf("unexpected response type: %T, expected RootCompare", recv.Data)
	}
	if !rootCompare.RootCompare.TreeFound {
		return nil, false, fmt.Errorf("server tree not found for group: %s", group)
	}
	if rootCompare.RootCompare.RootShaMatch {
		return roots[0], true, nil
	}

	var slots []*repairTreeNode
	for {
		slots, err = reader.read(roots[0], gossipMerkleTreeReadPageSize, false)
		if err != nil {
			return nil, false, fmt.Errorf("failed to read slots for group %s: %w", group, err)
		}
		if len(slots) == 0 {
			break
		}

		slotReq := &propertyv1.TreeSlots{
			SlotSha: make([]*propertyv1.TreeSlotSHA, 0, len(slots)),
		}
		for _, s := range slots {
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
			return nil, false, fmt.Errorf("failed to send tree slots for group %s: %w", group, err)
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
		return nil, false, fmt.Errorf("failed to send empty tree slots for group %s: %w", group, err)
	}

	return roots[0], false, nil
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
	rootNode, rootMatch, err := r.sendTreeSummary(reader, request.Group, request.ShardId, stream)
	if err != nil {
		// if the tree summary cannot be built, we should abort the propagation
		return errors.Wrapf(gossip.ErrAbortPropagation, "failed to query/send tree summary on client side: %v", err)
	}
	// if the root node matched, then ignore the repair
	if rootMatch {
		return nil
	}

	syncShard, err := r.scheduler.db.loadShard(ctx, common.ShardID(request.ShardId))
	if err != nil {
		return errors.Wrapf(gossip.ErrAbortPropagation, "shard %d load failure on client side: %v", request.ShardId, err)
	}
	firstTreeSummaryResp := true

	leafReader := newRepairBufferLeafReader(reader)
	var currentComparingClientNode *repairTreeNode
	var notProcessingClientNode *repairTreeNode
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
			// there no different nodes, we can skip repair
			if firstTreeSummaryResp && len(resp.DifferTreeSummary.Nodes) == 0 {
				return nil
			}
			r.handleDifferSummaryFromServer(ctx, stream, resp.DifferTreeSummary, reader, syncShard, rootNode, leafReader, &notProcessingClientNode, &currentComparingClientNode)
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
				r.scheduler.l.Debug().Msgf("successfully repaired property %s on client side", sync.Id)
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
	rootNode *repairTreeNode,
	bufSlotReader *repairBufferLeafReader,
	notProcessingClientNode **repairTreeNode,
	currentComparingClientNode **repairTreeNode,
) {
	// if their no more different nodes, means the client side could be send the no more property sync request to notify the server
	if len(differTreeSummary.Nodes) == 0 {
		// if the current comparing client nodes still not empty, means the client side has leaf nodes that are not processed yet
		// then queried and sent property to server
		if *currentComparingClientNode != nil {
			// reading all reduced properties from the client side, and send to the server
			r.readingReduceLeafAndSendProperties(ctx, syncShard, stream, bufSlotReader, *currentComparingClientNode)
			*currentComparingClientNode = nil
		}
		if *notProcessingClientNode != nil {
			// if there still have difference client node not processing, means the client has property but server don't have
			// then queried and sent property to server
			r.queryPropertyAndSendToServer(ctx, syncShard, (*notProcessingClientNode).entity, stream)
		}
		err := stream.Send(&propertyv1.RepairRequest{
			Data: &propertyv1.RepairRequest_NoMorePropertySync{
				NoMorePropertySync: &propertyv1.NoMorePropertySync{},
			},
		})
		if err != nil {
			r.scheduler.l.Warn().Err(err).Msgf("failed to send no more property sync request to server")
		}
		return
	}

	// keep reading the tree summary until there are no more different nodes
	for _, node := range differTreeSummary.Nodes {
		select {
		case <-ctx.Done():
			r.scheduler.l.Warn().Msgf("context done while handling differ summary from server")
			return
		default:
			break
		}
		// if the repair node doesn't exist in the server side, then should send all the real property data to server
		if !node.Exists {
			clientSlotNode, findError := r.findSlotNodeByRoot(reader, rootNode, node.SlotIndex)
			if findError != nil {
				r.scheduler.l.Warn().Err(findError).Msgf("client slot %d not exist", node.SlotIndex)
				continue
			}
			// read the leaf nodes from the client side and send properties to the server
			r.readingReduceLeafAndSendProperties(ctx, syncShard, stream, bufSlotReader, clientSlotNode)
			continue
		}

		needsFindSlot := false
		if *currentComparingClientNode != nil && (*currentComparingClientNode).slotInx != node.SlotIndex {
			// the comparing node has changed, checks the client side still has reduced properties or not
			// reading all reduced properties from the client side, and send to the server
			r.readingReduceLeafAndSendProperties(ctx, syncShard, stream, bufSlotReader, *currentComparingClientNode)
			needsFindSlot = true
		} else if *currentComparingClientNode == nil {
			needsFindSlot = true
		}

		if needsFindSlot {
			clientSlotNode, findError := r.findSlotNodeByRoot(reader, rootNode, node.SlotIndex)
			// if slot not exists in client side, then the client should ask the server for the property data of leaf nodes
			if findError != nil {
				r.sendPropertyMissing(stream, node.Entity)
				continue
			}
			*currentComparingClientNode = clientSlotNode
		}

		r.readingClientNodeAndCompare(ctx, syncShard, node, bufSlotReader, *currentComparingClientNode, stream, notProcessingClientNode)
	}
}

func (r *repairGossipClient) readingReduceLeafAndSendProperties(
	ctx context.Context,
	syncShard *shard,
	stream grpclib.BidiStreamingClient[propertyv1.RepairRequest, propertyv1.RepairResponse],
	reader *repairBufferLeafReader, parent *repairTreeNode,
) {
	// read the leaf nodes from the client side
	for {
		leafNode, err := reader.next(parent)
		if err != nil {
			r.scheduler.l.Warn().Err(err).Msgf("failed to read leaf nodes from client side")
			break
		}
		if leafNode == nil {
			break
		}
		// reading the real property data from the leaf nodes and sending to the server
		r.queryPropertyAndSendToServer(ctx, syncShard, leafNode.entity, stream)
	}
}

func (r *repairGossipClient) readingClientNodeAndCompare(
	ctx context.Context,
	syncShard *shard,
	serverNode *propertyv1.TreeLeafNode,
	bufReader *repairBufferLeafReader,
	clientSlotNode *repairTreeNode,
	stream grpclib.BidiStreamingClient[propertyv1.RepairRequest, propertyv1.RepairResponse],
	notProcessingClientNode **repairTreeNode,
) {
	var clientLeafNode *repairTreeNode
	// if the latest node is not nil, means the client side has a leaf node that is not processed yet
	if *notProcessingClientNode != nil {
		clientLeafNode = *notProcessingClientNode
	} else {
		node, err := bufReader.next(clientSlotNode)
		if err != nil {
			r.sendPropertyMissing(stream, serverNode.Entity)
			return
		}
		clientLeafNode = node
	}

	// if the client current leaf node is nil, means the client side doesn't have the leaf node,
	// we should send the property missing request to the server
	if clientLeafNode == nil {
		r.sendPropertyMissing(stream, serverNode.Entity)
		return
	}

	// compare the entity of the server leaf node with the client leaf node
	entityCompare := strings.Compare(serverNode.Entity, clientLeafNode.entity)
	if entityCompare == 0 {
		// if the entity is the same, check the sha value
		if serverNode.Sha != clientLeafNode.shaValue {
			r.queryPropertyAndSendToServer(ctx, syncShard, serverNode.Entity, stream)
		}
		*notProcessingClientNode = nil
		return
	} else if entityCompare < 0 {
		// if the entity of the server leaf node is less than the client leaf node,
		// it means the server leaf node does not exist in the client leaf nodes
		r.sendPropertyMissing(stream, serverNode.Entity)
		// means the client node is still not processing, waiting for the server node to compare
		*notProcessingClientNode = clientLeafNode
		return
	}
	// otherwise, the entity of the server leaf node is greater than the client leaf node,
	// it means the client leaf node does not exist in the server leaf nodes,

	// we should query the property from the client side and send it to the server
	r.queryPropertyAndSendToServer(ctx, syncShard, clientLeafNode.entity, stream)
	// cleanup the unprocess node, and let the client side leaf nodes keep reading
	*notProcessingClientNode = nil
	// cycle to read the next leaf node from the client side to make sure they have synced to the same entity
	r.readingClientNodeAndCompare(ctx, syncShard, serverNode, bufReader, clientSlotNode, stream, notProcessingClientNode)
}

func (r *repairGossipClient) queryPropertyAndSendToServer(
	ctx context.Context,
	syncShard *shard,
	entity string,
	stream grpclib.BidiStreamingClient[propertyv1.RepairRequest, propertyv1.RepairResponse],
) {
	// otherwise, we need to send the property to the server
	property, p, err := r.queryProperty(ctx, syncShard, entity)
	if err != nil {
		r.scheduler.l.Warn().Err(err).Msgf("failed to query property for leaf node entity %s", entity)
		return
	}
	if property == nil {
		return
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
		r.scheduler.l.Warn().Err(err).Msgf("failed to send property sync request to server, entity: %s", entity)
	}
}

func (r *repairGossipClient) findSlotNodeByRoot(reader repairTreeReader, root *repairTreeNode, index int32) (*repairTreeNode, error) {
	firstRead := true
	for {
		slotNodes, err := reader.read(root, gossipMerkleTreeReadPageSize, firstRead)
		if err != nil {
			return nil, err
		}
		if len(slotNodes) == 0 {
			return nil, fmt.Errorf("failed to find slot node by root: %d", index)
		}
		for _, s := range slotNodes {
			if s.slotInx == index {
				return s, nil
			}
		}

		firstRead = false
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
	summary, reader, err := r.combineTreeSummary(s)
	if err != nil {
		return fmt.Errorf("failed to receive tree summary request: %w", err)
	}
	defer reader.close()
	// if no need to compare the tree, we can skip the rest of the process
	if summary.ignoreCompare {
		r.scheduler.l.Debug().Msgf("tree root for group %s, shard %d is the same, skip tree slots", summary.group, summary.shardID)
		return nil
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

	serverSlotNodes, err := reader.read(summary.rootNode, int64(r.scheduler.treeSlotCount), false)
	if err != nil {
		r.scheduler.l.Warn().Err(err).Msgf("failed to read slot nodes on server side")
		return r.sendEmptyDiffer(s)
	}
	// client missing slots or server slots with different SHA values
	clientMismatchSlots := make([]*repairTreeNode, 0)
	// server missing slots
	serverMissingSlots := make([]int32, 0)
	clientSlotMap := make(map[int32]string, len(summary.slots))
	for _, clientSlot := range summary.slots {
		clientSlotMap[clientSlot.index] = clientSlot.shaValue
	}
	serverSlotSet := make(map[int32]bool, len(serverSlotNodes))
	for _, serverSlot := range serverSlotNodes {
		serverSlotSet[serverSlot.slotInx] = true
		// if the client slot exists but the SHA value is different, or client slot does not exist
		// then we should add it to the client mismatch slots
		if clientSha, ok := clientSlotMap[serverSlot.slotInx]; ok && clientSha != serverSlot.shaValue {
			clientMismatchSlots = append(clientMismatchSlots, serverSlot)
		} else if !ok {
			clientMismatchSlots = append(clientMismatchSlots, serverSlot)
		}
	}
	// if the client slot exists but the server slot does not exist, we should add it to the server missing slots
	for _, clientSlot := range summary.slots {
		if _, ok := serverSlotSet[clientSlot.index]; !ok {
			serverMissingSlots = append(serverMissingSlots, clientSlot.index)
		}
	}
	sent, err := r.sendDifferSlots(reader, clientMismatchSlots, serverMissingSlots, s)
	if err != nil {
		r.scheduler.l.Warn().Err(err).Msgf("failed to send different slots to client")
	}
	// send the tree and no more different slots needs to be sent
	err = r.sendEmptyDiffer(s)
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

func (r *repairGossipServer) combineTreeSummary(
	s grpclib.BidiStreamingServer[propertyv1.RepairRequest, propertyv1.RepairResponse],
) (*repairTreeSummary, repairTreeReader, error) {
	var summary *repairTreeSummary
	var reader repairTreeReader
	for {
		recvData, err := s.Recv()
		if err != nil {
			r.scheduler.l.Warn().Err(err).Msgf("failed to receive tree summary from client")
			return nil, nil, err
		}

		switch data := recvData.Data.(type) {
		case *propertyv1.RepairRequest_TreeRoot:
			summary, reader, err = r.handleTreeRootRequest(s, data)
			if err != nil {
				if sendError := r.sendRootCompare(s, false, false); sendError != nil {
					r.scheduler.l.Warn().Err(sendError).Msgf("failed to send root compare response to client")
				}
				return nil, nil, err
			}
			if err = r.sendRootCompare(s, true, summary.ignoreCompare); err != nil {
				_ = reader.close()
				return nil, nil, err
			}
			if summary.ignoreCompare {
				return summary, reader, nil
			}
		case *propertyv1.RepairRequest_TreeSlots:
			if len(data.TreeSlots.SlotSha) == 0 {
				return summary, reader, nil
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

func (r *repairGossipServer) handleTreeRootRequest(
	s grpclib.BidiStreamingServer[propertyv1.RepairRequest, propertyv1.RepairResponse],
	req *propertyv1.RepairRequest_TreeRoot,
) (*repairTreeSummary, repairTreeReader, error) {
	summary := &repairTreeSummary{
		group:   req.TreeRoot.Group,
		shardID: req.TreeRoot.ShardId,
	}

	reader, exist, err := r.getTreeReader(s.Context(), summary.group, summary.shardID)
	if err != nil || !exist {
		return nil, nil, fmt.Errorf("tree not found or not exist: %w", err)
	}
	rootNode, err := reader.read(nil, 1, false)
	if err != nil {
		_ = reader.close()
		return nil, nil, fmt.Errorf("failed to read tree root for group %s: %w", summary.group, err)
	}
	if len(rootNode) == 0 {
		_ = reader.close()
		return nil, nil, fmt.Errorf("failed to read tree root for group %s: %w", summary.group, err)
	}
	summary.rootNode = rootNode[0]
	summary.ignoreCompare = req.TreeRoot.RootSha == rootNode[0].shaValue
	return summary, reader, nil
}

func (r *repairGossipServer) sendRootCompare(s grpclib.BidiStreamingServer[propertyv1.RepairRequest, propertyv1.RepairResponse], treeFound, rootMatch bool) error {
	return s.Send(&propertyv1.RepairResponse{
		Data: &propertyv1.RepairResponse_RootCompare{
			RootCompare: &propertyv1.RootCompare{
				TreeFound:    treeFound,
				RootShaMatch: rootMatch,
			},
		},
	})
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
		r.scheduler.l.Warn().Err(err).Msgf("failed to repair property %s from server side", sync.Id)
		r.scheduler.metrics.totalRepairFailedCount.Inc(1, group, fmt.Sprintf("%d", syncShard.id))
		return false
	}
	if updated {
		r.scheduler.l.Debug().Msgf("successfully repaired property %s on server side", sync.Id)
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
			leafNodes, err = reader.read(node, gossipMerkleTreeReadPageSize, false)
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
						Nodes: mismatchLeafNodes,
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
					Nodes: missingSlots,
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

func (r *repairGossipServer) sendEmptyDiffer(s grpclib.BidiStreamingServer[propertyv1.RepairRequest, propertyv1.RepairResponse]) error {
	return s.Send(&propertyv1.RepairResponse{
		Data: &propertyv1.RepairResponse_DifferTreeSummary{
			DifferTreeSummary: &propertyv1.DifferTreeSummary{},
		},
	})
}

type repairTreeSummary struct {
	rootNode      *repairTreeNode
	group         string
	slots         []*repairTreeSummarySlot
	shardID       uint32
	ignoreCompare bool
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
