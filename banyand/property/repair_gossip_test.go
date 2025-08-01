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
	"net"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"go.uber.org/multierr"
	"google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/gossip"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const (
	testGroup1 = "test-group1"
)

func TestPropertyRepairGossip(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Property Repair Gossip Suite")
}

var _ = ginkgo.Describe("Property repair gossip", func() {
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())

	var ctrl *gomock.Controller
	var nodes []*nodeContext
	ginkgo.BeforeEach(func() {
		ctrl = gomock.NewController(ginkgo.GinkgoT())
		gomega.Expect(ctrl).NotTo(gomega.BeNil(), "gomock controller should not be nil")
	})

	ginkgo.AfterEach(func() {
		for _, node := range nodes {
			if node != nil {
				node.stopAll()
			}
		}
		nodes = nil
	})

	ginkgo.It("all repair tree not built", func() {
		startingEachTest(&nodes, ctrl, &testCase{
			groups: []group{
				{name: testGroup1, shardCount: 2, replicasCount: 1},
			},
			nodes: []node{
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 1}}},
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 2}}},
			},
			propagation: func(nodes []*nodeContext) error {
				return nodes[0].messenger.Propagation([]string{nodes[0].nodeID, nodes[1].nodeID}, testGroup1, 0)
			},
			result: func(original []node) []node {
				// all nodes should keep their existing properties
				return original
			},
		})
	})

	ginkgo.It("all repair tree with client built", func() {
		startingEachTest(&nodes, ctrl, &testCase{
			groups: []group{
				{name: testGroup1, shardCount: 2, replicasCount: 1},
			},
			nodes: []node{
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 1}}, treeBuilt: true},
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 2}}},
			},
			propagation: func(nodes []*nodeContext) error {
				return nodes[0].messenger.Propagation([]string{nodes[0].nodeID, nodes[1].nodeID}, testGroup1, 0)
			},
			result: func(original []node) []node {
				// all nodes should keep their existing properties
				return original
			},
		})
	})

	ginkgo.It("gossip two data nodes with client version < server version", func() {
		startingEachTest(&nodes, ctrl, &testCase{
			groups: []group{
				{name: testGroup1, shardCount: 2, replicasCount: 1},
			},
			nodes: []node{
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 1}}, treeBuilt: true},
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 2}}, treeBuilt: true},
			},
			propagation: func(nodes []*nodeContext) error {
				return nodes[0].messenger.Propagation([]string{nodes[0].nodeID, nodes[1].nodeID}, testGroup1, 0)
			},
			result: func(original []node) []node {
				// the first node property should be updated to the version 2
				original[0].properties[0].version = 2
				return original
			},
		})
	})

	ginkgo.It("gossip two data nodes with client version < server version", func() {
		startingEachTest(&nodes, ctrl, &testCase{
			groups: []group{
				{name: testGroup1, shardCount: 2, replicasCount: 1},
			},
			nodes: []node{
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 2}}, treeBuilt: true},
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 1}}, treeBuilt: true},
			},
			propagation: func(nodes []*nodeContext) error {
				return nodes[0].messenger.Propagation([]string{nodes[0].nodeID, nodes[1].nodeID}, testGroup1, 0)
			},
			result: func(original []node) []node {
				// the first node property should be updated to version 2
				original[1].properties[0].version = 2
				return original
			},
		})
	})

	ginkgo.It("gossip two data nodes with client version = server version", func() {
		startingEachTest(&nodes, ctrl, &testCase{
			groups: []group{
				{name: testGroup1, shardCount: 2, replicasCount: 1},
			},
			nodes: []node{
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 2}}, treeBuilt: true},
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 2}}, treeBuilt: true},
			},
			propagation: func(nodes []*nodeContext) error {
				return nodes[0].messenger.Propagation([]string{nodes[0].nodeID, nodes[1].nodeID}, testGroup1, 0)
			},
			result: func(original []node) []node {
				// keep the properties as is, since the versions are equal
				return original
			},
		})
	})

	ginkgo.It("gossip two data nodes with client missing but server exist", func() {
		startingEachTest(&nodes, ctrl, &testCase{
			groups: []group{
				{name: testGroup1, shardCount: 2, replicasCount: 1},
			},
			nodes: []node{
				{properties: nil, shardCount: 2, treeBuilt: true},
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 2}}, shardCount: 2, treeBuilt: true},
			},
			propagation: func(nodes []*nodeContext) error {
				return nodes[0].messenger.Propagation([]string{nodes[0].nodeID, nodes[1].nodeID}, testGroup1, 0)
			},
			result: func(original []node) []node {
				// the first node should get the property from the second node
				original[0].properties = original[1].properties
				return original
			},
		})
	})

	ginkgo.It("gossip two data nodes with client exist but server missing", func() {
		startingEachTest(&nodes, ctrl, &testCase{
			groups: []group{
				{name: testGroup1, shardCount: 2, replicasCount: 1},
			},
			nodes: []node{
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 2}}, shardCount: 2, treeBuilt: true},
				{properties: nil, shardCount: 2, treeBuilt: true},
			},
			propagation: func(nodes []*nodeContext) error {
				return nodes[0].messenger.Propagation([]string{nodes[0].nodeID, nodes[1].nodeID}, testGroup1, 0)
			},
			result: func(original []node) []node {
				// the second node should get the property from the first node
				original[1].properties = original[0].properties
				return original
			},
		})
	})

	ginkgo.It("gossip only repair one shard", func() {
		startingEachTest(&nodes, ctrl, &testCase{
			groups: []group{
				{name: testGroup1, shardCount: 2, replicasCount: 1},
			},
			nodes: []node{
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 1}}, shardCount: 2, treeBuilt: true},
				{properties: []property{{group: testGroup1, shard: 1, id: "2", version: 2}}, treeBuilt: true},
			},
			propagation: func(nodes []*nodeContext) error {
				return nodes[0].messenger.Propagation([]string{nodes[0].nodeID, nodes[1].nodeID}, testGroup1, 1)
			},
			result: func(original []node) []node {
				// the first node should sync the property from the second node
				original[0].properties = append(original[0].properties, original[1].properties[0])
				return original
			},
		})
	})

	ginkgo.It("gossip with three nodes", func() {
		startingEachTest(&nodes, ctrl, &testCase{
			groups: []group{
				{name: testGroup1, shardCount: 2, replicasCount: 1},
			},
			nodes: []node{
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 1}}, treeBuilt: true},
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 2}}, treeBuilt: true},
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 3}}, treeBuilt: true},
			},
			propagation: func(nodes []*nodeContext) error {
				return nodes[0].messenger.Propagation([]string{nodes[0].nodeID, nodes[1].nodeID, nodes[2].nodeID}, testGroup1, 0)
			},
			result: func(original []node) []node {
				original[0].properties[0] = original[2].properties[0]
				original[1].properties[0] = original[2].properties[0]
				return original
			},
		})
	})

	ginkgo.It("gossip with three nodes and only one slot", func() {
		startingEachTest(&nodes, ctrl, &testCase{
			groups: []group{
				{name: testGroup1, shardCount: 2, replicasCount: 3},
			},
			nodes: []node{
				{properties: []property{{group: testGroup1, shard: 0, id: "2", version: 1}}, treeBuilt: true, treeSlotCount: 1},
				{properties: []property{{group: testGroup1, shard: 0, id: "1", version: 1}}, treeBuilt: true, treeSlotCount: 1},
				{properties: []property{{group: testGroup1, shard: 0, id: "3", version: 1}}, treeBuilt: true, treeSlotCount: 1},
			},
			propagation: func(nodes []*nodeContext) error {
				return nodes[0].messenger.Propagation([]string{nodes[0].nodeID, nodes[1].nodeID, nodes[2].nodeID}, testGroup1, 0)
			},
			result: func(original []node) []node {
				// should all nodes have all properties
				original[0].properties = append(original[0].properties, original[1].properties[0])
				original[0].properties = append(original[0].properties, original[2].properties[0])

				original[1].properties = append(original[1].properties, original[0].properties[0])
				original[1].properties = append(original[1].properties, original[2].properties[0])

				original[2].properties = append(original[2].properties, original[0].properties[0])
				original[2].properties = append(original[2].properties, original[1].properties[0])
				return original
			},
		})
	})
})

type testCase struct {
	propagation func(nodes []*nodeContext) error
	result      func(original []node) []node
	groups      []group
	nodes       []node
}

func startingEachTest(nodes *[]*nodeContext, ctrl *gomock.Controller, c *testCase) {
	*nodes = startDataNodes(ctrl, c.nodes, c.groups)

	// adding the wait group the node context, to make sure the gossip server is synced at least once
	once := sync.Once{}
	leastOnceChannel := make(chan struct{})
	for _, n := range *nodes {
		n.clientWrapper.once = &once
		n.clientWrapper.c = leastOnceChannel
	}
	err := c.propagation(*nodes)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// make sure the gossip server is synced at least once
	gomega.Eventually(leastOnceChannel, flags.EventuallyTimeout).Should(gomega.BeClosed())

	original := nodeContextToParentSlice(*nodes)
	updatedResult := c.result(original)
	for inx, r := range updatedResult {
		relatedCtx := (*nodes)[inx]
		for _, updatedProperty := range r.properties {
			queryPropertyWithVerify(relatedCtx.database, updatedProperty)
		}
	}
}

func startDataNodes(ctrl *gomock.Controller, nodes []node, groups []group) []*nodeContext {
	result := make([]*nodeContext, 0, len(nodes))
	for _, n := range nodes {
		result = append(result, startEachNode(ctrl, n, groups))
	}

	// registering the node in the gossip system
	for _, m := range result {
		for _, n := range result {
			m.messenger.(schema.EventHandler).OnAddOrUpdate(schema.Metadata{
				TypeMeta: schema.TypeMeta{
					Name: n.nodeID,
					Kind: schema.KindNode,
				},
				Spec: &databasev1.Node{
					Metadata: &commonv1.Metadata{
						Name: n.nodeID,
					},
					Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
					GrpcAddress: n.nodeID,

					PropertyRepairGossipGrpcAddress: n.nodeID,
				},
			})
		}
	}
	return result
}

func startEachNode(ctrl *gomock.Controller, node node, groups []group) *nodeContext {
	if node.treeSlotCount == 0 {
		node.treeSlotCount = 32 // default value for tree slot count
	}
	result := &nodeContext{node: node}
	dbLocation, dbLocationDefer, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	result.appendStop(dbLocationDefer)
	repairLocation, repairLocationDefer, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	result.appendStop(repairLocationDefer)
	mockGroup := schema.NewMockGroup(ctrl)
	groupDefines := make([]*commonv1.Group, 0, len(groups))
	for _, g := range groups {
		groupDefines = append(groupDefines, &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Group: g.name,
				Name:  g.name,
			},
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: uint32(g.shardCount),
				Replicas: uint32(g.replicasCount),
			},
		})
	}
	mockGroup.EXPECT().ListGroup(gomock.Any()).Return(groupDefines, nil).AnyTimes()

	mockRepo := metadata.NewMockRepo(ctrl)
	mockRepo.EXPECT().RegisterHandler("", schema.KindGroup, gomock.Any()).MaxTimes(1)
	mockRepo.EXPECT().GroupRegistry().Return(mockGroup).AnyTimes()

	ports, err := test.AllocateFreePorts(1)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	messenger := gossip.NewMessengerWithoutMetadata(observability.NewBypassRegistry(), ports[0])
	addr := fmt.Sprintf("127.0.0.1:%d", ports[0])
	result.nodeID = addr
	err = messenger.Validate()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	messenger.(run.PreRunner).PreRun(context.WithValue(context.Background(), common.ContextNodeKey, common.Node{
		NodeID:                    addr,
		PropertyGossipGrpcAddress: addr,
	}))

	ctx := context.WithValue(context.Background(), common.ContextNodeKey, common.Node{
		NodeID: addr,
	})
	var db *database
	db, err = openDB(ctx,
		dbLocation, time.Minute*10, time.Minute*10, int(node.treeSlotCount), observability.NewBypassRegistry(),
		fs.NewLocalFileSystem(), true, repairLocation, "@every 10m", time.Minute*10, "* 2 * * *",
		messenger, mockRepo, func(context.Context) (string, error) {
			snapshotDir, defFunc, newSpaceErr := test.NewSpace()
			if newSpaceErr != nil {
				return "", newSpaceErr
			}
			result.appendStop(defFunc)
			sList := db.sLst.Load()
			var snpError error
			for _, s := range *sList {
				snpDir := path.Join(snapshotDir, filepath.Base(s.location))
				lfs.MkdirPanicIfExist(snpDir, storage.DirPerm)
				if e := s.store.TakeFileSnapshot(snpDir); e != nil {
					snpError = multierr.Append(snpError, e)
				}
			}
			return snapshotDir, snpError
		})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	result.database = db

	// wrap the server and client in gossip messenger to for getting the sync status
	messenger.RegisterServices(result.database.repairScheduler.registerServerToGossip())
	gossipClient := &repairGossipClientWrapper{repairGossipClient: newRepairGossipClient(result.database.repairScheduler)}
	result.clientWrapper = gossipClient
	messenger.Subscribe(gossipClient)
	db.repairScheduler.registerClientToGossip(messenger)

	messenger.Serve(make(chan struct{}))
	result.messenger = messenger

	// check gossip server is up
	gomega.Eventually(func() error {
		conn, connectErr := net.DialTimeout("tcp", addr, time.Second*2)
		if connectErr == nil {
			_ = conn.Close()
		}
		return connectErr
	}, flags.EventuallyTimeout).Should(gomega.Succeed())

	result.appendStop(messenger.GracefulStop)

	// initialize shard in to db
	for i := int32(0); i < node.shardCount; i++ {
		shardID := common.ShardID(i)
		_, err = db.loadShard(context.Background(), shardID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// adding data to the node
	for _, p := range node.properties {
		applyPropertyUpdate(db, p)
	}

	if node.treeBuilt {
		// building the gossip tree for the node
		err = db.repairScheduler.buildingTree(nil, "", true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	return result
}

type repairGossipClientWrapper struct {
	*repairGossipClient
	c    chan struct{}
	once *sync.Once
}

func (w *repairGossipClientWrapper) Rev(ctx context.Context, nextNode *grpc.ClientConn, request *propertyv1.PropagationRequest) error {
	err := w.repairGossipClient.Rev(ctx, nextNode, request)
	w.once.Do(func() {
		close(w.c)
	})
	return err
}

func applyPropertyUpdate(db *database, p property) {
	s, err := db.loadShard(context.Background(), common.ShardID(p.shard))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	update := &propertyv1.Property{
		Metadata: &commonv1.Metadata{
			Group:       p.group,
			Name:        "test-name",
			ModRevision: p.version,
		},
		Id: p.id,
	}
	if p.deleted {
		err = s.delete(context.Background(), [][]byte{GetPropertyID(update)})
	} else {
		err = s.update(GetPropertyID(update), update)
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func queryPropertyWithVerify(db *database, p property) {
	s, err := db.loadShard(context.Background(), common.ShardID(p.shard))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	query, err := inverted.BuildPropertyQuery(&propertyv1.QueryRequest{
		Groups: []string{p.group},
		Name:   "test-name",
		Ids:    []string{p.id},
	}, groupField, entityID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	gomega.Eventually(func() *property {
		dataList, err := s.search(context.Background(), query, 10)
		if err != nil {
			return nil
		}
		var latestData *queryProperty
		for _, data := range dataList {
			if latestData == nil || data.timestamp > latestData.timestamp {
				latestData = data
			}
		}
		if latestData == nil {
			return nil
		}
		return &property{group: p.group, shard: p.shard, id: p.id, version: latestData.timestamp, deleted: latestData.deleteTime > 0}
	}, flags.EventuallyTimeout).Should(gomega.Equal(&p))
}

type group struct {
	name          string
	shardCount    int32
	replicasCount int32
}

type node struct {
	properties []property
	treeBuilt  bool
	// for the no data scenario, we need to specify the shard count
	shardCount    int32
	treeSlotCount int32
}

type property struct {
	group   string
	id      string
	version int64
	shard   int32
	deleted bool
}

type nodeContext struct {
	messenger     gossip.Messenger
	database      *database
	clientWrapper *repairGossipClientWrapper
	nodeID        string
	stop          []func()
	node
	stopMutex sync.RWMutex
}

func (n *nodeContext) appendStop(f func()) {
	n.stopMutex.Lock()
	defer n.stopMutex.Unlock()
	n.stop = append(n.stop, f)
}

func (n *nodeContext) stopAll() {
	n.stopMutex.RLock()
	result := make([]func(), 0, len(n.stop))
	result = append(result, n.stop...)
	n.stopMutex.RUnlock()
	for _, f := range result {
		f()
	}
}

func nodeContextToParentSlice(ncs []*nodeContext) []node {
	nodes := make([]node, 0, len(ncs))
	for _, nc := range ncs {
		nodes = append(nodes, nc.node)
	}
	return nodes
}
