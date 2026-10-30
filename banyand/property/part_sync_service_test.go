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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	metadataservice "github.com/apache/skywalking-banyandb/banyand/metadata/service"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/db"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// propertyNode is one running property service on its own in-process pipeline.
type propertyNode struct {
	pipeline queue.Queue
	svc      *service
	root     string
}

func startPropertyNode(t *testing.T, root string) *propertyNode {
	t.Helper()
	pipeline := queue.Local()
	metadataService, err := metadataservice.NewService()
	require.NoError(t, err)
	svc, err := NewService(metadataService, pipeline, nil, observability.BypassRegistry, protector.NewMemory(observability.BypassRegistry))
	require.NoError(t, err)
	ports, err := test.AllocateFreePorts(1)
	require.NoError(t, err)
	stop := test.SetupModules([]string{
		"--schema-server-root-path=" + t.TempDir(), fmt.Sprintf("--schema-server-grpc-port=%d", ports[0]),
		"--schema-server-grpc-host=127.0.0.1", "--property-root-path=" + root, "--property-repair-enabled=false",
	}, pipeline, metadataService, svc)
	t.Cleanup(stop)
	return &propertyNode{pipeline: pipeline, svc: svc.(*service), root: root}
}

func (n *propertyNode) publish(t *testing.T, topic bus.Topic, request any) any {
	t.Helper()
	future, err := n.pipeline.Publish(context.Background(), topic, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), request))
	require.NoError(t, err)
	message, err := future.Get()
	require.NoError(t, err)
	if commonErr, ok := message.Data().(*common.Error); ok {
		t.Fatalf("%s failed: %v", topic, commonErr)
	}
	return message.Data()
}

// apply writes one revision of an entity through the update topic a liaison publishes to.
func (n *propertyNode) apply(t *testing.T, id string, revision int64) {
	t.Helper()
	property := newPartSyncProperty(id, revision)
	n.publish(t, data.TopicPropertyUpdate, &propertyv1.InternalUpdateRequest{ShardId: 0, Id: db.GetPropertyID(property), Property: property})
}

// remove deletes one revision of an entity through the delete topic.
func (n *propertyNode) remove(t *testing.T, id string, revision int64) {
	t.Helper()
	n.publish(t, data.TopicPropertyDelete, &propertyv1.InternalDeleteRequest{Ids: [][]byte{db.GetPropertyID(newPartSyncProperty(id, revision))}})
}

// query reads the newest revision of every entity through the query topic.
func (n *propertyNode) query(t *testing.T) map[string]revisionState {
	t.Helper()
	response, ok := n.publish(t, data.TopicPropertyQuery, &propertyv1.QueryRequest{
		Groups: []string{partSyncGroup}, Name: partSyncName, Limit: 1000,
	}).(*propertyv1.InternalQueryResponse)
	require.True(t, ok)
	states := make(map[string]revisionState)
	for i, source := range response.GetSources() {
		var property propertyv1.Property
		require.NoError(t, protojson.Unmarshal(source, &property))
		revision := property.GetMetadata().GetModRevision()
		if state, exists := states[property.GetId()]; exists && state.revision > revision {
			continue
		}
		states[property.GetId()] = revisionState{revision: revision, deleted: response.GetDeletes()[i] > 0}
	}
	return states
}

// exportShard takes an export snapshot of the group and returns its shard 0 directory.
func (n *propertyNode) exportShard(t *testing.T) string {
	t.Helper()
	require.NoError(t, n.svc.TakeExportSnapshot(context.Background(), "session", []string{partSyncGroup}))
	return filepath.Join(n.svc.GetExportSnapshotDir(), "session", partSyncGroup, "shard-0")
}

// importShard pushes every file of the shard directory to the node's registered part sync handler.
func (n *propertyNode) importShard(t *testing.T, shardDir string) {
	t.Helper()
	require.NoError(t, pushShard(t, n.svc.partSync, shardDir, 0))
}

func (n *propertyNode) stagingRoot() string {
	return filepath.Join(n.root, "property", externalPartTempDirName)
}

func TestPartSyncServiceWriteImportQuery(t *testing.T) {
	gomega.RegisterFailHandler(func(message string, _ ...int) { panic(message) })

	source := startPropertyNode(t, t.TempDir())
	source.apply(t, "kept", 1)
	source.apply(t, "updated", 1)
	source.apply(t, "updated", 2)
	source.apply(t, "removed", 1)
	source.remove(t, "removed", 1)
	want := map[string]revisionState{
		"kept":    {revision: 1},
		"updated": {revision: 2},
		"removed": {revision: 1, deleted: true},
	}
	require.Equal(t, want, source.query(t))
	shardDir := source.exportShard(t)

	// A crash leaves a staged part behind; the target drops it when it starts.
	targetRoot := t.TempDir()
	stale := filepath.Join(targetRoot, "property", externalPartTempDirName, "part-stale")
	require.NoError(t, os.MkdirAll(stale, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(stale, "000000000001.seg"), []byte("stale"), 0o600))
	target := startPropertyNode(t, targetRoot)
	_, err := os.Stat(target.stagingRoot())
	require.ErrorIs(t, err, os.ErrNotExist, "the staging directory must be cleaned at startup")

	// The target already holds a live copy of an entity the source deleted: the deletion must arrive.
	target.apply(t, "removed", 1)

	target.importShard(t, shardDir)
	require.Equal(t, want, target.query(t))

	target.importShard(t, shardDir)
	require.Equal(t, want, target.query(t), "importing the same shard twice must not change the result")

	entries, err := os.ReadDir(target.stagingRoot())
	require.NoError(t, err)
	require.Empty(t, entries, "no staged part may outlive its import")
}
