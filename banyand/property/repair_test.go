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
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

var (
	defaultGroupName    = "default_group"
	defaultPropertyName = "default_property"
	defaultValue        = 22
)

func TestBuildTree(t *testing.T) {
	tests := []struct {
		existingDoc      func(s *shard) ([]index.Document, error)
		statusVerify     func(t *testing.T, s *shard, tree *repairStatus)
		afterFirstBuild  func(s *shard) ([]index.Document, error)
		nextStatusVerify func(t *testing.T, s *shard, before, after *repairStatus)
		name             string
	}{
		{
			name: "empty shard",
			statusVerify: func(t *testing.T, _ *shard, status *repairStatus) {
				basicStatusVerify(t, status)
			},
		},
		{
			name: "single property",
			existingDoc: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{id: "test1"},
				)
			},
			statusVerify: func(t *testing.T, s *shard, status *repairStatus) {
				basicStatusVerify(t, status, defaultGroupName, 1)
				verifyContainsProperty(t, s, status, defaultGroupName, propertyBuilder{id: "test1"})
			},
		},
		{
			name: "single property with multiple versions",
			existingDoc: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{id: "test1", version: 1},
					propertyBuilder{id: "test1", version: 2, deleteTime: 2000},
				)
			},
			statusVerify: func(t *testing.T, s *shard, status *repairStatus) {
				basicStatusVerify(t, status, defaultGroupName, 1)
				verifyContainsProperty(t, s, status, defaultGroupName,
					propertyBuilder{id: "test1", version: 2, deleteTime: 2000})
			},
		},
		{
			name: "multiple properties",
			existingDoc: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{group: "test1", id: "test1"},
					propertyBuilder{group: "test2", id: "test2"},
					propertyBuilder{group: "test3", id: "test3"},
				)
			},
			statusVerify: func(t *testing.T, s *shard, status *repairStatus) {
				basicStatusVerify(t, status, "test1", 1, "test2", 1, "test3", 1)
				verifyContainsProperty(t, s, status, "test1", propertyBuilder{group: "test1", id: "test1"})
				verifyContainsProperty(t, s, status, "test2", propertyBuilder{group: "test2", id: "test2"})
				verifyContainsProperty(t, s, status, "test3", propertyBuilder{group: "test3", id: "test3"})
			},
		},
		{
			name: "build with multiple groups",
			existingDoc: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{group: "group1", id: "test1"},
					propertyBuilder{group: "group2", id: "test2"},
					propertyBuilder{group: "group3", id: "test3"},
				)
			},
			statusVerify: func(t *testing.T, s *shard, status *repairStatus) {
				basicStatusVerify(t, status, "group1", 1, "group2", 1, "group3", 1)
				verifyContainsProperty(t, s, status, "group1", propertyBuilder{group: "group1", id: "test1"})
				verifyContainsProperty(t, s, status, "group2", propertyBuilder{group: "group2", id: "test2"})
				verifyContainsProperty(t, s, status, "group3", propertyBuilder{group: "group3", id: "test3"})
			},
		},
		{
			name: "build multiple times",
			existingDoc: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{id: "test1"},
				)
			},
			statusVerify: func(t *testing.T, s *shard, status *repairStatus) {
				basicStatusVerify(t, status, defaultGroupName, 1)
				verifyContainsProperty(t, s, status, defaultGroupName, propertyBuilder{id: "test1"})
			},
			nextStatusVerify: func(t *testing.T, _ *shard, before, after *repairStatus) {
				basicStatusVerify(t, before, defaultGroupName, 1)
				basicStatusVerify(t, after, defaultGroupName, 1)
				if before.LastSnapshotID != after.LastSnapshotID {
					t.Fatalf("expected last snapshot ID to be the same, got before: %d, after: %d",
						before.LastSnapshotID, after.LastSnapshotID)
				}
				if before.Trees[defaultGroupName].Root.ShaValue != after.Trees[defaultGroupName].Root.ShaValue {
					t.Fatalf("expected Root sha value to be the same, got before: %s, after: %s",
						before.Trees[defaultGroupName].Root.ShaValue, after.Trees[defaultGroupName].Root.ShaValue)
				}
			},
		},
		{
			name: "build multiple times with new properties",
			existingDoc: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{id: "test1"},
				)
			},
			statusVerify: func(t *testing.T, s *shard, status *repairStatus) {
				basicStatusVerify(t, status, defaultGroupName, 1)
				verifyContainsProperty(t, s, status, defaultGroupName, propertyBuilder{id: "test1"})
			},
			afterFirstBuild: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{id: "test2"},
				)
			},
			nextStatusVerify: func(t *testing.T, s *shard, before, after *repairStatus) {
				basicStatusVerify(t, after, defaultGroupName, 2)
				verifyContainsProperty(t, s, after, defaultGroupName,
					propertyBuilder{id: "test1"},
					propertyBuilder{id: "test2"},
				)
				if before.LastSnapshotID == after.LastSnapshotID {
					t.Fatalf("expected last snapshot ID to be incremented by 1, got before: %d, after: %d",
						before.LastSnapshotID, after.LastSnapshotID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var defers []func()
			defer func() {
				for _, f := range defers {
					f()
				}
			}()

			dir, deferFunc, err := test.NewSpace()
			if err != nil {
				t.Fatal(err)
			}
			defers = append(defers, deferFunc)
			db, err := openDB(context.Background(), dir, 3*time.Second, time.Hour, 32, observability.BypassRegistry, fs.NewLocalFileSystem())
			if err != nil {
				t.Fatal(err)
			}
			defers = append(defers, func() {
				_ = db.close()
			})

			newShard, err := db.loadShard(context.Background(), 0)
			if err != nil {
				t.Fatal(err)
			}

			var docs []index.Document
			if tt.existingDoc != nil {
				docs, err = tt.existingDoc(newShard)
				if err != nil {
					t.Fatal(err)
				}
				if err = newShard.updateDocuments(docs); err != nil {
					t.Fatal(err)
				}
			}

			status, err := newShard.repairState.buildStatus()
			if err != nil {
				t.Fatalf("failed to build status: %v", err)
			}
			tt.statusVerify(t, newShard, status)

			if tt.afterFirstBuild != nil {
				docs, err := tt.afterFirstBuild(newShard)
				if err != nil {
					t.Fatal(err)
				}
				if err := newShard.updateDocuments(docs); err != nil {
					t.Fatal(err)
				}
			}

			if tt.nextStatusVerify != nil {
				statusAfter, err := newShard.repairState.buildStatus()
				if err != nil {
					t.Fatalf("failed to build status after update: %v", err)
				}
				tt.nextStatusVerify(t, newShard, status, statusAfter)
			} else {
				statusAfter, err := newShard.repairState.buildStatus()
				if err != nil {
					t.Fatalf("failed to build status after update: %v", err)
				}
				tt.statusVerify(t, newShard, statusAfter)
			}
		})
	}
}

func basicStatusVerify(t *testing.T, status *repairStatus, groupWithSlots ...interface{}) {
	if status == nil {
		t.Fatal("status is nil")
	}
	trees := status.Trees
	if trees == nil {
		t.Fatal("trees is nil")
	}
	if len(groupWithSlots)/2 != len(trees) {
		t.Fatalf("expected %d groups in Trees, but got %d", len(groupWithSlots)/2, len(trees))
	}
	if len(groupWithSlots) == 0 {
		return
	}
	for i := 0; i < len(groupWithSlots); i += 2 {
		group := groupWithSlots[i].(string)
		slotCount := groupWithSlots[i+1].(int)

		tree := trees[group]
		if tree == nil {
			t.Fatalf("expected group %s to be present in Trees, but it is not", group)
		}
		if tree.Root == nil {
			t.Fatalf("expected group %s to have a Root, but it is nil", group)
		}
		if len(tree.Root.Children) != slotCount {
			t.Fatalf("expected group %s to have %d slots, but it has %d", group, slotCount, len(tree.Root.Children))
		}
		if slotCount == 0 {
			continue
		}

		if tree.Root.ShaValue == "" {
			t.Fatalf("expected group %s Root to have a sha value, but it is empty", group)
		}

		for _, child := range tree.Root.Children {
			if child == nil {
				t.Fatalf("expected group %s Root child to be non-nil, but it is nil", group)
			}
			if child.ShaValue == "" {
				t.Fatalf("expected group %s Root child to have a sha value, but it is empty", group)
			}
			if len(child.Children) == 0 {
				t.Fatalf("expected group %s Root child to have Children, but it has none", group)
			}
			for _, grandChild := range child.Children {
				if grandChild == nil {
					t.Fatalf("expected group %s Root child grandchild to be non-nil, but it is nil", group)
				}
				if grandChild.ShaValue == "" {
					t.Fatalf("expected group %s Root child grandchild to have a sha value, but it is empty", group)
				}
			}
		}
	}
}

func buildPropertyDocuments(s *shard, builders ...propertyBuilder) ([]index.Document, error) {
	result := make([]index.Document, 0, len(builders))
	for _, builder := range builders {
		p := buildProperties(builder)
		document, err := s.buildUpdateDocument([]byte(GetEntity(p)), p, builder.deleteTime)
		if err != nil {
			return nil, err
		}
		result = append(result, *document)
	}
	return result, nil
}

func buildProperties(builder propertyBuilder) *propertyv1.Property {
	return &propertyv1.Property{
		Metadata: &commonv1.Metadata{
			Group:       stringDefaultValue(builder.group, defaultGroupName),
			Name:        stringDefaultValue(builder.name, defaultPropertyName),
			ModRevision: builder.version,
		},
		Id: builder.id,
		Tags: []*modelv1.Tag{
			{
				Key: "test_key",
				Value: &modelv1.TagValue{
					Value: &modelv1.TagValue_Int{
						Int: &modelv1.Int{
							Value: int64DefaultValue(builder.value, int64(defaultValue)),
						},
					},
				},
			},
		},
	}
}

type propertyBuilder struct {
	group      string
	name       string
	id         string
	value      int64
	version    int64
	deleteTime int64
}

func stringDefaultValue(current, defaultVal string) string {
	if current == "" {
		return defaultVal
	}
	return current
}

func int64DefaultValue(current, defaultVal int64) int64 {
	if current == 0 {
		return defaultVal
	}
	return current
}

func generatePropertyEntity(group, name, id string) string {
	return fmt.Sprintf("%s/%s/%s",
		stringDefaultValue(group, defaultGroupName),
		stringDefaultValue(name, defaultPropertyName),
		id,
	)
}

func generateShaValue(t *testing.T, s *shard, builder propertyBuilder) string {
	property := buildProperties(builder)
	marshal, err := protojson.Marshal(property)
	if err != nil {
		t.Fatalf("failed to marshal property: %v", err)
	}
	shaValue, err := s.repairState.buildingShaValue(marshal, builder.deleteTime)
	if err != nil {
		t.Fatalf("failed to build sha value: %v", err)
	}
	return shaValue
}

func verifyContainsProperty(t *testing.T, s *shard, state *repairStatus, group string, properties ...propertyBuilder) {
	if len(properties) == 0 {
		return
	}
	for _, builder := range properties {
		entity := generatePropertyEntity(builder.group, builder.name, builder.id)
		found := false
		for _, child := range state.Trees[group].Root.Children {
			for _, grandChild := range child.Children {
				if grandChild.ID == entity {
					found = true
					if grandChild.ShaValue != generateShaValue(t, s, builder) {
						t.Fatalf("expected sha value '%s', got '%s'", child.ShaValue, generateShaValue(t, s, builder))
					}
					break
				}
			}
		}
		if !found {
			t.Fatalf("property %s not found in the Trees", entity)
		}
	}
}
